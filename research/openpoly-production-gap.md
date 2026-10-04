# OpenPoly: production gap analysis and roadmap

*Research note only. No application code was changed.*

| | |
|---|---|
| Repository | `andylaikawai/OpenPoly` (a fork of `KoNananachan/OpenPoly`), branch `main` |
| Commit analysed | [`950970d`][c-commit] |
| Date | 2026-10-04 (UTC) |
| Scope | The Python backend under `openpoly/`, the repo docs, the test suite, a read-only check against live Polymarket APIs, and published research. The React frontend was only checked where it affects runtime behaviour. |

## How to read this

Every claim carries one of these labels:

- **[code]**: verified by reading the code at the commit above. Code links are permalinks to exact lines.
- **[repo doc]**: what the repo's README, docs, or comments claim. These are not necessarily what the code does.
- **[observed]**: something I ran on 2026-10-04 (the test suite, or read-only calls to Polymarket's public APIs using the repo's own code).
- **[venue doc]**: official documentation from Polymarket, TradingNews, or Anthropic.
- **[peer-reviewed]**, **[preprint]**, **[report]**, **[news]**: external evidence, roughly in decreasing order of weight.
- **[judgement]**: my own inference or recommendation. Treat it as opinion.

What I could not do: I had no TradingNews key, LLM key, or wallet, so I did not run the news pipeline, the LLM analyzer, or the live executor end to end. I also could not verify the README's live-trading results: the underlying trade data is deliberately kept out of the repo (`material/` is gitignored, see [`.gitignore`][c-gitignore]) and no ledger export is committed. Where a finding depends on live behaviour I could not exercise, I say so.

## Summary

1. **What exists.** A tidy, broadly tested, single-process pipeline. A TradingNews WebSocket feed goes through a MiniLM similarity filter and then one Claude call, followed by an edge-threshold entry, paper or live execution on Polymarket's order book, threshold exits, settlement, and on-chain reconciliation. It is a reasonable chassis for experiments. It is not a production trading system, and today it cannot tell you whether it has an edge.

2. **The edge cannot be measured from what the system records.** The LLM's probability, confidence, and rationale, and the order book it saw, live only in 200-entry in-memory rings that are lost on restart. There is no backtest or replay. Paper fills happen instantly at a REST order-book snapshot up to 60 seconds old, which usually predates the news. That biases paper results in the strategy's favour exactly when news moves prices. The README's live window (25 positions) is too small to distinguish skill from noise: 13 wins out of 21 closed trades has a one-sided p-value of about 0.19 against a coin flip.

3. **The tradeable universe has collapsed.** By default the discovery filter keeps only markets with zero taker fees. Polymarket now charges taker fees in every category except geopolitics. Running the repo's own discovery code against live data today kept **15 markets, all in four events, three of which concern the US–Iran conflict**. The fee parser also predates Polymarket's current fee schema, and fees are never included in the edge calculation or in P&L.

4. **The signal design runs against the published evidence.** The analyzer sees a headline and market titles only: no resolution rules, no market price, no retrieval. It trades whenever its probability differs from the ask by 5¢. Peer-reviewed work finds that LLMs on their own forecast worse than markets and crowds, and improve when they are given the crowd's forecast. Without calibration against the market price, most of what this system calls "edge" is likely model error. [judgement grounded in sources in §2.2]

5. **Live trading has defects that must be fixed before any capital is at risk.**
   - An accepted good-till-cancelled (GTC) order that does not fill immediately appears never to be cancelled, so it can rest on the book and fill later without being recorded. This is suspected from the code and venue docs, not confirmed against the live API.
   - Live order calls block the asyncio event loop, including multi-second sleeps.
   - After any restart, exit thresholds silently revert to their defaults.
   - The kill switch only blocks new entries, is off by default, and looks only at realized P&L. The architecture docs say it force-closes positions.
   - The HTTP API has no authentication, and live mode persists across restarts.
   - Polymarket retires Data API v1, which the reconciliation monitor uses, on 2026-10-24.

6. **Compliance.** The documented "separated deployment" exists so that someone located in a region where Polymarket blocks order placement can still place orders from a server elsewhere. Polymarket's help centre states that using "VPNs or similar tools" to bypass geographic restrictions violates its Terms of Service. A production version must not depend on this setup.

7. **Roadmap.** Four stages; the first three are paper-only:
   - **Stage 0:** correctness fixes and a durable decision log.
   - **Stage 1:** an honest paper venue, plus an evaluation harness that scores the model against the market price net of fees.
   - **Stage 2:** signal work, gated by that harness.
   - **Stage 3:** a live safety envelope, then a pre-registered micro-capital trial.

   No real capital should go in until Stage 2 shows net-of-cost edge with a confidence interval that excludes zero.

---

## Part 1: What the codebase actually implements

### 1.1 Shape, entry points, and data flow

The system is one FastAPI process, started with `uv run uvicorn openpoly.api.main:app`. Everything is wired in the app lifespan ([`openpoly/api/main.py`][c-lifespan]) [code], in this order:

1. `runtime_state.load()` reads `~/.openpoly/runtime.json`, which holds the exec mode (paper or live) and the wallet reference.
2. The pipeline orchestrator is built, with section parameters taken from the persisted canvas, and hooked to the news manager.
3. The SQLite engine and two write-behind writers start (one for order books, one for news).
4. A `PortfolioStore` is injected into the paper executor. If a wallet is configured, a live executor is built too.
5. The exit monitor (every 120 s), settlement monitor (every 300 s), and, only when a wallet is configured, the reconciliation monitor (every 300 s, acting only in live mode) start.
6. The embedding warm loop (every 300 s) starts.
7. The market and news sources autostart unless `OPENPOLY_AUTOSTART_SOURCES=0`.

```text
TradingNews WS ─► NewsWSClient ─► NewsSourceManager._on_item ─┬─► news_item table (write-behind)
                                                               └─► orchestrator queue (max 100, one worker)
                                                                     │
      EmbeddingFilterV0 ......... runs inline on the event loop  ◄───┘
      LLMAnalyzerV0 ............. worker thread (blocking Anthropic call)
      EdgeThresholdEntryV0 ...... worker thread (optional price-history fetch)
      ExecutorDispatcher.execute_buy ... inline on the event loop
          ├─ PaperExecutor ─► PortfolioStore (fill + position rows)
          └─ LiveExecutor ──► py-clob-client-v2 ─► Polymarket CLOB ─► PortfolioStore

Gamma /events, every 900 s ─► normalize ─► discovery filter ─► MarketStore (in memory)
CLOB /book, every 60 s, top 3 levels, YES and NO tokens ─► MarketStore + order_book_snapshot table

ExitMonitor (120 s) ─► ThresholdExitV0 ─► execute_sell
SettlementMonitor (300 s) ─► Gamma /markets?closed=true ─► close position in DB at 0 or 1
ReconciliationMonitor (300 s, live only) ─► Data API /positions ─► close or log an alert
```

The HTTP API exposes routes under `/api/news`, `/api/market`, `/api/inspect`, `/api/secrets`, `/api/{embedding,analyzer,entry,exit,settlement}/log`, `/api/positions`, `/api/fills`, `/api/portfolio/equity`, `/api/wallet`, `/api/system/mode`, and `/api/canvas/template`. **None of these routes is authenticated** [code]. The only protection is that uvicorn binds `127.0.0.1` by default.

There are two scripts. `scripts/deploy.sh` rsyncs the repo to a VPS and restarts a systemd unit. `scripts/run_pipeline.py` is a manual smoke runner, but it is broken: it calls `executor.configure(...)` ([lines 114–116][c-run-pipeline]), and the exported executor is an `ExecutorDispatcher` that only has `configure_paper` and `configure_live`. Its docstring also still describes the LLM as "a stub" [observed].

### 1.2 News

**Implemented [code]:**

- **One provider.** The TradingNews WebSocket firehose, authenticated with the key as a query parameter. The parser keeps `id`, `content`, `urgency`, `sentiment`, and `published_at`, and stamps its own `received_at` ([`ws_client.py`][c-ws-parse]).
- **Reconnects** use exponential backoff from 1 s to 30 s. An HTTP rejection during the WebSocket upgrade (`InvalidStatus`) is treated as an auth failure and stops retries ([lines 142–178][c-ws-client]).
- **Freshness filter.** An item is silently dropped if `received_at − published_at` is more than 1,800 s. Items "published in the future" pass. A missing `published_at` falls back to the receive time, so such items always look fresh.
- **Fan-out.** Every accepted item goes both to the pipeline and to the `news_item` table. The table write is write-behind, and overflow drops the newest row.

**Gaps and limits:**

- **No deduplication or story clustering.** Every item, including repeats and wire rewrites, triggers a new embedding pass and possibly a new LLM call [code]. The design doc lists "cluster/dedup" as a fixed service ([`02-strategy-sections.md`][c-sections-doc]) [repo doc]; it does not exist.
- **No source attribution.** TradingNews' stream schema carries only `id`, `content`, `urgency`, `sentiment`, and timestamps (plus `tickers` on the top plan). It has no outlet and no URL ([TradingNews docs][s-tn-ws]) [venue doc]. The system therefore cannot weight sources by quality.
- **Protocol mismatch on errors.** TradingNews signals a bad key, a wrong plan, or a *second concurrent connection* by accepting the connection and then closing it with code 1008 [venue doc]. The client only treats HTTP-upgrade rejections as fatal, so a 1008 close is retried forever as if it were transient [code; inferred, not exercised]. Because only one connection is allowed per key, a local dev backend and a VPS backend sharing a key cannot both stream: the second is rejected and keeps retrying.
- **Urgency filter blind spot.** The filter only ranks `low`, `medium`, and `high` ([`tradingnews_ws.py` L30–31][c-urgency]). TradingNews' documented example value is `"breaking"`. Any filter setting other than the default `"all"` would drop those items [code + venue doc].
- **Three different default key references:** `env:OPENPOLY_TRADINGNEWS_KEY` in the section config, `OPENPOLY_TRADINGNEWS_API_KEY` in `.env.example`, and `local:tradingnews-key` on autostart [code].

### 1.3 Market data and persistence

**Implemented [code]:**

- **Discovery.** Gamma `GET /events?closed=false&order=volume24hr&limit=100` runs every 900 s ([`markets/manager.py` L59–81][c-market-config]). Each market is normalised into a `Market` object, with the liveness flags failing closed if missing ([`markets/models.py` L19–49][c-model-market]).
- **Discovery filter** ([`filters.py`][c-filter]). A market is kept only if it passes all of these:
  - it is open, accepting orders, and has an order book;
  - its event does not carry the `sports` tag;
  - its taker fee is zero (unknown fees are rejected);
  - it has at least 24 h to expiry;
  - 24 h volume is at least $1,000 and liquidity is at least $500;
  - the YES price is inside [0.03, 0.97];
  - the spread is at most 0.15.
- **Holding sync.** Markets with open positions that drop out of discovery are fetched individually, so they stay in the catalog.
- **Order books.** CLOB `/book` is fetched for both the YES and NO token of every catalog market every 60 s, keeping the **top 3 levels** ([`fetch_book`][c-book-depth]). Books are held in memory and appended to `order_book_snapshot`.
- **Price history.** CLOB `/prices-history` (10-minute fidelity) backs the optional late-buy veto.
- **Storage.** SQLite in WAL mode with five tables: `order_book_snapshot`, `news_item`, `market_embedding`, `fill`, and `position` ([`db/tables.py`][c-tables]). There is no migration framework; one column migration is done ad hoc.

**Not implemented or limited:**

- **No streaming market data.** Everything is REST polling. Polymarket offers a public market WebSocket with `book`, `price_change`, `last_trade_price`, `best_bid_ask`, `new_market`, and `market_resolved` events ([Real-Time Data][s-pm-realtime]) [venue doc].
- **The `Market` model keeps only the title (`question`) and end date.** It drops the description and rules, resolution source, `umaResolutionStatus`, outcome labels, tick size, and minimum order size [code]. In my snapshot, every Gamma market row carried a `description` (the rules text) and most carried a `resolutionSource` [observed].
- **No trade tape** and no stream of the account's own orders and fills.
- **No persisted decisions.** The only durable link from a trade to its signal is `fill.news_id`. The LLM's `p_model`, confidence, and rationale, the entry signals, and the exit decisions all live in 200-entry in-memory rings ([`section_log.py`][c-section-log]). [`01-isolation.md`][c-iso-doc] says "every decision [is] persisted with its hard-data snapshot" and that this enables "bit-level replay" [repo doc]. Neither exists in code.
- **The write-behind writer can lose or delay in-flight rows at shutdown** ([`writer.py`][c-writer]). In my runs, the persistence test failed once in the full suite and in 2 of 8 isolated reruns, on different tests [observed]. The production impact is small (only book and news rows), but the test suite is not deterministic.

**Observed today.** I ran the repo's own `discover_events` and `filter_markets` read-only against live Gamma (details in Appendix A) [observed]:

- The 100 top events expanded to 12,684 market rows. Individual NFL game events carry about 330 sub-markets each.
- Rejections: 7,752 not live, 4,109 sports-tagged, 779 with a non-zero fee, 21 below the volume floor, and 8 at extreme prices. **15 markets were kept.**
- The 15 kept markets belong to four events:
  - a US–Iran ceasefire date ladder (7 markets);
  - "US announces end of Iranian blockade by …" (4);
  - "Prime Minister of Israel after the next election?" (3);
  - "Will the U.S. invade Iran before 2027?" (1).

### 1.4 Signal: embedding filter and LLM analyzer

**Embedding filter [code]:**

- **Model and scoring.** The model is `all-MiniLM-L6-v2` (sentence-transformers, running locally on CPU). Each market's `question` text, truncated to 200 characters, is embedded every 300 s and cached in SQLite ([`embedding/manager.py` L196–210][c-embed-question]). For each news item, the filter computes cosine similarity against every warm market and keeps the top 10 above 0.35.
- **Only the title is embedded**, not the rules, description, or event title.
- **New markets are invisible for a while.** A new market can't be matched until the next warm cycle, which runs up to 300 s after discovery, and discovery itself runs every 900 s.
- **Dead knobs.** The section's `embedding_model`, `max_question_chars`, and `warm_interval_seconds` settings ([`minilm_v0.py` L29–59][c-embed-config]) are never read. The manager always starts with hardcoded defaults ([`main.py` L167][c-embed-start]), so these canvas settings have no effect.

**LLM analyzer [code]** ([`llm_v0.py` L96–177][c-analyzer]):

- **Call shape.** Defaults are `claude-haiku-4-5`, temperature 0.2, a 1,024-token cap, and a forced call to a `submit_analysis` tool that returns `selected_index`, `p_yes`, `confidence`, and `rationale` ([`llm/client.py` L110–127][c-llm]).
- **Inputs:**
  - the current UTC time;
  - the news publication time and urgency;
  - the news text;
  - a numbered candidate list of the form `question — resolves <end_date>`.

  It **deliberately withholds the market price** to avoid anchoring. It gets no resolution rules, no other news, and no retrieval.
- **Output gating.** Index 0 or an out-of-range index counts as an abstain. Results below `min_confidence` (default `medium`) are dropped, and malformed probabilities are rejected.
- **Hidden reasoning.** The prompt tells the model to "work the self-check" before answering. With a forced `tool_choice`, Anthropic documents that the model "will not emit a natural language response or explanation before `tool_use` content blocks, even if explicitly asked to" ([Anthropic tool-use docs][s-anthropic-tools]) [venue doc]. Anthropic also says its testing shows this should not reduce performance, so accuracy may be unaffected. The practical consequence is that the only reasoning trace is the one-to-two-sentence `rationale`, and that is not persisted.
- **One market per news item**, even when the news moves several, such as every rung of a date ladder.
- **No reproducibility.** There is no calibration step, no ensemble, and no model or prompt version recorded with decisions. Because the prompt includes wall-clock time, outputs cannot be reproduced, which contradicts the "determinism contract" in the design doc [repo doc].

### 1.5 Trading

**Entry** ([`edge_threshold_v0.py` L266–305][c-entry-edge]) [code]:

- **Rule.** The side is YES if `p_model ≥ 0.5`, otherwise NO ([L192][c-entry-side]). Edge is the model's probability for the chosen side minus that side's best ask. A trade needs edge ≥ 0.05 and spread ≤ 0.05. Quantity is $10 divided by the ask.
- **The side is chosen by which side of 50% the model is on, not by which side has positive edge.** Example: `p_model = 0.55` and the YES ask is 0.70. The code picks YES, computes an edge of −0.15, and skips, even though buying NO at about 0.31 would have about +0.14 of edge by the model's own numbers. It is unclear from the code whether this "directional" behaviour is intended. Either way, it throws away one whole class of disagreements with the market.
- **No fee term** in the edge calculation. `slippage_tolerance` exists but is marked "Reserved — dormant" ([L67–72][c-entry-slip]).
- **No staleness check.** It reads the in-memory book, which can be up to 60 s old.
- **Optional gates, all off by default:**
  - a late-buy veto: skip if the price already moved 10¢ in 60 minutes (from 10-minute-resolution history);
  - a same-market cooldown or lifetime lockout;
  - a heat cap on the total cost basis of open positions;
  - three "kill switch" brakes: consecutive losses, 24 h realized loss, and realized drawdown ([L130–167][c-entry-kill-cfg], [L381–448][c-entry-kill]).

**Paper execution** ([`execution/executor.py` L50–141][c-paper]) [code]:

- A buy fills instantly at the cached level-1 ask, capped at the size available at that level.
- A sell closes the full quantity at the cached level-1 bid, *without* capping by the size available at that bid.
- Fee is always 0.
- No latency, queue position, partial fills, or walking deeper into the book are modelled.
- There is no cash balance, so paper capital is effectively unlimited. The only limit is one open position per (market, side).

**Live execution** ([`execution/live_executor.py`][c-live]) [code]:

- **Stack.** `py-clob-client-v2==1.0.1rc1` (a release candidate, pinned in [`pyproject.toml` L18][c-pyproject]). It uses a Polymarket V2 Deposit Wallet with signature type POLY_1271, signed with the **owner EOA's private key**. A "Cloudflare-bypass patch" injects browser `User-Agent`, `Origin`, and `Referer` headers into every SDK request ([`clob_patch.py` L19–37][c-clob-patch]).
- **Buy path:**
  - quantize to whole shares and enforce a $1.10 minimum notional;
  - refresh the collateral allowance and read the CTF token balance;
  - post a **GTC limit at the entry section's cached ask**, not a fresh price ([L230–244][c-live-gtc]);
  - on a partial fill, cancel the remainder and read the final matched size;
  - if the request throws, infer whether it filled from the change in token balance ("lost response" handling).
- **Sell path:** wait up to about 5 s for the token balance to sync, then post a GTC limit at the cached bid, with the same partial-fill and lost-response handling. The database close is retried up to five times.
- The module docstring says the executor submits "IOC (FAK)" orders; the code uses GTC, with a comment explaining why.

**Suspected bug: unmatched GTC orders are never cancelled.** If the CLOB accepts a GTC order and nothing matches immediately (a `taking` or `making` amount of 0), the code returns `live_no_match` *without cancelling the order* (buy: [L284–297][c-live-nomatch-buy], sell: [L406–419][c-live-nomatch-sell]). Polymarket documents that a non-marketable order "rests on the book … until another order matches against it, you cancel it, [or] it expires (GTD orders only)", and that status `live` means "resting on the book" ([Order Lifecycle][s-pm-lifecycle], [Place Orders][s-pm-place]) [venue doc]. The test for this path was written for FAK semantics and never checks that a cancel happened ([`test_live_executor.py` L241–256][c-test-zero-match]). If the bug is real:

- a buy can rest at a stale price and fill later with no position recorded (the reconciliation monitor would only log it);
- each 120 s exit tick could add another resting sell;
- open orders reserve balance, per Polymarket's order-size rule.

I could not confirm this against the live API.

**Blocking I/O on the event loop in live mode.** The orchestrator calls `execute_buy` inline from async code ([L319–333][c-orch-exec]). The exit monitor calls `execute_sell` inline from its async loop ([`exit_monitor.py` L199–208][c-exit-loop], [L266][c-exit-sell]). The manual close routes do the same. In live mode each of these makes several synchronous HTTP round-trips and can `time.sleep` for about 4–5 s while polling token balances, plus about 2 s of database retries ([L180–194][c-live-sleep-1], [L350–365][c-live-sleep-2], [L448–481][c-live-sleep-3]). While that happens, the news socket, the market polling loops, and the API are all stalled. The repo's own operations doc warns against exactly this pattern ([`05-runtime-network-risk.md` L36–43][c-risk-doc-ops]) [repo doc].

**Wiring gaps in live mode:**

- The live executor is only installed at process start. `POST /api/system/mode` builds one for its preflight checks and then discards it ([`wallet_routes.py` L226][c-set-mode-build]). If you configure a wallet after boot and switch to live, every order is skipped as `live_not_ready` until the process restarts [code].
- The preflight checks that there are no open positions, the wallet secret resolves, the pUSD balance is at least $1, and allowances of at least 10,000 pUSD exist for both V2 exchanges. It does **not** check the geoblock status and does not cap how much capital can be deployed.

**Exits** ([`threshold_v0.py` L103–162][c-exit-section]) [code]:

- **Rules.** Every 120 s ([L52][c-exit-interval]), each open position is marked at the cached level-1 bid. The rules are checked in order:
  1. stop-loss at −15%;
  2. peak-retrace of 12%, once the peak gain is at least max($1, 1% of cost);
  3. take-profit at +20%.
- **Spread-triggered stops.** Return is measured from the entry ask to the current bid, so the spread counts as a loss immediately. With the entry gate's 5¢ maximum spread and a 15% stop, any entry with an ask of 33¢ or less can be stopped out on the first tick with no price change at all (0.05 / 0.333 = 15%). With a 3¢ spread, the threshold is an ask of 20¢ or less [arithmetic on code defaults].
- **Exits are blind to the thesis.** They ignore the model's probability, the time to resolution, and fees. There is no hold-to-resolution mode other than setting the thresholds very wide.
- **Config drift after restart.** The exit-monitor singleton is constructed at import time with `ThresholdExitConfig()` defaults ([L331–336][c-exit-singleton]) and is never rebuilt from the persisted canvas at startup. The canvas hot-reload only rebuilds it when a save *changes* the exit config ([`canvas_routes.py`][c-canvas-reload]). So after any restart (including a systemd auto-restart), exits run on the defaults while the canvas keeps showing the operator's values. The same happens to the market and news sources:
  - autostart launches them with default configs ([`main.py` L95–106][c-autostart]);
  - both managers ignore a Start request while already running ([market][c-market-start-guard], [news][c-news-start-guard]);
  - so the canvas config is not applied until the operator does Pause → Run.

**Settlement** ([`settlement_monitor.py` L185–265][c-settle]) [code]:

- Every 300 s the monitor fetches each held condition with `closed=true`. If `outcomePrices` is a clean 0/1 pair, it closes the position in the database at 0 or 1.
- **No on-chain redemption** happens; the code calls it "out of slice E scope". So in live mode, winning tokens stay unredeemed and the capital stays locked.
- It does not check `umaResolutionStatus`, even though the repo's own API doc lists that field as part of resolution detection ([`06-polymarket-api.md`][c-api-doc]).
- A 50/50 resolution is skipped as `ambiguous_outcome` on every tick, so that position stays open forever.

**Reconciliation** ([`reconciliation_monitor.py` L174–205][c-recon]) [code]:

- **Live mode only**, using Data API v1 `GET /positions`.
- Positions that are open in the database but not held on-chain (after a 5-minute grace period) are closed **at the entry price, recording a realized P&L of exactly 0** ([L184][c-recon-zero]). That can hide real losses from P&L and from the kill-switch counters.
- On-chain holdings that the database doesn't know about produce a log line and an in-memory entry. There is no alert channel.

### 1.6 Paper mode specifically

Paper is the default: the mode is paper whenever `runtime.json` is absent or says `"exec_mode": "paper"` ([`runtime_state.py`][c-runtime-state]) [code]. Paper mode runs the full pipeline on real Polymarket data and writes fills to the same ledger as live. The table below lists what it does not model and which way each omission biases results [code, with direction of bias as judgement]:

| Not modelled | Direction of bias |
|---|---|
| Order-book age (up to 60 s) plus pipeline latency | In most cases paper fills at a price observed before the news reached the bot. If the pipeline takes L seconds, the snapshot it uses was taken before the news arrived about (1 − L/60) of the time. That is optimistic exactly when news moves the market. |
| Adverse selection of stale limit orders | A live limit at a stale price misses when the market runs away and fills when it doesn't. Paper always fills, so it overstates both fill rate and fill quality. |
| Taker fees | Zero by construction. The current universe happens to be fee-free, but nothing would charge fees if the filter were relaxed. |
| Sell-side depth | The full quantity sells at the best bid regardless of the size available there. |
| Capital | No bankroll and no cash constraint. |
| Resting orders, partial fills, failed trades | None. Polymarket trades can end in `FAILED` after matching ([Order Lifecycle][s-pm-lifecycle]); nothing tracks trade status. |

The CHANGELOG says paper and live share one code path "so paper results stay an honest rehearsal of live behavior" [repo doc]. They share the *dispatcher*, but they use different order semantics: paper fills instantly, while live posts a GTC limit at a stale price. Paper results from this code should be read as an upper bound, not a rehearsal [judgement].

### 1.7 Risk controls actually in the code

| Control | Where | Default | What the code does | What repo docs say |
|---|---|---|---|---|
| Paper by default | `runtime_state` | paper | The mode persists in `~/.openpoly/runtime.json`. Once set to live, a restarted process boots straight into live with sources autostarting. | "Live trading requires explicit opt-in." |
| Live preflight | `wallet_routes` | — | No open positions; pUSD ≥ $1; V2 allowances present. | Fail loudly at mode switch. |
| Order size | entry | $10 flat | Fixed notional per trade. | Flat sizing is intentional for now. |
| One position per (market, side) | executor + DB index | on | Enforced. | — |
| Heat cap | entry | off | Blocks new entries once the open cost basis reaches the cap. | — |
| "Kill switch" (3 brakes) | entry | off | Blocks **new entries only**, using **realized** P&L from the newest 500 positions. Nothing ever closes a position with reason `kill_switch`. | "Force-closes all open positions" ([`01-isolation.md`][c-iso-doc], [`05-runtime-network-risk.md`][c-risk-doc-exit]) |
| Cooldown / lockout / late-buy veto | entry | off | Optional entry filters. | — |
| Stop-loss / take-profit / retrace | exit | on | 120 s cadence on a cached bid. Thresholds revert to defaults after any restart. | — |
| Per-event or correlation limit | — | — | **None.** All 15 markets in today's universe sit in four related events. | — |
| Cancel-all, order heartbeat | — | — | **None.** | Polymarket recommends both (see §2.3). |
| API authentication | — | — | **None.** Relies on the loopback bind. | "Backend HTTP must bind loopback." |

### 1.8 Implemented versus stubbed versus intent-only

**Implemented and covered by tests:**

- the news WebSocket client and ring buffer;
- Gamma discovery, normalisation, and the filter;
- order-book sampling and persistence;
- the embedding cache and matching;
- the LLM analyzer, with a fake client in tests;
- the edge-threshold entry and its gates;
- the paper and live executors (live against a fake CLOB);
- the fill/position ledger and equity curve;
- the exit, settlement, and reconciliation monitors;
- the canvas store with ETag locking and hot-reload of section parameters;
- the secrets store and `*_ref` resolution for `env:` and `local:`;
- wallet configuration and the mode switch.

**Explicit stubs or deferrals in code:**

- `slippage_tolerance` is dormant.
- The `vault:` and `keychain:` secret schemes raise `NotImplementedError` ([`news/secrets.py`][c-secrets]), although the README lists keychain among supported schemes.
- The `kill_switch` close reason is defined and never used.
- Section-log "detail drawers" are deferred, and so are Alembic migrations.
- On-chain redemption is "out of scope".

**Intent only (described in docs or comments, absent from code):**

- From the v8 baseline in [`02-strategy-sections.md`][c-sections-doc] (the doc itself says the implemented scope is narrower):
  - capability injection through a `SectionContext` (sections instead import process-wide singletons);
  - per-section `TIMEOUT_MS`, retry policy, and the determinism contract;
  - a position sizer (fixed, edge-scaled, or Kelly);
  - a 13-gate entry check;
  - an execution simulator that walks the book;
  - a stand-alone risk-guard service;
  - news freshness decay with cluster/dedup;
  - the CLOB `/fee-rate` endpoint as the fee authority.
- From [`01-isolation.md`][c-iso-doc]: the decision audit log, bit-level replay, and a kill switch that force-closes positions.
- **Pluggable user strategies.** The README says implementations dropped into `openpoly/user_sections/` are "discovered at startup", and they are: they appear in the catalog. But the orchestrator and the canvas reload always construct the same four hardcoded v0 classes and take only *parameters* from the canvas ([`orchestrator.py` L384–427][c-orch-hardcoded], [`canvas_routes.py` L225–266][c-canvas-build]). A user section therefore cannot be run without editing framework code.

### 1.9 Code health

- **Tests:** 662 collected; 661 passed and 1 failed in the full run (`tests/test_db_manager.py::test_start_then_enqueue_persists`). Isolated reruns of that file failed 2 of 8 times, on a different test each time, which is consistent with a shutdown race in the write-behind writer [observed].
- **Lint:** `ruff check .` passes [observed].
- **CI:** The upstream CI config gates on pytest, ruff, typecheck, and eslint. This fork has no CI runs recorded [observed].
- **Doc drift.** `.env.example` points at `/api/news_source/start`, but the real route is `/api/news/source/start`. The `ws_client` docstring describes an `X-API-Key` header, but the code passes the key as a query parameter. The live executor docstring says FAK; the code uses GTC.
- **References to unpublished material.** Many comments cite "v8 §…", "slice C design doc", "handoff doc", "plan §…", or a "memory". `docs/plan/`, `docs/reports/`, and `material/` are all gitignored, so a reader of this repo cannot follow those references.

---

## Part 2: What a production news-driven prediction-market system needs

### 2.1 Market structure: how Polymarket clears, charges, and resolves

**Clearing [venue doc].**

- *Order flow.* Polymarket orders are EIP-712-signed limit orders. They are matched off-chain by an operator and settled on Polygon by the exchange contract. "Market orders" are simply limit orders priced to execute immediately. Order types are GTC, GTD, FOK, and FAK, plus post-only ([Order Lifecycle][s-pm-lifecycle], [Place Orders][s-pm-place]).
- *Delays.* Some markets apply taker delays: 250 ms on selected crypto and finance markets, and longer windows on configured sports markets.
- *Trade status.* After matching, a trade moves through `MATCHED → MINED → CONFIRMED`, and it can end as `FAILED`.
- *Engine restarts.* The matching engine restarts periodically. During a restart, order requests get HTTP 425. For two minutes afterwards only post-only orders are accepted ([Matching Engine Restarts][s-pm-matching]).
- *Safety tools.* An order heartbeat cancels all of an account's open orders if no heartbeat arrives within 10 s, and there is a `DELETE /cancel-all` endpoint ([Manage Orders][s-pm-manage]).

**Fees [venue doc]** ([Fees][s-pm-fees]). Only takers pay fees, computed as `fee = C × feeRate × p × (1 − p)`, where C is shares and p is price. The rate depends on category:

| Category | Taker fee rate |
|---|---|
| Crypto | 0.07 |
| Sports, Economics, Culture, Weather, Other | 0.05 |
| Finance, Politics, Mentions, Tech | 0.04 |
| Geopolitics | 0 |

Makers pay nothing and receive 15–25% of fees back as rebates. "Geopolitical and world events markets are fee-free."

Live Gamma data matches this table [observed]:

- Fee-enabled markets expose a `feeSchedule` with fields `rate`, `exponent`, `takerOnly`, and `rebateRate`. Fee-free markets have none.
- The `takerBaseFee` field was 1000 on *every* fee-enabled market, including markets of type `zero_fees` whose schedule rate is 0.
- CLOB `/fee-rate` returned `base_fee: 1000` for both a politics market (rate 0.04) and a crypto market (rate 0.07), and 0 for a geopolitics market.

So `takerBaseFee` can tell you whether fees are on, but it is not the rate. The repo divides `takerBaseFee` by 10,000 and so treats every fee-bearing market as a 10% fee ([`markets/models.py` L99–109][c-fee-parse]) [code].

*Worked example [arithmetic].* For a politics market at 50¢, the taker fee is 0.04 × 0.25 = **1¢ per share**, or 2% of notional. Entering and exiting as a taker costs about 2¢, which is 40% of the bot's 5¢ minimum edge. Holding to resolution costs only the entry fee. A crypto market at 50¢ costs 1.75¢ per share each way.

**Resolution [venue doc]** ([Resolution][s-pm-resolution]).

- *Mechanism.* Markets resolve through UMA's Optimistic Oracle. A proposer posts a bond (typically $750) and a 2-hour challenge window follows. A dispute escalates to a UMA token-holder vote; disputed resolutions take 4–6 days in total.
- *Odd outcomes.* An "Unknown/50-50" outcome pays $0.50 per token, and Polymarket can issue clarifications after trading has started.
- *Redemption.* Winning tokens must be actively redeemed through the CTF collateral adapter to get pUSD back.
- *Rules versus title.* The docs say explicitly: *"The market title describes the question, but the rules define how it resolves."*
- *Documented failure case.* In March 2025, a roughly $7M market on a Ukraine–US minerals deal resolved YES without a deal, amid allegations that a large UMA holder swung the vote. Polymarket said this was not a "market failure" and issued no refunds ([CoinDesk][s-uma-ukraine]) [news].

Resolution risk is therefore a real, fat-tailed part of P&L, and it is driven by the rules text that this system never reads.

**Multi-outcome events [venue doc].** Events like "Prime Minister of Israel after the next election?" are "negative risk" sets of mutually exclusive binaries. A NO in one market can be converted into a YES in every other market of the event. Polymarket advises trading only named outcomes, and avoiding placeholder outcomes and "Other", whose meaning changes over time ([Negative Risk][s-pm-negrisk]). The repo passes the `neg_risk` flag on orders but has no outcome-type filter [code]. Prices across such sets must stay consistent. Saguillo et al. estimate about $40M of arbitrage profit was realised on Polymarket between April 2024 and April 2025 from inconsistencies within and across markets ([AFT 2025 / arXiv][s-saguillo]) [peer-reviewed]. That shows structural mispricings exist, and that others are already harvesting them.

**Access and compliance [venue doc].**

- *Geoblocking.* Polymarket publishes a geoblock check (`GET https://polymarket.com/api/geoblock`). The United States, United Kingdom, France, Germany, Australia, Singapore, and others are "close-only on frontend and API": existing positions can be closed but new ones cannot be opened ([Geographic Restrictions][s-pm-geoblock]).
- *Circumvention.* The help centre states that Polymarket "strictly prohibits the use of VPNs or similar tools to bypass geographic restrictions", which violates Terms of Service §2.1.4 ([help centre][s-pm-help-geo]).
- *US route.* A regulated US venue exists: QCX LLC d/b/a Polymarket US was designated a CFTC contract market on 2025-07-09 ([CFTC][s-pmus-dcm]). Its API access requires an application, a compliance review, and sandbox testing ([Polymarket US developers][s-pmus-dev]).
- *Infrastructure.* Polymarket lists its primary servers as `eu-west-2`, and co-location there is offered after KYC/KYB ([Geographic Restrictions][s-pm-geoblock]). That is a signal that latency-sensitive participants trade there.

**API lifecycle [venue doc].**

- *Data API.* "Data API v1 is retired on October 24, 2026." The repo calls v1 `GET /positions` (reconciliation) and `GET /value` (wallet balance) ([`polymarket_api.py` L188–254][c-data-api]) ([migration guide][s-pm-dataapi-v2]).
- *SDK.* Polymarket's migration guide tells Python users to remove `py-clob-client-v2` and move to the unified SDK ([SDK migration][s-pm-sdk-migrate]).
- *Signing keys.* Polymarket offers Session Keys (in beta): a separate signer scoped to trading that "cannot withdraw funds from the Deposit Wallet" ([Session Keys][s-pm-sessionkeys]).
- *Rate limits* (for example `/book` at 1,500 requests per 10 s) are far above what this bot uses ([Rate Limits][s-pm-ratelimits]).

### 2.2 The information edge: latency, source quality, and what is already priced in

**Markets are a strong baseline.** Prediction-market prices "are typically fairly accurate, and … outperform most moderately sophisticated benchmarks" ([Wolfers & Zitzewitz 2004][s-wz2004]) [peer-reviewed]. For Polymarket specifically:

- A study of 124.5 million trades finds prices broadly well calibrated, with mispricing concentrated early in a market's life and close to resolution ([Reichenbach & Walther 2025][s-rw2025]) [preprint, SSRN].
- A 2026 study of 353 million trades on Kalshi and Polymarket finds calibration depends on domain. The most robust pattern is that political markets are underconfident, with prices compressed toward 50% ([arXiv 2602.19520][s-calib2026]) [preprint].

Known structural deviations like these are candidate edges in their own right, independent of news.

**Speed depends on the kind of news.**

- *Liquid equities.* Busse and Green found that public TV analyst reports were priced within seconds, with positive reports "fully incorporated within one minute". Only traders who executed within 15 seconds made (small) profits ([JFE 2002][s-bg2002]) [peer-reviewed].
- *Mechanical Polymarket markets.* In Polymarket's BTC markets, quotes move one tick a median of 347 ms after large Binance moves. The same author tried to trade that relationship and found *no tradable edge* out of sample after fees and slippage ([Young 2026, arXiv 2607.26245][s-openmarket]) [preprint].
- *Slower markets.* In a 2024 CPI leak case study, Polymarket's Fed-cut contracts showed essentially no response for 35 minutes while CME futures moved within seconds ([Aktuğ & Torul 2026][s-cpi]) [preprint]. A non-peer-reviewed industry note measured a median of about 80 minutes from headline to the two-hour price peak across about 90,000 Polymarket reactions in 2026 ([Vera/Crypto Briefing][s-vera]) [report, weak]. That measures time-to-peak, not time to the first move.

[judgement] There is no latency edge available to a system that samples books every 60 seconds and spends seconds in an LLM call. Anything mechanical is priced in hundreds of milliseconds. The only plausible edge is interpretive: correctly reading slower, ambiguous, rules-sensitive news in thinner markets. That is exactly where a model that never sees the rules and gets only a headline is weakest.

**Who is on the other side.** Informed trading has been documented in the same kind of geopolitical and world-event markets this bot is currently confined to.

- The US Department of Justice charged a soldier with using classified information to bet on Maduro- and Venezuela-related Polymarket contracts. About $33,000 of bets allegedly made about $409,000 ([DOJ][s-doj]) [DOJ press release; allegations].
- Nobel officials investigated suspicious Polymarket bets placed hours before the 2025 Peace Prize announcement ([The Block][s-nobel]) [news].

[judgement] By the time a public wire headline reaches this bot, the people who knew first have often traded already.

**Source quality.** Volume is a noisy signal on Polymarket: a Columbia study estimates about 25% of historical volume was likely wash trading. That includes 45% of sports volume and 12% of politics volume, peaking near 60% of weekly volume in December 2024 ([Sirolly, Ma, Kanoria & Sethi 2025][s-wash], [Columbia Business School][s-wash-cbs]) [preprint]. The discovery filter ranks markets by 24 h volume and keeps them above a volume floor, so it inherits that noise [code + judgement].

On the news side, TradingNews delivers a single `content` string with no outlet [venue doc]. The analyzer is asked "is this genuinely new information, or a wire rewrite?" without being shown any prior items [code]. That question cannot be answered from a single headline [judgement].

**LLMs as forecasters.**

- *Halawi et al. (NeurIPS 2024).* A retrieval-augmented GPT-4 system reached a Brier score of 0.179, against 0.149 for the human crowd. Lower is better, and always answering 0.5 scores 0.25. Removing retrieval worsened the base model from 0.186 to 0.206. Most models used zero-shot scored "around or worse than random guessing". Averaging the system with the crowd improved the crowd from 0.149 to 0.146. The system only beat the crowd in a *selective* setting, where the crowd was uncertain (0.3–0.7) and relevant articles were available ([arXiv 2402.18563][s-halawi]) [peer-reviewed].
- *ForecastBench (ICLR 2025).* Superforecasters significantly outperformed the best model. The top models "all had access to the crowd forecast on market questions"; the best model *without* it scored worse (0.136 against 0.122). Adding recent topical news "did not improve performance" ([ICLR 2025][s-forecastbench]) [peer-reviewed]. A 2025 update still has superforecasters ahead, 0.081 against 0.101 ([FRI update][s-forecastbench-2025]) [report].
- *Schoenegger et al. (Science Advances 2024).* GPT-4 and Claude 2 forecasts improved 17–28% when the models were shown the human median. Simply averaging human and model forecasts was better still ([Science Advances][s-schoenegger]) [peer-reviewed].

[judgement] Taken together:

1. The market price is the strongest single input available.
2. An LLM that never sees the price, has no retrieval, and is given only a headline and a title will on average be noisier than the market.
3. So `p_model − price` mostly measures model noise.
4. The literature supports combining model and market, and trading only where measured skill exists. That requires the decision log and the market-relative evaluation this repo does not have.

Two further LLM risks are unaddressed in the code [judgement]:

- **Prompt injection.** The model's output places orders, and the news text is untrusted input.
- **Knowledge cutoff.** The model may simply not know the current state of the world it is being asked about.

### 2.3 Execution and inventory

**Taker versus maker [peer-reviewed + venue doc].**

- *Takers* pay fees and cross the spread.
- *Makers* pay no fee, earn rebates, and can earn liquidity rewards ([Market Making][s-pm-mm]). In exchange they bear adverse selection, meaning informed traders pick off stale quotes ([Glosten & Milgrom 1985][s-gm1985]), and inventory risk ([Avellaneda & Stoikov 2008][s-as2008]).
- *A news strategy is naturally a taker*, because it has to trade before the price moves.

Its cost per round trip is the spread, plus two taker fees (or one if held to resolution), plus slippage beyond the top level, plus the adverse-selection cost of acting late [judgement].

**Venue-documented practices** that a production system should follow ([Market Making][s-pm-mm], [Manage Orders][s-pm-manage], [Matching Engine][s-pm-matching]) [venue doc]:

- use real-time data rather than polling;
- cancel stale quotes immediately;
- use GTD orders that expire before known catalysts;
- batch orders;
- run "a kill switch — cancel all open orders when errors or position limits require the strategy to stop";
- monitor fills through the authenticated user channel;
- after reconnecting, fetch open orders and recent trades before resuming;
- run order heartbeats;
- honour HTTP 425 and the post-only mode that follows a restart.

**For a taker news strategy** [judgement]:

- Use marketable FAK or FOK orders with a price cap derived from the model's fair value minus fees and the required edge, computed on a *fresh* book.
- Never leave unintended resting orders.
- Size to visible depth.
- Treat `delayed` and `unmatched` statuses explicitly.
- Track trades through to `CONFIRMED` rather than trusting the immediate response.

**Inventory and capital [venue doc].**

- Positions lock pUSD until you exit or the market resolves. Resolution takes at least the 2-hour challenge window after someone proposes an outcome, and 4–6 days if disputed. Winning tokens must then be redeemed.
- Complete sets can be merged back into pUSD, and neg-risk NO positions can be converted ([Market Making][s-pm-mm], [Negative Risk][s-pm-negrisk]).
- With a small bankroll, capital locked in unredeemed or slow-resolving positions is a real constraint on how many independent bets you can make [judgement].

**Exit policy [judgement].**

- If the thesis is about the final outcome, holding to resolution avoids the exit spread and the exit fee.
- Exiting early only makes sense if the thesis was a short-term repricing.
- A production system has to decide which of the two it is trading, per position, and measure accordingly.
- The current exits mix both and are biased by the spread (see §1.5).

### 2.4 Operational pieces

The table below separates what is established practice from what is my recommendation.

| Area | What a production system needs | Basis |
|---|---|---|
| Data | Streaming books and trades for every traded market; the account's own orders and fills from the user channel; point-in-time archives of books, trades, news with first-seen times, market rules and clarifications, and fee schedules, so any decision can be replayed. Take trade direction from on-chain data: one study found direction inferred from the public feed matches on-chain truth only about 59% of the time ([Dubach 2026, arXiv 2604.24366][s-anatomy]). | venue doc; preprint; judgement |
| Models | Evaluation against the market price at decision time (Brier score, log loss, calibration curves); a calibration layer; versioned prompts and model ids; champion/challenger testing; inputs that include the resolution rules; retrieval of prior coverage; ensembles; cost and latency budgets. | peer-reviewed (§2.2); judgement |
| Risk | A risk engine *independent of the strategy*: limits per order, per market, per event or correlation cluster, and on gross exposure; mark-to-market daily-loss and drawdown limits that cancel orders and, optionally, flatten positions; caps on resolution risk (ambiguous rules, markets close to resolution); price guards; heartbeat plus cancel-all. | venue doc (kill switch, heartbeat, price guards); judgement |
| Sizing and capital | Fractional Kelly on *calibrated* probabilities. Errors in the estimated edge make Kelly bets dangerous: bet less, and never above Kelly (betting twice Kelly gives zero growth) ([MacLean, Thorp & Ziemba][s-mtz-kelly]). With an unproven edge, size for learning, not for growth. | peer-reviewed / book chapter; judgement |
| Monitoring | Data-staleness and event-loop-lag alarms; intents compared with fills; reconciliation of orders, positions, and cash; P&L attribution (signal, execution, fees, resolution); alerts that reach a human rather than a log file; runbooks. | judgement |
| Security and compliance | A Session Key that cannot withdraw; keys in a secrets manager or OS keychain; an authenticated control plane; a geoblock check before every order; a legal eligibility review; the Polymarket US venue for US persons. | venue doc; judgement |

---

## Part 3: The gap between this repo and a production-ready version

Status meanings:

- **Meets**: good enough to build on.
- **Partial**: exists but has material holes.
- **Missing**: not present.
- **Contradicted**: the docs claim it, but the code does not do it.
- **Defect**: present but wrong.

| # | Requirement | Status | Evidence |
|---|---|---|---|
| 1 | Trade only from an eligible jurisdiction; check geoblock before ordering | Missing | No geoblock check exists; [`docs/deploy`][c-deploy-doc] documents a "geoblock workaround" |
| 2 | Supported SDK and APIs | Partial | Pinned release candidate of a superseded SDK; Data API v1 retires 2026-10-24; spoofed browser headers |
| 3 | Least-privilege signing key | Missing | The owner EOA key lives in env or a plaintext JSON store; Session Keys are not used |
| 4 | Authenticated control plane | Missing | No auth on any route, including `/api/system/mode` |
| 5 | Streaming order books with staleness detection | Missing | REST every 60 s, top 3 levels, no age checks |
| 6 | Market rules and resolution metadata in the decision | Missing | `Market` keeps only title and end date |
| 7 | Correct per-market fee model | Defect | `takerBaseFee / 10000` is read as the rate; `feeSchedule` is ignored; fees are absent from edge and P&L |
| 8 | Point-in-time archive for replay | Partial | Books and news are archived; decisions, trades, and market metadata are not |
| 9 | News dedup, clustering, and source quality | Missing | Every item is processed; the provider has no source field |
| 10 | Market-relative evaluation and calibration | Missing | No Brier or log-loss scoring, no use of price, no calibration layer |
| 11 | Durable decision log with full inputs and outputs | Contradicted | Docs promise an audit log and replay; the code keeps 200-entry memory rings |
| 12 | Reproducibility and versioning of model calls | Missing | Wall-clock time in the prompt; outputs not stored |
| 13 | Side selection by sign of edge, with fees in the edge | Defect | Side chosen by `p ≥ 0.5`; no fee term |
| 14 | Fresh-book, fee-aware, marketable-limit orders | Partial | Limit at a stale cached price; GTC instead of FAK/FOK |
| 15 | No unintended resting orders; heartbeat; cancel-all | Defect / Missing | Suspected no-match leak (§1.5); no heartbeat; no cancel-all |
| 16 | Fills tracked from venue events through to `CONFIRMED` | Partial | Response parsing plus balance inference; no user channel or trade status |
| 17 | Partial fills and lost responses | Meets | Remainder cancel, balance-delta inference, and retried DB close, all with tests |
| 18 | Non-blocking I/O in the trading loop | Defect | Live calls and `time.sleep` run on the asyncio loop |
| 19 | Handle matching-engine restarts and post-only mode | Missing | No handling of HTTP 425 or 503 |
| 20 | Ledger of record | Partial | Fill and position tables exist and work, but fees are always 0 and reconciliation books 0 P&L |
| 21 | Limits per market, event, and gross exposure | Partial | Uniqueness per (market, side) only; heat cap off by default; no event or correlation cap |
| 22 | Mark-to-market loss limits and a kill switch that cancels and flattens | Contradicted | Entry-only, realized-only, off by default; docs say it force-closes |
| 23 | Config integrity across restarts | Defect | Exit and source configs revert to defaults |
| 24 | No automatic live boot after a restart | Missing | Mode persists; sources autostart |
| 25 | Resolution handling, redemption, and disputes | Partial | Closes in DB only; no redemption; ignores UMA status; 50/50 positions stuck |
| 26 | Reconciliation of orders, positions, and cash | Partial | Positions only, on the retiring v1 API; no orders or cash |
| 27 | Paper simulator with latency, fees, depth, and adverse selection | Missing | Instant fill at a stale level-1 price |
| 28 | Historical replay or backtest | Missing | — |
| 29 | Pre-registered evaluation of edge | Missing | README sample is n = 25 |
| 30 | Alerting, metrics, structured logs | Missing | stdout logs and UI polling only |
| 31 | Deterministic tests and CI | Partial | 662 tests, but one flaky; no CI runs on the fork; broken smoke script |
| 32 | Ledger backups | Missing | One SQLite file, no backup procedure |
| 33 | Sizing tied to measured edge and bankroll | Missing | Flat $10 per trade; no bankroll in paper |
| 34 | Pluggable strategies actually runnable | Contradicted | User sections are discovered but cannot be wired in |

**What it already does well** (specific, and worth keeping):

- Market parsing fails closed, so a schema change cannot silently make a market tradeable.
- Typed Pydantic configs act as a single parameter schema for both backend and canvas.
- Secret values are never returned by the API.
- The live executor handles real failure modes carefully: lost responses, partial fills, remainder cancellation, and retried database writes after an irreversible on-chain fill.
- Reconciliation works in both directions.
- The canvas has optimistic locking.
- There is a broad, mostly hermetic test suite.

These are real strengths. They are also mostly *plumbing*. The parts that decide whether the system makes money (what signal it trusts, how it measures that signal, how it executes, how much it risks) are the parts that are missing or defective.

**Most consequential gaps, ranked by what they block:**

1. **Blocks learning anything** (rows 10, 11, 27–29): no durable decision data, no market-relative evaluation, and a paper simulator biased toward the strategy. Until these exist, no change to the strategy can be judged.
2. **Blocks a meaningful universe** (rows 7, 13): an obsolete fee model and a zero-fee-only filter leave 15 correlated markets; fees are absent from edge and P&L.
3. **Blocks safe live trading** (rows 1–4, 15, 18, 22–24): compliance, key handling, the orphan-order risk, a blocked event loop, a kill switch that is nominal only, configs that revert silently, and live mode surviving restarts with no authentication.
4. **Blocks a credible signal** (rows 6, 9, 12, plus §2.2): no rules text, no deduplication, no price awareness, no retrieval, and no calibration.

---

## Part 4: Roadmap

**Principles** [judgement]:

- Measure before changing strategy.
- Make paper *pessimistic* rather than optimistic.
- Put no capital in until edge is measured net of costs.
- Keep the number of stages small, and give each one an objective exit test.

### Stage 0: Make it correct and observable (paper-only)

**Goal:** the system does what its configuration and docs say, and every decision can be reconstructed afterwards.

**Work:**

1. **Durable decision log.** An append-only table per news item with:
   - the raw payload;
   - the candidate set with scores;
   - the full prompt and model id;
   - the raw LLM output: `p_model`, confidence, rationale;
   - the book snapshot and its age;
   - entry signals, the intent, and the executor result.

   Link fills to it by `decision_id`. Everything later depends on this item.
2. **Config integrity.**
   - Build the exit section from the canvas at startup.
   - Start sources with their canvas configs.
   - Make Start apply a new config, or refuse with a clear message.
   - Remove the dead embedding settings, or wire them up.
3. **Live-path correctness.** Fix these now even if live stays off; they can be tested with the existing fake CLOB.
   - Cancel any accepted-but-unmatched order, or switch to FAK/FOK.
   - Move executor calls off the event loop.
   - Re-check book age at execution time.
   - Install the live executor when the mode switches.
4. **Fee model.**
   - Parse `feeSchedule` (rate, exponent, takerOnly).
   - Put taker fees into the edge calculation, the fills, and P&L.
   - Choose side by the sign of the edge.
   - Make the zero-fee filter a deliberate setting rather than a crutch.
5. **Hygiene.**
   - Fix the write-behind shutdown race and the broken smoke script.
   - Migrate Data API v1 to v2 *before 2026-10-24*.
   - Decide whether to stay on the pinned release-candidate SDK or migrate to the unified one.
   - Align the docs with the code: kill switch, audit log, replay, keychain, user sections.

**Exit test:**

- Each fix lands with a regression test, for example:
  - "an accepted GTC with zero match → cancel is called";
  - "after restart, the exit config equals the canvas";
  - "a fee-bearing market's edge includes the fee".
- A full week of paper running produces a decision log from which every position can be reconstructed together with its inputs.
- The test suite passes 20 runs in a row.

**Depends on:** nothing. **Capital:** none.

### Stage 1: An honest paper venue and an evaluation harness (paper-only)

**Goal:** be able to answer "does the model beat the market price, net of costs?" with a confidence interval.

**Work:**

1. **Streaming data.** Subscribe to the market WebSocket for catalog and held markets, persist the event stream, and add staleness alarms.
2. **Pessimistic fill model:**
   - fill against the book as it stood at decision time plus measured latency;
   - walk the depth;
   - charge taker fees;
   - allow partial fills;
   - reject the order if the price moved past the limit;
   - cap sells by available depth;
   - enforce a cash bankroll.
3. **Replay.** Run the pipeline over recorded news and books, using logged LLM outputs or re-querying with logged prompts, so changes can be compared on the same history.
4. **Evaluation.**
   - For every decision, store the market mid at decision time and the eventual outcome.
   - Report Brier score and log loss of `p_model` against the market, calibration curves, and net-of-cost P&L by category and by event cluster.
   - Compare against baselines: never trade; the market price itself; a timing placebo (random entries at the same moments); and a headline-direction rule with no LLM.
5. **Pre-registration.** Write down the metrics, the minimum sample, and the stop rules *before* looking at results. [judgement] The README's n = 25 cannot separate skill from noise: 13 of 21 wins has a 95% Wilson interval of 41–79%. Even a high share of "positions that peaked in the trade's favour" is expected from noise: in a toy driftless random walk with a 2-tick spread, 59–89% of paths peak above the entry ask, depending on holding time (Appendix A). Expect to need hundreds of decisions spread across independent events.

**Exit test:**

- Replaying a recorded period reproduces paper results exactly.
- The timing placebo shows no "edge", which sanity-checks the simulator.
- The harness emits a report with confidence intervals.

**Decision point:** if, after the pre-registered sample, the system does not beat the market price net of costs in any category, stop or pivot. Stage 2 improves a *measured* signal; it should not be used to hunt for one by tuning thresholds.

**Depends on:** Stage 0 (decision log, fee model). **Capital:** none.

### Stage 2: Signal work, gated by the harness (paper-only)

**Goal:** a signal with measured, out-of-sample, net-of-cost edge. Every change in this stage is A/B-tested in the Stage 1 harness.

**Work:**

1. **Inputs:**
   - the market's rules or description, resolution source, and clarifications;
   - sibling markets in the same event;
   - deduplicated, clustered recent news;
   - source tiers.
2. **Use the market.** Either show the model the price, or combine `p_model` with the market mid (for example, shrink toward the price), and only trade when the combined estimate disagrees by more than costs. Prefer selective trading in the regimes where measured skill exists (§2.2).
3. **Sizing.** Fractional Kelly on calibrated, shrunk probabilities, with per-event caps, replacing the flat $10.
4. **Exits.** Choose explicitly between "repricing" and "hold to resolution" for each thesis. Make stops fee- and spread-aware, so the spread alone never triggers a stop. Use time to resolution.
5. **Robustness:**
   - ensembles;
   - pinned model versions;
   - treat news text as untrusted data (prompt-injection hardening);
   - an LLM cost and latency budget.
6. **Universe.** Once fees are modelled, choose categories by measured edge rather than fee status. Exclude markets with ambiguous rules and neg-risk placeholder or "Other" outcomes.

**Exit test:** over a pre-registered out-of-sample period:

- net-of-cost expected value per trade has a confidence interval that excludes zero, with standard errors clustered by event;
- the combined forecast is at least as well calibrated as the market;
- the result holds in more than one event cluster.

**Depends on:** Stage 1. **Capital:** none.

### Stage 3: Live safety envelope, then a pre-registered micro-capital trial

**Goal:** prove that live execution matches the simulator and cannot lose more than intended. The build work can run in parallel with Stage 2; the trial waits for Stage 2's evidence.

**Build:**

1. **Compliance:**
   - a legal eligibility review for the operator's own jurisdiction;
   - a geoblock check before every order;
   - retire the geoblock-workaround deployment;
   - for US persons, Polymarket US, which is a separate integration.
2. **Keys and access:**
   - a Session Key that cannot withdraw;
   - a secrets manager or OS keychain;
   - API authentication;
   - live mode must be re-armed by a human after any restart.
3. **Independent risk engine:**
   - limits per order, market, event, and gross exposure;
   - mark-to-market daily-loss and drawdown limits;
   - a kill switch that runs cancel-all and then flattens;
   - an order heartbeat;
   - price guards;
   - handling of HTTP 425/503 and post-only mode.
4. **Venue truth:**
   - the user channel for orders and fills;
   - trade status tracked to `CONFIRMED`;
   - reconciliation of orders, positions, and cash on Data API v2;
   - a redemption flow and awareness of UMA resolution status;
   - alerts that reach a human.
5. **Runbook and chaos tests.** Kill the process mid-order; drop the network; inject HTTP 425 and 503; lock the database; replay duplicate news. Verify that no orders are orphaned and the ledger is correct afterwards.

**Trial:**

- Pre-register the size, duration, and stop rules.
- Use a bankroll you can afford to lose entirely, with fractional sizing.
- For each decision, compare live fills with what the simulator predicted (slippage, fill rate). This is what validates the paper venue.

**Exit test (the capital gate, all must hold before increasing size):**

- the chaos tests pass;
- there are zero unreconciled orders or positions over the whole trial;
- live-versus-simulator divergence stays within pre-set bounds;
- the net-of-cost edge persists.

### What must exist before any real capital

1. A clean jurisdiction review and a geoblock check, with no workaround in use.
2. A Session Key, an authenticated API, and no automatic live boot.
3. A tested guarantee of no orphan orders: cancel or FAK/FOK, a heartbeat, and cancel-all.
4. An independent mark-to-market risk engine with a kill switch that cancels orders.
5. A durable decision log, reconciliation on Data API v2, and alerting.
6. Fees included in both edge and P&L.
7. Pre-registered, net-of-cost edge from Stages 1–2.

### What can stay paper-only for now

Everything in Stages 0–2. That includes the canvas, user sections (once they can actually be wired in), prompt and model iteration, and universe selection. The live-executor fixes can be developed and tested against the existing fake CLOB without placing a single order.

### Dependencies at a glance

```text
Stage 0 (correctness + decision log) ──► Stage 1 (honest paper + harness) ──► Stage 2 (signal) ──► Stage 3 trial
                                                                    Stage 3 build ──────────────┘ (runs in parallel with Stage 2)
Time-boxed, independent: Data API v1 → v2 before 2026-10-24 (matters only for live and reconciliation).
```

**A note on direction** [judgement]. The evidence in §2.2 suggests that a news-reading taker bot with retail latency faces efficient prices, informed counterparties, and fees. The most plausible remaining edge is careful interpretation of rules-sensitive, slower-moving markets. Structural strategies (consistency arbitrage, liquidity provision with rebates) are documented sources of profit too ([Saguillo et al.][s-saguillo], [Market Making][s-pm-mm]). They are different businesses with different infrastructure, though. The Stage 1 harness is the cheapest way to find out which of these, if any, this project should pursue.

---

## Appendix A: Observations made for this note (2026-10-04, read-only)

**Live universe check.** I ran the repo's own `openpoly.markets.polymarket_api.discover_events(limit=100)`, `normalize_gamma_market`, and `filter_markets(MarketFilterConfig())` from the commit above, with no keys and no orders:

- 100 events → 12,684 market rows. Rejections: `market_resolved` 7,752, `excluded_tag` 4,109, `fee_not_zero` 779, `low_volume` 21, `price_extreme` 8. Kept: 15.
- Live, tradeable, non-sports rows totalled 823. Of those, 779 had fees (`feeSchedule.rate` of 0.04, 0.05, or 0.07) and 44 were fee-free.
- The fee-free rows came from five events: Israel next PM (28 markets), US–Iran ceasefire (9), Iranian blockade (5), US invades Iran (1), and China invades Taiwan (1).
- Every fee-enabled row had `takerBaseFee = 1000`, including the 3,640 live rows with `feeType = zero_fees` (all sports in this snapshot).
- CLOB `/fee-rate`: politics token → `{"base_fee":1000}`; crypto token → `{"base_fee":1000}`; geopolitics token → `{"base_fee":0}`.
- Field presence, from a second call that returned 12,488 rows: `description` was present on every row, `resolutionSource` on 11,494, and `umaResolutionStatus` mostly on closed markets (49 of 4,935 live rows).
- These are snapshots of the top-100-by-volume window that the code uses. The counts move between calls and from day to day.

**Statistics on the README sample** (from the README's own figures):

- 13 wins out of 21 closed trades: one-sided binomial p ≈ 0.19 against p = 0.5; Wilson 95% interval [0.41, 0.79]. Win rate is also the wrong metric for asymmetric binary payoffs. Expected net P&L per trade, with its interval, is the right one, and it cannot be computed from the repo.
- Toy simulation: a symmetric ±1-tick random walk entered at the ask with a 2-tick spread. The bid ends up above the entry ask at some point in 59% of paths over 30 steps, 78% over 120 steps, and 89% over 480 steps. So "23/25 positions moved in the trade's favor" is not, on its own, evidence of edge. This is an illustration, not a model of Polymarket.

## Appendix B: Test and lint run

- Environment: Python 3.12.3, `uv sync` from the committed `uv.lock`, `OPENPOLY_AUTOSTART_SOURCES=0`.
- `uv run pytest -o addopts="" -q` → `1 failed, 661 passed in 10.88s`. The failure was `tests/test_db_manager.py::test_start_then_enqueue_persists`.
- Rerunning `tests/test_db_manager.py` alone 8 times gave 2 failures, both in `test_status_reports_table_counts`.
- `uv run ruff check .` → `All checks passed!`

## Appendix C: Sources

**Repository (permalinks at `950970d`).** The code links are inline above. The design documents are [`docs/architecture/`][c-arch], the [README results section][c-readme-results], and the [CHANGELOG][c-changelog].

**Venue and vendor documentation**

- Polymarket: [Fees][s-pm-fees] · [Order Lifecycle][s-pm-lifecycle] · [Place Orders][s-pm-place] · [Manage Orders (heartbeats, cancel-all)][s-pm-manage] · [Matching Engine Restarts][s-pm-matching] · [Resolution][s-pm-resolution] · [Negative Risk][s-pm-negrisk] · [Real-Time Data][s-pm-realtime] · [Market Making][s-pm-mm] · [Session Keys][s-pm-sessionkeys] · [Geographic Restrictions][s-pm-geoblock] · [Help centre: geographic restrictions][s-pm-help-geo] · [Data API v1 → v2][s-pm-dataapi-v2] · [Unified SDK migration][s-pm-sdk-migrate] · [Rate Limits][s-pm-ratelimits] · [Get fee rate][s-pm-feerate]
- Polymarket US: [CFTC DCM list][s-pmus-dcm] · [Developer access][s-pmus-dev]
- TradingNews: [WebSocket stream][s-tn-ws]
- Anthropic: [Tool use: forcing tool use][s-anthropic-tools]

**Peer-reviewed**

- Wolfers & Zitzewitz (2004), *Prediction Markets*, Journal of Economic Perspectives 18(2). [link][s-wz2004]
- Busse & Green (2002), *Market efficiency in real time*, Journal of Financial Economics 65(3). [link][s-bg2002]
- Glosten & Milgrom (1985), *Bid, ask and transaction prices in a specialist market with heterogeneously informed traders*, JFE 14(1). [link][s-gm1985]
- Avellaneda & Stoikov (2008), *High-frequency trading in a limit order book*, Quantitative Finance 8(3). [link][s-as2008]
- Halawi, Zhang, Chen & Steinhardt (2024), *Approaching Human-Level Forecasting with Language Models*, NeurIPS 2024. [link][s-halawi]
- Karger et al. (2025), *ForecastBench*, ICLR 2025. [link][s-forecastbench]
- Schoenegger et al. (2024), *Wisdom of the silicon crowd*, Science Advances 10(45). [link][s-schoenegger]
- Saguillo, Ghafouri, Kiffer & Suarez-Tangil (2025), *Unravelling the Probabilistic Forest: Arbitrage in Prediction Markets*, AFT 2025. [link][s-saguillo]
- MacLean, Thorp & Ziemba, *Good and bad properties of the Kelly criterion* (book chapter). [link][s-mtz-kelly]

**Preprints and reports** (lower weight)

- Reichenbach & Walther (2025), *Exploring Decentralized Prediction Markets: Accuracy, Skill, and Bias on Polymarket*, SSRN. [link][s-rw2025]
- *Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets* (2026), arXiv 2602.19520. [link][s-calib2026]
- Sirolly, Ma, Kanoria & Sethi (2025), *Network-Based Detection of Wash Trading*. [paper][s-wash] · [Columbia Business School][s-wash-cbs]
- Aktuğ & Torul (2026), *Informational Inertia in a Decentralized Prediction Market: Evidence from the May 2024 CPI Leak*. [link][s-cpi]
- Young (2026), *OpenMarket: A Synchronized Polymarket–Binance Dataset*, arXiv 2607.26245. [link][s-openmarket]
- Dubach (2026), *The Anatomy of a Decentralized Prediction Market*, arXiv 2604.24366. [link][s-anatomy]
- Forecasting Research Institute (2025), ForecastBench update. [link][s-forecastbench-2025]
- Vera / Crypto Briefing (2026), *The Minutes Myth* (industry note, not peer-reviewed). [link][s-vera]

**News and official filings**

- US Department of Justice (2026), soldier charged over Polymarket bets using classified information. [link][s-doj]
- The Block (2025), Nobel officials probe suspicious Polymarket trades. [link][s-nobel]
- CoinDesk (2025), the Ukraine minerals market resolution dispute. [link][s-uma-ukraine]

<!-- repository permalinks -->
[c-commit]: https://github.com/andylaikawai/OpenPoly/tree/950970d76a0741bd3f8d80b79d1b6a999242a811
[c-gitignore]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/.gitignore#L62-L63
[c-lifespan]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/api/main.py#L109-L191
[c-autostart]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/api/main.py#L95-L106
[c-embed-start]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/api/main.py#L167
[c-run-pipeline]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/scripts/run_pipeline.py#L114-L116
[c-ws-parse]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/news/ws_client.py#L55-L79
[c-ws-client]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/news/ws_client.py#L142-L178
[c-urgency]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/news_source/tradingnews_ws.py#L30-L31
[c-sections-doc]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/docs/architecture/02-strategy-sections.md
[c-market-config]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/markets/manager.py#L59-L81
[c-market-start-guard]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/markets/manager.py#L151-L154
[c-news-start-guard]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/news/manager.py#L183-L186
[c-model-market]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/markets/models.py#L19-L49
[c-fee-parse]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/markets/models.py#L99-L109
[c-filter]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/markets/filters.py#L40-L153
[c-book-depth]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/markets/polymarket_api.py#L168-L185
[c-data-api]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/markets/polymarket_api.py#L188-L254
[c-tables]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/db/tables.py
[c-section-log]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/section_log.py#L204-L268
[c-iso-doc]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/docs/architecture/01-isolation.md#L53-L60
[c-writer]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/db/writer.py#L78-L117
[c-embed-question]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/embedding/manager.py#L196-L210
[c-embed-config]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/embedding/minilm_v0.py#L29-L59
[c-analyzer]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/analyzer/llm_v0.py#L96-L177
[c-llm]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/llm/client.py#L110-L127
[c-entry-edge]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/entry/edge_threshold_v0.py#L266-L305
[c-entry-side]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/entry/edge_threshold_v0.py#L192
[c-entry-slip]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/entry/edge_threshold_v0.py#L67-L72
[c-entry-kill-cfg]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/entry/edge_threshold_v0.py#L130-L167
[c-entry-kill]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/entry/edge_threshold_v0.py#L381-L448
[c-paper]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/executor.py#L50-L141
[c-live]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/live_executor.py
[c-pyproject]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/pyproject.toml#L18
[c-clob-patch]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/clob_patch.py#L19-L37
[c-live-gtc]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/live_executor.py#L230-L244
[c-live-nomatch-buy]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/live_executor.py#L284-L297
[c-live-nomatch-sell]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/live_executor.py#L406-L419
[c-live-sleep-1]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/live_executor.py#L180-L194
[c-live-sleep-2]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/live_executor.py#L350-L365
[c-live-sleep-3]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/execution/live_executor.py#L448-L481
[c-test-zero-match]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/tests/test_live_executor.py#L241-L256
[c-orch-exec]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/orchestrator.py#L319-L333
[c-orch-hardcoded]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/orchestrator.py#L384-L427
[c-exit-loop]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/exit_monitor.py#L199-L208
[c-exit-sell]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/exit_monitor.py#L266
[c-exit-interval]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/exit_monitor.py#L52
[c-exit-singleton]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/exit_monitor.py#L331-L336
[c-exit-section]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/sections/exit/threshold_v0.py#L103-L162
[c-risk-doc-ops]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/docs/architecture/05-runtime-network-risk.md#L36-L43
[c-risk-doc-exit]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/docs/architecture/05-runtime-network-risk.md#L20-L28
[c-set-mode-build]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/api/wallet_routes.py#L222-L234
[c-canvas-reload]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/api/canvas_routes.py#L209-L222
[c-canvas-build]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/api/canvas_routes.py#L225-L266
[c-settle]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/settlement_monitor.py#L185-L265
[c-api-doc]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/docs/architecture/06-polymarket-api.md#L110-L115
[c-recon]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/reconciliation_monitor.py#L174-L205
[c-recon-zero]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/runtime/reconciliation_monitor.py#L184
[c-runtime-state]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/wallet/runtime_state.py#L80-L115
[c-secrets]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/openpoly/news/secrets.py#L39-L63
[c-deploy-doc]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/docs/deploy/README.md
[c-arch]: https://github.com/andylaikawai/OpenPoly/tree/950970d76a0741bd3f8d80b79d1b6a999242a811/docs/architecture
[c-readme-results]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/README.md#L89-L123
[c-changelog]: https://github.com/andylaikawai/OpenPoly/blob/950970d76a0741bd3f8d80b79d1b6a999242a811/CHANGELOG.md

<!-- external sources -->
[s-pm-fees]: https://docs.polymarket.com/trading/fees
[s-pm-lifecycle]: https://docs.polymarket.com/concepts/order-lifecycle
[s-pm-place]: https://docs.polymarket.com/trading/place-orders
[s-pm-manage]: https://docs.polymarket.com/trading/manage-orders
[s-pm-matching]: https://docs.polymarket.com/trading/matching-engine
[s-pm-resolution]: https://docs.polymarket.com/concepts/resolution
[s-pm-negrisk]: https://docs.polymarket.com/concepts/negative-risk
[s-pm-realtime]: https://docs.polymarket.com/market-data/realtime-data
[s-pm-mm]: https://docs.polymarket.com/trading/market-making
[s-pm-sessionkeys]: https://docs.polymarket.com/trading/session-keys
[s-pm-geoblock]: https://docs.polymarket.com/developers/CLOB/geoblock
[s-pm-help-geo]: https://help.polymarket.com/en/articles/13364163-geographic-restrictions
[s-pm-dataapi-v2]: https://docs.polymarket.com/migrate/data-api-v1-to-v2
[s-pm-sdk-migrate]: https://docs.polymarket.com/migrate/clob-sdk-to-unified-sdk
[s-pm-ratelimits]: https://docs.polymarket.com/api-reference/rate-limits
[s-pm-feerate]: https://docs.polymarket.com/api-reference/market-data/get-fee-rate
[s-pmus-dcm]: https://www.cftc.gov/IndustryOversight/IndustryFilings/TradingOrganizations
[s-pmus-dev]: https://polymarketexchange.com/developers.html
[s-tn-ws]: https://docs.tradingnews.press/api-reference/websocket-stream
[s-anthropic-tools]: https://docs.anthropic.com/en/docs/agents-and-tools/tool-use/implement-tool-use
[s-wz2004]: https://www.aeaweb.org/articles?id=10.1257%2F0895330041371321
[s-bg2002]: https://www.sciencedirect.com/science/article/abs/pii/S0304405X02001484
[s-gm1985]: https://doi.org/10.1016/0304-405X(85)90044-3
[s-as2008]: https://doi.org/10.1080/14697680701381228
[s-halawi]: https://arxiv.org/abs/2402.18563
[s-forecastbench]: https://proceedings.iclr.cc/paper_files/paper/2025/file/ea74e45a229dac70b5b63b28d8934db6-Paper-Conference.pdf
[s-forecastbench-2025]: https://forecastingresearch.substack.com/p/ai-llm-forecasting-model-forecastbench-benchmark
[s-schoenegger]: https://doi.org/10.1126/sciadv.adp1528
[s-saguillo]: https://arxiv.org/abs/2508.03474
[s-mtz-kelly]: https://www.stat.berkeley.edu/~aldous/157/Papers/Good_Bad_Kelly.pdf
[s-rw2025]: https://dx.doi.org/10.2139/ssrn.5910522
[s-calib2026]: https://arxiv.org/abs/2602.19520
[s-wash]: https://gamblingharm.org/wp-content/uploads/2025/11/Polymarket-Wash-Trading-Study.pdf
[s-wash-cbs]: https://business.columbia.edu/faculty/press/polymarket-volume-inflated-artificial-activity-study-finds
[s-cpi]: https://web.bogazici.edu.tr/torul/inertia.pdf
[s-openmarket]: https://arxiv.org/abs/2607.26245
[s-anatomy]: https://arxiv.org/abs/2604.24366
[s-vera]: https://cryptobriefing.com/research-minutes-myth/
[s-doj]: https://www.justice.gov/opa/pr/us-soldier-charged-using-classified-information-profit-prediction-market-bets
[s-nobel]: https://www.theblock.co/news/web3/2025-10-10-officials-probe-polymarket-over-suspicious-trades-predicting-nobel-peace-prize-winner-reports-374199
[s-uma-ukraine]: https://www.coindesk.com/markets/2025/03/27/polymarket-uma-communities-lock-horns-after-usd7m-ukraine-bet-resolves

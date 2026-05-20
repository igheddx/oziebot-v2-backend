# 1. Executive Summary

Oziebot is a multi-service crypto trading platform with a Next.js web frontend and Python backend services for market data ingestion, strategy evaluation, risk review, execution, alerts, diagnostics, and billing. The current implementation supports multi-tenant auth, paper and live trading modes, Coinbase integration, per-user strategy configs, capital allocation buckets, token allowlists and token-strategy policy, an admin diagnostics stack, and a new `strategic_aggressive_allocation` strategy with its own config surface.

The platform is materially implemented, but several areas still read as operationally fragile rather than finished-product stable. The biggest themes are: complex multi-layer trade gating, fragmented UX for understanding why a strategy is quiet, a hybrid infrastructure story (legacy ECS assets plus current lean/Lightsail deployment tooling), and a diagnostics/AI review feature that is useful but still largely deterministic. The most important risks are correctness around execution/accounting reconciliation, user trust when non-DCA strategies appear inert, and operational/security concerns around live trading credentials, browser-stored auth tokens, and manual host-managed lean deployments.

# 2. Repository Structure

This inventory spans **two Git repositories** in one working tree:

- **Backend repo root:** `/Users/oplyft/Documents/Development/Application/oziebot`
- **Nested frontend repo:** `/Users/oplyft/Documents/Development/Application/oziebot/frontend`

## Top-level layout

```text
/Users/oplyft/Documents/Development/Application/oziebot
├── backend/
│   ├── packages/
│   │   ├── domain/
│   │   └── py-common/
│   └── services/
│       ├── api/
│       ├── alerts-worker/
│       ├── execution-engine/
│       ├── market-data-ingestor/
│       ├── risk-engine/
│       └── strategy-engine/
├── frontend/
│   └── apps/
│       └── web/
├── infrastructure/
│   ├── aws/
│   └── lean/
├── .github/workflows/
├── docker-compose.yml
├── docker-compose.lean.yml
├── docker-compose.lean.edge.yml
├── README_LEAN_MODE.md
└── STRATEGY_EXTENSION_GUIDE.md
```

## Major folders and files

| Path | Purpose |
| --- | --- |
| `frontend/apps/web/` | Next.js 15 App Router frontend. |
| `backend/services/api/` | FastAPI HTTP API, models, migrations, schemas, services. |
| `backend/services/strategy-engine/` | Strategy runner and strategy implementations. |
| `backend/services/risk-engine/` | Risk rules, risk decisions, queue consumer. |
| `backend/services/execution-engine/` | Paper/live order creation, fills, positions, reconciliation. |
| `backend/services/market-data-ingestor/` | Coinbase WS/REST ingestion and runtime cache population. |
| `backend/services/alerts-worker/` | Slack/SMS/Telegram/ops alert delivery. |
| `backend/packages/domain/` | Shared domain event, signal, intent, risk, trading-mode models. |
| `backend/packages/py-common/` | Queue names, Postgres outbox, health helpers, token policy, defaults, runtime KV. |
| `backend/services/api/alembic/versions/` | Schema migrations `001` through `034`. |
| `.github/workflows/backend-ci-deploy.yml` | Backend CI plus lean host deploy workflow. |
| `infrastructure/aws/backend/` | ECS/Fargate-era task definitions, env maps, service map. |
| `infrastructure/lean/` | Lightsail/EC2 Docker Compose deployment, backup, healthcheck, scaling scripts. |
| `README_LEAN_MODE.md` | Current cost-cut / lean deployment operating model. |

# 3. Frontend Inventory

## Framework, layout, and providers

- **Framework:** Next.js 15.5.15 App Router (`frontend/apps/web/package.json`, `frontend/apps/web/next.config.ts`)
- **React:** 19.1.0
- **Build mode:** `output: "export"` with `trailingSlash: true` (`frontend/apps/web/next.config.ts`)
- **Styling:** Tailwind CSS v4 + global CSS variables (`frontend/apps/web/app/globals.css`)
- **Theme:** dark mode only; `<html className="dark">` is hardcoded in `frontend/apps/web/app/layout.tsx`
- **Fonts:** Space Grotesk + IBM Plex Mono in `frontend/apps/web/app/layout.tsx`
- **Providers:** `AuthProvider` then `TradingModeProvider` in `frontend/apps/web/app/layout.tsx`

## Auth flow

- **Provider:** `frontend/apps/web/components/providers/auth-provider.tsx`
- **Storage:** access and refresh tokens are stored in browser `localStorage` by `frontend/apps/web/lib/auth-service.ts`
- **Bootstrap:** non-login pages hydrate via `GET /v1/auth/session`-style bootstrap helper, then redirect unauthenticated users to `/login`
- **Role gating:** `root_admin` controls whether admin nav items render

## Trading mode toggle

- **Provider:** `frontend/apps/web/components/providers/trading-mode-provider.tsx`
- **Persistence:** `?mode=paper|live` query param + `localStorage`
- **Presentation:** header mode badge and toggle components; live mode uses amber styling, paper mode sky-blue

## Hamburger / navigation structure

Defined in `frontend/apps/web/components/nav/app-nav-links.ts`:

- **Primary:** Dashboard, Strategies, Tokens, Allocation, Strategic Allocation, Alerts
- **Secondary:** Analytics, Trade Log, Export trades (CSV), Setup
- **Admin:** Runtime, AI Diagnostic Review, Trading Diagnostics, Admin (token policy), Fee Settings

## Important routes

| Route | Purpose | Main API calls | Key files | Gaps / issues |
| --- | --- | --- | --- | --- |
| `/login` | Email/password login | auth service login + bootstrap | `app/login/page.tsx`, `lib/auth-service.ts` | Browser token storage; sparse account recovery UX. |
| `/dashboard` | Portfolio, positions, active trades, rejection summaries | `/v1/me/dashboard/summary`, `/v1/me/dashboard/details`, `/v1/me/dashboard/rejections` | `app/dashboard/page.tsx`, `components/dashboard/*`, `lib/dashboard-api.ts` | Good summary, but “why not trade” detail is still fragmented. |
| `/strategies` | Enable/disable standard strategies and save per-strategy token filters | `/v1/me/strategies/catalog`, `/v1/me/strategies`, `PATCH /v1/me/strategies/{strategy_id}`, plus token matrix reads | `app/strategies/page.tsx` | Standard strategies still use a fairly raw config UX; no health timeline or expected cadence display. |
| `/tokens` | Global user token allowlist plus admin token management | `/v1/me/tokens`, `/v1/me/token-strategy-policy`, `/v1/admin/tokens` | `app/tokens/page.tsx` | User view is global on/off, not a true strategy assignment screen. |
| `/allocation` | Strategy capital allocation and bucket balances | `/v1/me/allocations/*` | `app/allocation/page.tsx` | Shows capital state, but strategy health and deployment reasoning remain indirect. |
| `/strategic-allocation` | Dedicated config UI for `strategic_aggressive_allocation` | `/v1/me/strategies/strategic-aggressive-allocation/*` | `app/strategic-allocation/page.tsx` | Separate and clearer than the standard strategy UI; still dense for mobile. |
| `/analytics` | Trade outcomes and strategy analytics | `/v1/me/analytics/*` | `app/analytics/page.tsx` | Useful, but diagnostics and lifecycle traces live elsewhere. |
| `/trade-log` | Trade/history view | trade-log endpoints in `/v1/logs` / dashboard-derived APIs | `app/trade-log/page.tsx` | Helpful for history, but not a full execution timeline viewer. |
| `/alerts` | Notification channel preferences | alerts endpoints | `app/alerts/page.tsx` | Operationally useful; not strongly tied back to diagnostics findings. |
| `/coinbase` | Configure and validate live Coinbase credentials | `/v1/integrations/coinbase/*` | `app/coinbase/page.tsx` | Live-trading readiness hinges on this page; UX depends on backend validation messages. |
| `/onboarding` | Guided setup checklist | mostly navigational / supporting reads | `app/onboarding/page.tsx` | Good orientation, but not a complete safety/readiness wizard. |
| `/trading-performance-export` | CSV export of trade history | export API helpers in frontend and backend export endpoints | `app/trading-performance-export/page.tsx` | Present, but adjacent frontend export files were left as unrelated local changes during prior work. |
| `/admin/ai-diagnostics` | AI Diagnostic Review screen | `/v1/admin/ai-diagnostics/snapshots`, `/reviews`, `/findings/{id}` | `app/admin/ai-diagnostics/page.tsx` | Strongest “explain the system” UI, but underlying provider is mostly rule-based today. |
| `/admin/trading-diagnostics` | Deep diagnostics JSON/report view | admin trading diagnostics endpoints | `app/admin/trading-diagnostics/page.tsx` | Power-user/admin oriented; not translated into simpler end-user health indicators. |
| `/admin/token-policy` | Root-admin token-strategy policy matrix | admin platform token policy APIs | `app/admin/token-policy/page.tsx` | Powerful but operationally dense. |
| `/admin/runtime` | Service/runtime heartbeat view | runtime status APIs | `app/admin/runtime/page.tsx` | Admin only; does not directly bridge runtime state to user confidence. |
| `/subscription` | Billing/subscription summary | `/v1/billing/summary`, checkout flow | `app/subscription/page.tsx` | Billing exists, but strategy entitlement UX is still secondary to trading UX. |

## Components and UI patterns

- **Shell/layout:** `frontend/apps/web/components/layout/app-shell.tsx`
- **Dashboard widgets:** `frontend/apps/web/components/dashboard/*`
- **Navigation:** `frontend/apps/web/components/nav/*`
- **Providers:** `frontend/apps/web/components/providers/*`
- **Skeleton/loading UI:** `frontend/apps/web/components/ui/skeleton.tsx`

## AI Diagnostic Review screen

- **Route:** `/admin/ai-diagnostics`
- **Purpose:** Browse diagnostic snapshots, create a review, inspect findings, patch finding status
- **Backend contract:** `backend/services/api/src/oziebot_api/api/v1/admin_ai_diagnostics.py`
- **Current state:** the UX exists, but the backend currently stores deterministic rule-based findings first and only exposes a placeholder “openai-compatible” provider hook.

## Trade/history views and export

- **Trade log:** `/trade-log`
- **Analytics:** `/analytics`
- **CSV export:** `/trading-performance-export`
- **Observation:** these pages improve transparency but still do not present one end-to-end lifecycle view from signal → risk → execution → position close for normal users.

## Mobile responsiveness and dark mode

- Mobile-first classes are used across pages; the nav supports a drawer plus a bottom tabbar.
- The visual system is cohesive and optimized for dark backgrounds (`globals.css`).
- There is **no light theme** and no user-selectable theme.

## Known frontend UX issues

1. User understanding is still fragmented across Dashboard, Trading Diagnostics, AI Diagnostics, and Trade Log.
2. Standard strategy configuration is still more “JSON/config plumbing” than polished trading product UX.
3. The platform now supports per-strategy token selection on `/strategies`, but this is still a control panel, not an explanatory strategy-health experience.
4. Live-vs-paper warnings exist, but overall “am I safe / why is the bot quiet / what is healthy?” messaging is still incomplete.

# 4. Backend Inventory

## Framework and startup

- **API framework:** FastAPI (`backend/services/api/src/oziebot_api/main.py`)
- **Server:** Uvicorn via `backend/services/api/docker-entrypoint.sh`
- **Python runtime:** 3.12+ (`backend/services/api/pyproject.toml`)
- **Startup behavior:** optional `alembic upgrade head` on container start, then app boot
- **Middleware / observability:** request IDs, request timing, DB query timing, simple SLO monitor in `main.py`

## Configuration and dependency injection

- **Settings:** `backend/services/api/src/oziebot_api/config.py`
- **DB/session deps:** `backend/services/api/src/oziebot_api/deps/`
- **Key env-backed settings:** `DATABASE_URL`, `JWT_SECRET`, Stripe keys, `EXCHANGE_CREDENTIALS_ENCRYPTION_KEY`, Coinbase base URL, AI diagnostic provider settings, API slow-request/query thresholds

## Auth / JWT / session handling

- **Auth routes:** `backend/services/api/src/oziebot_api/api/v1/auth.py`
- **Session model:** `backend/services/api/src/oziebot_api/models/auth_session.py` (`user_sessions`)
- **Current pattern:** JWT access token + DB-backed refresh/session state
- **Root-admin gate:** `backend/services/api/src/oziebot_api/deps/auth.py`

## Tenant and user isolation

- **Users:** `models/user.py`
- **Tenants:** `models/tenant.py`
- **Memberships:** `models/membership.py`
- **Isolation pattern:** most `/me` queries scope by `CurrentUser`, then derive tenant context through membership helpers such as `services/tenant_scope.py`
- **Trading partitioning:** `trading_mode` is carried through strategies, orders, positions, capital buckets, and queues

## Error handling and validation

- Mostly route-local `HTTPException` conversion from service-layer `ValueError` / domain exceptions.
- Validation is split across:
  - Pydantic schemas in `backend/services/api/src/oziebot_api/schemas/`
  - route/query param constraints
  - service-layer checks
  - DB uniqueness/foreign-key constraints
- There is **no single global domain error envelope**, so behavior varies by endpoint.

## API route modules

| Module | Prefix / feature | Notes |
| --- | --- | --- |
| `api/v1/auth.py` | `/v1/auth` | Register, login, refresh, logout, session bootstrap. |
| `api/v1/me.py` | `/v1/me` | Largest route module; dashboard, analytics, profile, mode switching. |
| `api/v1/strategies.py` | `/v1/me/strategies` | Catalog, CRUD, performance, signals, runtime state. |
| `api/v1/strategic_aggressive_allocation.py` | `/v1/me/strategies/strategic-aggressive-allocation` | Dedicated SAA config, positions, performance, profit history, rebalance preview/execute, backtest preview. |
| `api/v1/tokens.py` | mixed `/v1/me/tokens`, `/v1/admin/tokens` | User token permissions and root-admin platform token management. |
| `api/v1/allocations.py` | `/v1/me/allocations` | Strategy allocation plans, buckets, reserve/lock/settle actions. |
| `api/v1/integrations_coinbase.py` | `/v1/integrations/coinbase` | Create/validate/update/delete Coinbase tenant integration. |
| `api/v1/admin_ai_diagnostics.py` | `/v1/admin/ai-diagnostics` | Snapshots, reviews, finding status changes. |
| `api/v1/admin_trading_diagnostics.py` | `/v1/admin/trading-diagnostics` | Diagnostics JSON/report/export surfaces. |
| `api/v1/admin_strategy_lifecycle.py` | `/v1/admin/strategy-lifecycle` | Lifecycle funnel and trace views. |
| `api/v1/admin_platform.py` | `/v1/admin/platform` | Platform strategies, token-policy overrides, fee/admin controls. |
| `api/v1/alerts.py` | alerts | Notification preferences/configuration. |
| `api/v1/backtests.py` | backtests | Backtest-oriented APIs. |
| `api/v1/billing.py` | `/v1/billing` | Checkout and billing summary. |
| `api/v1/health.py` | `/v1/health`, `/v1/ready` | Readiness and health routes. |
| `api/v1/logs.py` | logs | Trade/export/history-style API surfaces. |
| `api/v1/tenants.py` | tenants | Tenant-facing administration. |

## Major backend service modules

| Path | Responsibility |
| --- | --- |
| `services/strategy_allocation.py` | Allocation plans, capital buckets, reserve/lock/settle logic. |
| `services/strategic_aggressive_allocation.py` | SAA config persistence, validation, preview, rebalance execution. |
| `services/token_policy.py` | Token market profiles, user/admin matrix views, recommendations/defaults. |
| `services/admin_trading_diagnostics.py` | Builds the main diagnostics JSON/report used by admin diagnostics. |
| `services/admin_ai_diagnostics.py` | Stores diagnostic snapshots, generates rule-based findings, review lifecycle. |
| `services/live_coinbase.py` / `services/coinbase.py` | Coinbase account/balance support from API side. |
| `services/stripe_service.py` and billing helpers | Billing checkout and summary logic. |

# 5. Database / Data Model Inventory

Schema is managed by Alembic in `backend/services/api/alembic/versions/` and currently runs through **migration `034_strategic_allocation`**.

## Core identity, tenancy, and billing

| Table | Purpose | Important fields / relationships | Indexes / constraints | Risks |
| --- | --- | --- | --- | --- |
| `users` | User identities | email, password hash, `is_root_admin`, `is_active` | unique email | Browser token storage on frontend raises session theft risk if XSS exists. |
| `user_sessions` | Refresh/session state | user FK, token hash, expiry/revocation | session lookup indexes | Session model exists, but auth UX remains simple. |
| `tenants` | Tenant boundary and trial state | name, `default_trading_mode`, trial timestamps | PK/indexes | Current product still behaves close to single-owner despite multi-tenant schema. |
| `tenant_memberships` | User↔tenant relationship | user FK, tenant FK, role | unique `(user_id, tenant_id)` | Tenant selection UX is limited compared with schema flexibility. |
| `tenant_entitlements` | Feature gating | tenant FK, feature code, entitlement value | tenant/feature indexes | Entitlements add another dimension of “why strategy unavailable” complexity. |
| `stripe_customers` / `stripe_subscriptions` / `stripe_subscription_items` / `billing_checkout_sessions` / `subscription_plans` | Billing state | tenant FK, Stripe IDs, status, plans | Stripe IDs and tenant indexes | Billing exists but is secondary to trading; operational coupling with strategy access can confuse users. |

## Strategy config and allocation

| Table | Purpose | Important fields / relationships | Indexes / constraints | Risks |
| --- | --- | --- | --- | --- |
| `platform_strategies` | Catalog of strategies | slug, display name, description, config schema | strategy identifiers | Admin catalog and runtime implementation can drift if not kept aligned. |
| `user_strategies` | Per-user strategy config | `strategy_id`, `is_enabled`, JSON `config` | user/strategy uniqueness patterns | Standard strategies persist config as generic JSON, which keeps UX loose. |
| `user_strategy_states` | Persisted runtime state | user, strategy, trading_mode, JSON state | scoped lookups | State integrity matters for DCA interval and exit tracking. |
| `strategy_allocation_plans` | Per-user per-mode capital allocation plan | `user_id`, `trading_mode`, allocation mode, total capital | unique `(user_id, trading_mode)` | Allocation accuracy matters for risk and buying power. |
| `strategy_allocation_items` | Per-strategy allocation percentages | plan FK, strategy id, bps, assigned capital | unique `(plan_id, strategy_id)` | BPS sum enforcement is critical. |
| `strategy_capital_buckets` | Strategy-level cash/PnL bucket state | assigned, available, reserved, locked, realized/unrealized | unique `(user_id, strategy_id, trading_mode)` | Divergence from orders/positions can break dashboard trust. |
| `strategy_capital_ledger` | Immutable cash/PnL audit trail | event type, before/after bucket balances, metadata | user/strategy/time indexes | Ledger quality determines whether reconciliation can explain drifts. |
| `strategic_aggressive_allocation_configs` | Dedicated SAA config store | user, strategy_id, trading_mode, bucket config, selected tokens, rules | unique `(user_id, strategy_id, trading_mode)` | Separate storage improves isolation but adds another config model style. |
| `strategic_aggressive_allocation_profit_events` | SAA profit-taking history | symbol, bucket_id, event_type, signal/correlation IDs | many indexes on user/mode/symbol/bucket/time | Purpose-built and useful, but strategy-specific analytics live outside the generic model set. |

## Token controls and policy

| Table | Purpose | Important fields / relationships | Indexes / constraints | Risks |
| --- | --- | --- | --- | --- |
| `platform_token_allowlist` | Admin-approved token universe | symbol, display name, enabled flag, metadata | symbol/order indexes | If platform tokens drift from market universe, strategies go quiet. |
| `user_token_permissions` | Per-user token enable/disable | user FK, platform token FK, enabled state | user/token scope | Global user token list is separate from per-strategy symbol selection. |
| `token_market_profile` | Computed token market profile | liquidity/spread/volatility/trend/reversion scores | token FK | Recommendation quality depends on fresh market profile data. |
| `token_strategy_policy` | Token↔strategy pair policy | admin enabled, recommendation status, override fields, sizing overrides | unique `(token_id, strategy_id)` | Policy is enforced in multiple layers, which is safer but harder to reason about. |

## Signals, lifecycle, diagnostics, and AI review

| Table | Purpose | Important fields / relationships | Indexes / constraints | Risks |
| --- | --- | --- | --- | --- |
| `strategy_runs` | Strategy run audit | user, strategy, mode, timings | run indexes | High-frequency runners can create noisy audit volume. |
| `strategy_signals` | Emitted signal records | strategy, symbol, confidence, signal payload | strategy/time indexes | Critical for diagnostics; signal verbosity can grow quickly. |
| `risk_events` | Risk decisions / rejections | user/tenant, strategy, symbol, reason, payload | time and scope indexes | Good rejection visibility, but still admin-heavy. |
| `strategy_signal_snapshots` | Signal snapshot for trade intelligence | signal metadata and features | signal indexes | Useful for diagnostics/AI, adds more analytical state to manage. |
| `strategy_decision_audits` | Decision-stage audit trail | decision JSON, stage, reasoning | time/scope indexes | High-value observability store. |
| `trade_outcome_features` | Feature store for trade outcomes | trade FK, feature data | trade/time indexes | Future-facing ML table; current operational value is analytical. |
| `ai_inference_records` | AI-ready trade inference audit | model inputs/outputs | time indexes | AI footprint exists before strong production AI loop exists. |
| `strategy_lifecycle_events` | Lifecycle trace events | strategy stage/status, correlation IDs, reasons, metadata | strategy/time/correlation indexes | Central to understanding quiet strategies. |
| `diagnostic_snapshots` | Stored diagnostics JSON | tenant, mode/strategy/token filters, raw JSON | time/filter indexes | Can get large/expensive if overused. |
| `ai_diagnostic_reviews` | Review header | snapshot FK, status, health, summary, model | status/time indexes | Current “AI” is mostly rules with placeholder provider integration. |
| `ai_diagnostic_findings` | Individual findings | severity, category, evidence, recommendation, tuning flags | severity/category/strategy/token/status indexes | Good review surface, but recommendations are not autonomous. |
| `ai_diagnostic_recommendation_audit` | Admin action audit on findings | finding FK, action, status change, admin FK | finding/time indexes | Useful for human approval path. |

## Orders, executions, positions, and reconciliation

| Table | Purpose | Important fields / relationships | Indexes / constraints | Risks |
| --- | --- | --- | --- | --- |
| `execution_orders` | Order lifecycle record | `intent_id`, `correlation_id`, user/tenant, strategy, symbol, state, quantity, fee/slippage fields, payloads | unique `(intent_id, trading_mode)`, unique idempotency key, unique client order id, many scope indexes | This is the central correctness table; if it drifts from fills/positions the platform looks unreliable. |
| `execution_fills` | Fill records per order | order FK, venue fill ID, quantity/price, fee cents | unique `(order_id, venue_fill_id)` | Needed for partial-fill correctness. |
| `execution_trades` | Executed trade records | order/fill FK, strategy, symbol, side, quantity, price, realized pnl, post-trade position fields | order/fill/user/strategy/symbol/mode/time indexes | DCA interval enforcement reads this table for last successful buy. |
| `execution_positions` | Current position per strategy/symbol/mode | quantity, avg entry price, realized pnl, lifecycle timestamps | unique scope `(tenant_id, user_id, strategy_id, symbol, trading_mode)` | Quiet/incorrect exit behavior shows up here first. |
| `execution_reconciliation_events` | Reconciliation audit | event type, mismatch/repair details | time indexes | Reconciliation exists, but users do not see it directly. |

## Market data and notifications

| Table | Purpose | Important fields / relationships | Indexes / constraints | Risks |
| --- | --- | --- | --- | --- |
| `market_data_candles` | Candle history | product/granularity/window OHLCV | product/time indexes | Strategy quality depends on ordering, dedupe, and freshness. |
| `market_data_trade_snapshots` | Recent trade samples | product, side, price, size, time | product/time indexes | Used for token market profiles and diagnostics. |
| `market_data_bbo_snapshots` | Best bid/offer snapshots | bid, ask, sizes, event time | product/time indexes | Spread/slippage and stale-data checks depend on this. |
| `notification_channel_configs` / `notification_preferences` / `notification_delivery_attempts` | Alerts configuration and delivery state | channel configs, per-user prefs, delivery attempts | scope/time indexes | Alerts are helpful but not central to trading correctness. |

## Queue and runtime state

- **`worker_message_outbox`**: Postgres-backed queue table used by workers (migration 030)
- **`runtime_kv`**: Postgres-backed runtime KV cache used for heartbeat/state/cache patterns

# 6. Trading Strategy Inventory

Configs come from a combination of:

1. **Baseline/default strategy config:** `backend/packages/py-common/src/oziebot_common/strategy_defaults.py`
2. **Per-user strategy config JSON:** `user_strategies.config`
3. **Dedicated SAA config tables:** `strategic_aggressive_allocation_*`
4. **Token-strategy policy:** `token_strategy_policy`
5. **Capital allocation buckets:** `strategy_capital_buckets`

## DCA

- **Implementation:** `backend/services/strategy-engine/src/oziebot_strategy_engine/strategies/dca.py`
- **Defaults:** `buy_amount_usd=100`, `buy_interval_hours=24`, `only_on_green_days=false`, dynamic sizing on, `min_trade_usd=100`, `max_trade_usd=150`, target utilization `0.50` (`strategy_defaults.py`)
- **Schedule / interval logic:** runner evaluates DCA every **300 seconds** (`runner.py` `STRATEGY_INTERVAL_SECONDS`), but BUY eligibility is additionally enforced by `_last_successful_dca_buy_at()` and `buy_interval_hours`. The check is per **user + symbol + trading_mode + strategy='dca'**, using `execution_trades` and runtime state (`runner.py` around lines 2114-2221).
- **Token eligibility:** global user token permission + platform token allowlist + token-strategy policy. Standard DCA can also be narrowed by `config.symbols` from the UI.
- **Sizing:** fixed notional intent plus dynamic-sizing overlay from shared sizing helpers.
- **Risk path:** same risk engine as other strategies, but paper mode relaxes some rules.
- **Execution path:** risk-approved BUY → execution engine → positions/bucket updates.
- **Known issues:** DCA appears “active” more often because its runner cadence is frequent and its intent is deterministic; user confusion can result even when buys are correctly suppressed by interval logic.

## Momentum

- **Implementation:** `.../strategies/momentum.py`
- **Defaults:** short/long windows `10/40`, `strength_threshold=0.02`, `position_size_fraction=0.25`, `stop_loss_pct=0.035`, `take_profit_pct=0.045`, `trailing_stop_pct=0.018`, partial take-profit, volume confirmation, max hold 300 minutes, `min_trade_usd=75`, `max_trade_usd=300`.
- **Indicators used:** moving averages, price momentum, candle closes/volumes.
- **Signal rules:** confidence thresholds come from platform defaults plus shared signal rules.
- **Entry logic:** bullish short/long MA crossover with required strength and volume.
- **Exit logic:** bearish crossover, stop-loss, take-profit, trailing stop, partial take-profit, max hold timeout.
- **Sizing:** configured fraction plus shared dynamic sizing and risk caps.
- **Known issues:** more likely than DCA to be quiet because it depends on candle history quality, volume confirmation, confidence, fee economics, and stale-data / spread gates.

## Day Trading

- **Implementation:** `.../strategies/day_trading.py`
- **Defaults:** entry/exit thresholds, `stop_loss_pct=0.008`, `position_size_fraction=0.15`, volume and volatility minimums, trend alignment, min confirmations, max position age 3 hours, trailing stop and partial TP, `min_trade_usd=50`, `max_trade_usd=200`.
- **Indicators used:** breakout / intraday range logic, volume confirmation, volatility, trend alignment, candle history.
- **Signal rules:** shared signal rule layer plus strategy-specific config.
- **Entry logic:** short-term breakout-style entry when multiple confirmations pass.
- **Exit logic:** stop-loss, profit take, trailing stop, force exit by age.
- **Sizing:** fraction of assigned capital with dynamic sizing enabled.
- **Known issues:** very sensitive to stale/insufficient candle history and strict volume/trend checks; appears quiet when market-data windows degrade or when risk/fee rules filter it heavily.

## Mean Reversion

- **Implementation:** `.../strategies/reversion.py`
- **Defaults:** `band_window=20`, `rsi_period=14`, `zscore_entry=2.0`, `zscore_exit=0.5`, `rsi_buy=30`, `rsi_exit=50`, `rsi_sell=65`, `min_bandwidth=0.012`, `use_trend_filter=true`, `ema_long_window=200`, `position_size_fraction=0.10`, `stop_loss_pct=0.02`, `take_profit_pct=0.04`, `max_hold_minutes=120`, `min_trade_usd=30`, `max_trade_usd=100`.
- **Indicators used:** z-score, RSI, bandwidth, EMA trend filter.
- **Entry logic:** oversold / stretched downside conditions with optional trend filter and minimum bandwidth.
- **Exit logic:** snap-back / RSI exit, stop-loss, take-profit, max hold timeout.
- **Sizing:** dynamic sizing enabled, smaller caps than momentum/day trading.
- **Known issues:** conservative defaults plus trend filter can make it very selective; no dedicated end-user explanation exists when it stays inactive.

## Strategic Aggressive Allocation

- **Implementation:** `backend/services/strategy-engine/src/oziebot_strategy_engine/strategies/strategic_aggressive_allocation.py`
- **Persistence:** dedicated API models/tables in `models/strategic_aggressive_allocation.py`
- **UI:** `frontend/apps/web/app/strategic-allocation/page.tsx`
- **Defaults:** hourly evaluation, four bucket model, explicit selected tokens, stop-loss/trailing/profit rules, dry powder bucket never trades.
- **Isolation:** implemented as a separate pluggable strategy with its own config routes and token selection path.

## Config persistence and control model

- **User-specific:** `user_strategies.config`, `user_strategy_states`, SAA config rows, token permissions
- **Admin/platform-controlled:** `platform_strategies`, platform default config, token policy overrides, platform token allowlist
- **Token-strategy policy enforcement:** handled in **three layers** for BUY flow:
  1. strategy runner / signal stage suppresses blocked entry symbols
  2. risk engine enforces policy and sizing rules
  3. execution engine re-checks token policy before order submission

# 7. Trading Data Flow

## End-to-end flow

1. **Market data ingestion**  
   - **Files:** `backend/services/market-data-ingestor/src/oziebot_market_data_ingestor/*`
   - **Tables:** `market_data_candles`, `market_data_trade_snapshots`, `market_data_bbo_snapshots`
   - **Runtime state:** Postgres runtime KV / cache
   - **Gaps:** stale or malformed candle windows directly suppress short-horizon strategies

2. **Strategy evaluation**  
   - **Files:** `strategy-engine/.../runner.py`, `registry.py`, strategy files
   - **Inputs:** user strategy config, platform defaults, token permissions, market snapshots, position state, capital context
   - **Queue output:** `oziebot:queue:signal_generated:{mode}`
   - **Logs:** strategy-runner logs plus lifecycle/decision audit writes

3. **Signal generation**  
   - **Files:** individual strategy classes under `strategy-engine/.../strategies/`
   - **Tables:** `strategy_runs`, `strategy_signals`, `strategy_signal_snapshots`
   - **Failure handling:** HOLD/skip reasons, lifecycle failure records, diagnostics visibility

4. **Signal validation / token policy check**  
   - **Files:** runner policy checks, `oziebot_common/token_policy.py`, `services/token_policy.py`
   - **Tables:** `token_strategy_policy`, `platform_token_allowlist`, `user_token_permissions`
   - **Gaps:** policy reasoning exists, but it is spread across user token screens, admin matrix, diagnostics, risk events, and execution metadata

5. **Risk engine**  
   - **Files:** `risk-engine/src/oziebot_risk_engine/service.py`, `rules.py`
   - **Queue:** consumes `signal_generated`, emits `intent_approved` / `intent_rejected`
   - **Tables:** `risk_events`, `strategy_decision_audits`, `strategy_lifecycle_events`
   - **Failure handling:** structured rejection reasons and lifecycle failure events

6. **Allocation / buying power checks**  
   - **Files:** risk rules + allocation services + execution pre-validation
   - **Tables:** `strategy_capital_buckets`, `strategy_capital_ledger`
   - **Gaps:** correctness depends on bucket state staying aligned with executions and positions

7. **Order creation / execution**  
   - **Files:** `execution-engine/src/oziebot_execution_engine/service.py`, adapters
   - **Queue:** consumes `intent_approved`; produces execution reconciliation events and notifications
   - **Tables:** `execution_orders`, `execution_fills`, `execution_trades`
   - **Failure handling:** execution validation failures, policy re-check failures, retry/reconciliation path

8. **Position update**  
   - **Files:** execution service post-fill accounting methods
   - **Tables:** `execution_positions`, `execution_trades`, allocation bucket/ledger tables
   - **Logs:** lifecycle success/failure and decision audits

9. **PnL and closeout**  
   - **Files:** execution service accounting code, dashboard/analytics services
   - **Tables:** `execution_positions`, `execution_trades`, `trade_outcome_features`, analytics tables
   - **Gaps:** users do not get one unified “position accounting explanation” surface

10. **Diagnostics**  
    - **Files:** `services/admin_trading_diagnostics.py`, `services/admin_ai_diagnostics.py`, admin route modules
    - **Tables:** `diagnostic_snapshots`, AI review tables, lifecycle / decision audit tables
    - **Gaps:** rich for admins, still indirect for normal users

# 8. Strategy Lifecycle Observability

Primary stores:

- **Lifecycle events:** `strategy_lifecycle_events`
- **Decision audits:** `strategy_decision_audits`
- **Risk events:** `risk_events`
- **Signal records:** `strategy_signals`
- **Diagnostics views:** admin lifecycle + trading diagnostics + AI diagnostics

| Step | Tracked? | Where stored | Logged / surfaced | Notes |
| --- | --- | --- | --- | --- |
| `signal_generated` | Yes | `strategy_signals`, lifecycle events | Trading diagnostics | Core trace point exists. |
| `signal_emitted` | Yes | signal pipeline tables | Diagnostics | Emission is visible. |
| `validation_started` | Partial | lifecycle/decision audit metadata | Mostly admin diagnostics | Not every validation sub-step is a first-class named stage. |
| `confidence_validation` | Partial | rejection metadata / diagnostics | Diagnostics summaries | Usually represented as reason codes, not dedicated lifecycle stage rows. |
| `volume_validation` | Partial | strategy HOLD/reason text + diagnostics | Diagnostics | Same pattern as confidence. |
| `trend_validation` | Partial | strategy reasoning metadata | Diagnostics | Strategy-specific, not uniform. |
| `cooldown_validation` | Partial | risk decision / diagnostics | Diagnostics | Better represented on DCA and loss-cooldown paths. |
| `allocation_validation` | Partial | risk / allocation metadata | Diagnostics | Visible via rejections and capital utilization, not a dedicated stage everywhere. |
| `policy_validation` | Yes | lifecycle + risk + execution metadata | Admin diagnostics | Enforced in multiple layers. |
| `risk_validation` | Yes | `risk_events`, lifecycle, decision audits | Admin diagnostics | Strongest formal trace after signal generation. |
| `execution_requested` | Yes | lifecycle + `execution_orders` | Logs / diagnostics | Explicit execution-request stage exists. |
| `execution_succeeded` | Yes | orders/fills/trades + lifecycle | Diagnostics | Good operational trace. |
| `execution_failed` | Yes | order failure fields + lifecycle | Diagnostics | Validation failures now preserved as explicit codes. |
| `position_opened` | Yes | `execution_positions`, lifecycle | Dashboard + diagnostics | Visible, but not always in a narrative timeline. |
| `exit_monitoring_started` | Partial | runtime state / lifecycle | Not very user-visible | Exit monitoring exists, but surface is weak. |
| `take_profit_triggered` | Yes/partial | lifecycle and trade metadata | Better for SAA and managed exits | Strategy-specific richness varies. |
| `stop_loss_triggered` | Yes/partial | lifecycle and execution metadata | Diagnostics | Present, especially in execution exit reasons. |
| `trailing_stop_triggered` | Yes/partial | lifecycle/exit metadata | Diagnostics | Present but not consistently visible to end users. |
| `exit_execution_requested` | Yes | lifecycle + orders | Diagnostics | Explicit stage exists. |
| `position_closed` | Yes | positions/trades/lifecycle | Dashboard, analytics, diagnostics | Closing path is tracked. |

# 9. Risk Engine Inventory

## Core controls

- Token allowlist and user token enablement
- Token-strategy policy status, size multipliers, per-token overrides
- Strategy enabled / entitlement checks
- Capital bucket availability and allocation checks
- Max position size / strategy exposure / token concentration
- Daily loss and global loss guards
- Cooldown after losses
- Spread/slippage / execution-quality checks
- Fee economics and expected-edge checks
- Minimum/maximum trade size behavior
- Stale market data protection

## Paper vs live safeguards

- Queue names are partitioned by trading mode.
- Strategy runner resolves modes separately.
- Risk engine deliberately relaxes some rules in paper mode: `max_daily_loss`, `cooldown_after_losses`, `fee_economics`, `execution_quality` (`risk-engine/service.py`).
- Live mode additionally depends on valid Coinbase credentials and live account state.

## Possible bypass points

1. **Paper mode relaxed rules** can make paper and live behavior diverge materially.
2. **Exit-only behavior** can still sell blocked or disabled symbols to unwind existing positions; this is safer for capital recovery but can confuse policy expectations.
3. The platform uses several gating layers, so a bug in one layer may be masked by another instead of being obvious.

# 10. Execution Engine Inventory

## Implementation

- **Core service:** `backend/services/execution-engine/src/oziebot_execution_engine/service.py`
- **Paper execution:** adapter-backed simulation; no Coinbase call path
- **Live execution:** Coinbase adapter after credential decrypt/validation

## Order flow

1. Risk-approved intent enters execution engine
2. Existing order check by `(intent_id, trading_mode)` and idempotency key
3. Execution engine re-checks token policy
4. Request validation runs
5. `execution_orders` row is written
6. Adapter submits / simulates fills
7. Fills and trades are persisted
8. Position and bucket accounting update
9. Reconciliation path runs separately

## Precision, min order, and zero-value guards

Verified in `execution-engine/service.py`:

- `MAX_COINBASE_DECIMAL_PLACES = 8`
- `COINBASE_MIN_NOTIONAL_USD = Decimal("1.00")`
- Request validation rejects:
  - non-finite quantity
  - quantity `<= 0`
  - excessive quantity precision
  - missing / invalid price hint for BUY
  - non-finite or non-positive notional
  - notional rounded to zero cents
  - below minimum notional
  - insufficient allocation / buying power

## Duplicate prevention / idempotency

- unique `(intent_id, trading_mode)`
- unique `idempotency_key`
- unique `client_order_id`

## Retry behavior

- Worker queue messages are leased, retried with `retry_after`, and reclaimed if leases go stale.
- Reconciliation exists to repair live-state drift.

## Key issues to flag

- Zero quantity / zero notional risk is now guarded more explicitly than before, but it remains a critical area because sizing, policy multipliers, and rounding all interact before submission.
- Token policy is intentionally re-applied at execution, which is good for safety but can surprise operators when earlier stages appeared to approve.

# 11. Position, Balance, and PnL Accounting

## How positions are managed

- **Current positions:** `execution_positions`
- **Trade ledger:** `execution_trades`
- **Order/fill detail:** `execution_orders` and `execution_fills`

## Realized and unrealized PnL

- **Realized PnL:** stored per trade and rolled into positions / capital buckets
- **Unrealized PnL:** maintained in `strategy_capital_buckets.unrealized_pnl_cents` and surfaced through dashboard/accounting services
- **Fees:** captured in cents in orders/fills/trades and included in execution accounting

## Paper vs live balances

- **Paper:** simulated through execution adapter + bucket/accounting state
- **Live:** depends on Coinbase accounts plus internal execution/accounting records

## Reconciliation

- Reconciliation support exists in the execution engine and records events into `execution_reconciliation_events`.
- Internal reconciliation reporting was added to compare executions, positions, and bucket accounting.

## Accounting gaps / risks

1. The accounting model is strong on audit trail, but trust still depends on reconciliation being run and surfaced.
2. Users do not get a simple explanation when balances, positions, and capital buckets temporarily diverge.
3. Paper and live are partitioned, but paper’s relaxed risk behavior means “paper correctness” does not perfectly prove live correctness.

# 12. Diagnostics and AI Diagnostics

## Diagnostics endpoints and structure

- **Trading diagnostics routes:** `backend/services/api/src/oziebot_api/api/v1/admin_trading_diagnostics.py`
- **Core builder:** `services/admin_trading_diagnostics.py`
- **Report shape:** generated JSON includes strategy summary, token summary, execution activity, open positions, signal funnel, capital utilization, exit analysis, and active strategy config

## AI Diagnostic Review

- **Routes:** `api/v1/admin_ai_diagnostics.py`
- **Storage:** `diagnostic_snapshots`, `ai_diagnostic_reviews`, `ai_diagnostic_findings`, `ai_diagnostic_recommendation_audit`
- **Provider abstraction:** `RuleBasedDiagnosticProvider` plus `OpenAICompatibleDiagnosticProvider` placeholder in `services/admin_ai_diagnostics.py`
- **Current reality:** rule-based findings are the primary implementation; external AI settings exist but do not yet drive a full autonomous analysis loop

## Known findings currently detected

The rule-based AI diagnostics service explicitly checks for:

- DCA interval violations
- zero-value/zero-notional executions
- strategy execution gaps / “mapped tokens but no executions”
- closed-trade gaps
- generic rejection concentration
- token policy conflicts
- position reconciliation issues
- capital utilization anomalies

## Future hooks already present

- finding status transitions
- approval-required flags
- proposed config change JSON
- rollback plan / expected impact fields
- `eligible_for_auto_tune` and related metadata

# 13. AWS / Infrastructure Inventory

## Current hosting approach

The repo contains **both** legacy/scale-up AWS ECS assets and an actively used **lean Lightsail/EC2 Docker Compose** deployment path.

### Frontend

- Separate frontend repo builds a static Next export
- Intended hosting pattern: **S3 + CloudFront**
- Relevant note: deep-link route alias handling matters for static export refreshes

### Backend

- **Lean path:** `docker-compose.lean.yml` + optional edge/TLS file `docker-compose.lean.edge.yml`
- **Legacy/scale-up path:** ECS/Fargate task definitions under `infrastructure/aws/backend/task-definitions/`
- **Current backend deploy workflow:** `.github/workflows/backend-ci-deploy.yml` rsyncs repo to a Lightsail/EC2 host, requires `.env.lean` to already exist on-host, then runs `infrastructure/lean/deploy-lean-host.sh --remote-only`

## Database, queue, and secrets

- **Database:** PostgreSQL
- **Queue/runtime state:** Postgres outbox + runtime KV; Redis still exists in lean compose but core worker queuing was moved to Postgres-backed outbox
- **Secrets management:** repo contains AWS/Secrets Manager references for production-style assets; lean mode uses `.env.lean` on host
- **Credential encryption:** `EXCHANGE_CREDENTIALS_ENCRYPTION_KEY` required for live Coinbase secret decryption

## Domains / TLS / monitoring

- **Frontend domain:** `app.oziebot.com` (static frontend)
- **API health paths:** `/health` and `/v1/ready`; ECS service map references `/v1/ready`
- **TLS in lean mode:** Caddy via `docker-compose.lean.edge.yml` + `infrastructure/lean/Caddyfile`
- **Logs:** CloudWatch in ECS-era configs; Docker json-file rotation in lean mode

## Cost-sensitive areas

- Old ECS/Fargate/RDS/ElastiCache/ALB assets can become expensive if left active.
- `README_LEAN_MODE.md` explicitly frames lean mode as a 70–90% cost-reduction path.
- The biggest infra simplification opportunity is choosing one production story and retiring the other.

# 14. Background Workers / Scheduling

## Workers

| Service | Primary role | Main cadence / poll |
| --- | --- | --- |
| `strategy-engine` | Evaluate enabled strategies and emit signals | loop poll default `1.0s`; per-strategy intervals inside runner |
| `risk-engine` | Consume signals and approve/reject intents | queue-driven |
| `execution-engine` | Consume approved intents and execute / reconcile | queue-driven + reconciliation interval |
| `market-data-ingestor` | Coinbase market-data ingest | stream-driven + refresh logic |
| `alerts-worker` | Deliver notifications and ops alerts | queue-driven |

## Queue names

Defined in `backend/packages/py-common/src/oziebot_common/queues.py`:

- `oziebot:queue:signal_generated:{mode}`
- `oziebot:queue:intent_submitted:{mode}`
- `oziebot:queue:intent_approved:{mode}`
- `oziebot:queue:intent_rejected:{mode}`
- `oziebot:queue:alerts:{mode}`
- `oziebot:queue:alerts_retry:{mode}`
- `oziebot:queue:execution_events:{mode}`
- `oziebot:queue:execution_reconciliation:{mode}`
- `oziebot:queue:ops_alerts`

## Strategy run cadence

From `strategy-engine/runner.py`:

- `momentum`: 30s
- `day_trading`: 60s
- `dca`: 300s
- `strategic_aggressive_allocation`: 3600s
- anything else (including `reversion`): default 60s

## DCA schedule enforcement

- Runner cadence is frequent, but actual buy eligibility is enforced with `buy_interval_hours`
- Enforcement uses both runtime state and last successful DCA BUY from `execution_trades`
- Duplicate worker cycles are blocked with a short lease (`DCA_EXECUTION_LEASE_SECONDS = 120`)

## Failure/retry logic

- Postgres queue rows are leased, retried with `retry_after`, and reclaimed when leases expire
- Alerts queues are bounded to prevent unbounded growth

## Duplicate execution risks

- Stronger than average protections exist (idempotency keys, unique constraints, DCA execution lease)
- Residual risk still exists whenever multiple worker instances race, especially if reconciliation is the main recovery path instead of prevention

# 15. Performance and Efficiency Review

## Likely bottlenecks

1. High-frequency polling in the strategy runner compared with how often many strategies can actually trade.
2. Diagnostics report generation aggregates across many tables and can be expensive if requested frequently or on broad windows.
3. Token policy, diagnostics, lifecycle, and accounting each add additional writes for the same logical trade path.
4. Frontend understanding is spread across many route/API calls rather than one unified “bot state” read model.
5. Mixed infra story (ECS assets + lean host tooling) increases operational overhead and cost-review complexity.

## Specific efficiency observations

- `strategy-engine` runs every second, even though most strategies then apply longer per-strategy intervals.
- DCA is evaluated often even when it will be skipped by interval logic.
- Admin diagnostics and AI review are valuable but can become heavyweight as historical data grows.
- Multiple policy/risk/execution checks increase safety, but also increase repeated lookups and operator complexity.

# 16. Security Review

## Good controls present

- Root-admin route guard via auth dependency
- Encrypted Coinbase secrets at rest (`exchange_connections.encrypted_secret`)
- Queue names and data partitioning include trading mode
- Audit trails exist for admin actions, diagnostic actions, and many trading decisions
- Live Coinbase use requires validated credentials and trade-enabled status

## Main concerns

1. **Frontend auth tokens in localStorage**: vulnerable to XSS theft if the frontend ever gains a script injection issue.
2. **Lean mode uses `.env.lean` on host**: operationally practical, but weaker than a fully managed runtime secret model.
3. **Default settings include insecure fallbacks** in code (acceptable for dev, dangerous if env setup slips).
4. **Two deployment models** raise the chance of misconfigured environments, drift, and forgotten assets.
5. **Complex policy stack** is safer than a single gate but harder for humans to audit quickly.

# 17. Known Bugs / Risks / Gaps

## Critical

1. **Execution/accounting drift risk**: correctness depends on orders, fills, positions, and capital buckets staying aligned.
2. **Live-trading operational security**: browser-stored auth tokens + host-managed `.env.lean` + live exchange credentials create a larger real-world attack surface than a fully managed deployment.
3. **Zero quantity / zero notional remains a critical hotspot**: strong guards now exist, but this is still the most dangerous class of execution bug if future changes regress.

## High

1. **Quiet strategy trust gap**: DCA is easy to understand; momentum/day trading/reversion are still much harder for users to interpret when inactive.
2. **Fragmented observability UX**: Dashboard, Trading Diagnostics, AI Diagnostics, and Trade Log each explain part of the story, but not one coherent lifecycle.
3. **Paper/live behavior divergence**: paper mode relaxes some risk rules, so paper success can overstate live readiness.
4. **Infra duality**: ECS/Fargate assets and lean host deployment coexist, increasing drift risk.

## Medium

1. Standard strategy config UX is still control-panel-like rather than product-grade.
2. AI Diagnostic Review is strong on structure but still mostly deterministic in implementation.
3. End-user route surfaces do not always make risk controls and strategy health obvious.
4. Export/trade-history surfaces appear to be evolving and not yet fully harmonized.

## Low

1. Dark mode is fixed, not user-selectable.
2. Navigation is rich but can feel crowded on small screens.
3. Admin terminology and user terminology are not always aligned.

# 18. Recommended Improvement Areas

Do **not** treat these as implementation instructions; they are review targets.

## UI

- Build one unified “why traded / why did not trade / is healthy” strategy state view for end users.
- Replace raw/loose standard strategy config editing with schema-driven forms and clearer validation.
- Surface reconciliation health and policy/risk status in user-facing pages, not just admin pages.

## Trading strategy / execution

- Continue tightening correctness tests around quantity/notional rounding, fee/slippage, and partial fills.
- Make quiet-strategy explanations explicit in-product, not only in admin diagnostics.
- Review whether paper-mode relaxed rules should be more configurable or more visible.

## Data-flow efficiency

- Reconsider runner poll frequency vs. actual strategy cadence.
- Build more focused read models for dashboard/strategy health instead of repeated multi-source aggregation.

## Diagnostics / observability

- Promote lifecycle traces into a simpler UI.
- Keep expanding deterministic findings, but separate “finding generation” from “AI narrative” clearly.

## AWS / cost

- Pick a primary production topology and retire the secondary one.
- Continue simplifying around lean mode if that is the intended operating model.

## Safety controls

- Harden browser/session storage strategy.
- Reduce manual secret handling on hosts where possible.
- Keep execution validation and reconciliation under strong regression coverage.

# 19. Questions for Human Owner

1. Is Oziebot intended to remain primarily **single-owner/personal** despite the multi-tenant schema?
2. What is the target **live trading readiness** threshold before broader use?
3. What is the acceptable monthly **AWS / infrastructure budget**?
4. Should paper mode intentionally remain more permissive than live mode, or should it become closer to live behavior?
5. What cadence do you actually want for momentum, day trading, and reversion in production?
6. Which token set is the real intended operating universe vs. experimental tokens?
7. Is the long-term UI direction closer to “operator console” or “consumer trading product”?
8. Should blocked token-policy pairs always allow exit-only execution, or do you want stricter unwinding semantics?

# 20. Files Most Important for Review

| File | Why it matters |
| --- | --- |
| `backend/services/strategy-engine/src/oziebot_strategy_engine/runner.py` | Central orchestration point for strategy cadence, symbol resolution, DCA interval enforcement, mode handling, lifecycle emission. |
| `backend/services/execution-engine/src/oziebot_execution_engine/service.py` | Most important correctness file for execution, validation, idempotency, fills, positions, accounting, and policy re-checks. |
| `backend/services/risk-engine/src/oziebot_risk_engine/service.py` | Central risk evaluation path and paper/live rule divergence. |
| `backend/services/risk-engine/src/oziebot_risk_engine/rules.py` | Actual risk control definitions and likely bypass/strictness points. |
| `backend/packages/py-common/src/oziebot_common/token_policy.py` | Shared token-policy semantics used across diagnostics, risk, and execution. |
| `backend/services/api/src/oziebot_api/services/token_policy.py` | API/admin view of token policy, defaults, matrix export, user matrix. |
| `backend/services/api/src/oziebot_api/services/admin_trading_diagnostics.py` | Main diagnostics JSON builder and one of the best “what is the platform doing?” files. |
| `backend/services/api/src/oziebot_api/services/admin_ai_diagnostics.py` | AI Diagnostic Review logic, rule-based findings, future auto-tune hooks. |
| `backend/services/api/src/oziebot_api/models/execution.py` | Core order/fill/trade/position schema. |
| `backend/services/api/src/oziebot_api/models/strategy_allocation.py` | Capital bucket and ledger model; essential for buying power and PnL interpretation. |
| `backend/services/api/src/oziebot_api/models/ai_diagnostics.py` | AI review and finding persistence model. |
| `backend/services/api/src/oziebot_api/api/v1/me.py` | Largest API surface; dashboard and analytics behavior lives here. |
| `backend/services/api/src/oziebot_api/api/v1/strategic_aggressive_allocation.py` | Dedicated new strategy API surface and a good example of isolated pluggable strategy design. |
| `backend/services/market-data-ingestor/src/oziebot_market_data_ingestor/postgres_market_cache.py` | Candle/BBO/trade runtime state quality directly affects quiet-strategy behavior. |
| `frontend/apps/web/app/dashboard/page.tsx` | Primary user trust surface. |
| `frontend/apps/web/app/strategies/page.tsx` | Standard strategy UX, per-strategy token selection, and current config ergonomics. |
| `frontend/apps/web/app/strategic-allocation/page.tsx` | Most complete modern strategy-config UX in the product. |
| `frontend/apps/web/app/tokens/page.tsx` | Global token controls and token-policy visibility. |
| `frontend/apps/web/components/providers/auth-provider.tsx` | Frontend auth/session model and route gating. |
| `frontend/apps/web/components/providers/trading-mode-provider.tsx` | Paper/live mode UX and routing behavior. |
| `.github/workflows/backend-ci-deploy.yml` | Current backend CI/deploy path to lean host. |
| `infrastructure/lean/deploy-lean-host.sh` | The actual remote deploy behavior, including compose teardown/rebuild semantics. |
| `README_LEAN_MODE.md` | Best current summary of the active lean hosting model and cost posture. |

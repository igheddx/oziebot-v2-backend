# Oziebot System Dump

This dump extracts only trading-relevant configuration, behavior, and runtime enforcement from the checked-in codebase.

**Scope**
- Strategies: `momentum`, `reversion` (mean reversion), `day_trading`, `dca`
- Services: strategy-engine, risk-engine, execution-engine, allocation/capital services
- Data sources: code only

**Unavailable from this environment**
- Actual live/current `platform_strategies.config_schema` rows in PostgreSQL
- Actual live/current allowlisted tokens in PostgreSQL
- Actual production trade counts, win rate, drawdown, and signal rejection rates

The local API settings resolved to no usable DB connection for runtime extraction, so anything that depends on current database rows is marked **unavailable** instead of guessed.

---

## 1. Strategy Configuration

### 1.1 Momentum

**Code**
- `backend/services/strategy-engine/src/oziebot_strategy_engine/strategies/momentum.py`
- Entry/exit orchestration from `MomentumStrategy.generate_signal()` and `_check_exit()`

| Item | Value | Enforcement |
| --- | --- | --- |
| Strategy cadence | `30s` | `runner.py:STRATEGY_INTERVAL_SECONDS` |
| Entry indicators | short/long moving averages from `candle_closes` | `momentum.py` |
| Entry formula | `momentum = (short_ma - long_ma) / long_ma` | `momentum.py` |
| Entry threshold | schema default `0.02`; generate path fallback `0.015` | `momentum.py` |
| Short window | schema default `5`; generate path fallback `8` | `momentum.py` |
| Long window | schema default `20`; generate path fallback `34` | `momentum.py` |
| Buy condition | `momentum > strength_threshold` and no open position | `MomentumStrategy.generate_signal()` |
| Buy confidence | `0.7` | `MomentumStrategy._buy_signal()` |
| Position size fraction | `0.1` default | signal metadata -> runner sizing |
| Stop loss | `0.03` | `_check_exit()` |
| Take profit | `0.06` | `_check_exit()` |
| Trailing stop | `0.025` | `_check_exit()` |
| Max hold time | `240m` | `_check_exit()` |
| Dynamic exit | bearish MA reversal when `momentum < -strength_threshold` | `_check_exit()` |

**Weak area**
- Declared/schema defaults and runtime fallback defaults diverge for `short_window`, `long_window`, and `strength_threshold`. The schema says `5/20/0.02`; signal generation fallback uses `8/34/0.015`.

### 1.2 Reversion / Mean Reversion

**Code**
- `backend/services/strategy-engine/src/oziebot_strategy_engine/strategies/reversion.py`

| Item | Value | Enforcement |
| --- | --- | --- |
| Strategy cadence | default `60s` | `runner.py` uses `.get(strategy_name, 60)` |
| Band window | `20` | `ReversionStrategy.generate_signal()` |
| RSI period | `14` | same |
| Entry z-score | `1.8` | same |
| Exit z-score | `0.35` | `_check_exit()` |
| RSI buy threshold | `32` | entry gate |
| RSI exit threshold | `52` | exit gate |
| RSI sell threshold | `68` | exit gate |
| Min bandwidth | `0.015` | entry gate |
| Position size fraction | `0.08` | signal metadata -> runner sizing |
| Stop loss | `0.025` | `_check_exit()` |
| Take profit | `0.045` | `_check_exit()` |
| Max hold time | `180m` | `_check_exit()` |
| Optional fear filter | `False` by default, `fear_index_buy_max=35`, `fear_index_sell_min=60` | entry/exit gates |
| Optional trend filter | `False` by default, `ema_long_window=200` | entry gate |

**Entry logic**
- Compute z-score from rolling mean/stddev
- Compute RSI from closes
- Require `zscore <= -1.8`
- Require `rsi <= 32`
- Require bandwidth >= `1.5%`
- Optional fear/trend filters if enabled
- Confidence: `min(0.9, 0.65 + abs(zscore)*0.05 + max(0, (50-rsi)/100))`

**Exit logic**
- Stop loss at `-2.5%`
- Take profit at `+4.5%`
- Mean-reversion exit when `abs(zscore) <= 0.35` and `rsi >= 52`
- Overbought bounce exit when `zscore > 0` and `rsi >= 68`
- Max hold `180m`

### 1.3 Day Trading

**Code**
- `backend/services/strategy-engine/src/oziebot_strategy_engine/strategies/day_trading.py`
- Position age guard additionally enforced in execution: `execution-engine/service.py:_enforce_day_trading_position_age()`

| Item | Value | Enforcement |
| --- | --- | --- |
| Strategy cadence | `60s` | `runner.py:STRATEGY_INTERVAL_SECONDS` |
| Entry threshold | schema default `0.01`; generate fallback `0.008` | `day_trading.py` |
| Exit threshold | schema default `0.02`; generate fallback `0.015` | `day_trading.py` |
| Stop loss | schema default `0.01`; generate fallback `0.007` | `day_trading.py` |
| Breakout lookback | `5` candles | entry confirmations |
| Require trend alignment | `True` | EMA9 > EMA21 |
| Min volume multiplier | `1.5x` | entry confirmations |
| Min volatility pct | `0.006` | entry confirmations |
| Min entry confirmations | `2` | entry confirmations |
| Max position age | `4h` | **execution-engine**, not strategy class |
| Position size fraction | hard-coded `0.1` | signal metadata |

**Entry logic**
- Uses up to `390` candles for session range
- Needs price near session low
- Confirmation pool:
  - latest volume >= `1.5x` 20-candle average
  - EMA9 > EMA21 if trend alignment enabled
  - price >= recent breakout high over `5` candles
  - 10-candle volatility >= `0.6%`
- Needs at least `2` confirmations
- Confidence: `min(0.95, 0.55 + confirmation_count*0.1)`

**Exit logic**
- Profit exit at `+1.5%` fallback
- Stop exit at `-0.7%` fallback
- Separate execution-side forced close if held longer than `max_position_age_hours`

**Weak area**
- `max_position_age_hours` is not enforced inside the strategy class; it is enforced later in execution only.
- Schema defaults differ from runtime generate fallbacks.

### 1.4 DCA

**Code**
- `backend/services/strategy-engine/src/oziebot_strategy_engine/strategies/dca.py`
- Scheduler enforcement in `runner.py:_scheduler_reason()`

| Item | Value | Enforcement |
| --- | --- | --- |
| Strategy cadence | runner check every `300s` | `STRATEGY_INTERVAL_SECONDS` |
| Buy amount USD | `100` | signal metadata -> runner sizing |
| Buy interval hours | `24` | runner scheduler, not strategy class |
| Only on green days | `False` | `DCAStrategy.generate_signal()` |
| Confidence | `0.9` | DCA buy signal |
| Exit logic | none | buy-and-hold accumulation |

**Entry logic**
- If `only_on_green_days=true`, skip when `close_price <= open_price`
- Otherwise emits buy each eligible cycle
- Actual cycle timing enforced by runner using `last_buy_at + buy_interval_hours`

**Weak area**
- `buy_interval_hours` is declared in strategy config but enforced outside the strategy class.

### 1.5 Shared signal rules and throttles

**Code**
- `backend/services/strategy-engine/src/oziebot_strategy_engine/runner.py:_suppression_reason()`

| Rule | Default if absent | Enforcement |
| --- | --- | --- |
| `paper_only` | `False` | reject live signal |
| `min_confidence` | `0` | reject below threshold |
| `require_volume_confirmation` | `False` | reject if `market.volume_24h <= 0` |
| `only_during_liquid_hours` | `False` | UTC hour must be `13 <= hour < 22` |
| `cooldown_seconds` | `0` | reject if last action signal too recent |
| `max_signals_per_day` | `0` | reject once daily count reached |
| `risk_caps.max_open_positions` | `0` | reject new buys over cap |
| `risk_caps.max_position_usd` | `0` | reject if current + next notional exceeds cap |
| `risk_caps.max_daily_loss_pct` | `0` | reject buys if daily realized loss pct reached |

### 1.6 Strategy-side sizing conversion

**Code**
- `backend/services/strategy-engine/src/oziebot_strategy_engine/runner.py:_to_signal_event()`

| Input source | Formula |
| --- | --- |
| explicit signal quantity | use as-is |
| close/sell with open position | use full open quantity |
| `buy_amount_usd` metadata | `suggested_size = (buy_amount_usd * confidence) / current_price` |
| `position_size_fraction` + `risk_caps.max_position_usd` | `suggested_size = (fraction * max_position_usd * confidence) / current_price` |
| fallback if no `max_position_usd` | `fraction * confidence` |

**Profit leak**
- If `risk_caps.max_position_usd` is unset, fraction-based strategies fall back to a raw fraction as quantity, not USD-normalized size.

---

## 2. Risk Engine Rules

**Code**
- `backend/services/risk-engine/src/oziebot_risk_engine/config.py`
- `backend/services/risk-engine/src/oziebot_risk_engine/rules.py`
- `backend/services/risk-engine/src/oziebot_risk_engine/service.py`

### 2.1 Global defaults

| Setting | Value |
| --- | --- |
| `risk_max_per_trade_risk_pct` | `0.2` |
| `risk_max_position_size_cents` | `200000` |
| `risk_max_strategy_allocation_pct` | `1.0` |
| `risk_max_token_concentration_pct` | `0.5` |
| `risk_max_daily_loss_cents` | `50000` |
| `risk_cooldown_loss_count` | `3` |
| `risk_cooldown_minutes` | `60` |
| `risk_stale_trade_seconds` | `20` |
| `risk_stale_bbo_seconds` | `15` |
| `risk_stale_candle_seconds` | `180` |
| `risk_max_spread_pct` | `0.01` |
| `risk_max_slippage_pct` | `0.02` |
| `risk_relaxed_paper_rules` | `max_daily_loss,cooldown_after_losses` |

### 2.2 Rule order

Exact order from `default_rules(settings)`:
1. `platform_pause`
2. `subscription_entitlement`
3. `token_allowlist`
4. `user_token_enabled`
5. `strategy_enabled`
6. `token_strategy_policy`
7. `token_strategy_discouraged`
8. `capital_bucket`
9. `max_per_trade_risk`
10. `max_position_size`
11. `max_strategy_allocation`
12. `max_token_concentration`
13. `token_strategy_position_override`
14. `max_strategy_exposure`
15. `max_token_exposure`
16. `max_daily_loss`
17. `global_daily_loss_guard`
18. `cooldown_after_losses`
19. `stale_data`
20. `execution_quality`

### 2.3 Exact enforced behavior

| Rule | Behavior | Code |
| --- | --- | --- |
| Platform pause | reject all trades if `platform_settings['trading.global.pause']` paused | `PlatformPauseRule` |
| Subscription entitlement | reject if tenant lacks active entitlement; paper may bypass if `billing.allow_paper_without_subscription` enabled | `SubscriptionEntitlementRule`, `_load_facts()` |
| Token allowlist | reject if `platform_token_allowlist.is_enabled=false` | `TokenAllowlistRule` |
| User token permission | reject if `user_token_permissions.is_enabled=false` | `UserTokenRule` |
| Strategy enabled | reject if `user_strategies.is_enabled=false` | `StrategyEnabledRule` |
| Token policy blocked/admin disabled | reject | `TokenStrategyPolicyRule` |
| Token policy discouraged | reduce size by policy multiplier, currently `0.5` | `DiscouragedTokenPolicySizingRule` |
| Capital bucket | reject/reduce if buying power insufficient | `CapitalBucketRule` |
| Per-trade risk | reduce if notional > `available_buying_power_cents * 0.2` | `MaxPerTradeRiskRule` |
| Max position size | reduce/reject if locked + requested > `200000` cents | `MaxPositionSizeRule` |
| Max strategy allocation | reduce if notional > `assigned_capital_cents * 1.0` | `MaxStrategyAllocationRule` |
| Max token concentration | reduce if requested notional / total capital > `0.5` | `MaxTokenConcentrationRule` |
| Token policy max position override | reduce/reject against strategy-token cap | `TokenStrategyPositionOverrideRule` |
| Strategy exposure cap | reduce/reject if projected exposure > configured cap | `MaxStrategyExposureRule` |
| Token exposure cap | reduce/reject if projected total token exposure > configured cap | `MaxTokenExposureRule` |
| Daily loss | reject if realized daily loss cents >= `50000` | `MaxDailyLossRule` |
| Global daily loss guard | reject if configured percent threshold reached | `GlobalDailyLossGuardRule` |
| Cooldown after losses | reject if consecutive-loss cooldown active | `CooldownAfterLossesRule` |
| Stale data | reject if any trade/BBO/candle timestamp stale | `StaleDataRule` |
| Execution quality | reject on spread, slippage, or fee-vs-profit-buffer violation | `ExecutionQualityRule` |

### 2.4 Paper vs live differences

In `RiskEngineService.evaluate()`:
- paper mode skips `max_daily_loss`
- paper mode skips `cooldown_after_losses`
- all other rules still run

### 2.5 Where risk inputs come from

`RiskEngineService._load_facts()` loads:
- BBO from Redis `oziebot:md:bbo:{symbol}`
- stale timestamps from Redis
- `platform_settings`
- `user_strategies`
- `platform_strategies`
- `platform_token_allowlist`
- `token_strategy_policy`
- `user_token_permissions`
- `strategy_capital_buckets`
- `execution_positions`
- `strategy_capital_ledger`

### 2.6 Weak or restrictive areas

- **Binary stale-data rejection**: no degraded mode; any stale flag blocks.
- **Absolute daily loss cap**: `50000` cents is static, not scaled by account size.
- **Exposure caps may effectively be disabled** unless DB config populates nonzero caps.
- **Cooldown logic depends on recent settled losses**, not broader drawdown state.
- **Token policy position override is strategy-token scoped**, not global cross-strategy token scoped.

---

## 3. Token Universe

### 3.1 Storage and gating

**Code**
- `backend/services/api/src/oziebot_api/models/platform_token.py`
- `backend/services/api/src/oziebot_api/models/user_token_permission.py`
- `backend/services/strategy-engine/src/oziebot_strategy_engine/runner.py:_load_allowed_symbols()`

Tradable token requires both:
1. `platform_token_allowlist.is_enabled = true`
2. `user_token_permissions.is_enabled = true`

### 3.2 Token-strategy eligibility

**Code**
- `backend/services/api/src/oziebot_api/models/token_strategy_policy.py`
- `backend/packages/py-common/src/oziebot_common/token_policy.py`

Per `(token_id, strategy_id)` policy stores:
- `admin_enabled`
- `suitability_score`
- `recommendation_status`
- `recommendation_reason`
- `recommendation_status_override`
- `recommendation_reason_override`
- `max_position_pct_override`

Effective resolution:
- no policy row -> allowed, multiplier `1.0`
- `admin_enabled=false` -> blocked, multiplier `0`
- `blocked` -> multiplier `0`
- `discouraged` -> multiplier `0.5`
- `allowed` / `preferred` -> multiplier `1.0`

### 3.3 Current token list

**Actual current allowlisted tokens**: **unavailable from this environment**

**Only checked-in seed examples**
- `BTC-USD`
- `ETH-USD`

Source: `backend/services/api/src/oziebot_api/scripts/seed_platform_catalog.py`

### 3.4 Filtering before strategies run

Order in strategy engine:
1. user enabled strategy loaded
2. allowed symbols loaded from token permission join
3. market snapshot loaded
4. token strategy policy loaded
5. blocked/admin-disabled token-strategy combinations suppressed before signal emission

---

## 4. Trade Generation Flow

**Code**
- `backend/services/strategy-engine/src/oziebot_strategy_engine/runner.py:run_once()`

### 4.1 Runtime flow

`user strategy -> allowed symbols -> market snapshot -> token policy -> strategy signal generation -> suppression rules -> strategy_signal event -> risk evaluation -> trade_intent -> execution request -> adapter`

### 4.2 How often strategies run

| Strategy | Interval |
| --- | --- |
| momentum | `30s` |
| reversion | `60s` default |
| day_trading | `60s` |
| dca | `300s` scheduler check |

### 4.3 How many tokens per cycle

Depends on:
- count of enabled `user_strategies`
 - count of joined enabled token permissions per user
- two trading modes per strategy-symbol: `paper` and `live`

**Actual production token count per cycle**: **unavailable**

### 4.4 When signals are emitted

Signal is emitted only if all of the following pass:
- schedule due
- token policy not blocked/admin-disabled
- strategy returns non-suppressed signal
- shared signal rules pass
- risk caps in strategy runner pass

Then runner persists:
- `strategy_runs`
- `strategy_signals`
- pushes to Redis queue `QueueNames.signal_generated(mode)`

### 4.5 Conditions that drop signals

Strategy stage drop reasons from runner:
- `token strategy disabled by admin`
- `token strategy blocked: ...`
- `dca interval active until ...`
- `paper_only strategy`
- `below min_confidence`
- `volume confirmation failed`
- `outside liquid-hours window`
- `cooldown active`
- `max_signals_per_day reached`
- `max_open_positions reached`
- `max_position_usd exceeded`
- `max_daily_loss_pct reached`

### 4.6 Percentage of signals rejected

**Tracked in tables**: enough data exists in `strategy_runs`, `strategy_signals`, and `risk_events`

**Actual current percentages**: **unavailable from this environment**

---

## 5. Execution Details

**Code**
- `backend/services/execution-engine/src/oziebot_execution_engine/service.py`
- `backend/services/execution-engine/src/oziebot_execution_engine/adapters.py`
- `backend/services/execution-engine/src/oziebot_execution_engine/coinbase_client.py`

### 5.1 How orders are placed

1. Risk engine converts approved signal to `TradeIntent` with `OrderType.MARKET`
2. Execution service builds `ExecutionRequest`
3. For buys, execution reapplies token strategy policy
4. Capital is reserved/locked
5. Adapter submits:
   - paper -> simulated immediate fill
   - live -> Coinbase Advanced Trade API

### 5.2 Price determination

`ExecutionService._market_price_hint()`:
- buy uses `best_ask_price`
- sell uses `best_bid_price`
- source: Redis BBO

Paper adapter:
- buy fill base price = `best_ask_price`
- sell fill base price = `best_bid_price`
- applies slippage

Live adapter:
- sends Coinbase market IOC payload
- venue determines final execution price

### 5.3 Spread, slippage, fee assumptions

| Item | Paper | Live |
| --- | --- | --- |
| Spread used in price pick | yes, via ask/bid side selection | yes, indirectly through venue market execution |
| Explicit slippage | `8 bps` default | none applied locally |
| Explicit fee | `15 bps` default | stored as `0` in current Coinbase client path |
| Price hint required for token-policy cap | yes | yes, same execution code |

### 5.4 Paper vs live differences

**Paper**
- immediate `FILLED`
- slippage applied
- fees applied
- fails if no executable market data

**Live**
- submits to Coinbase
- may remain `PENDING`
- no explicit local slippage model
- current fill fee extraction is missing; live fills default to zero-fee in this code path

### 5.5 Execution/sizing token policy enforcement

`ExecutionService._apply_token_strategy_policy()`:
- only runs for `BUY`
- reject if admin-disabled
- reject if blocked
- reduce quantity by multiplier for discouraged
- cap quantity via `max_position_pct_override`
- record proof in `intent_payload.metadata.token_policy_execution`

### 5.6 Day-trading execution guard

`_enforce_day_trading_position_age()`:
- loads `max_position_age_hours` from config
- reads runtime state `opened_at`
- if held longer than allowed, creates forced market sell request

### 5.7 Weak areas and profit leaks

- **Live fee handling missing**: Coinbase fill path stores fee as zero.
- **Limit orders not implemented** in execution adapters despite order type support in domain.
- **Price normalization depends on Redis BBO**; missing hint can break token-policy position override.

---

## 6. Current Performance

### 6.1 Available metrics in code/model layer

Backtesting tables store:
- `total_trades`
- `win_rate`
- `avg_return_bps`
- `max_drawdown`
- `sharpe_like`
- `avg_slippage_bps`
- `fee_impact_cents`
- `avg_holding_seconds`

Source: `backend/services/api/src/oziebot_api/models/backtesting.py`

Execution/runtime tables also store:
- order/fill/trade history
- realized PnL
- positions
- capital ledger events

### 6.2 Actual current live metrics

| Metric | Status |
| --- | --- |
| Number of trades generated | unavailable |
| Win rate | unavailable |
| Average win % | unavailable |
| Average loss % | unavailable |
| Max drawdown | unavailable |
| Distribution by strategy | unavailable |

Reason: current DB/runtime state could not be queried from this environment.

---

## 7. Capital Allocation

**Code**
- `backend/services/api/src/oziebot_api/models/strategy_allocation.py`
- `backend/services/api/src/oziebot_api/services/strategy_allocation.py`

### 7.1 Capital split presets

`PRESET_WEIGHTS`:

| Preset | dca | momentum | day_trading |
| --- | --- | --- | --- |
| conservative | `6000` bps | `2500` bps | `1500` bps |
| balanced | `4000` bps | `3500` bps | `2500` bps |
| aggressive | `2500` bps | `3000` bps | `4500` bps |

`reversion` is not included in preset weights.

### 7.2 Capital bucket fields

Per `(user, strategy, trading_mode)`:
- `assigned_capital_cents`
- `available_cash_cents`
- `reserved_cash_cents`
- `locked_capital_cents`
- `realized_pnl_cents`
- `unrealized_pnl_cents`
- `available_buying_power_cents`

### 7.3 Bucket mechanics

| Step | Effect |
| --- | --- |
| reserve | available -> reserved |
| lock | reserved -> locked |
| settle | locked released, realized PnL added back to available |
| release | reserved returned to available |
| mark_unrealized | only updates unrealized field |

Buying power recompute:
- no leverage
- `available_buying_power_cents = max(0, available_cash_cents)`

### 7.4 Rebalancing

- manual / API-driven only via `apply_allocations()`
- no automatic rebalancing loop found

### 7.5 Unused capital tracking

Tracked implicitly as:
- `available_cash_cents`
- `available_buying_power_cents`

### 7.6 Weak areas

- `unrealized_pnl_cents` has storage and ledger support, but no complete live mark-to-market pipeline was found in the audited runtime path.
- Presets exclude `reversion`, so preset-guided allocation does not automatically weight that strategy.

---

## 8. Missing or Non-Enforced Logic

| Item | Status | Impact |
| --- | --- | --- |
| Momentum schema defaults vs generate defaults | inconsistent | runtime may not match admin expectation |
| Day trading schema defaults vs generate defaults | inconsistent | runtime may not match admin expectation |
| `day_trading.max_position_age_hours` | enforced in execution, not strategy | split-brain control point |
| `dca.buy_interval_hours` | enforced in runner, not strategy class | split-brain control point |
| Fraction sizing without `max_position_usd` | falls back to raw quantity fraction | can distort sizing |
| Live Coinbase fees | not extracted in current path | live PnL overstated |
| Limit order support | not implemented in adapters | config/domain surface exceeds runtime |
| Actual current runtime metrics | unavailable here | optimization cannot be based on current observed stats yet |

---

## 9. Optimization-Relevant Weak Areas

### Overly restrictive
- stale data rejection is binary
- fixed `50000` cent daily loss cap can be too tight for larger accounts and too loose for smaller ones
- paper mode bypasses some loss protection, so paper/live behavior diverges

### Potential profit leaks
- live fees missing from execution accounting
- spread/slippage protection in live relies mostly on pre-trade estimates and venue behavior
- fraction sizing fallback without `max_position_usd`
- schema/runtime default mismatches can cause unnoticed configuration drift

### Missing controls
- no extracted live rejection-rate dashboard from this environment
- no extracted live per-strategy Sharpe/win-rate/drawdown snapshot from this environment
- no automatic allocation rebalance loop

---

## 10. End-to-End Enforcement Map

1. **Strategy engine**
   - loads allowed symbols via token allowlist + user token permission
   - loads `token_strategy_policy`
   - suppresses blocked/admin-disabled token-strategy pairs before signal emission
   - annotates signal metadata with effective token policy

2. **Risk engine**
   - reloads token policy and exposure facts
   - rejects blocked/admin-disabled trades
   - reduces discouraged trades
   - enforces `max_position_pct_override`

3. **Execution engine**
   - rebuilds execution request from approved risk decision
   - rechecks token policy for buys
   - rejects/reduces/caps size again before adapter submission

4. **Persistence for audit**
   - `strategy_runs`
   - `strategy_signals`
   - `risk_events`
   - `execution_orders`
   - `execution_fills`
   - `execution_positions`
   - `strategy_capital_ledger`


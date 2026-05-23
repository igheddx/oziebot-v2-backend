# Volatility Harvest Strategy

`strategy_id = volatility_harvest`

## Goal

Build long-term conviction positions while rotating only the trading slice of each token:

- keep a protected core bag
- harvest partial profits during volatility expansions
- hold harvested cash for staged pullback rebuys
- grow net token ownership over time

## Architecture

The strategy is implemented as a fully separate plug-in and does **not** reuse the momentum, day trading, or DCA logic paths.

### Backend surfaces

- strategy engine module: `backend/services/strategy-engine/src/oziebot_strategy_engine/strategies/volatility_harvest.py`
- API service/router: `backend/services/api/src/oziebot_api/services/volatility_harvest.py`
- API router: `backend/services/api/src/oziebot_api/api/v1/volatility_harvest.py`
- persistence:
  - `volatility_harvest_config`
  - `volatility_harvest_positions`
  - `volatility_harvest_transactions`
  - `volatility_harvest_metrics`

### Queue isolation

Volatility Harvest publishes to dedicated strategy-specific signal and intent queues while still flowing through the shared risk and execution engines:

- `oziebot:queue:signal_generated:{mode}:volatility_harvest`
- `oziebot:queue:intent_approved:{mode}:volatility_harvest`

This keeps the pipeline modular without introducing a second execution engine.

### Runtime state

Mutable per-symbol state stays in `user_strategy_states` for low-latency strategy decisions:

- core quantity vs trading quantity
- completed entry layers
- completed harvest bands
- completed rebuy bands
- harvested cash balance
- last local high
- pending harvest/rebuy action
- token accumulation delta

Dedicated volatility-harvest tables are derived snapshots for reporting, metrics, and UI reads.

## Default behavior

- core position: `70%`
- trading position: `30%`
- layered entries:
  - `40%` immediate
  - `30%` on `-5%` pullback
  - `30%` on `-10%` pullback
- harvest bands:
  - `+5% / sell 15%`
  - `+10% / sell 20%`
  - `+15% / sell 25%`
  - `+20% / sell 25%`
- rebuy bands:
  - `-5% / deploy 35% cash`
  - `-8% / deploy 35% cash`
  - `-12% / deploy 30% cash`

## Fee-aware controls

Harvests are blocked unless expected profit clears fee + slippage + spread assumptions:

- Coinbase fee bps
- slippage bps
- spread buffer bps
- minimum net profit after fees

## Risk controls

- max allocation per token
- daily max sell count
- daily max rebuy count
- cooldown between actions
- spread cap
- emergency stop-loss
- BTC regime gating for rebuys
- ATR-based band widening

## Example AERO configuration

```json
{
  "trading_mode": "paper",
  "enabled": true,
  "selected_tokens": ["AERO-USD"],
  "total_allocated_amount_usd": {
    "target": 1000,
    "source": "manual"
  },
  "core_position_percentage": 70,
  "trading_position_percentage": 30,
  "entry_layers": [
    { "id": "entry_layer_1", "allocation_pct": 40, "pullback_pct": 0 },
    { "id": "entry_layer_2", "allocation_pct": 30, "pullback_pct": 5 },
    { "id": "entry_layer_3", "allocation_pct": 30, "pullback_pct": 10 }
  ],
  "harvest_bands": [
    { "id": "harvest_1", "trigger_pct": 5, "sell_pct": 15 },
    { "id": "harvest_2", "trigger_pct": 10, "sell_pct": 20 },
    { "id": "harvest_3", "trigger_pct": 15, "sell_pct": 25 },
    { "id": "harvest_4", "trigger_pct": 20, "sell_pct": 25 }
  ],
  "rebuy_bands": [
    { "id": "rebuy_1", "trigger_pct": 5, "deploy_cash_pct": 35 },
    { "id": "rebuy_2", "trigger_pct": 8, "deploy_cash_pct": 35 },
    { "id": "rebuy_3", "trigger_pct": 12, "deploy_cash_pct": 30 }
  ]
}
```

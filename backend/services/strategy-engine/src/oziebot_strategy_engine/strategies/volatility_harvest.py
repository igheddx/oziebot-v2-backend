"""Volatility harvest strategy."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.trading import Instrument, OrderType, Quantity, Side
from oziebot_strategy_engine.strategy import (
    MarketSnapshot,
    PositionState,
    StrategyContext,
    TradingStrategy,
)

STRATEGY_ID = "volatility_harvest"

DEFAULT_ENTRY_LAYERS = [
    {"id": "entry_layer_1", "allocation_pct": 40.0, "pullback_pct": 0.0},
    {"id": "entry_layer_2", "allocation_pct": 30.0, "pullback_pct": 5.0},
    {"id": "entry_layer_3", "allocation_pct": 30.0, "pullback_pct": 10.0},
]

DEFAULT_HARVEST_BANDS = [
    {"id": "harvest_1", "trigger_pct": 5.0, "sell_pct": 15.0},
    {"id": "harvest_2", "trigger_pct": 10.0, "sell_pct": 20.0},
    {"id": "harvest_3", "trigger_pct": 15.0, "sell_pct": 25.0},
    {"id": "harvest_4", "trigger_pct": 20.0, "sell_pct": 25.0},
]

DEFAULT_REBUY_BANDS = [
    {"id": "rebuy_1", "trigger_pct": 5.0, "deploy_cash_pct": 35.0},
    {"id": "rebuy_2", "trigger_pct": 8.0, "deploy_cash_pct": 35.0},
    {"id": "rebuy_3", "trigger_pct": 12.0, "deploy_cash_pct": 30.0},
]

DEFAULT_VOLATILITY_SETTINGS = {
    "atr_period": 14,
    "atr_reference_pct": 0.035,
    "atr_band_widening_multiplier": 1.35,
    "minimum_atr_pct_to_trade": 0.01,
    "rsi_period": 14,
    "rsi_rebuy_threshold": 35.0,
    "require_rsi_confirmation_for_rebuy": False,
}

DEFAULT_RISK_CONTROLS = {
    "max_allocation_per_token_pct": 22.5,
    "daily_max_sell_count": 4,
    "daily_max_rebuy_count": 3,
    "cooldown_minutes_between_actions": 90,
    "max_spread_bps": 60,
    "minimum_profit_spread_bps": 80,
    "minimum_net_profit_after_fees_usd": 3.0,
    "emergency_stop_loss_pct": 18.0,
    "suspend_rebuys_on_btc_breakdown": True,
    "btc_breakdown_return_pct": -7.0,
    "btc_abnormal_atr_pct": 8.0,
}

DEFAULT_FEE_SETTINGS = {
    "coinbase_fee_bps": 60,
    "slippage_bps": 12,
    "spread_buffer_bps": 10,
}

DEFAULT_MODE_SETTINGS = {
    "evaluation_interval_minutes": 30,
    "minimum_order_size_usd": 25.0,
}


def default_strategy_config() -> dict[str, object]:
    return {
        "strategy_type": STRATEGY_ID,
        "trading_mode": "paper",
        "enabled": False,
        "selected_tokens": [],
        "total_allocated_amount_usd": {
            "target": 0.0,
            "source": "allocation_plan",
        },
        "core_position_percentage": 70.0,
        "trading_position_percentage": 30.0,
        "entry_layers": deepcopy(DEFAULT_ENTRY_LAYERS),
        "harvest_bands": deepcopy(DEFAULT_HARVEST_BANDS),
        "rebuy_bands": deepcopy(DEFAULT_REBUY_BANDS),
        "volatility_settings": deepcopy(DEFAULT_VOLATILITY_SETTINGS),
        "risk_controls": deepcopy(DEFAULT_RISK_CONTROLS),
        "fee_settings": deepcopy(DEFAULT_FEE_SETTINGS),
        "mode_settings": deepcopy(DEFAULT_MODE_SETTINGS),
        "example_config_symbol": "AERO-USD",
    }


def normalize_strategy_config(config: dict | None) -> dict[str, object]:
    normalized = default_strategy_config()
    raw = dict(config or {})
    for key in (
        "trading_mode",
        "enabled",
        "selected_tokens",
        "total_allocated_amount_usd",
        "core_position_percentage",
        "trading_position_percentage",
    ):
        if key in raw and raw[key] is not None:
            normalized[key] = raw[key]
    for key in (
        "volatility_settings",
        "risk_controls",
        "fee_settings",
        "mode_settings",
    ):
        if isinstance(raw.get(key), dict):
            normalized[key].update(raw[key])  # type: ignore[index]
    for key in ("entry_layers", "harvest_bands", "rebuy_bands"):
        if isinstance(raw.get(key), list) and raw[key]:
            normalized[key] = list(raw[key])
    normalized["selected_tokens"] = [
        str(symbol).upper()
        for symbol in (normalized.get("selected_tokens") or [])
        if symbol
    ]
    normalized["strategy_type"] = STRATEGY_ID
    return normalized


def selected_trade_symbols(config: dict | None) -> list[str]:
    normalized = normalize_strategy_config(config)
    seen: set[str] = set()
    symbols: list[str] = []
    for symbol in normalized.get("selected_tokens") or []:
        upper = str(symbol).upper()
        if upper and upper not in seen:
            seen.add(upper)
            symbols.append(upper)
    return symbols


def _to_decimal(value: object, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _pct(value: object) -> Decimal:
    return _to_decimal(value) / Decimal("100")


def _compute_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for index in range(-period, 0):
        delta = closes[index] - closes[index - 1]
        if delta > 0:
            gains += delta
        elif delta < 0:
            losses += abs(delta)
    if losses == 0:
        return 100.0
    rs = gains / losses if losses else 0.0
    return 100 - (100 / (1 + rs))


def _compute_atr_pct(
    highs: list[float], lows: list[float], closes: list[float], period: int
) -> float:
    if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
        return 0.0
    true_ranges: list[float] = []
    for index in range(-period, 0):
        high = highs[index]
        low = lows[index]
        previous_close = closes[index - 1]
        true_ranges.append(
            max(high - low, abs(high - previous_close), abs(low - previous_close))
        )
    latest_close = closes[-1]
    if latest_close <= 0:
        return 0.0
    return (sum(true_ranges) / len(true_ranges)) / latest_close


def _window_return(closes: list[float], window: int) -> float:
    if len(closes) <= window or closes[-window - 1] <= 0:
        return 0.0
    return ((closes[-1] - closes[-window - 1]) / closes[-window - 1]) * 100.0


def _daily_counter_value(counter: dict[str, object] | None, now: datetime) -> int:
    if not isinstance(counter, dict):
        return 0
    if str(counter.get("date")) != now.date().isoformat():
        return 0
    return int(counter.get("count") or 0)


def _band_multiplier(
    atr_pct: float, reference_pct: float, widening_multiplier: float
) -> Decimal:
    if reference_pct <= 0 or atr_pct <= reference_pct:
        return Decimal("1")
    expansion = ((atr_pct - reference_pct) / reference_pct) * widening_multiplier
    return Decimal(str(max(1.0, 1.0 + expansion)))


def _market_regime_from_btc(
    market_map: dict[str, MarketSnapshot],
    *,
    breakdown_return_pct: float,
    abnormal_atr_pct: float,
    atr_period: int,
) -> dict[str, object]:
    btc = market_map.get("BTC-USD")
    if btc is None:
        return {
            "regime": "unknown",
            "btc_return_pct": 0.0,
            "btc_atr_pct": 0.0,
            "suspend_rebuys": False,
            "harvest_sell_multiplier": 1.0,
        }
    closes = [float(v) for v in btc.metadata.get("candle_closes", [])]
    highs = [float(v) for v in btc.metadata.get("candle_highs", [])]
    lows = [float(v) for v in btc.metadata.get("candle_lows", [])]
    btc_return = _window_return(closes, min(24, max(1, len(closes) - 1)))
    btc_atr_pct = _compute_atr_pct(highs, lows, closes, atr_period) * 100.0
    if btc_return <= breakdown_return_pct or btc_atr_pct >= abnormal_atr_pct:
        return {
            "regime": "defensive",
            "btc_return_pct": btc_return,
            "btc_atr_pct": btc_atr_pct,
            "suspend_rebuys": True,
            "harvest_sell_multiplier": 0.85,
        }
    if btc_return <= (breakdown_return_pct / 2):
        return {
            "regime": "cautious",
            "btc_return_pct": btc_return,
            "btc_atr_pct": btc_atr_pct,
            "suspend_rebuys": False,
            "harvest_sell_multiplier": 0.9,
        }
    return {
        "regime": "normal",
        "btc_return_pct": btc_return,
        "btc_atr_pct": btc_atr_pct,
        "suspend_rebuys": False,
        "harvest_sell_multiplier": 1.0,
    }


def build_volatility_harvest_plan(
    *,
    config: dict | None,
    market_map: dict[str, MarketSnapshot],
    positions: dict[str, PositionState],
    runtime_state: dict[str, dict[str, object]],
    token_profiles: dict[str, dict[str, object]],
    capital_context: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized = normalize_strategy_config(config)
    symbols = selected_trade_symbols(normalized)
    assigned_capital_usd = _to_decimal(
        (capital_context or {}).get("assigned_capital_usd")
        or (normalized.get("total_allocated_amount_usd") or {}).get("target")
        or 0
    )
    available_capital_usd = _to_decimal(
        (capital_context or {}).get("available_capital_usd"), str(assigned_capital_usd)
    )
    risk_controls = dict(normalized.get("risk_controls") or {})
    volatility_settings = dict(normalized.get("volatility_settings") or {})
    regime = _market_regime_from_btc(
        market_map,
        breakdown_return_pct=float(
            risk_controls.get("btc_breakdown_return_pct") or -7.0
        ),
        abnormal_atr_pct=float(risk_controls.get("btc_abnormal_atr_pct") or 8.0),
        atr_period=int(volatility_settings.get("atr_period") or 14),
    )

    symbol_contexts: dict[str, dict[str, object]] = {}
    max_per_token_pct = _pct(risk_controls.get("max_allocation_per_token_pct") or 22.5)
    per_symbol_capital = (
        assigned_capital_usd / Decimal(str(max(len(symbols), 1)))
        if assigned_capital_usd > 0
        else Decimal("0")
    )
    if max_per_token_pct > 0 and assigned_capital_usd > 0:
        per_symbol_capital = min(
            per_symbol_capital, assigned_capital_usd * max_per_token_pct
        )

    for symbol in symbols:
        market = market_map.get(symbol)
        if market is None:
            continue
        runtime = dict(runtime_state.get(symbol, {}))
        position = positions.get(symbol) or PositionState(symbol=symbol)
        current_quantity = _to_decimal(position.quantity)
        core_pct = _pct(normalized.get("core_position_percentage") or 70)
        trading_pct = _pct(normalized.get("trading_position_percentage") or 30)
        tracked_core = _to_decimal(runtime.get("core_quantity"))
        tracked_trading = _to_decimal(runtime.get("trading_quantity"))
        if tracked_core <= 0 and tracked_trading <= 0 and current_quantity > 0:
            tracked_core = (current_quantity * core_pct).quantize(Decimal("0.00000001"))
            tracked_trading = current_quantity - tracked_core
        tracked_core = max(Decimal("0"), min(tracked_core, current_quantity))
        tracked_trading = max(Decimal("0"), current_quantity - tracked_core)
        avg_trading_entry = _to_decimal(
            runtime.get("avg_trading_entry_price")
            or position.entry_price
            or market.current_price,
            str(market.current_price),
        )
        avg_core_entry = _to_decimal(
            runtime.get("avg_core_entry_price")
            or position.entry_price
            or market.current_price,
            str(market.current_price),
        )
        harvested_cash_usd = _to_decimal(runtime.get("harvested_cash_cents")) / Decimal(
            "100"
        )
        entry_anchor_price = _to_decimal(
            runtime.get("entry_anchor_price")
            or position.entry_price
            or market.current_price,
            str(market.current_price),
        )
        closes = [float(v) for v in market.metadata.get("candle_closes", [])]
        highs = [float(v) for v in market.metadata.get("candle_highs", [])]
        lows = [float(v) for v in market.metadata.get("candle_lows", [])]
        atr_pct = _compute_atr_pct(
            highs,
            lows,
            closes,
            int(volatility_settings.get("atr_period") or 14),
        )
        band_scale = _band_multiplier(
            atr_pct,
            float(volatility_settings.get("atr_reference_pct") or 0.035),
            float(volatility_settings.get("atr_band_widening_multiplier") or 1.35),
        )
        harvest_bands = [
            {
                **dict(band),
                "scaled_trigger_pct": float(
                    _to_decimal(band.get("trigger_pct")) * band_scale
                ),
            }
            for band in normalized.get("harvest_bands") or []
        ]
        rebuy_bands = [
            {
                **dict(band),
                "scaled_trigger_pct": float(
                    _to_decimal(band.get("trigger_pct")) * band_scale
                ),
            }
            for band in normalized.get("rebuy_bands") or []
        ]
        mark_value = current_quantity * market.current_price
        target_token_capital_usd = min(
            per_symbol_capital, available_capital_usd + mark_value
        )
        profile = token_profiles.get(symbol, {})
        score = (
            float(profile.get("volatility_score") or 0.0) * 0.45
            + float(profile.get("liquidity_score") or 0.0) * 0.35
            + float(profile.get("trend_score") or 0.0) * 0.2
        )
        symbol_contexts[symbol] = {
            "symbol": symbol,
            "score": score,
            "target_token_capital_usd": float(target_token_capital_usd),
            "target_core_capital_usd": float(target_token_capital_usd * core_pct),
            "target_trading_capital_usd": float(target_token_capital_usd * trading_pct),
            "current_value_usd": float(mark_value),
            "current_quantity": float(current_quantity),
            "core_quantity": float(tracked_core),
            "trading_quantity": float(tracked_trading),
            "avg_core_entry_price": float(avg_core_entry),
            "avg_trading_entry_price": float(avg_trading_entry),
            "harvested_cash_usd": float(harvested_cash_usd),
            "entry_anchor_price": float(entry_anchor_price),
            "atr_pct": atr_pct,
            "rsi": _compute_rsi(
                closes, int(volatility_settings.get("rsi_period") or 14)
            ),
            "harvest_bands": harvest_bands,
            "rebuy_bands": rebuy_bands,
            "entry_layers": list(normalized.get("entry_layers") or []),
            "last_local_high": float(
                _to_decimal(
                    runtime.get("last_local_high")
                    or position.peak_price
                    or market.current_price
                )
            ),
            "completed_entry_layers": list(runtime.get("completed_entry_layers") or []),
            "completed_harvest_bands": list(
                runtime.get("completed_harvest_bands") or []
            ),
            "completed_rebuy_bands": list(runtime.get("completed_rebuy_bands") or []),
            "pending_vh_action": runtime.get("pending_vh_action"),
            "daily_sell_count": _daily_counter_value(
                runtime.get("daily_sell_count"), datetime.now(UTC)
            ),
            "daily_rebuy_count": _daily_counter_value(
                runtime.get("daily_rebuy_count"), datetime.now(UTC)
            ),
            "last_harvest_at": runtime.get("last_harvest_at"),
            "last_rebuy_at": runtime.get("last_rebuy_at"),
            "last_action_at": runtime.get("last_action_at"),
            "regime": regime,
            "risk_controls": risk_controls,
            "fee_settings": dict(normalized.get("fee_settings") or {}),
            "volatility_settings": volatility_settings,
        }

    return {
        "strategy_id": STRATEGY_ID,
        "config": normalized,
        "market_regime": regime,
        "symbol_contexts": symbol_contexts,
    }


class VolatilityHarvestStrategy(TradingStrategy):
    strategy_id = STRATEGY_ID
    display_name = "Volatility Harvest Strategy"
    description = (
        "Core-plus-trading accumulation strategy that harvests rallies, rebuys pullbacks, "
        "and targets long-term token accumulation."
    )
    version = "1.0"

    def validate_config(self, config: dict) -> bool:
        normalized = normalize_strategy_config(config)
        trading_mode = str(normalized.get("trading_mode") or "").lower()
        if trading_mode not in {"paper", "live"}:
            raise ValueError("trading_mode must be 'paper' or 'live'")
        if not selected_trade_symbols(normalized):
            raise ValueError("Select at least one token for volatility harvesting")
        core_pct = _to_decimal(normalized.get("core_position_percentage"))
        trading_pct = _to_decimal(normalized.get("trading_position_percentage"))
        if (
            core_pct <= 0
            or trading_pct <= 0
            or (core_pct + trading_pct) != Decimal("100")
        ):
            raise ValueError(
                "core and trading percentages must both be positive and total 100"
            )
        if sum(
            _to_decimal(layer.get("allocation_pct"))
            for layer in normalized["entry_layers"]
        ) != Decimal("100"):
            raise ValueError("entry layer percentages must total 100")
        interval = int(
            (normalized.get("mode_settings") or {}).get("evaluation_interval_minutes")
            or 0
        )
        if interval < 15:
            raise ValueError("evaluation_interval_minutes must be at least 15")
        return True

    def get_default_config(self) -> dict:
        return default_strategy_config()

    def get_config_schema(self) -> dict:
        return default_strategy_config()

    def generate_signal(
        self,
        context: StrategyContext,
        config: dict,
        signal_id: UUID,
        correlation_id: UUID,
    ) -> StrategySignal:
        plan = dict((context.extra or {}).get("volatility_harvest") or {})
        runtime = dict((context.extra or {}).get("runtime_symbol_state") or {})
        market = context.market_snapshot
        position = context.position_state

        if not plan:
            return self._hold_signal(
                context, signal_id, correlation_id, "No harvest plan available"
            )
        if plan.get("pending_vh_action"):
            return self._hold_signal(
                context, signal_id, correlation_id, "Waiting for prior harvest action"
            )

        current_price = market.current_price
        if current_price <= 0:
            return self._hold_signal(
                context, signal_id, correlation_id, "Missing market price"
            )

        risk_controls = dict(plan.get("risk_controls") or {})
        fee_settings = dict(plan.get("fee_settings") or {})
        volatility_settings = dict(plan.get("volatility_settings") or {})
        spread_bps = (
            ((market.ask_price - market.bid_price) / current_price) * Decimal("10000")
            if current_price > 0 and market.ask_price > 0 and market.bid_price > 0
            else Decimal("0")
        )
        if spread_bps > _to_decimal(risk_controls.get("max_spread_bps") or 60):
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Spread too wide for harvest ({spread_bps:.1f}bps)",
            )

        if self._cooldown_active(runtime, risk_controls):
            return self._hold_signal(
                context, signal_id, correlation_id, "Harvest cooldown active"
            )

        current_quantity = _to_decimal(position.quantity)
        core_quantity = _to_decimal(plan.get("core_quantity"))
        trading_quantity = _to_decimal(plan.get("trading_quantity"))
        total_entry_price = _to_decimal(
            position.entry_price or current_price, str(current_price)
        )
        emergency_stop = _pct(risk_controls.get("emergency_stop_loss_pct") or 18)
        regime = dict(plan.get("regime") or {})
        if current_quantity > 0 and emergency_stop > 0 and total_entry_price > 0:
            if current_price <= total_entry_price * (Decimal("1") - emergency_stop):
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    reason=f"emergency_stop_loss current={current_price} entry={total_entry_price}",
                    quantity=current_quantity,
                    metadata=self._pending_action_patch(
                        action_type="emergency_exit",
                        code="emergency_stop_loss",
                        ref_price=current_price,
                        extra={"bucket_type": "all"},
                    ),
                )

        if current_quantity <= 0:
            return self._initial_entry_signal(
                context, signal_id, correlation_id, plan=plan, regime=regime
            )

        next_entry = self._next_entry_layer(plan, current_price)
        if next_entry is not None:
            return self._buy_signal(
                context,
                signal_id,
                correlation_id,
                reason=(
                    f"layered_entry {next_entry['id']} pullback={next_entry['pullback_pct']}% "
                    f"anchor={plan.get('entry_anchor_price')}"
                ),
                quantity=next_entry["quantity"],
                confidence=0.72,
                metadata=self._pending_action_patch(
                    action_type="entry_buy",
                    code=str(next_entry["id"]),
                    ref_price=current_price,
                    extra={
                        "deployed_cash_cents": int(
                            next_entry["capital_usd"] * Decimal("100")
                        ),
                        "core_allocation_ratio": float(
                            _pct(config.get("core_position_percentage") or 70)
                        ),
                        "transaction_type": "entry_buy",
                    },
                ),
            )

        if trading_quantity > 0:
            harvest_signal = self._maybe_harvest_signal(
                context=context,
                signal_id=signal_id,
                correlation_id=correlation_id,
                trading_quantity=trading_quantity,
                avg_trading_entry=_to_decimal(
                    plan.get("avg_trading_entry_price"), str(total_entry_price)
                ),
                current_price=current_price,
                plan=plan,
                fee_settings=fee_settings,
            )
            if harvest_signal is not None:
                return harvest_signal

        if bool(regime.get("suspend_rebuys")) and bool(
            risk_controls.get("suspend_rebuys_on_btc_breakdown", True)
        ):
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"BTC regime {regime.get('regime')} suspended rebuys",
            )

        rebuy_signal = self._maybe_rebuy_signal(
            context=context,
            signal_id=signal_id,
            correlation_id=correlation_id,
            trading_quantity=trading_quantity,
            current_price=current_price,
            plan=plan,
            volatility_settings=volatility_settings,
        )
        if rebuy_signal is not None:
            return rebuy_signal

        return self._hold_signal(
            context,
            signal_id,
            correlation_id,
            (
                f"Holding core={core_quantity:.8f} trading={trading_quantity:.8f} "
                f"harvested_cash=${float(plan.get('harvested_cash_usd') or 0):.2f}"
            ),
        )

    @staticmethod
    def _cooldown_active(
        runtime: dict[str, object], risk_controls: dict[str, object]
    ) -> bool:
        raw_last = (
            runtime.get("last_action_at")
            or runtime.get("last_harvest_at")
            or runtime.get("last_rebuy_at")
        )
        if not raw_last:
            return False
        try:
            last_action = datetime.fromisoformat(str(raw_last).replace("Z", "+00:00"))
        except Exception:
            return False
        cooldown_minutes = int(
            risk_controls.get("cooldown_minutes_between_actions") or 90
        )
        if cooldown_minutes <= 0:
            return False
        return (datetime.now(UTC) - last_action).total_seconds() < (
            cooldown_minutes * 60
        )

    def _initial_entry_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        *,
        plan: dict[str, object],
        regime: dict[str, object],
    ) -> StrategySignal:
        if str(regime.get("regime")) == "defensive":
            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                "Defensive BTC regime blocked new entries",
            )
        layers = list(plan.get("entry_layers") or [])
        if not layers:
            return self._hold_signal(
                context, signal_id, correlation_id, "No entry layers configured"
            )
        first_layer = dict(layers[0])
        capital_usd = _to_decimal(plan.get("target_token_capital_usd")) * _pct(
            first_layer.get("allocation_pct") or 0
        )
        if capital_usd <= 0:
            return self._hold_signal(
                context, signal_id, correlation_id, "No initial entry capital"
            )
        quantity = capital_usd / context.market_snapshot.current_price
        if quantity <= 0:
            return self._hold_signal(
                context, signal_id, correlation_id, "Initial entry size below zero"
            )
        return self._buy_signal(
            context,
            signal_id,
            correlation_id,
            reason=f"initial_layered_entry {first_layer['id']}",
            quantity=quantity,
            confidence=0.74,
            metadata=self._pending_action_patch(
                action_type="entry_buy",
                code=str(first_layer["id"]),
                ref_price=context.market_snapshot.current_price,
                extra={
                    "deployed_cash_cents": int(capital_usd * Decimal("100")),
                    "entry_anchor_price": float(context.market_snapshot.current_price),
                    "core_allocation_ratio": float(
                        _to_decimal(plan.get("target_core_capital_usd"))
                        / _to_decimal(plan.get("target_token_capital_usd") or 1)
                    ),
                    "transaction_type": "entry_buy",
                },
            ),
        )

    def _next_entry_layer(
        self, plan: dict[str, object], current_price: Decimal
    ) -> dict[str, object] | None:
        completed = {str(value) for value in plan.get("completed_entry_layers") or []}
        anchor_price = _to_decimal(plan.get("entry_anchor_price"), str(current_price))
        target_capital = _to_decimal(plan.get("target_token_capital_usd"))
        if target_capital <= 0 or anchor_price <= 0:
            return None
        for layer in plan.get("entry_layers") or []:
            layer_id = str(layer.get("id") or "")
            if not layer_id or layer_id in completed:
                continue
            pullback_pct = _pct(layer.get("pullback_pct") or 0)
            if pullback_pct <= 0:
                continue
            trigger_price = anchor_price * (Decimal("1") - pullback_pct)
            if current_price > trigger_price:
                continue
            capital_usd = target_capital * _pct(layer.get("allocation_pct") or 0)
            quantity = (
                capital_usd / current_price if current_price > 0 else Decimal("0")
            )
            if quantity <= 0:
                continue
            return {
                "id": layer_id,
                "pullback_pct": float(_to_decimal(layer.get("pullback_pct"))),
                "capital_usd": capital_usd,
                "quantity": quantity,
            }
        return None

    def _maybe_harvest_signal(
        self,
        *,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        trading_quantity: Decimal,
        avg_trading_entry: Decimal,
        current_price: Decimal,
        plan: dict[str, object],
        fee_settings: dict[str, object],
    ) -> StrategySignal | None:
        if trading_quantity <= 0 or avg_trading_entry <= 0:
            return None
        completed = {str(value) for value in plan.get("completed_harvest_bands") or []}
        daily_count = int(plan.get("daily_sell_count") or 0)
        max_daily = int(
            (plan.get("risk_controls") or {}).get("daily_max_sell_count") or 4
        )
        if max_daily > 0 and daily_count >= max_daily:
            return None

        min_net_profit = _to_decimal(
            (plan.get("risk_controls") or {}).get("minimum_net_profit_after_fees_usd")
            or 3
        )
        fee_bps = _to_decimal(fee_settings.get("coinbase_fee_bps") or 60)
        slippage_bps = _to_decimal(fee_settings.get("slippage_bps") or 12)
        spread_buffer_bps = _to_decimal(fee_settings.get("spread_buffer_bps") or 10)

        pnl_pct = ((current_price - avg_trading_entry) / avg_trading_entry) * Decimal(
            "100"
        )
        sell_multiplier = _to_decimal(
            (plan.get("regime") or {}).get("harvest_sell_multiplier") or 1
        )
        for band in plan.get("harvest_bands") or []:
            band_id = str(band.get("id") or "")
            if not band_id or band_id in completed:
                continue
            trigger_pct = _to_decimal(
                band.get("scaled_trigger_pct") or band.get("trigger_pct") or 0
            )
            if pnl_pct < trigger_pct:
                continue
            sell_pct = _pct(band.get("sell_pct") or 0) * sell_multiplier
            quantity = (trading_quantity * sell_pct).quantize(Decimal("0.00000001"))
            if quantity <= 0:
                continue
            gross_profit = (current_price - avg_trading_entry) * quantity
            fees = (current_price * quantity) * (
                (fee_bps + slippage_bps + spread_buffer_bps) / Decimal("10000")
            )
            net_profit = gross_profit - fees
            if net_profit < min_net_profit:
                continue
            return self._close_signal(
                context,
                signal_id,
                correlation_id,
                reason=f"harvest_trigger {band_id} pnl={pnl_pct:.2f}%",
                quantity=min(quantity, context.position_state.quantity),
                metadata=self._pending_action_patch(
                    action_type="harvest_sell",
                    code=band_id,
                    ref_price=current_price,
                    extra={
                        "transaction_type": "harvest_sell",
                        "bucket_type": "trading",
                        "band_code": band_id,
                        "estimated_net_profit_cents": int(net_profit * Decimal("100")),
                        "estimated_gross_notional_cents": int(
                            current_price * quantity * Decimal("100")
                        ),
                        "sell_quantity": float(quantity),
                    },
                ),
            )
        return None

    def _maybe_rebuy_signal(
        self,
        *,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        trading_quantity: Decimal,
        current_price: Decimal,
        plan: dict[str, object],
        volatility_settings: dict[str, object],
    ) -> StrategySignal | None:
        harvested_cash_usd = _to_decimal(plan.get("harvested_cash_usd"))
        if harvested_cash_usd <= 0:
            return None
        daily_count = int(plan.get("daily_rebuy_count") or 0)
        max_daily = int(
            (plan.get("risk_controls") or {}).get("daily_max_rebuy_count") or 3
        )
        if max_daily > 0 and daily_count >= max_daily:
            return None
        completed = {str(value) for value in plan.get("completed_rebuy_bands") or []}
        local_high = _to_decimal(plan.get("last_local_high"), str(current_price))
        if local_high <= 0:
            return None
        pullback_pct = ((local_high - current_price) / local_high) * Decimal("100")
        atr_pct = _to_decimal(plan.get("atr_pct"))
        if atr_pct < _to_decimal(
            volatility_settings.get("minimum_atr_pct_to_trade") or 0.01
        ):
            return None
        rsi = plan.get("rsi")
        require_rsi = bool(
            volatility_settings.get("require_rsi_confirmation_for_rebuy")
        )
        threshold = float(volatility_settings.get("rsi_rebuy_threshold") or 35)
        if require_rsi and (rsi is None or float(rsi) > threshold):
            return None
        missing_trading_value = max(
            Decimal("0"),
            _to_decimal(plan.get("target_trading_capital_usd"))
            - (trading_quantity * current_price),
        )
        for band in plan.get("rebuy_bands") or []:
            band_id = str(band.get("id") or "")
            if not band_id or band_id in completed:
                continue
            trigger_pct = _to_decimal(
                band.get("scaled_trigger_pct") or band.get("trigger_pct") or 0
            )
            if pullback_pct < trigger_pct:
                continue
            deploy_cash = min(
                harvested_cash_usd * _pct(band.get("deploy_cash_pct") or 0),
                harvested_cash_usd,
                missing_trading_value
                if missing_trading_value > 0
                else harvested_cash_usd,
            )
            if deploy_cash <= 0:
                continue
            quantity = (deploy_cash / current_price).quantize(Decimal("0.00000001"))
            if quantity <= 0:
                continue
            return self._buy_signal(
                context,
                signal_id,
                correlation_id,
                reason=f"rebuy_trigger {band_id} pullback={pullback_pct:.2f}%",
                quantity=quantity,
                confidence=0.7,
                metadata=self._pending_action_patch(
                    action_type="rebuy_buy",
                    code=band_id,
                    ref_price=current_price,
                    extra={
                        "transaction_type": "rebuy_buy",
                        "bucket_type": "trading",
                        "band_code": band_id,
                        "deployed_cash_cents": int(deploy_cash * Decimal("100")),
                    },
                ),
            )
        return None

    @staticmethod
    def _pending_action_patch(
        *,
        action_type: str,
        code: str,
        ref_price: Decimal,
        extra: dict[str, object] | None = None,
    ) -> dict[str, object]:
        patch = {
            "pending_vh_action": {
                "type": action_type,
                "code": code,
                "ref_price": str(ref_price),
                "occurred_at": datetime.now(UTC).isoformat(),
            },
            "last_action_at": datetime.now(UTC).isoformat(),
        }
        if extra:
            patch["pending_vh_action"].update(extra)
            if "entry_anchor_price" in extra:
                patch["entry_anchor_price"] = extra["entry_anchor_price"]
        return {
            "strategy_context": "volatility_harvest",
            "event_type": (extra or {}).get("transaction_type") or action_type,
            "bucket_type": (extra or {}).get("bucket_type"),
            "band_code": (extra or {}).get("band_code") or code,
            "runtime_state_patch": patch,
        }

    def _buy_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        *,
        reason: str,
        quantity: Decimal,
        confidence: float,
        metadata: dict | None = None,
    ) -> StrategySignal:
        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.BUY,
            instrument=Instrument(symbol=context.market_snapshot.symbol),
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=Quantity(amount=quantity),
            confidence=confidence,
            reason=reason,
            metadata=metadata or {},
        )

    def _close_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        *,
        reason: str,
        quantity: Decimal,
        metadata: dict | None = None,
    ) -> StrategySignal:
        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.CLOSE,
            instrument=Instrument(symbol=context.market_snapshot.symbol),
            side=Side.SELL,
            order_type=OrderType.MARKET,
            quantity=Quantity(amount=quantity),
            confidence=0.8,
            reason=reason,
            metadata=metadata or {},
        )

    def _hold_signal(
        self,
        context: StrategyContext,
        signal_id: UUID,
        correlation_id: UUID,
        reason: str,
    ) -> StrategySignal:
        return StrategySignal(
            signal_id=signal_id,
            correlation_id=correlation_id,
            tenant_id=context.tenant_id,
            strategy_id=self.strategy_id,
            strategy_version=self.version,
            trading_mode=context.trading_mode,
            signal_type=SignalType.HOLD,
            confidence=0.5,
            reason=reason,
            metadata={},
        )

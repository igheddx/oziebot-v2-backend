"""Strategic Aggressive Allocation strategy."""

from __future__ import annotations

from copy import deepcopy
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

STRATEGY_ID = "strategic_aggressive_allocation"

HIGH_CONVICTION_BUCKET = "high_conviction_momentum"
ROTATION_BUCKET = "rotation_trades"
MOONBAG_BUCKET = "speculative_moonbag"
DRY_POWDER_BUCKET = "dry_powder"
TRADED_BUCKET_IDS = (
    HIGH_CONVICTION_BUCKET,
    ROTATION_BUCKET,
    MOONBAG_BUCKET,
)
ALL_BUCKET_IDS = (*TRADED_BUCKET_IDS, DRY_POWDER_BUCKET)


DEFAULT_BUCKETS: list[dict[str, object]] = [
    {
        "id": HIGH_CONVICTION_BUCKET,
        "label": "High Conviction Momentum Bucket",
        "allocation_pct": 42.5,
        "max_positions": 3,
        "max_allocation_per_token_pct": 50.0,
        "stop_loss_pct": 18.0,
        "profit_targets_pct": [35.0, 75.0],
        "trailing_stop_activation_pct": 50.0,
        "trailing_stop_pct": 12.0,
        "selected_tokens": [],
        "prefer_base_ecosystem": True,
    },
    {
        "id": ROTATION_BUCKET,
        "label": "Rotation Trades Bucket",
        "allocation_pct": 32.0,
        "max_positions": 4,
        "max_allocation_per_token_pct": 35.0,
        "stop_loss_pct": 12.0,
        "profit_targets_pct": [20.0, 45.0],
        "trailing_stop_activation_pct": 30.0,
        "trailing_stop_pct": 10.0,
        "selected_tokens": [],
        "prefer_base_ecosystem": False,
    },
    {
        "id": MOONBAG_BUCKET,
        "label": "Speculative Moonbag Bucket",
        "allocation_pct": 15.0,
        "max_positions": 3,
        "max_allocation_per_token_pct": 40.0,
        "stop_loss_pct": 25.0,
        "profit_targets_pct": [60.0, 120.0],
        "trailing_stop_activation_pct": 80.0,
        "trailing_stop_pct": 18.0,
        "selected_tokens": [],
        "prefer_base_ecosystem": True,
    },
    {
        "id": DRY_POWDER_BUCKET,
        "label": "Dry Powder / Cash Reserve",
        "allocation_pct": 10.5,
        "max_positions": 0,
        "max_allocation_per_token_pct": 0.0,
        "stop_loss_pct": 0.0,
        "profit_targets_pct": [],
        "trailing_stop_activation_pct": 0.0,
        "trailing_stop_pct": 0.0,
        "selected_tokens": [],
        "prefer_base_ecosystem": False,
    },
]

DEFAULT_PROFIT_TAKING_RULES = {
    "scale_out_fraction_pct": 25.0,
    "cost_basis_recovery_enabled": True,
    "cost_basis_recovery_trigger_pct": 100.0,
}

DEFAULT_REBALANCE_SETTINGS = {
    "mode": "manual",
    "drift_threshold_pct": 10.0,
    "aggressive_rebalance": False,
    "cadence": "weekly",
}

DEFAULT_MODE_SETTINGS = {
    "evaluation_interval_minutes": 60,
    "minimum_order_size_usd": 25.0,
    "max_total_open_positions": 10,
}


def default_strategy_config() -> dict[str, object]:
    return {
        "strategy_type": STRATEGY_ID,
        "trading_mode": "paper",
        "enabled": False,
        "total_allocated_amount_usd": {
            "target": 0.0,
            "source": "allocation_plan",
        },
        "bucket_allocations": deepcopy(DEFAULT_BUCKETS),
        "selected_tokens": {bucket["id"]: [] for bucket in DEFAULT_BUCKETS},
        "max_allocation_per_token": {
            bucket["id"]: bucket["max_allocation_per_token_pct"]
            for bucket in DEFAULT_BUCKETS
        },
        "profit_taking_rules": deepcopy(DEFAULT_PROFIT_TAKING_RULES),
        "stop_loss_rules": {
            bucket["id"]: bucket["stop_loss_pct"]
            for bucket in DEFAULT_BUCKETS
            if bucket["id"] in TRADED_BUCKET_IDS
        },
        "trailing_stop_rules": {
            bucket["id"]: {
                "activation_pct": bucket["trailing_stop_activation_pct"],
                "trailing_pct": bucket["trailing_stop_pct"],
            }
            for bucket in DEFAULT_BUCKETS
            if bucket["id"] in TRADED_BUCKET_IDS
        },
        "rebalance_settings": deepcopy(DEFAULT_REBALANCE_SETTINGS),
        "mode_settings": deepcopy(DEFAULT_MODE_SETTINGS),
    }


def normalize_strategy_config(config: dict | None) -> dict[str, object]:
    normalized = default_strategy_config()
    raw = dict(config or {})
    for key in (
        "trading_mode",
        "enabled",
        "total_allocated_amount_usd",
        "selected_tokens",
        "max_allocation_per_token",
        "profit_taking_rules",
        "stop_loss_rules",
        "trailing_stop_rules",
        "rebalance_settings",
        "mode_settings",
    ):
        value = raw.get(key)
        if isinstance(value, dict):
            normalized[key].update(value)  # type: ignore[index]
        elif value is not None:
            normalized[key] = value

    bucket_overrides = {
        str(item.get("id") or ""): dict(item)
        for item in raw.get("bucket_allocations") or []
    }
    buckets: list[dict[str, object]] = []
    selected = normalized["selected_tokens"]
    for default_bucket in deepcopy(DEFAULT_BUCKETS):
        bucket_id = str(default_bucket["id"])
        override = bucket_overrides.get(bucket_id, {})
        default_bucket.update(override)
        if isinstance(selected, dict):
            default_bucket["selected_tokens"] = list(
                selected.get(bucket_id) or default_bucket.get("selected_tokens") or []
            )
        buckets.append(default_bucket)
    normalized["bucket_allocations"] = buckets
    normalized["strategy_type"] = STRATEGY_ID
    return normalized


def selected_trade_symbols(config: dict | None) -> list[str]:
    normalized = normalize_strategy_config(config)
    symbols: list[str] = []
    seen: set[str] = set()
    for bucket in normalized["bucket_allocations"]:
        bucket_id = str(bucket["id"])
        if bucket_id == DRY_POWDER_BUCKET:
            continue
        for symbol in bucket.get("selected_tokens") or []:
            upper = str(symbol or "").upper()
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
    for idx in range(-period, 0):
        delta = closes[idx] - closes[idx - 1]
        if delta > 0:
            gains += delta
        elif delta < 0:
            losses += abs(delta)
    if losses == 0:
        return 100.0
    rs = gains / losses if losses else 0.0
    return 100 - (100 / (1 + rs))


def _return_over_window(closes: list[float], window: int) -> float:
    if len(closes) <= window or closes[-window - 1] <= 0:
        return 0.0
    return (closes[-1] - closes[-window - 1]) / closes[-window - 1]


def _score_symbol(
    *,
    symbol: str,
    market: MarketSnapshot,
    token_profile: dict[str, object],
    prefer_base_ecosystem: bool,
    btc_return: float,
    eth_return: float,
) -> float:
    closes = [
        float(v) for v in market.metadata.get("candle_closes", []) if float(v) > 0
    ]
    volumes = [
        float(v) for v in market.metadata.get("candle_volumes", []) if float(v) >= 0
    ]
    short_return = _return_over_window(closes, 5)
    medium_return = _return_over_window(closes, 20)
    latest_volume = volumes[-1] if volumes else 0.0
    prior_volumes = volumes[-11:-1] if len(volumes) >= 11 else volumes[:-1]
    avg_prior_volume = (
        sum(prior_volumes) / len(prior_volumes) if prior_volumes else latest_volume
    )
    volume_delta = (
        ((latest_volume - avg_prior_volume) / avg_prior_volume)
        if avg_prior_volume > 0
        else 0.0
    )
    rsi = _compute_rsi(closes)
    rsi_score = 0.0 if rsi is None else max(-0.2, min(0.2, (70.0 - rsi) / 100))
    trend_score = float(token_profile.get("trend_score") or 0.0)
    rel_strength = medium_return - max(btc_return, eth_return)
    base_tags = token_profile.get("tags") or []
    ecosystem = str(token_profile.get("ecosystem") or "").lower()
    is_base = ecosystem == "base" or any(
        str(tag).lower() == "base" for tag in base_tags
    )
    base_bonus = 0.06 if prefer_base_ecosystem and is_base else 0.0
    return (
        (short_return * 0.34)
        + (medium_return * 0.24)
        + (volume_delta * 0.14)
        + (trend_score * 0.18)
        + (rel_strength * 0.14)
        + rsi_score
        + base_bonus
    )


def build_portfolio_plan(
    *,
    config: dict | None,
    market_map: dict[str, MarketSnapshot],
    positions: dict[str, PositionState],
    runtime_state: dict[str, dict[str, object]],
    token_profiles: dict[str, dict[str, object]],
    capital_context: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized = normalize_strategy_config(config)
    capital = dict(capital_context or {})
    assigned_capital_usd = _to_decimal(
        capital.get("assigned_capital_usd")
        or (normalized.get("total_allocated_amount_usd") or {}).get("target")
        or "0"
    )
    available_capital_usd = _to_decimal(
        capital.get("available_capital_usd"), str(assigned_capital_usd)
    )
    btc_market = market_map.get("BTC-USD")
    eth_market = market_map.get("ETH-USD")
    btc_return = (
        _return_over_window(
            [float(v) for v in btc_market.metadata.get("candle_closes", [])], 20
        )
        if btc_market
        else 0.0
    )
    eth_return = (
        _return_over_window(
            [float(v) for v in eth_market.metadata.get("candle_closes", [])], 20
        )
        if eth_market
        else 0.0
    )

    bucket_plans: dict[str, dict[str, object]] = {}
    symbol_contexts: dict[str, dict[str, object]] = {}

    for bucket in normalized["bucket_allocations"]:
        bucket_id = str(bucket["id"])
        allocation_pct = _pct(bucket["allocation_pct"])
        target_bucket_capital = (assigned_capital_usd * allocation_pct).max(
            Decimal("0")
        )
        if bucket_id == DRY_POWDER_BUCKET:
            bucket_plans[bucket_id] = {
                "bucket_id": bucket_id,
                "label": bucket["label"],
                "allocation_pct": float(bucket["allocation_pct"]),
                "target_bucket_capital_usd": float(target_bucket_capital),
                "tradable": False,
            }
            continue

        held_symbols: list[str] = []
        held_exposure = Decimal("0")
        selected_symbols = [
            str(symbol).upper() for symbol in bucket.get("selected_tokens") or []
        ]
        unique_selected = list(dict.fromkeys(selected_symbols))
        scored: list[tuple[str, float]] = []

        for symbol in unique_selected:
            position = positions.get(symbol)
            runtime = runtime_state.get(symbol, {})
            inferred_bucket = str(runtime.get("saa_bucket_id") or "")
            if position and position.quantity > 0 and inferred_bucket == bucket_id:
                held_symbols.append(symbol)
                held_exposure += (
                    (position.quantity * market_map[symbol].current_price)
                    if symbol in market_map
                    else Decimal("0")
                )
            market = market_map.get(symbol)
            if market is None:
                continue
            scored.append(
                (
                    symbol,
                    _score_symbol(
                        symbol=symbol,
                        market=market,
                        token_profile=token_profiles.get(symbol, {}),
                        prefer_base_ecosystem=bool(bucket.get("prefer_base_ecosystem")),
                        btc_return=btc_return,
                        eth_return=eth_return,
                    ),
                )
            )
        scored.sort(key=lambda item: item[1], reverse=True)
        max_positions = max(0, int(bucket.get("max_positions") or 0))
        ranked_symbols = [symbol for symbol, _ in scored]
        active_symbols = list(
            dict.fromkeys([*held_symbols, *ranked_symbols[:max_positions]])
        )[:max_positions]
        open_slots = max(0, max_positions - len(held_symbols))
        planned_new_symbols = [
            symbol for symbol in ranked_symbols if symbol not in held_symbols
        ][:open_slots]
        max_alloc_per_token_pct = _pct(bucket.get("max_allocation_per_token_pct") or 0)
        max_token_capital = target_bucket_capital * max_alloc_per_token_pct
        remaining_bucket_capital = max(
            Decimal("0"),
            min(target_bucket_capital, available_capital_usd * allocation_pct)
            - held_exposure,
        )
        per_new_symbol_capital = (
            min(
                max_token_capital,
                remaining_bucket_capital / Decimal(len(planned_new_symbols)),
            )
            if planned_new_symbols
            else Decimal("0")
        )
        plan = {
            "bucket_id": bucket_id,
            "label": bucket["label"],
            "allocation_pct": float(bucket["allocation_pct"]),
            "target_bucket_capital_usd": float(target_bucket_capital),
            "remaining_bucket_capital_usd": float(remaining_bucket_capital),
            "max_positions": max_positions,
            "active_symbols": active_symbols,
            "planned_new_symbols": planned_new_symbols,
            "max_allocation_per_token_pct": float(
                bucket.get("max_allocation_per_token_pct") or 0
            ),
            "per_new_symbol_capital_usd": float(per_new_symbol_capital),
            "stop_loss_pct": float(bucket.get("stop_loss_pct") or 0),
            "profit_targets_pct": [
                float(value) for value in (bucket.get("profit_targets_pct") or [])
            ],
            "trailing_stop_activation_pct": float(
                bucket.get("trailing_stop_activation_pct") or 0
            ),
            "trailing_stop_pct": float(bucket.get("trailing_stop_pct") or 0),
            "scores": {symbol: score for symbol, score in scored},
        }
        bucket_plans[bucket_id] = plan

        for rank, (symbol, score) in enumerate(scored, start=1):
            symbol_contexts[symbol] = {
                "bucket_id": bucket_id,
                "bucket_label": bucket["label"],
                "rank": rank,
                "score": score,
                "should_enter": symbol in planned_new_symbols,
                "per_new_symbol_capital_usd": float(per_new_symbol_capital),
                "stop_loss_pct": float(bucket.get("stop_loss_pct") or 0),
                "profit_targets_pct": [
                    float(value) for value in (bucket.get("profit_targets_pct") or [])
                ],
                "trailing_stop_activation_pct": float(
                    bucket.get("trailing_stop_activation_pct") or 0
                ),
                "trailing_stop_pct": float(bucket.get("trailing_stop_pct") or 0),
                "scale_out_fraction_pct": float(
                    (normalized.get("profit_taking_rules") or {}).get(
                        "scale_out_fraction_pct"
                    )
                    or 25.0
                ),
                "cost_basis_recovery_enabled": bool(
                    (normalized.get("profit_taking_rules") or {}).get(
                        "cost_basis_recovery_enabled", True
                    )
                ),
                "cost_basis_recovery_trigger_pct": float(
                    (normalized.get("profit_taking_rules") or {}).get(
                        "cost_basis_recovery_trigger_pct"
                    )
                    or 100.0
                ),
                "max_positions": max_positions,
                "bucket_plan": plan,
            }

    return {
        "bucket_plans": bucket_plans,
        "symbol_contexts": symbol_contexts,
        "capital_context": {
            "assigned_capital_usd": float(assigned_capital_usd),
            "available_capital_usd": float(available_capital_usd),
        },
    }


class StrategicAggressiveAllocationStrategy(TradingStrategy):
    strategy_id = STRATEGY_ID
    display_name = "Strategic Aggressive Allocation"
    description = "Aggressive multi-bucket portfolio rotation with dry powder, scale-outs, and trailing exits."
    version = "1.0"

    def validate_config(self, config: dict) -> bool:
        normalized = normalize_strategy_config(config)
        trading_mode = str(normalized.get("trading_mode") or "").lower()
        if trading_mode not in {"paper", "live"}:
            raise ValueError("trading_mode must be 'paper' or 'live'")
        bucket_ids: list[str] = []
        all_symbols: set[str] = set()
        allocation_total = Decimal("0")
        for bucket in normalized["bucket_allocations"]:
            bucket_id = str(bucket.get("id") or "")
            if bucket_id not in ALL_BUCKET_IDS:
                raise ValueError(f"Unknown bucket '{bucket_id}'")
            if bucket_id in bucket_ids:
                raise ValueError(f"Duplicate bucket '{bucket_id}'")
            bucket_ids.append(bucket_id)
            allocation_total += _to_decimal(bucket.get("allocation_pct"))
            selected_tokens = [
                str(symbol).upper() for symbol in bucket.get("selected_tokens") or []
            ]
            if bucket_id == DRY_POWDER_BUCKET and selected_tokens:
                raise ValueError("Dry powder bucket cannot contain tradable tokens")
            for symbol in selected_tokens:
                if symbol in all_symbols:
                    raise ValueError(
                        f"Token '{symbol}' cannot belong to multiple buckets"
                    )
                all_symbols.add(symbol)
        if set(bucket_ids) != set(ALL_BUCKET_IDS):
            raise ValueError("All four strategic allocation buckets are required")
        if allocation_total != Decimal("100"):
            raise ValueError("Bucket percentages must total 100%")
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
        plan = dict((context.extra or {}).get("strategic_allocation") or {})
        market = context.market_snapshot
        position = context.position_state
        runtime = dict((context.extra or {}).get("runtime_symbol_state") or {})
        if not plan:
            return self._hold_signal(
                context, signal_id, correlation_id, "No strategic bucket plan"
            )

        bucket_id = str(plan.get("bucket_id") or "")
        score = float(plan.get("score") or 0.0)
        entry_quantity = _to_decimal(
            runtime.get("entry_quantity"), str(position.quantity or 0)
        )
        if entry_quantity <= 0:
            entry_quantity = position.quantity
        current_price = market.current_price
        entry_price = position.entry_price or current_price
        pnl_pct = (
            ((current_price - entry_price) / entry_price)
            if position.quantity > 0 and entry_price > 0
            else Decimal("0")
        )
        profit_targets = [
            Decimal(str(value)) / Decimal("100")
            for value in plan.get("profit_targets_pct") or []
        ]
        scale_fraction = Decimal(
            str(plan.get("scale_out_fraction_pct") or 25.0)
        ) / Decimal("100")
        pending_event = str(runtime.get("pending_profit_event") or "")
        completed = {
            str(value) for value in runtime.get("completed_profit_events") or []
        }

        if position.quantity > 0:
            stop_loss_pct = Decimal(str(plan.get("stop_loss_pct") or 0)) / Decimal(
                "100"
            )
            trailing_activation = Decimal(
                str(plan.get("trailing_stop_activation_pct") or 0)
            ) / Decimal("100")
            trailing_pct = Decimal(str(plan.get("trailing_stop_pct") or 0)) / Decimal(
                "100"
            )

            if stop_loss_pct > 0 and current_price <= entry_price * (
                Decimal("1") - stop_loss_pct
            ):
                return self._close_signal(
                    context,
                    signal_id,
                    correlation_id,
                    reason=f"stop_loss_triggered bucket={bucket_id} pnl={pnl_pct:.2%}",
                    metadata={"bucket_id": bucket_id, "exit_type": "stop_loss"},
                )

            if trailing_activation > 0 and pnl_pct >= trailing_activation:
                peak_price = position.peak_price or current_price
                if trailing_pct > 0 and current_price <= peak_price * (
                    Decimal("1") - trailing_pct
                ):
                    return self._close_signal(
                        context,
                        signal_id,
                        correlation_id,
                        reason=f"trailing_stop_triggered bucket={bucket_id} pnl={pnl_pct:.2%}",
                        metadata={"bucket_id": bucket_id, "exit_type": "trailing_stop"},
                    )

            for index, target_pct in enumerate(profit_targets, start=1):
                event_code = f"profit_target_{index}"
                if (
                    target_pct <= 0
                    or event_code in completed
                    or pending_event == event_code
                ):
                    continue
                if pnl_pct >= target_pct:
                    quantity = max(Decimal("0"), (position.quantity * scale_fraction))
                    if quantity > 0:
                        return self._close_signal(
                            context,
                            signal_id,
                            correlation_id,
                            reason=f"{event_code}_triggered bucket={bucket_id} pnl={pnl_pct:.2%}",
                            metadata={
                                "bucket_id": bucket_id,
                                "exit_type": event_code,
                                "runtime_state_patch": {
                                    "saa_bucket_id": bucket_id,
                                    "pending_profit_event": event_code,
                                },
                            },
                            quantity=quantity,
                        )

            if (
                bool(plan.get("cost_basis_recovery_enabled"))
                and "cost_basis_recovery" not in completed
                and pending_event != "cost_basis_recovery"
            ):
                trigger_pct = Decimal(
                    str(plan.get("cost_basis_recovery_trigger_pct") or 100.0)
                ) / Decimal("100")
                if pnl_pct >= trigger_pct and current_price > 0 and entry_quantity > 0:
                    basis_quantity = min(
                        position.quantity,
                        (entry_price * entry_quantity) / current_price,
                    )
                    if basis_quantity > 0:
                        return self._close_signal(
                            context,
                            signal_id,
                            correlation_id,
                            reason=f"cost_basis_recovery bucket={bucket_id} pnl={pnl_pct:.2%}",
                            metadata={
                                "bucket_id": bucket_id,
                                "exit_type": "cost_basis_recovery",
                                "runtime_state_patch": {
                                    "saa_bucket_id": bucket_id,
                                    "pending_profit_event": "cost_basis_recovery",
                                },
                            },
                            quantity=basis_quantity,
                        )

            return self._hold_signal(
                context,
                signal_id,
                correlation_id,
                f"Holding bucket={bucket_id} score={score:.4f} pnl={pnl_pct:.2%}",
            )

        target_capital = Decimal(str(plan.get("per_new_symbol_capital_usd") or 0))
        if bool(plan.get("should_enter")) and target_capital > 0 and current_price > 0:
            quantity = target_capital / current_price
            if quantity > 0:
                return self._buy_signal(
                    context,
                    signal_id,
                    correlation_id,
                    reason=f"ranked entry bucket={bucket_id} rank={plan.get('rank')} score={score:.4f}",
                    quantity=quantity,
                    metadata={
                        "bucket_id": bucket_id,
                        "strategic_score": score,
                        "strategic_rank": plan.get("rank"),
                        "per_new_symbol_capital_usd": float(target_capital),
                        "runtime_state_patch": {"saa_bucket_id": bucket_id},
                    },
                )
        return self._hold_signal(
            context,
            signal_id,
            correlation_id,
            f"Skipped entry bucket={bucket_id} should_enter={bool(plan.get('should_enter'))} capital={target_capital}",
        )

    def _buy_signal(
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
            signal_type=SignalType.BUY,
            instrument=Instrument(symbol=context.market_snapshot.symbol),
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=Quantity(amount=quantity),
            confidence=0.78,
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
        metadata: dict | None = None,
        quantity: Decimal | None = None,
    ) -> StrategySignal:
        exit_quantity = quantity or context.position_state.quantity
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
            quantity=Quantity(amount=exit_quantity),
            confidence=0.84,
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

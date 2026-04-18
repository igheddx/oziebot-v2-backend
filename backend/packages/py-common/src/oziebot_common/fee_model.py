from __future__ import annotations

from copy import deepcopy
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Literal

from oziebot_domain.trading_mode import TradingMode

SETTING_EXECUTION_FEE_MODEL = "execution.fee_model"

ExecutionPreference = Literal["maker_preferred", "taker_allowed", "taker_only"]
FallbackBehavior = Literal["cancel", "reprice", "convert_to_taker"]
OrderFillType = Literal["maker", "taker", "mixed"]

_DEFAULT_SYMBOL_OVERRIDES: dict[str, dict[str, Any]] = {}

DEFAULT_FEE_MODEL_SETTINGS: dict[str, Any] = {
    "enabled": True,
    "paper": {
        "maker_fee_bps": 40,
        "taker_fee_bps": 60,
        "estimated_slippage_bps": 8,
        "spread_buffer_bps": 3,
        "safety_buffer_bps": 5,
        "coinbase_one_rebate_percent": 0,
    },
    "live": {
        "maker_fee_bps": 40,
        "taker_fee_bps": 60,
        "estimated_slippage_bps": 12,
        "spread_buffer_bps": 4,
        "safety_buffer_bps": 8,
        "coinbase_one_rebate_percent": 0,
    },
    "defaults": {
        "execution_preference": "maker_preferred",
        "entry_fill_type": "maker",
        "exit_fill_type": "taker",
        "limit_price_offset_bps": 2,
        "maker_timeout_seconds": 15,
        "fallback_behavior": "convert_to_taker",
        "min_notional_per_trade": 25,
        "min_expected_edge_bps": 25,
        "min_expected_net_profit_dollars": 0.5,
        "max_fee_percent_of_expected_profit": 0.65,
        "max_slippage_bps": 35,
        "skip_trade_if_fee_too_high": True,
    },
    "strategy_overrides": {
        "momentum": {
            "min_expected_edge_bps": 80,
            "entry_fill_type": "maker",
            "exit_fill_type": "taker",
            "execution_preference": "maker_preferred",
            "expected_gross_edge_bps": 800,
        },
        "day_trading": {
            "min_expected_edge_bps": 35,
            "entry_fill_type": "maker",
            "exit_fill_type": "taker",
            "execution_preference": "maker_preferred",
            "expected_gross_edge_bps": 150,
        },
        "reversion": {
            "min_expected_edge_bps": 45,
            "entry_fill_type": "maker",
            "exit_fill_type": "taker",
            "execution_preference": "maker_preferred",
            "expected_gross_edge_bps": 400,
        },
        "dca": {
            "execution_preference": "taker_allowed",
            "entry_fill_type": "taker",
            "exit_fill_type": "taker",
            "maker_timeout_seconds": 0,
            "fallback_behavior": "cancel",
            "min_expected_edge_bps": 0,
            "min_expected_net_profit_dollars": 0,
            "skip_trade_if_fee_too_high": False,
            "expected_gross_edge_bps": 120,
        },
    },
    "symbol_overrides": _DEFAULT_SYMBOL_OVERRIDES,
}


def default_fee_model_settings() -> dict[str, Any]:
    return deepcopy(DEFAULT_FEE_MODEL_SETTINGS)


def normalize_execution_preference(value: Any) -> ExecutionPreference:
    candidate = str(value or "maker_preferred").strip().lower()
    if candidate in {"maker_preferred", "taker_allowed", "taker_only"}:
        return candidate
    return "maker_preferred"


def normalize_fallback_behavior(value: Any) -> FallbackBehavior:
    candidate = str(value or "convert_to_taker").strip().lower()
    if candidate in {"cancel", "reprice", "convert_to_taker"}:
        return candidate
    return "convert_to_taker"


def normalize_fill_type(value: Any, *, default: OrderFillType) -> OrderFillType:
    candidate = str(value or default).strip().lower()
    if candidate in {"maker", "taker", "mixed"}:
        return candidate
    return default


def normalize_fee_model_settings(raw: Any) -> dict[str, Any]:
    merged = default_fee_model_settings()
    payload = raw if isinstance(raw, dict) else {}
    _merge_dict(merged, payload)
    return merged


def resolve_fee_profile(
    raw: Any,
    *,
    trading_mode: TradingMode | str,
    strategy_id: str,
    symbol: str,
) -> dict[str, Any]:
    settings = normalize_fee_model_settings(raw)
    mode_key = (
        trading_mode.value
        if isinstance(trading_mode, TradingMode)
        else str(trading_mode).strip().lower()
    )
    profile: dict[str, Any] = {}
    _merge_dict(profile, settings.get("defaults") or {})
    _merge_dict(profile, settings.get(mode_key) or {})
    _merge_dict(
        profile,
        (settings.get("strategy_overrides") or {}).get(str(strategy_id).strip().lower())
        or {},
    )
    _merge_dict(
        profile,
        (settings.get("symbol_overrides") or {}).get(str(symbol).strip().upper()) or {},
    )

    profile["enabled"] = bool(settings.get("enabled", True))
    profile["execution_preference"] = normalize_execution_preference(
        profile.get("execution_preference")
    )
    profile["fallback_behavior"] = normalize_fallback_behavior(
        profile.get("fallback_behavior")
    )
    profile["entry_fill_type"] = normalize_fill_type(
        profile.get("entry_fill_type"),
        default="maker"
        if profile["execution_preference"] == "maker_preferred"
        else "taker",
    )
    profile["exit_fill_type"] = normalize_fill_type(
        profile.get("exit_fill_type"),
        default="taker",
    )
    return profile


def calculate_round_trip_cost_bps(
    order_entry_type: OrderFillType | str,
    order_exit_type: OrderFillType | str,
    slippage_bps: int | float | Decimal,
    spread_buffer_bps: int | float | Decimal,
    safety_buffer_bps: int | float | Decimal,
    rebate_percent: int | float | Decimal = 0,
    *,
    maker_fee_bps: int | float | Decimal = 0,
    taker_fee_bps: int | float | Decimal = 0,
) -> int:
    maker_fee = Decimal(str(maker_fee_bps))
    taker_fee = Decimal(str(taker_fee_bps))
    total_fee = _fill_type_fee_bps(
        order_entry_type, maker_fee, taker_fee
    ) + _fill_type_fee_bps(order_exit_type, maker_fee, taker_fee)
    rebate_multiplier = Decimal("1") - (Decimal(str(rebate_percent)) / Decimal("100"))
    total_fee *= max(Decimal("0"), rebate_multiplier)
    total = (
        total_fee
        + Decimal(str(slippage_bps))
        + Decimal(str(spread_buffer_bps))
        + Decimal(str(safety_buffer_bps))
    )
    return int(total.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def is_trade_net_positive(
    expected_move_bps: int | float | Decimal,
    estimated_total_cost_bps: int | float | Decimal,
    minimum_net_edge_bps: int | float | Decimal,
) -> bool:
    net = Decimal(str(expected_move_bps)) - Decimal(str(estimated_total_cost_bps))
    return net >= Decimal(str(minimum_net_edge_bps))


def bps_to_decimal(value: int | float | Decimal) -> Decimal:
    return Decimal(str(value)) / Decimal("10000")


def estimate_signal_expected_edge_bps(
    *,
    strategy_id: str,
    action: str,
    config: dict[str, Any],
    fee_profile: dict[str, Any],
) -> int:
    if str(action).lower() not in {"buy", "sell", "close"}:
        return 0
    expected: Decimal
    slug = str(strategy_id).strip().lower()
    if slug == "momentum":
        expected = Decimal(str(config.get("take_profit_pct", 0.08))) * Decimal("10000")
    elif slug == "day_trading":
        expected = Decimal(str(config.get("exit_threshold", 0.015))) * Decimal("10000")
    elif slug == "reversion":
        expected = Decimal(str(config.get("take_profit_pct", 0.04))) * Decimal("10000")
    elif slug == "dca":
        expected = Decimal(str(fee_profile.get("expected_gross_edge_bps", 120)))
    else:
        expected = Decimal(str(fee_profile.get("expected_gross_edge_bps", 0)))
    return int(expected.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _fill_type_fee_bps(
    fill_type: OrderFillType | str,
    maker_fee_bps: Decimal,
    taker_fee_bps: Decimal,
) -> Decimal:
    normalized = normalize_fill_type(fill_type, default="taker")
    if normalized == "maker":
        return maker_fee_bps
    if normalized == "mixed":
        return (maker_fee_bps + taker_fee_bps) / Decimal("2")
    return taker_fee_bps


def _merge_dict(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge_dict(target[key], value)
        elif value is not None:
            target[key] = deepcopy(value)

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

GLOBAL_SIGNAL_RULE_DEFAULTS: dict[str, Any] = {
    "min_confidence": 0.6,
    "only_during_liquid_hours": False,
    "cooldown_seconds": 15,
    "max_signals_per_day": 0,
    "require_volume_confirmation": True,
}

_BASELINE_PLATFORM_CONFIGS: dict[str, dict[str, dict[str, Any]]] = {
    "momentum": {
        "strategy_params": {
            "short_window": 10,
            "long_window": 40,
            "strength_threshold": 0.02,
            "position_size_fraction": 0.25,
            "stop_loss_pct": 0.035,
            "take_profit_pct": 0.045,
            "trailing_stop_pct": 0.018,
            "partial_take_profit_pct": 0.02,
            "partial_take_profit_fraction": 0.5,
            "min_volume_multiplier": 1.2,
            "max_hold_minutes": 300,
            "dynamic_sizing_enabled": True,
            "min_trade_usd": 75,
            "max_trade_usd": 300,
            "target_bucket_utilization_pct": 0.65,
            "drawdown_size_reduction_enabled": True,
            "drawdown_reduction_multiplier": 0.75,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "min_confidence": 0.6,
        },
        "risk_caps": {
            "max_position_usd": 300,
        },
    },
    "day_trading": {
        "strategy_params": {
            "entry_threshold": 0.012,
            "exit_threshold": 0.02,
            "stop_loss_pct": 0.008,
            "position_size_fraction": 0.15,
            "min_volume_multiplier": 1.8,
            "min_volatility_pct": 0.008,
            "require_trend_alignment": True,
            "min_entry_confirmations": 3,
            "max_position_age_hours": 3,
            "breakout_lookback_candles": 5,
            "trailing_stop_pct": 0.01,
            "trailing_stop_activation_pct": 0.02,
            "partial_take_profit_pct": 0.02,
            "partial_take_profit_fraction": 0.5,
            "dynamic_sizing_enabled": True,
            "min_trade_usd": 50,
            "max_trade_usd": 200,
            "target_bucket_utilization_pct": 0.55,
            "drawdown_size_reduction_enabled": True,
            "drawdown_reduction_multiplier": 0.75,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
        },
        "risk_caps": {
            "max_position_usd": 200,
        },
    },
    "reversion": {
        "strategy_params": {
            "band_window": 20,
            "rsi_period": 14,
            "zscore_entry": 2.0,
            "zscore_exit": 0.5,
            "rsi_buy": 30,
            "rsi_exit": 50,
            "rsi_sell": 65,
            "min_bandwidth": 0.012,
            "use_trend_filter": True,
            "ema_long_window": 200,
            "position_size_fraction": 0.10,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.04,
            "max_hold_minutes": 120,
            "dynamic_sizing_enabled": True,
            "min_trade_usd": 30,
            "max_trade_usd": 100,
            "target_bucket_utilization_pct": 0.45,
            "drawdown_size_reduction_enabled": True,
            "drawdown_reduction_multiplier": 0.75,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "min_confidence": 0.6,
        },
        "risk_caps": {
            "max_position_usd": 100,
        },
    },
    "dca": {
        "strategy_params": {
            "buy_amount_usd": 100,
            "buy_interval_hours": 24,
            "only_on_green_days": False,
            "dynamic_sizing_enabled": True,
            "min_trade_usd": 100,
            "max_trade_usd": 150,
            "target_bucket_utilization_pct": 0.50,
            "drawdown_size_reduction_enabled": True,
            "drawdown_reduction_multiplier": 0.75,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "min_confidence": 0.9,
        },
        "risk_caps": {},
    },
    "strategic_aggressive_allocation": {
        "strategy_params": {
            "evaluation_interval_minutes": 60,
            "minimum_order_size_usd": 25,
            "max_total_open_positions": 10,
            "target_bucket_utilization_pct": 0.85,
            "drawdown_size_reduction_enabled": False,
            "drawdown_reduction_multiplier": 1.0,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "min_confidence": 0.6,
        },
        "risk_caps": {
            "max_open_positions": 10,
        },
    },
    "volatility_harvest": {
        "strategy_params": {
            "core_position_percentage": 70,
            "trading_position_percentage": 30,
            "evaluation_interval_minutes": 30,
            "minimum_order_size_usd": 25,
            "minimum_net_profit_after_fees_usd": 3,
            "daily_max_sell_count": 4,
            "daily_max_rebuy_count": 3,
            "cooldown_minutes_between_actions": 90,
            "max_allocation_per_token_pct": 22.5,
            "emergency_stop_loss_pct": 18,
            "atr_reference_pct": 0.035,
            "atr_band_widening_multiplier": 1.35,
            "rsi_rebuy_threshold": 35,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "min_confidence": 0.6,
        },
        "risk_caps": {
            "max_position_usd": 500,
            "max_open_positions": 8,
        },
    },
}

_STRATEGY_PARAM_ALIASES: dict[str, dict[str, str]] = {
    "momentum": {
        "entry_threshold": "strength_threshold",
        "position_size": "position_size_fraction",
        "short_ma_window": "short_window",
        "long_ma_window": "long_window",
    },
    "day_trading": {
        "stop_loss": "stop_loss_pct",
        "min_entry_signals": "min_entry_confirmations",
        "position_size": "position_size_fraction",
    },
    "reversion": {
        "entry_zscore": "zscore_entry",
        "exit_zscore": "zscore_exit",
        "rsi_buy_threshold": "rsi_buy",
        "rsi_exit_threshold": "rsi_exit",
        "rsi_sell_threshold": "rsi_sell",
        "position_size": "position_size_fraction",
        "min_bandwidth_pct": "min_bandwidth",
    },
}

_COMMON_STRATEGY_PARAM_KEYS = {
    "max_spread_pct",
    "max_slippage_pct",
    "fee_pct",
    "expected_profit_buffer_pct",
    "dynamic_sizing_enabled",
    "min_trade_usd",
    "max_trade_usd",
    "target_bucket_utilization_pct",
    "drawdown_size_reduction_enabled",
    "drawdown_reduction_multiplier",
}

_SIGNAL_RULE_KEYS = {
    "min_confidence",
    "only_during_liquid_hours",
    "cooldown_seconds",
    "max_signals_per_day",
    "paper_only",
    "require_volume_confirmation",
    "skip_if_spread_bps_over",
}

_RISK_CAP_KEYS = {
    "max_position_usd",
    "max_daily_loss_pct",
    "max_open_positions",
    "max_exposure_per_strategy",
    "max_exposure_per_token",
    "max_consecutive_losses",
    "loss_cooldown_minutes",
}


def strategy_platform_config(strategy_id: str) -> dict[str, dict[str, Any]]:
    slug = str(strategy_id).strip().lower()
    baseline = _BASELINE_PLATFORM_CONFIGS.get(slug)
    if baseline is None:
        return {"strategy_params": {}, "signal_rules": {}, "risk_caps": {}}
    return deepcopy(baseline)


def normalize_platform_strategy_config(
    strategy_id: str, raw_config: Any
) -> dict[str, dict[str, Any]]:
    slug = str(strategy_id).strip().lower()
    normalized = strategy_platform_config(slug)
    raw = _as_dict(raw_config)
    if not raw:
        return normalized

    if any(key in raw for key in ("strategy_params", "signal_rules", "risk_caps")):
        strategy_params = _as_dict(raw.get("strategy_params"))
        signal_rules = _as_dict(raw.get("signal_rules"))
        risk_caps = _as_dict(raw.get("risk_caps"))
    else:
        strategy_params, signal_rules, risk_caps = _split_flat_config(slug, raw)

    normalized["strategy_params"].update(
        _normalize_strategy_params(slug, strategy_params)
    )
    normalized["signal_rules"].update(_sanitize_values(signal_rules))
    normalized["risk_caps"].update(_sanitize_values(risk_caps))
    return normalized


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return {}
    return dict(value) if isinstance(value, dict) else {}


def _split_flat_config(
    strategy_id: str, raw: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    strategy_param_keys = _strategy_param_keys(strategy_id)
    strategy_params: dict[str, Any] = {}
    signal_rules: dict[str, Any] = {}
    risk_caps: dict[str, Any] = {}

    for key, value in raw.items():
        if key in _SIGNAL_RULE_KEYS:
            signal_rules[key] = value
        elif key in _RISK_CAP_KEYS:
            risk_caps[key] = value
        elif key in strategy_param_keys:
            strategy_params[key] = value

    return strategy_params, signal_rules, risk_caps


def _strategy_param_keys(strategy_id: str) -> set[str]:
    baseline = _BASELINE_PLATFORM_CONFIGS.get(strategy_id, {})
    keys = set(_COMMON_STRATEGY_PARAM_KEYS)
    keys.update(baseline.get("strategy_params", {}).keys())
    keys.update(_STRATEGY_PARAM_ALIASES.get(strategy_id, {}).keys())
    return keys


def _normalize_strategy_params(
    strategy_id: str, params: dict[str, Any]
) -> dict[str, Any]:
    aliases = _STRATEGY_PARAM_ALIASES.get(strategy_id, {})
    normalized: dict[str, Any] = {}
    for key, value in params.items():
        canonical = aliases.get(key, key)
        normalized[canonical] = value
    return _sanitize_values(normalized)


def _sanitize_values(values: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None}

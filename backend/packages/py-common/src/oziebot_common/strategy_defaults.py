from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

GLOBAL_SIGNAL_RULE_DEFAULTS: dict[str, Any] = {
    "min_confidence": 0.55,
    "only_during_liquid_hours": False,
    "cooldown_seconds": 30,
    "max_signals_per_day": 150,
}

_BASELINE_PLATFORM_CONFIGS: dict[str, dict[str, dict[str, Any]]] = {
    "momentum": {
        "strategy_params": {
            "short_window": 8,
            "long_window": 34,
            "strength_threshold": 0.012,
            "position_size_fraction": 0.12,
            "stop_loss_pct": 0.035,
            "take_profit_pct": 0.08,
            "trailing_stop_pct": 0.03,
            "max_hold_minutes": 300,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "min_confidence": 0.6,
            "cooldown_seconds": 45,
        },
        "risk_caps": {
            "max_position_usd": 120,
        },
    },
    "day_trading": {
        "strategy_params": {
            "entry_threshold": 0.007,
            "exit_threshold": 0.015,
            "stop_loss_pct": 0.008,
            "position_size_fraction": 0.08,
            "min_volume_multiplier": 1.3,
            "min_volatility_pct": 0.005,
            "require_trend_alignment": True,
            "min_entry_confirmations": 1,
            "max_position_age_hours": 3,
            "breakout_lookback_candles": 5,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "cooldown_seconds": 20,
        },
        "risk_caps": {
            "max_position_usd": 80,
        },
    },
    "reversion": {
        "strategy_params": {
            "band_window": 20,
            "rsi_period": 14,
            "zscore_entry": 1.6,
            "zscore_exit": 0.4,
            "rsi_buy": 30,
            "rsi_exit": 50,
            "rsi_sell": 65,
            "min_bandwidth": 0.012,
            "use_trend_filter": True,
            "ema_long_window": 200,
            "position_size_fraction": 0.05,
            "stop_loss_pct": 0.025,
            "take_profit_pct": 0.04,
            "max_hold_minutes": 120,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "min_confidence": 0.6,
            "cooldown_seconds": 60,
        },
        "risk_caps": {
            "max_position_usd": 50,
        },
    },
    "dca": {
        "strategy_params": {
            "buy_amount_usd": 50,
            "buy_interval_hours": 24,
            "only_on_green_days": False,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "min_confidence": 0.9,
        },
        "risk_caps": {},
    },
}

_STRATEGY_PARAM_ALIASES: dict[str, dict[str, str]] = {
    "momentum": {
        "position_size": "position_size_fraction",
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

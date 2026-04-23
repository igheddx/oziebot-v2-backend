"""raise baseline dynamic sizing defaults

Revision ID: 026_dynamic_sizing_defaults
Revises: 025_user_full_name
Create Date: 2026-04-23 00:30:00.000000
"""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "026_dynamic_sizing_defaults"
down_revision: Union[str, None] = "025_user_full_name"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


GLOBAL_SIGNAL_RULE_DEFAULTS: dict[str, Any] = {
    "min_confidence": 0.55,
    "only_during_liquid_hours": False,
    "cooldown_seconds": 30,
    "max_signals_per_day": 150,
}

BASELINE_PLATFORM_CONFIGS: dict[str, dict[str, dict[str, Any]]] = {
    "momentum": {
        "strategy_params": {
            "short_window": 8,
            "long_window": 34,
            "strength_threshold": 0.012,
            "position_size_fraction": 0.25,
            "stop_loss_pct": 0.035,
            "take_profit_pct": 0.08,
            "trailing_stop_pct": 0.03,
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
            "cooldown_seconds": 45,
        },
        "risk_caps": {"max_position_usd": 300},
    },
    "day_trading": {
        "strategy_params": {
            "entry_threshold": 0.007,
            "exit_threshold": 0.015,
            "stop_loss_pct": 0.008,
            "position_size_fraction": 0.15,
            "min_volume_multiplier": 1.3,
            "min_volatility_pct": 0.005,
            "require_trend_alignment": True,
            "min_entry_confirmations": 1,
            "max_position_age_hours": 3,
            "breakout_lookback_candles": 5,
            "dynamic_sizing_enabled": True,
            "min_trade_usd": 50,
            "max_trade_usd": 200,
            "target_bucket_utilization_pct": 0.55,
            "drawdown_size_reduction_enabled": True,
            "drawdown_reduction_multiplier": 0.75,
        },
        "signal_rules": {
            **GLOBAL_SIGNAL_RULE_DEFAULTS,
            "cooldown_seconds": 20,
        },
        "risk_caps": {"max_position_usd": 200},
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
            "position_size_fraction": 0.10,
            "stop_loss_pct": 0.025,
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
            "cooldown_seconds": 60,
        },
        "risk_caps": {"max_position_usd": 100},
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
}

ALIASES: dict[str, dict[str, str]] = {
    "momentum": {"position_size": "position_size_fraction"},
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

SIGNAL_RULE_KEYS = {
    "min_confidence",
    "only_during_liquid_hours",
    "cooldown_seconds",
    "max_signals_per_day",
    "paper_only",
    "require_volume_confirmation",
    "skip_if_spread_bps_over",
}

RISK_CAP_KEYS = {
    "max_position_usd",
    "max_daily_loss_pct",
    "max_open_positions",
    "max_exposure_per_strategy",
    "max_exposure_per_token",
    "max_consecutive_losses",
    "loss_cooldown_minutes",
}

COMMON_STRATEGY_PARAM_KEYS = {
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


def upgrade() -> None:
    bind = op.get_bind()
    platform_strategies = sa.table(
        "platform_strategies",
        sa.column("id", sa.Uuid()),
        sa.column("slug", sa.String()),
        sa.column("config_schema", sa.JSON()),
    )

    rows = bind.execute(
        sa.select(
            platform_strategies.c.id,
            platform_strategies.c.slug,
            platform_strategies.c.config_schema,
        ).where(platform_strategies.c.slug.in_(tuple(BASELINE_PLATFORM_CONFIGS.keys())))
    ).all()

    for row in rows:
        bind.execute(
            platform_strategies.update()
            .where(platform_strategies.c.id == row.id)
            .values(config_schema=_normalize_platform_config(str(row.slug), row.config_schema))
        )


def downgrade() -> None:
    pass


def _normalize_platform_config(
    strategy_id: str, raw_config: Any
) -> dict[str, dict[str, Any]]:
    normalized = deepcopy(BASELINE_PLATFORM_CONFIGS[strategy_id])
    raw = _as_dict(raw_config)
    if not raw:
        return normalized

    if any(key in raw for key in ("strategy_params", "signal_rules", "risk_caps")):
        strategy_params = _as_dict(raw.get("strategy_params"))
        signal_rules = _as_dict(raw.get("signal_rules"))
        risk_caps = _as_dict(raw.get("risk_caps"))
    else:
        strategy_params, signal_rules, risk_caps = _split_flat_config(strategy_id, raw)

    normalized["strategy_params"].update(_normalize_strategy_params(strategy_id, strategy_params))
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
    strategy_param_keys = set(COMMON_STRATEGY_PARAM_KEYS)
    strategy_param_keys.update(BASELINE_PLATFORM_CONFIGS[strategy_id]["strategy_params"].keys())
    strategy_param_keys.update(ALIASES.get(strategy_id, {}).keys())

    strategy_params: dict[str, Any] = {}
    signal_rules: dict[str, Any] = {}
    risk_caps: dict[str, Any] = {}
    for key, value in raw.items():
        if key in SIGNAL_RULE_KEYS:
            signal_rules[key] = value
        elif key in RISK_CAP_KEYS:
            risk_caps[key] = value
        elif key in strategy_param_keys:
            strategy_params[key] = value
    return strategy_params, signal_rules, risk_caps


def _normalize_strategy_params(strategy_id: str, params: dict[str, Any]) -> dict[str, Any]:
    aliases = ALIASES.get(strategy_id, {})
    normalized: dict[str, Any] = {}
    for key, value in params.items():
        normalized[aliases.get(key, key)] = value
    return _sanitize_values(normalized)


def _sanitize_values(values: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in values.items() if value is not None}

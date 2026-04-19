"""Shared non-domain utilities (queues, Redis, token policy helpers)."""

from oziebot_common.health import HealthState, start_health_server
from oziebot_common.fee_model import (
    DEFAULT_FEE_MODEL_SETTINGS,
    SETTING_EXECUTION_FEE_MODEL,
    bps_to_decimal,
    calculate_round_trip_cost_bps,
    default_fee_model_settings,
    estimate_signal_expected_edge_bps,
    is_trade_net_positive,
    normalize_fee_model_settings,
    resolve_fee_profile,
)
from oziebot_common.queues import (
    QueueNames,
    brpop_json_any,
    redis_from_url,
    redis_url_candidates,
)
from oziebot_common.strategy_defaults import (
    GLOBAL_SIGNAL_RULE_DEFAULTS,
    normalize_platform_strategy_config,
    strategy_platform_config,
)
from oziebot_common.token_policy import (
    DISCOURAGED_SIZE_MULTIPLIER,
    TOKEN_POLICY_RECOMMENDATIONS,
    TOKEN_POLICY_STRATEGIES,
    BboSample,
    CandleSample,
    StrategySuitabilityResult,
    TokenMarketProfileResult,
    TradeSample,
    compute_market_profile,
    resolve_effective_token_policy,
    score_strategy_suitability,
)
from oziebot_common.trade_log import (
    MAX_TRADE_LOG_LIMIT,
    MAX_TRADE_LOG_WINDOW_SECONDS,
    TRADE_LOG_REDIS_KEY,
    append_trade_log_event,
    build_trade_log_event,
    read_trade_log_events,
)

__all__ = [
    "BboSample",
    "CandleSample",
    "DISCOURAGED_SIZE_MULTIPLIER",
    "DEFAULT_FEE_MODEL_SETTINGS",
    "GLOBAL_SIGNAL_RULE_DEFAULTS",
    "HealthState",
    "QueueNames",
    "SETTING_EXECUTION_FEE_MODEL",
    "StrategySuitabilityResult",
    "TRADE_LOG_REDIS_KEY",
    "TOKEN_POLICY_RECOMMENDATIONS",
    "TOKEN_POLICY_STRATEGIES",
    "TokenMarketProfileResult",
    "TradeSample",
    "MAX_TRADE_LOG_LIMIT",
    "MAX_TRADE_LOG_WINDOW_SECONDS",
    "append_trade_log_event",
    "bps_to_decimal",
    "build_trade_log_event",
    "calculate_round_trip_cost_bps",
    "default_fee_model_settings",
    "estimate_signal_expected_edge_bps",
    "brpop_json_any",
    "compute_market_profile",
    "is_trade_net_positive",
    "normalize_fee_model_settings",
    "normalize_platform_strategy_config",
    "redis_from_url",
    "redis_url_candidates",
    "read_trade_log_events",
    "resolve_effective_token_policy",
    "resolve_fee_profile",
    "score_strategy_suitability",
    "start_health_server",
    "strategy_platform_config",
]

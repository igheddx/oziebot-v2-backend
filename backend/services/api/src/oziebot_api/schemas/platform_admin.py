from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field


class SettingValueBody(BaseModel):
    value: dict[str, Any]


class GlobalPauseBody(BaseModel):
    paused: bool
    reason: str | None = None


class TokenAllowlistCreate(BaseModel):
    symbol: str = Field(min_length=1, max_length=64)
    quote_currency: str = "USD"
    network: str = "mainnet"
    contract_address: str | None = None
    display_name: str | None = None
    is_enabled: bool = True
    sort_order: int = 0
    extra: dict[str, Any] | None = None


class TokenAllowlistPatch(BaseModel):
    symbol: str | None = None
    quote_currency: str | None = None
    network: str | None = None
    contract_address: str | None = None
    display_name: str | None = None
    is_enabled: bool | None = None
    sort_order: int | None = None
    extra: dict[str, Any] | None = None


class TokenStrategyPolicyPatch(BaseModel):
    is_enabled: bool | None = None
    admin_enabled: bool | None = None
    recommendation_status: Literal["preferred", "allowed", "discouraged", "blocked"] | None = None
    recommendation_reason: str | None = None
    size_multiplier: Annotated[float | None, Field(default=None, ge=0, le=1)] = None
    max_position_usd_override: Annotated[float | None, Field(default=None, ge=0)] = None
    max_position_pct_override: Annotated[float | None, Field(default=None, ge=0, le=1)] = None
    notes: str | None = None


class TokenPolicyTokenSummary(BaseModel):
    id: str
    symbol: str
    quote_currency: str
    display_name: str | None = None
    is_enabled: bool
    extra: dict[str, Any] | None = None


class TokenMarketProfileResponse(BaseModel):
    liquidity_score: float
    spread_score: float
    volatility_score: float
    trend_score: float
    reversion_score: float
    slippage_score: float
    avg_daily_volume_usd: float
    avg_spread_pct: float
    avg_intraday_volatility_pct: float
    last_computed_at: str
    raw_metrics_json: dict[str, Any] | None = None


class TokenPolicyProfileEntry(BaseModel):
    token: TokenPolicyTokenSummary
    market_profile: TokenMarketProfileResponse | None = None


class TokenStrategyPolicyResponse(BaseModel):
    id: str
    strategy_id: str
    strategy_display_name: str | None = None
    is_enabled: bool
    admin_enabled: bool
    suitability_score: float
    computed_recommendation_status: str
    computed_recommendation_reason: str | None = None
    effective_recommendation_status: str
    effective_recommendation_reason: str | None = None
    recommendation_status: str
    recommendation_reason: str | None = None
    recommendation_status_override: str | None = None
    recommendation_reason_override: str | None = None
    size_multiplier: float
    configured_size_multiplier: float | None = None
    max_position_usd_override: float | None = None
    max_position_pct_override: float | None = None
    notes: str | None = None
    created_at: str | None = None
    computed_at: str | None = None
    updated_at: str | None = None


class TokenPolicyDefaultsResponse(BaseModel):
    tokens_processed: int
    policies_written: int
    updated_symbols: list[str]


class TokenPolicyExportTokenEntry(BaseModel):
    token: TokenPolicyTokenSummary
    market_profile: TokenMarketProfileResponse | None = None
    strategies: dict[str, TokenStrategyPolicyResponse]


class TokenPolicyExportResponse(BaseModel):
    generated_at: str
    default_missing_policy_behavior: Literal["allowed", "blocked"]
    tokens: list[TokenPolicyExportTokenEntry]
    matrix: dict[str, dict[str, TokenStrategyPolicyResponse]]


class TokenPolicyDetailResponse(BaseModel):
    token: TokenPolicyTokenSummary
    market_profile: TokenMarketProfileResponse | None = None
    strategy_policies: list[TokenStrategyPolicyResponse]


class TradingDiagnosticsTradeDetail(BaseModel):
    trade_id: str
    strategy: str
    token: str
    trading_mode: str
    entry_time: str | None = None
    exit_time: str | None = None
    hold_minutes: float | None = None
    entry_price: float | None = None
    exit_price: float | None = None
    quantity: float | None = None
    size_usd: float | None = None
    fees_usd: float | None = None
    gross_pnl_usd: float | None = None
    net_pnl_usd: float | None = None
    pnl_pct: float | None = None
    exit_reason: str | None = None
    partial_profit_taken: bool | None = None
    max_favorable_excursion_pct: float | None = None
    max_adverse_excursion_pct: float | None = None
    peak_unrealized_pnl_pct: float | None = None
    profit_giveback_pct: float | None = None
    signal_confidence: float | None = None
    volume_confirmation_passed: bool | None = None
    rejected_before_execution: bool | None = None


class TradingDiagnosticsStrategySummary(BaseModel):
    strategy: str
    total_trades: int
    wins: int
    losses: int
    win_rate_pct: float | None = None
    avg_win_pct: float | None = None
    avg_loss_pct: float | None = None
    profit_factor: float | None = None
    total_net_pnl_usd: float | None = None
    total_net_pnl_pct: float | None = None
    max_drawdown_pct: float | None = None
    avg_hold_minutes: float | None = None
    stop_loss_exits: int
    take_profit_exits: int
    trailing_stop_exits: int
    partial_profit_exits: int
    max_hold_exits: int
    bearish_signal_exits: int
    avg_profit_giveback_pct: float | None = None


class TradingDiagnosticsTokenSummary(BaseModel):
    token: str
    total_trades: int
    win_rate_pct: float | None = None
    total_net_pnl_usd: float | None = None
    total_net_pnl_pct: float | None = None
    avg_trade_return_pct: float | None = None
    avg_hold_minutes: float | None = None
    avg_profit_giveback_pct: float | None = None
    best_strategy: str | None = None
    worst_strategy: str | None = None


class TradingDiagnosticsExecutionDetail(BaseModel):
    execution_trade_id: str
    order_id: str
    strategy: str
    token: str
    trading_mode: str
    side: str
    executed_at: str | None = None
    quantity: float | None = None
    price_usd: float | None = None
    notional_usd: float | None = None
    fees_usd: float | None = None
    realized_pnl_usd: float | None = None
    position_quantity_after: float | None = None
    position_closed: bool


class TradingDiagnosticsExecutionStrategySummary(BaseModel):
    strategy: str
    trading_mode: str
    total_executions: int
    buy_executions: int
    sell_executions: int
    flattened_executions: int
    total_notional_usd: float | None = None
    total_fees_usd: float | None = None
    total_realized_pnl_usd: float | None = None
    last_executed_at: str | None = None


class TradingDiagnosticsExecutionTokenSummary(BaseModel):
    token: str
    trading_mode: str
    total_executions: int
    buy_executions: int
    sell_executions: int
    flattened_executions: int
    total_notional_usd: float | None = None
    total_fees_usd: float | None = None
    total_realized_pnl_usd: float | None = None
    last_executed_at: str | None = None


class TradingDiagnosticsExecutionActivity(BaseModel):
    execution_count: int
    flattened_trade_count: int
    buy_count: int
    sell_count: int
    unique_tokens: int
    total_notional_usd: float | None = None
    total_fees_usd: float | None = None
    total_realized_pnl_usd: float | None = None
    data_source: str
    note: str | None = None
    strategy_summary: list[TradingDiagnosticsExecutionStrategySummary]
    token_summary: list[TradingDiagnosticsExecutionTokenSummary]
    execution_details: list[TradingDiagnosticsExecutionDetail]


class TradingDiagnosticsOpenPosition(BaseModel):
    position_id: str
    strategy: str
    token: str
    trading_mode: str
    quantity: float | None = None
    avg_entry_price: float | None = None
    position_notional_usd: float | None = None
    realized_pnl_usd: float | None = None
    opened_at: str | None = None
    last_trade_at: str | None = None
    updated_at: str | None = None
    closed_at: str | None = None


class TradingDiagnosticsOpenPositions(BaseModel):
    position_count: int
    unique_tokens: int
    total_position_notional_usd: float | None = None
    total_realized_pnl_usd: float | None = None
    exposure_by_strategy: dict[str, float | None]
    data_source: str
    note: str | None = None
    positions: list[TradingDiagnosticsOpenPosition]


class TradingDiagnosticsRejectionReasons(BaseModel):
    confidence: int | None = None
    volume: int | None = None
    allocation: int | None = None
    risk_engine: int | None = None
    token_strategy_policy: int | None = None
    cooldown: int | None = None
    liquidity_hours: int | None = None
    other: int | None = None


class TradingDiagnosticsSignalFunnel(BaseModel):
    signals_evaluated: int | None = None
    signals_emitted: int | None = None
    signals_rejected: int | None = None
    trades_executed: int
    rejection_reasons: TradingDiagnosticsRejectionReasons
    data_sources: dict[str, str] = Field(default_factory=dict)
    unavailable_metrics: list[str] = Field(default_factory=list)
    note: str | None = None


class TradingDiagnosticsCapitalUtilization(BaseModel):
    total_account_value: float | None = None
    avg_capital_deployed_pct: float | None = None
    peak_capital_deployed_pct: float | None = None
    avg_cash_idle_pct: float | None = None
    capital_by_strategy: dict[str, float | None]
    note: str | None = None


class TradingDiagnosticsExitAnalysis(BaseModel):
    most_common_exit_reason: str | None = None
    stop_loss_rate_pct: float | None = None
    avg_profit_before_trailing_exit_pct: float | None = None
    avg_profit_before_reversal_pct: float | None = None
    partial_take_profit_effectiveness_pct: float | None = None
    trades_that_were_positive_before_loss_pct: float | None = None


class TradingDiagnosticsActiveStrategyConfig(BaseModel):
    momentum_config: dict[str, Any] | None = None
    day_trading_config: dict[str, Any] | None = None
    reversion_config: dict[str, Any] | None = None
    dca_config: dict[str, Any] | None = None
    signal_rules: dict[str, dict[str, Any]]
    token_strategy_policy_matrix: dict[str, dict[str, TokenStrategyPolicyResponse]]
    default_missing_policy_behavior: Literal["allowed", "blocked"]


class TradingDiagnosticsResponse(BaseModel):
    generated_at: str
    trade_count: int
    trade_details: list[TradingDiagnosticsTradeDetail]
    strategy_summary: list[TradingDiagnosticsStrategySummary]
    token_summary: list[TradingDiagnosticsTokenSummary]
    execution_activity: TradingDiagnosticsExecutionActivity
    open_positions: TradingDiagnosticsOpenPositions
    signal_funnel: TradingDiagnosticsSignalFunnel
    capital_utilization: TradingDiagnosticsCapitalUtilization
    exit_analysis: TradingDiagnosticsExitAnalysis
    active_strategy_config: TradingDiagnosticsActiveStrategyConfig


class StrategyLifecycleFunnelStage(BaseModel):
    stage: str
    trace_count: int
    failed_count: int
    conversion_from_previous_pct: float | None = None


class StrategyLifecycleFailureReason(BaseModel):
    reason_code: str
    failure_count: int


class StrategyLifecycleStageFailure(BaseModel):
    stage: str
    failure_count: int
    top_reasons: list[StrategyLifecycleFailureReason]


class StrategyLifecycleOpenPosition(BaseModel):
    position_id: str
    strategy: str
    token: str
    trading_mode: str
    quantity: float | None = None
    avg_entry_price: float | None = None
    opened_at: str | None = None
    updated_at: str | None = None
    last_trade_at: str | None = None
    latest_correlation_id: str | None = None
    has_exit_request: bool
    is_stuck_open: bool


class StrategyLifecycleSummary(BaseModel):
    trace_count: int
    blocked_by_policy: int
    blocked_by_risk: int
    execution_failures: int
    exit_engine_failures: int
    positions_without_exits: int
    stuck_open_positions: int
    closed_positions: int


class StrategyLifecycleTraceEvent(BaseModel):
    stage: str
    status: str
    occurred_at: str
    side: str | None = None
    reason_code: str | None = None
    reason_detail: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class StrategyLifecycleTrace(BaseModel):
    correlation_id: str
    strategy: str
    token: str
    trading_mode: str
    current_stage: str
    current_status: str
    started_at: str | None = None
    last_event_at: str | None = None
    latest_reason_code: str | None = None
    latest_reason_detail: str | None = None
    events: list[StrategyLifecycleTraceEvent]


class StrategyLifecycleDiagnosticsResponse(BaseModel):
    generated_at: str
    summary: StrategyLifecycleSummary
    funnel: list[StrategyLifecycleFunnelStage]
    stage_failures: list[StrategyLifecycleStageFailure]
    open_positions: list[StrategyLifecycleOpenPosition]
    latest_traces: list[StrategyLifecycleTrace]
    data_sources: list[str] = Field(default_factory=list)
    note: str | None = None


class StrategyLifecycleTraceListResponse(BaseModel):
    generated_at: str
    trace_count: int
    traces: list[StrategyLifecycleTrace]


class TokenPolicySizingImpact(BaseModel):
    original_size: str | None = None
    final_size: str | None = None
    size_multiplier: str | None = None
    max_position_usd_override: str | None = None
    max_position_pct_override: str | None = None
    requested_quantity: str | None = None


class TokenPolicyDecisionResponse(BaseModel):
    record_id: str
    enforced_in: str
    strategy_name: str
    token: str
    trading_mode: str
    computed_recommendation_status: str
    effective_recommendation_status: str
    admin_enabled: bool
    confidence_score: float | None = None
    final_sizing_impact: TokenPolicySizingImpact
    decision_outcome: Literal["emitted", "reduced", "rejected", "executed"]
    decision_reason: str | None = None
    timestamp: str


class StrategyCatalogCreate(BaseModel):
    slug: str = Field(min_length=1, max_length=64)
    display_name: str = Field(min_length=1, max_length=256)
    description: str | None = None
    is_enabled: bool = True
    entry_point: str | None = None
    config_schema: dict[str, Any] | None = None
    sort_order: int = 0


class StrategyCatalogPatch(BaseModel):
    display_name: str | None = None
    description: str | None = None
    is_enabled: bool | None = None
    entry_point: str | None = None
    config_schema: dict[str, Any] | None = None
    sort_order: int | None = None


class SubscriptionPlanCreate(BaseModel):
    slug: str = Field(min_length=1, max_length=64)
    display_name: str = Field(min_length=1, max_length=256)
    description: str | None = None
    plan_kind: Literal["all_strategies", "per_strategy"] = "all_strategies"
    stripe_price_id: str = Field(min_length=1, max_length=255)
    stripe_product_id: str | None = None
    billing_interval: str = Field(pattern="^(month|year)$")
    amount_cents: int = Field(ge=0)
    currency: str = "usd"
    is_active: bool = True
    features: dict[str, Any] | None = None
    trial_days_override: int | None = Field(default=None, ge=0)
    sort_order: int = 0


class SubscriptionPlanPatch(BaseModel):
    display_name: str | None = None
    description: str | None = None
    plan_kind: Literal["all_strategies", "per_strategy"] | None = None
    stripe_price_id: str | None = None
    stripe_product_id: str | None = None
    billing_interval: str | None = None
    amount_cents: int | None = Field(default=None, ge=0)
    currency: str | None = None
    is_active: bool | None = None
    features: dict[str, Any] | None = None
    trial_days_override: int | None = None
    sort_order: int | None = None


class TrialPolicyBody(BaseModel):
    is_enabled: bool = True
    trial_duration_days: int = Field(ge=0, le=365)
    max_trials_per_tenant: int = Field(ge=0, le=100)
    grace_period_days: int = Field(ge=0, le=90)
    policy_metadata: dict[str, Any] | None = None


class TenantCoinbaseHealthPatch(BaseModel):
    """Simulate or record health probe (admin / cron)."""

    health_status: str | None = Field(
        default=None,
        description="healthy | unhealthy | unknown",
    )
    last_error: str | None = None
    connected: bool | None = None


class DiagnosticSnapshotResponse(BaseModel):
    id: str
    generated_at: str
    trading_mode: str
    strategy_filter: str
    token_filter: str | None = None
    days_filter: int
    created_at: str


class DiagnosticSnapshotListResponse(BaseModel):
    snapshots: list[DiagnosticSnapshotResponse]


class AiDiagnosticReviewCreate(BaseModel):
    snapshot_id: str | None = None
    trading_mode: Literal["paper", "live", "all"] = "all"
    strategy: str = "all"
    token: str | None = None
    days: int = Field(default=7, ge=1, le=365)


class AiDiagnosticReviewCreateResponse(BaseModel):
    review_id: str
    status: Literal["queued", "running", "completed", "failed"]


class AiDiagnosticFindingResponse(BaseModel):
    id: str
    review_id: str
    severity: Literal["critical", "warning", "info"]
    category: str
    strategy: str | None = None
    token: str | None = None
    finding_title: str
    finding_detail: str
    evidence_json: dict[str, Any] = Field(default_factory=dict)
    recommendation: str
    risk_if_ignored: str | None = None
    confidence_score: float | None = None
    automation_eligibility: Literal[
        "not_eligible",
        "future_human_approval_required",
        "future_auto_tune_candidate",
    ]
    status: Literal["new", "acknowledged", "dismissed", "resolved"]
    future_config_change_candidate: bool
    proposed_config_change_json: dict[str, Any] | None = None
    approval_required: bool
    eligible_for_auto_tune: bool
    rollback_plan: str | None = None
    expected_impact: str | None = None
    risk_level: str | None = None
    affected_strategy: str | None = None
    affected_token: str | None = None
    parameter_name: str | None = None
    current_value_json: dict[str, Any] | None = None
    proposed_value_json: dict[str, Any] | None = None
    created_at: str
    updated_at: str


class AiDiagnosticFindingStatusPatch(BaseModel):
    status: Literal["acknowledged", "dismissed", "resolved"]
    note: str | None = None


class AiDiagnosticReviewSummaryResponse(BaseModel):
    id: str
    snapshot_id: str
    status: Literal["queued", "running", "completed", "failed"]
    overall_health: Literal["healthy", "warning", "critical"] | None = None
    confidence_score: float | None = None
    summary: str | None = None
    model_name: str
    prompt_version: str
    started_at: str | None = None
    completed_at: str | None = None
    error_message: str | None = None
    created_at: str
    updated_at: str
    generated_at: str | None = None
    finding_count: int
    critical_count: int
    warning_count: int
    info_count: int
    status_counts: dict[str, int] = Field(default_factory=dict)


class AiDiagnosticReviewListResponse(BaseModel):
    reviews: list[AiDiagnosticReviewSummaryResponse]


class AiDiagnosticReviewDetailResponse(AiDiagnosticReviewSummaryResponse):
    snapshot: DiagnosticSnapshotResponse
    snapshot_raw_json: dict[str, Any] = Field(default_factory=dict)
    findings: list[AiDiagnosticFindingResponse]

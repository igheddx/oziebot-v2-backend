# Phase 1 Technical Spec: AI-Assisted Trade Filter

## 0) Goals and Non-Goals

### Goals
- Add an AI-assisted pre-risk trade filter that evaluates rule-based signals and returns one of:
  - allow
  - reduce_size
  - delay
  - skip
- Keep AI advisory/bounded so centralized risk remains authoritative.
- Make every AI decision auditable and explainable.
- Train on both PAPER and LIVE data and explicitly model mode gap.

### Non-Goals
- No direct AI order placement.
- No AI override of hard risk rules.
- No autonomous parameter mutation in production during Phase 1.

---

## 1) Recommended Phase 1 Implementation Order

1. Data contract and storage
- Define AI feature schema, inference record schema, and explanation schema.
- Add persistence and indexes first.

2. Offline dataset assembly
- Build point-in-time feature extraction from historical signal and execution data.
- Build labels from realized outcomes.

3. Baseline model training pipeline
- Train a conservative classifier that predicts action and confidence.
- Include mode-awareness features and PAPER vs LIVE gap targets.

4. Online inference in shadow mode
- Run model on live signals but do not affect trading decisions.
- Compare AI recommendation to realized outcomes and risk decisions.

5. Bounded policy integration
- Enable AI action mapping into pre-risk signal shaping only.
- Keep hard clamps and hard-risk final authority.

6. Gradual rollout with kill switches
- Per-tenant/per-strategy rollout percentages.
- Automatic fallback to allow on inference failure or stale model.

---

## 2) Architecture Changes Needed (Layered, Non-Breaking)

### Existing flow (unchanged authority)
- Strategy Engine -> Risk Engine -> Execution Engine

### Phase 1 additive components
1. AI Feature Builder (new, read-only)
- Builds point-in-time features for each candidate signal.

2. AI Inference Service (new, bounded)
- Returns action in {allow, reduce_size, delay, skip}, confidence, explanation.
- Cannot place orders; cannot call execution paths.

3. AI Policy Adapter (new, inside strategy-signal pipeline)
- Applies AI recommendation as a pre-risk transform.
- Emits transformed intent and audit trail.

4. AI Training Pipeline (offline)
- Feature extraction, labeling, training, evaluation, model registry publishing.

5. AI Comparator / Calibration Job (new)
- Measures PAPER vs LIVE divergence and computes confidence calibration updates.

### Integration point
- Insert AI Policy Adapter between strategy signal generation and risk submission:
  - strategy signal -> AI filter decision -> transformed intent -> centralized risk

### Hard invariants
- Risk engine remains final approval gate.
- AI does not bypass queue topology, idempotency, or execution state machine.

---

## 3) Feature-Engineering Plan

### 3.1 Feature groups

1. Price/indicator features
- Multi-horizon returns: 1, 3, 6, 12 bars.
- Volatility: rolling std, ATR-like proxy.
- Momentum and mean-reversion z-scores.
- Distance to moving averages and bands.

2. Order-book context features
- Top-of-book spread bps.
- Depth imbalance (bid vs ask across levels).
- Sweep risk estimate (expected impact at target size).
- Microprice drift proxy.

3. Spread/slippage context features
- Recent realized slippage by token and mode.
- Current spread percentile vs trailing history.
- Venue liquidity regime flag.

4. Regime features
- Regime class (trend/chop/high-vol/low-vol/risk-off).
- Regime confidence.
- Regime persistence duration.

5. Strategy metadata features
- strategy_id (embedded/one-hot).
- signal_type, confidence, reason-code buckets.
- strategy-level recent hit-rate and drawdown.

6. Outcome-history features
- Recent rolling PnL and win rate by strategy-token-mode.
- Recent adverse excursion and hold-time stats.
- Recent reject/retry/failure rates.

7. Token pattern features
- Token-specific win/slippage stability metrics.
- Token volatility bucket and liquidity bucket.

8. Sentiment/context features (optional extension)
- News/sentiment score and confidence.
- Macro event windows.

### 3.2 Feature quality rules
- Point-in-time correctness: no future leakage.
- Explicit missingness flags for each major source.
- Freshness timestamp and staleness bucket.
- Source reliability score per feature family.

### 3.3 Feature versioning
- feature_schema_version and feature_builder_version stored with each inference.

---

## 4) Labeling Strategy from Historical Outcomes

### 4.1 Primary label: action class
- allow: signal led to acceptable risk-adjusted outcome.
- reduce_size: positive edge but size was too aggressive under realized slippage/volatility.
- delay: setup quality improved when waiting N bars/seconds.
- skip: expected value negative or risk-adjusted quality poor.

### 4.2 Label construction recipe
1. Rebuild trade candidate timeline from strategy signals.
2. Join realized outcomes:
- execution fills
- slippage
- fees
- realized pnl
- drawdown contribution
3. Compute utility score U per candidate:
- U = pnl_net - slippage_penalty - drawdown_penalty - failure_penalty
4. Counterfactual variants:
- size scaled (for reduce_size)
- entry delayed by bounded horizon (for delay)
- no-trade baseline (for skip)
5. Choose class with highest constrained utility under risk policy.

### 4.3 PAPER vs LIVE-aware labels
- Keep mode in label rows.
- Add mode_gap targets:
- delta_pnl
- delta_slippage
- delta_fill_quality
- delta_drawdown_contribution

### 4.4 Confidence targets
- Train secondary confidence calibration target based on realized LIVE outcomes.

---

## 5) Offline Training Workflow

1. Dataset build job
- Pull strategy signals, market context, risk decisions, execution outcomes.
- Produce training rows with feature vector + action label + mode_gap targets.

2. Split strategy
- Time-based split only (train/validation/test by chronology).
- Group-aware checks by strategy_id and token to avoid leakage.

3. Baseline models
- Multi-class classifier for action.
- Optional regressors for expected slippage and expected adverse excursion.

4. Evaluation metrics
- Macro-F1 and per-class precision/recall.
- Cost-sensitive utility gain vs baseline allow-all policy.
- Calibration error (ECE/Brier), especially on LIVE subset.
- Stability across strategies/tokens/regimes.

5. Model packaging
- Store model artifact, feature schema hash, training window, metrics, and acceptance gates.

6. Promotion gates (must pass)
- No degradation in safety metrics.
- Minimum precision for skip and reduce_size classes.
- LIVE calibration within threshold.

---

## 6) Online Inference Workflow

1. Trigger
- For each rule-based signal candidate before risk submission.

2. Build inference request
- Snapshot features at decision time.
- Include mode (paper/live), regime, strategy metadata, and context freshness flags.

3. Inference output contract
- action: allow | reduce_size | delay | skip
- confidence: 0..1
- expected_impact: optional (slippage/pnl delta)
- explanation: top factors and reason codes

4. Policy adapter mapping
- allow: pass intent unchanged.
- reduce_size: multiply quantity by bounded factor [min_scale, 1.0].
- delay: enqueue with bounded delay window and re-evaluate once.
- skip: drop candidate and audit decision.

5. Submit to centralized risk
- Transformed intent always passes through existing risk engine.

6. Failure behavior
- On timeout/error/stale model -> fail-open as allow and emit anomaly event.

---

## 7) Safe Rollout Plan

1. Stage A: offline only
- No production influence. Validate historical utility and calibration.

2. Stage B: shadow mode
- Run online inference, store decisions, but do not alter intents.
- Compare hypothetical AI action vs realized outcomes.

3. Stage C: bounded live pilot
- Enable for PAPER first, then low-risk LIVE cohorts.
- Restrict to reduce_size and delay only initially.

4. Stage D: controlled expansion
- Add skip in selected strategies/tokens with strict confidence gates.
- Expand cohorts gradually with rollback thresholds.

5. Kill switches
- Global AI off switch.
- Per-tenant, per-strategy, per-mode switches.
- Auto-disable on anomaly spikes, stale models, or calibration drift.

---

## 8) Guardrails (AI Cannot Override Hard Risk)

1. Control-plane guardrails
- AI service has no credentials/access path to execution APIs.
- AI output is advisory payload only.

2. Data-plane guardrails
- Policy adapter enforces action bounds:
- reduce_size cannot increase size.
- delay limited by max_delay.
- skip requires confidence >= skip_confidence_min.

3. Risk supremacy
- Every transformed intent still evaluated by risk engine.
- Hard limits (max exposure, drawdown, leverage, venue controls) always authoritative.

4. Deterministic fallback
- If AI unavailable or invalid output, default to allow and continue normal path.

5. Audit immutability
- Store raw features hash, model version, output, and applied transform for every inference.

---

## 9) Data Model Additions (Phase 1)

### 9.1 ai_model_registry
- model_id (pk)
- model_name
- model_version
- feature_schema_version
- feature_hash
- trained_from_ts / trained_to_ts
- paper_live_mix_ratio
- eval_metrics_json
- calibration_metrics_json
- status (candidate/active/retired)
- created_at

### 9.2 ai_inference_records
- inference_id (pk)
- trace_id
- tenant_id
- user_id
- strategy_id
- token_symbol
- trading_mode
- signal_id / correlation_id
- model_id
- model_version
- feature_schema_version
- feature_vector_json
- feature_hash
- action_recommended
- confidence
- confidence_calibrated
- explanation_json
- latency_ms
- decision_status (applied/fallback/rejected)
- created_at

### 9.3 ai_policy_applications
- application_id (pk)
- inference_id (fk)
- original_intent_json
- transformed_intent_json
- transform_type (none/reduce_size/delay/skip)
- transform_parameters_json
- bounded_by_guardrail (bool)
- created_at

### 9.4 ai_outcome_links
- inference_id (fk)
- execution_order_id / trade_id
- realized_pnl_cents
- realized_slippage_bps
- realized_drawdown_contribution
- execution_quality_json
- label_action
- label_utility_score
- updated_at

### 9.5 ai_mode_gap_snapshots
- snapshot_id (pk)
- strategy_id
- token_symbol
- regime
- paper_win_rate
- live_win_rate
- delta_win_rate
- paper_slippage_bps
- live_slippage_bps
- delta_slippage_bps
- paper_drawdown
- live_drawdown
- delta_drawdown
- execution_quality_delta_json
- confidence_adjustment_factor
- created_at

---

## 10) PAPER vs LIVE Extension Requirements (Applied)

1. Training on mixed mode data
- Include both PAPER and LIVE rows in every training cycle.
- Use trading_mode as explicit feature and stratified sampling.

2. Mode-gap tracking
- Compute rolling deltas for win rate, slippage, drawdown, and execution quality.
- Persist in ai_mode_gap_snapshots.

3. Confidence calibration by real execution
- Calibrate model confidence primarily on LIVE outcomes.
- Apply confidence_adjustment_factor by strategy/token/regime.
- Reduce aggressive actions if LIVE calibration drifts.

4. Promotion policy
- New model cannot promote if LIVE calibration deteriorates even if PAPER metrics improve.

---

## 11) Service Boundary Updates (Concrete)

1. Strategy Engine
- New call: request AI advisory after signal generation.
- No execution permissions added.

2. API Service
- Persist inference records and policy application records.
- Expose read APIs for audit and analytics.

3. Risk Engine
- No authority changes.
- Optionally log AI-related context for post-trade attribution.

4. Execution Engine
- No direct AI integration required.
- Continue to emit execution quality metrics used downstream.

5. Alerts/Analytics Workers
- Add drift and anomaly notifications for mode-gap and calibration failures.

---

## 12) Initial API Surface (Phase 1)

1. POST /v1/me/ai/trade-filter/evaluate
- Input: signal context + feature payload refs
- Output: action, confidence, explanation, model_version

2. GET /v1/me/ai/trade-filter/inference-history
- Filter by strategy, token, mode, date

3. GET /v1/me/ai/trade-filter/inference-history.csv
- CSV export for compliance and analysis

4. GET /v1/me/ai/trade-filter/mode-gap
- Returns paper vs live deltas and confidence adjustments

5. GET /v1/admin/ai/models
- Model registry and promotion status

---

## 13) Explainability and Audit Requirements

For every inference, store:
- Top K feature contributors.
- Human-readable reason codes.
- Model and feature schema versions.
- Raw feature hash and retrieval timestamps.
- Original and transformed intent diff.
- Whether guardrails clipped the action.

This ensures replayability and regulator/compliance readiness.

---

## 14) Phase 1 Acceptance Criteria

1. Safety
- 100% of AI-transformed intents still pass through centralized risk.
- Zero incidents of AI bypassing hard risk checks.

2. Auditability
- 100% inference coverage with persisted explanation and lineage.

3. Performance
- In shadow mode, measured utility improvement over allow-all baseline.
- No adverse LIVE calibration drift beyond threshold.

4. Operability
- Kill switch verified in staging and production.
- End-to-end dashboards for inference latency, action distribution, and mode gap.

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Mapping, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Engine


class DecisionAuditStage(StrEnum):
    STRATEGY = "strategy"
    SUPPRESSION = "suppression"
    RISK = "risk"
    EXECUTION = "execution"


class DecisionAuditDecision(StrEnum):
    EMITTED = "emitted"
    REDUCED = "reduced"
    REJECTED = "rejected"
    EXECUTED = "executed"


class AIRecommendation(StrEnum):
    ALLOW = "allow"
    REDUCE_SIZE = "reduce_size"
    SKIP = "skip"


@dataclass(frozen=True)
class AIInferenceResult:
    recommendation: AIRecommendation
    confidence_score: float
    explanation_json: dict[str, Any]


class TradeIntelligenceScorer(Protocol):
    model_name: str
    model_version: str

    def score(self, snapshot: Mapping[str, Any]) -> AIInferenceResult: ...


class PlaceholderTradeIntelligenceScorer:
    model_name = "placeholder-advisory"
    model_version = "0.1"

    def score(self, snapshot: Mapping[str, Any]) -> AIInferenceResult:
        token_policy_status = str(snapshot.get("token_policy_status") or "allowed")
        recommendation = AIRecommendation.ALLOW
        confidence = 0.2
        if token_policy_status == "discouraged":
            recommendation = AIRecommendation.REDUCE_SIZE
            confidence = 0.35
        elif token_policy_status == "blocked":
            recommendation = AIRecommendation.SKIP
            confidence = 0.45
        return AIInferenceResult(
            recommendation=recommendation,
            confidence_score=confidence,
            explanation_json={
                "mode": "advisory_only",
                "source": "placeholder",
                "token_policy_status": token_policy_status,
                "summary": "Placeholder advisory inference stored for future AI scoring.",
            },
        )


def persist_signal_snapshot(
    engine: Engine,
    *,
    user_id: str,
    tenant_id: str,
    trading_mode: str,
    strategy_name: str,
    token_symbol: str,
    timestamp: datetime,
    current_price: Decimal,
    best_bid: Decimal,
    best_ask: Decimal,
    spread_pct: Decimal,
    estimated_slippage_pct: Decimal,
    volume: Decimal,
    volatility: Decimal | None,
    confidence_score: float | None,
    raw_feature_json: Mapping[str, Any] | None,
    token_policy_status: str | None,
    token_policy_multiplier: Decimal | None,
) -> str:
    snapshot_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO strategy_signal_snapshots (
                  id, user_id, tenant_id, trading_mode, strategy_name, token_symbol,
                  timestamp, current_price, best_bid, best_ask, spread_pct,
                  estimated_slippage_pct, volume, volatility, confidence_score,
                  raw_feature_json, token_policy_status, token_policy_multiplier
                ) VALUES (
                  :id, :user_id, :tenant_id, :trading_mode, :strategy_name, :token_symbol,
                  :timestamp, :current_price, :best_bid, :best_ask, :spread_pct,
                  :estimated_slippage_pct, :volume, :volatility, :confidence_score,
                  :raw_feature_json, :token_policy_status, :token_policy_multiplier
                )
                """
            ),
            {
                "id": snapshot_id,
                "user_id": user_id,
                "tenant_id": tenant_id,
                "trading_mode": trading_mode,
                "strategy_name": strategy_name,
                "token_symbol": token_symbol,
                "timestamp": timestamp,
                "current_price": str(current_price),
                "best_bid": str(best_bid),
                "best_ask": str(best_ask),
                "spread_pct": str(spread_pct),
                "estimated_slippage_pct": str(estimated_slippage_pct),
                "volume": str(volume),
                "volatility": str(volatility) if volatility is not None else None,
                "confidence_score": confidence_score,
                "raw_feature_json": json.dumps(
                    dict(raw_feature_json or {}), default=str
                ),
                "token_policy_status": token_policy_status,
                "token_policy_multiplier": (
                    str(token_policy_multiplier)
                    if token_policy_multiplier is not None
                    else None
                ),
            },
        )
    return snapshot_id


def persist_decision_audit(
    engine: Engine,
    *,
    signal_snapshot_id: str | None,
    stage: str,
    decision: str,
    reason_code: str | None,
    reason_detail: str | None,
    size_before: Decimal | None,
    size_after: Decimal | None,
    created_at: datetime,
) -> str:
    audit_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO strategy_decision_audits (
                  id, signal_snapshot_id, stage, decision, reason_code, reason_detail,
                  size_before, size_after, created_at
                ) VALUES (
                  :id, :signal_snapshot_id, :stage, :decision, :reason_code, :reason_detail,
                  :size_before, :size_after, :created_at
                )
                """
            ),
            {
                "id": audit_id,
                "signal_snapshot_id": signal_snapshot_id,
                "stage": stage,
                "decision": decision,
                "reason_code": reason_code,
                "reason_detail": reason_detail,
                "size_before": str(size_before) if size_before is not None else None,
                "size_after": str(size_after) if size_after is not None else None,
                "created_at": created_at,
            },
        )
    return audit_id


def persist_trade_outcome_feature(
    engine: Engine,
    *,
    trade_id: str,
    signal_snapshot_id: str | None,
    trading_mode: str,
    strategy_name: str,
    token_symbol: str,
    entry_price: Decimal,
    exit_price: Decimal,
    filled_size: Decimal,
    fee_paid: Decimal,
    slippage_realized: Decimal | None,
    hold_seconds: int | None,
    realized_pnl: Decimal,
    realized_return_pct: Decimal | None,
    max_favorable_excursion_pct: Decimal | None,
    max_adverse_excursion_pct: Decimal | None,
    exit_reason: str | None,
    win_loss_label: str,
    profitable_after_fees_label: str,
    created_at: datetime,
) -> str:
    outcome_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO trade_outcome_features (
                  id, trade_id, signal_snapshot_id, trading_mode, strategy_name, token_symbol,
                  entry_price, exit_price, filled_size, fee_paid, slippage_realized,
                  hold_seconds, realized_pnl, realized_return_pct,
                  max_favorable_excursion_pct, max_adverse_excursion_pct,
                  exit_reason, win_loss_label, profitable_after_fees_label, created_at
                ) VALUES (
                  :id, :trade_id, :signal_snapshot_id, :trading_mode, :strategy_name, :token_symbol,
                  :entry_price, :exit_price, :filled_size, :fee_paid, :slippage_realized,
                  :hold_seconds, :realized_pnl, :realized_return_pct,
                  :max_favorable_excursion_pct, :max_adverse_excursion_pct,
                  :exit_reason, :win_loss_label, :profitable_after_fees_label, :created_at
                )
                """
            ),
            {
                "id": outcome_id,
                "trade_id": trade_id,
                "signal_snapshot_id": signal_snapshot_id,
                "trading_mode": trading_mode,
                "strategy_name": strategy_name,
                "token_symbol": token_symbol,
                "entry_price": str(entry_price),
                "exit_price": str(exit_price),
                "filled_size": str(filled_size),
                "fee_paid": str(fee_paid),
                "slippage_realized": (
                    str(slippage_realized) if slippage_realized is not None else None
                ),
                "hold_seconds": hold_seconds,
                "realized_pnl": str(realized_pnl),
                "realized_return_pct": (
                    str(realized_return_pct)
                    if realized_return_pct is not None
                    else None
                ),
                "max_favorable_excursion_pct": (
                    str(max_favorable_excursion_pct)
                    if max_favorable_excursion_pct is not None
                    else None
                ),
                "max_adverse_excursion_pct": (
                    str(max_adverse_excursion_pct)
                    if max_adverse_excursion_pct is not None
                    else None
                ),
                "exit_reason": exit_reason,
                "win_loss_label": win_loss_label,
                "profitable_after_fees_label": profitable_after_fees_label,
                "created_at": created_at,
            },
        )
    return outcome_id


def persist_ai_inference_record(
    engine: Engine,
    *,
    signal_snapshot_id: str,
    model_name: str,
    model_version: str,
    recommendation: str,
    confidence_score: float | None,
    explanation_json: Mapping[str, Any] | None,
    created_at: datetime,
) -> str:
    inference_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ai_inference_records (
                  id, signal_snapshot_id, model_name, model_version,
                  recommendation, confidence_score, explanation_json, created_at
                ) VALUES (
                  :id, :signal_snapshot_id, :model_name, :model_version,
                  :recommendation, :confidence_score, :explanation_json, :created_at
                )
                """
            ),
            {
                "id": inference_id,
                "signal_snapshot_id": signal_snapshot_id,
                "model_name": model_name,
                "model_version": model_version,
                "recommendation": recommendation,
                "confidence_score": confidence_score,
                "explanation_json": json.dumps(
                    dict(explanation_json or {}), default=str
                ),
                "created_at": created_at,
            },
        )
    return inference_id


def extract_signal_snapshot_id(metadata: Mapping[str, Any] | None) -> str | None:
    if not isinstance(metadata, Mapping):
        return None
    intelligence = metadata.get("intelligence")
    if not isinstance(intelligence, Mapping):
        return None
    value = intelligence.get("signal_snapshot_id")
    return str(value) if value else None


def upsert_intelligence_metadata(
    metadata: Mapping[str, Any] | None,
    *,
    signal_snapshot_id: str,
    ai_inference_id: str | None = None,
    ai_recommendation: str | None = None,
    ai_confidence_score: float | None = None,
    model_name: str | None = None,
    model_version: str | None = None,
) -> dict[str, Any]:
    merged = dict(metadata or {})
    intelligence = dict(merged.get("intelligence") or {})
    intelligence["signal_snapshot_id"] = signal_snapshot_id
    if ai_inference_id:
        intelligence["ai_inference_id"] = ai_inference_id
    if ai_recommendation:
        intelligence["ai_recommendation"] = ai_recommendation
    if ai_confidence_score is not None:
        intelligence["ai_confidence_score"] = ai_confidence_score
    if model_name:
        intelligence["ai_model_name"] = model_name
    if model_version:
        intelligence["ai_model_version"] = model_version
    merged["intelligence"] = intelligence
    return merged

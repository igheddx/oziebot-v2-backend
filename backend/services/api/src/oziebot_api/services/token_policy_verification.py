from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_common.token_policy import resolve_effective_token_policy
from oziebot_api.models.execution import ExecutionOrder
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_signal_pipeline import StrategyRun, StrategySignalRecord


class TokenPolicyVerificationService:
    def __init__(self, db: Session):
        self._db = db

    def list_recent_decisions(
        self,
        *,
        symbol: str | None = None,
        strategy_id: str | None = None,
        trading_mode: str | None = None,
        outcome: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        signal_query = self._apply_filters(
            select(StrategySignalRecord).order_by(StrategySignalRecord.timestamp.desc()),
            symbol=symbol,
            strategy_id=strategy_id,
            trading_mode=trading_mode,
            symbol_column=StrategySignalRecord.symbol,
            strategy_column=StrategySignalRecord.strategy_name,
            trading_mode_column=StrategySignalRecord.trading_mode,
        )
        signals = self._db.scalars(signal_query.limit(limit * 3)).all()
        signal_by_id = {str(signal.signal_id): signal for signal in signals}

        records: list[dict[str, Any]] = []
        records.extend(
            self._strategy_stage_records(
                symbol=symbol,
                strategy_id=strategy_id,
                trading_mode=trading_mode,
                limit=limit * 3,
            )
        )
        records.extend(self._signal_stage_records(signals))
        records.extend(
            self._risk_stage_records(
                signal_by_id=signal_by_id,
                symbol=symbol,
                strategy_id=strategy_id,
                trading_mode=trading_mode,
                limit=limit * 3,
            )
        )
        records.extend(
            self._execution_stage_records(
                signal_by_id=signal_by_id,
                symbol=symbol,
                strategy_id=strategy_id,
                trading_mode=trading_mode,
                limit=limit * 3,
            )
        )
        if outcome:
            wanted = outcome.strip().lower()
            records = [
                record for record in records if record["decision_outcome"].lower() == wanted
            ]
        records.sort(key=lambda item: item["timestamp"], reverse=True)
        return records[:limit]

    def _strategy_stage_records(
        self,
        *,
        symbol: str | None,
        strategy_id: str | None,
        trading_mode: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        query = self._apply_filters(
            select(StrategyRun).order_by(StrategyRun.started_at.desc()),
            symbol=symbol,
            strategy_id=strategy_id,
            trading_mode=trading_mode,
            symbol_column=StrategyRun.symbol,
            strategy_column=StrategyRun.strategy_name,
            trading_mode_column=StrategyRun.trading_mode,
        )
        rows = self._db.scalars(query.limit(limit)).all()
        records: list[dict[str, Any]] = []
        for row in rows:
            metadata = self._as_dict(row.run_metadata)
            if not metadata.get("suppressed"):
                continue
            token_policy = self._policy_snapshot(metadata.get("token_policy"))
            if token_policy is None:
                continue
            records.append(
                {
                    "record_id": f"strategy-run:{row.run_id}",
                    "enforced_in": "strategy-engine",
                    "strategy_name": row.strategy_name,
                    "token": row.symbol,
                    "trading_mode": row.trading_mode,
                    "computed_recommendation_status": token_policy[
                        "computed_recommendation_status"
                    ],
                    "effective_recommendation_status": token_policy[
                        "effective_recommendation_status"
                    ],
                    "admin_enabled": token_policy["admin_enabled"],
                    "confidence_score": metadata.get("confidence"),
                    "final_sizing_impact": {
                        "original_size": None,
                        "final_size": None,
                        "size_multiplier": token_policy["size_multiplier"],
                        "max_position_pct_override": token_policy[
                            "max_position_pct_override"
                        ],
                    },
                    "decision_outcome": "rejected",
                    "decision_reason": metadata.get("suppression_reason")
                    or token_policy["effective_recommendation_reason"],
                    "timestamp": self._iso(row.completed_at or row.started_at),
                }
            )
        return records

    def _signal_stage_records(
        self,
        signals: list[StrategySignalRecord],
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for signal in signals:
            reasoning = self._as_dict(signal.reasoning_metadata)
            token_policy = self._policy_snapshot(reasoning.get("token_policy"))
            if token_policy is None:
                continue
            records.append(
                {
                    "record_id": f"strategy-signal:{signal.signal_id}",
                    "enforced_in": "strategy-engine",
                    "strategy_name": signal.strategy_name,
                    "token": signal.symbol,
                    "trading_mode": signal.trading_mode,
                    "computed_recommendation_status": token_policy[
                        "computed_recommendation_status"
                    ],
                    "effective_recommendation_status": token_policy[
                        "effective_recommendation_status"
                    ],
                    "admin_enabled": token_policy["admin_enabled"],
                    "confidence_score": signal.confidence,
                    "final_sizing_impact": {
                        "original_size": signal.suggested_size,
                        "final_size": signal.suggested_size,
                        "size_multiplier": token_policy["size_multiplier"],
                        "max_position_pct_override": token_policy[
                            "max_position_pct_override"
                        ],
                    },
                    "decision_outcome": "emitted",
                    "decision_reason": reasoning.get("decision_reason")
                    or reasoning.get("reason")
                    or token_policy["effective_recommendation_reason"],
                    "timestamp": self._iso(signal.timestamp),
                }
            )
        return records

    def _risk_stage_records(
        self,
        *,
        signal_by_id: dict[str, StrategySignalRecord],
        symbol: str | None,
        strategy_id: str | None,
        trading_mode: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        query = self._apply_filters(
            select(RiskEvent).order_by(RiskEvent.created_at.desc()),
            symbol=symbol,
            strategy_id=strategy_id,
            trading_mode=trading_mode,
            symbol_column=RiskEvent.symbol,
            strategy_column=RiskEvent.strategy_name,
            trading_mode_column=RiskEvent.trading_mode,
        )
        rows = self._db.scalars(query.limit(limit)).all()
        records: list[dict[str, Any]] = []
        for row in rows:
            payload = self._as_dict(row.signal_payload)
            reasoning = self._as_dict(payload.get("reasoning_metadata"))
            token_policy = self._policy_snapshot(reasoning.get("token_policy"))
            if token_policy is None:
                signal = signal_by_id.get(str(row.signal_id))
                if signal is None:
                    continue
                token_policy = self._policy_snapshot(
                    self._as_dict(signal.reasoning_metadata).get("token_policy")
                )
                if token_policy is None:
                    continue
            records.append(
                {
                    "record_id": f"risk-event:{row.id}",
                    "enforced_in": "risk-engine",
                    "strategy_name": row.strategy_name,
                    "token": row.symbol,
                    "trading_mode": row.trading_mode,
                    "computed_recommendation_status": token_policy[
                        "computed_recommendation_status"
                    ],
                    "effective_recommendation_status": token_policy[
                        "effective_recommendation_status"
                    ],
                    "admin_enabled": token_policy["admin_enabled"],
                    "confidence_score": payload.get("confidence"),
                    "final_sizing_impact": {
                        "original_size": row.original_size,
                        "final_size": row.final_size,
                        "size_multiplier": token_policy["size_multiplier"],
                        "max_position_pct_override": token_policy[
                            "max_position_pct_override"
                        ],
                    },
                    "decision_outcome": self._map_risk_outcome(row.outcome),
                    "decision_reason": row.detail
                    or row.reason
                    or token_policy["effective_recommendation_reason"],
                    "timestamp": self._iso(row.created_at),
                }
            )
        return records

    def _execution_stage_records(
        self,
        *,
        signal_by_id: dict[str, StrategySignalRecord],
        symbol: str | None,
        strategy_id: str | None,
        trading_mode: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        query = self._apply_filters(
            select(ExecutionOrder).order_by(ExecutionOrder.created_at.desc()),
            symbol=symbol,
            strategy_id=strategy_id,
            trading_mode=trading_mode,
            symbol_column=ExecutionOrder.symbol,
            strategy_column=ExecutionOrder.strategy_id,
            trading_mode_column=ExecutionOrder.trading_mode,
        )
        rows = self._db.scalars(query.limit(limit)).all()
        records: list[dict[str, Any]] = []
        for row in rows:
            risk_payload = self._as_dict(row.risk_payload)
            intent_payload = self._as_dict(row.intent_payload)
            token_policy = self._policy_snapshot(
                self._as_dict(intent_payload.get("metadata")).get("token_policy_execution")
            )
            signal = signal_by_id.get(str(risk_payload.get("signal_id")))
            if token_policy is None and signal is not None:
                token_policy = self._policy_snapshot(
                    self._as_dict(signal.reasoning_metadata).get("token_policy")
                )
            if token_policy is None:
                continue
            records.append(
                {
                    "record_id": f"execution-order:{row.id}",
                    "enforced_in": "execution/sizing",
                    "strategy_name": row.strategy_id,
                    "token": row.symbol,
                    "trading_mode": row.trading_mode,
                    "computed_recommendation_status": token_policy[
                        "computed_recommendation_status"
                    ],
                    "effective_recommendation_status": token_policy[
                        "effective_recommendation_status"
                    ],
                    "admin_enabled": token_policy["admin_enabled"],
                    "confidence_score": signal.confidence if signal is not None else None,
                    "final_sizing_impact": {
                        "original_size": risk_payload.get("final_size"),
                        "final_size": self._final_execution_size(row, intent_payload, risk_payload),
                        "size_multiplier": token_policy["size_multiplier"],
                        "max_position_pct_override": token_policy[
                            "max_position_pct_override"
                        ],
                        "requested_quantity": row.quantity,
                    },
                    "decision_outcome": self._map_execution_outcome(row, intent_payload, risk_payload),
                    "decision_reason": row.failure_detail
                    or self._as_dict(intent_payload.get("metadata")).get(
                        "policy_adjustment_reason"
                    )
                    or token_policy["effective_recommendation_reason"],
                    "timestamp": self._iso(
                        row.completed_at
                        or row.failed_at
                        or row.submitted_at
                        or row.created_at
                    ),
                }
            )
        return records

    @staticmethod
    def _apply_filters(
        query,
        *,
        symbol: str | None,
        strategy_id: str | None,
        trading_mode: str | None,
        symbol_column,
        strategy_column,
        trading_mode_column,
    ):
        if symbol:
            query = query.where(symbol_column == symbol.strip().upper())
        if strategy_id:
            query = query.where(strategy_column == strategy_id.strip().lower())
        if trading_mode:
            query = query.where(trading_mode_column == trading_mode.strip().lower())
        return query

    @staticmethod
    def _as_dict(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @classmethod
    def _policy_snapshot(cls, raw: Any) -> dict[str, Any] | None:
        policy = cls._as_dict(raw)
        if not policy:
            return None
        if "effective_recommendation_status" in policy:
            return {
                "admin_enabled": bool(policy.get("admin_enabled", True)),
                "computed_recommendation_status": policy.get(
                    "computed_recommendation_status", "allowed"
                ),
                "effective_recommendation_status": policy.get(
                    "effective_recommendation_status",
                    policy.get("recommendation_status", "allowed"),
                ),
                "effective_recommendation_reason": policy.get(
                    "effective_recommendation_reason",
                    policy.get("recommendation_reason"),
                ),
                "size_multiplier": cls._normalize_decimal(policy.get("size_multiplier"), "1"),
                "max_position_pct_override": cls._normalize_decimal(
                    policy.get("max_position_pct_override"),
                    None,
                ),
            }
        if "computed_recommendation_status" in policy and "recommendation_status" in policy:
            return {
                "admin_enabled": bool(policy.get("admin_enabled", True)),
                "computed_recommendation_status": policy.get(
                    "computed_recommendation_status", "allowed"
                ),
                "effective_recommendation_status": policy.get(
                    "recommendation_status", "allowed"
                ),
                "effective_recommendation_reason": policy.get("recommendation_reason"),
                "size_multiplier": cls._normalize_decimal(policy.get("size_multiplier"), "1"),
                "max_position_pct_override": cls._normalize_decimal(
                    policy.get("max_position_pct_override"),
                    None,
                ),
            }
        resolved = resolve_effective_token_policy(policy)
        return {
            "admin_enabled": resolved["admin_enabled"],
            "computed_recommendation_status": resolved["computed_recommendation_status"],
            "effective_recommendation_status": resolved["effective_recommendation_status"],
            "effective_recommendation_reason": resolved["effective_recommendation_reason"],
            "size_multiplier": str(resolved["size_multiplier"]),
            "max_position_pct_override": cls._normalize_decimal(
                resolved["max_position_pct_override"],
                None,
            ),
        }

    @staticmethod
    def _normalize_decimal(value: Any, default: str | None) -> str | None:
        if value is None:
            return default
        try:
            return str(Decimal(str(value)))
        except (InvalidOperation, ValueError):
            return default

    @staticmethod
    def _map_risk_outcome(outcome: str) -> str:
        match outcome.lower():
            case "reject":
                return "rejected"
            case "reduce_size":
                return "reduced"
            case _:
                return "emitted"

    @classmethod
    def _map_execution_outcome(
        cls,
        row: ExecutionOrder,
        intent_payload: dict[str, Any],
        risk_payload: dict[str, Any],
    ) -> str:
        if row.failure_code == "token_strategy_policy":
            return "rejected"
        final_size = cls._normalize_decimal(
            cls._final_execution_size(row, intent_payload, risk_payload),
            None,
        )
        requested_size = cls._normalize_decimal(risk_payload.get("final_size"), None)
        if (
            final_size is not None
            and requested_size is not None
            and Decimal(final_size) < Decimal(requested_size)
        ):
            return "reduced"
        if row.state.lower() in {"created", "submitted", "partially_filled", "filled", "completed"}:
            return "executed"
        return "emitted"

    @staticmethod
    def _final_execution_size(
        row: ExecutionOrder,
        intent_payload: dict[str, Any],
        risk_payload: dict[str, Any],
    ) -> Any:
        metadata = TokenPolicyVerificationService._as_dict(intent_payload.get("metadata"))
        return (
            metadata.get("adjusted_quantity")
            or metadata.get("policy_adjusted_quantity")
            or row.quantity
            or risk_payload.get("final_size")
        )

    @staticmethod
    def _iso(value: Any) -> str:
        return value.isoformat()

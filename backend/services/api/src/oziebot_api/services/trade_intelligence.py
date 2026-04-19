from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


def _json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return {}
    return dict(value) if isinstance(value, dict) else {}


class TradeIntelligenceService:
    def __init__(self, engine: Engine):
        self._engine = engine

    def export_training_data(
        self,
        *,
        trading_mode: str | None = None,
        strategy_name: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        filters: list[str] = []
        params: dict[str, Any] = {"limit": limit}
        if trading_mode:
            filters.append("tof.trading_mode = :trading_mode")
            params["trading_mode"] = trading_mode
        if strategy_name:
            filters.append("tof.strategy_name = :strategy_name")
            params["strategy_name"] = strategy_name
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        query = f"""
            SELECT
              tof.id,
              tof.trade_id,
              tof.signal_snapshot_id,
              tof.trading_mode,
              tof.strategy_name,
              tof.token_symbol,
              tof.entry_price,
              tof.exit_price,
              tof.filled_size,
              tof.fee_paid,
              tof.slippage_realized,
              tof.hold_seconds,
              tof.realized_pnl,
              tof.realized_return_pct,
              tof.max_favorable_excursion_pct,
              tof.max_adverse_excursion_pct,
              tof.exit_reason,
              tof.win_loss_label,
              tof.profitable_after_fees_label,
              tof.created_at,
              sss.timestamp AS signal_timestamp,
              sss.current_price,
              sss.best_bid,
              sss.best_ask,
              sss.spread_pct,
              sss.estimated_slippage_pct,
              sss.volume,
              sss.volatility,
              sss.confidence_score,
              sss.raw_feature_json,
              sss.token_policy_status,
              sss.token_policy_multiplier
            FROM trade_outcome_features tof
            JOIN strategy_signal_snapshots sss
              ON sss.id = tof.signal_snapshot_id
            {where}
            ORDER BY tof.created_at DESC
            LIMIT :limit
        """
        with self._engine.begin() as conn:
            rows = conn.execute(text(query), params).mappings().all()
            snapshot_ids = [
                str(row["signal_snapshot_id"]) for row in rows if row["signal_snapshot_id"]
            ]
            audits_by_snapshot: dict[str, list[dict[str, Any]]] = defaultdict(list)
            if snapshot_ids:
                audit_stmt = text(
                    """
                    SELECT signal_snapshot_id, stage, decision, reason_code, reason_detail,
                           size_before, size_after, created_at
                    FROM strategy_decision_audits
                    WHERE signal_snapshot_id IN :snapshot_ids
                    ORDER BY created_at ASC
                    """
                ).bindparams(bindparam("snapshot_ids", expanding=True))
                audit_rows = (
                    conn.execute(audit_stmt, {"snapshot_ids": snapshot_ids}).mappings().all()
                )
                for row in audit_rows:
                    audits_by_snapshot[str(row["signal_snapshot_id"])].append(dict(row))
        exported: list[dict[str, Any]] = []
        for row in rows:
            record = dict(row)
            record["raw_feature_json"] = _json_dict(record.get("raw_feature_json"))
            record["decision_audits"] = audits_by_snapshot.get(
                str(record["signal_snapshot_id"]), []
            )
            exported.append(record)
        return exported

    def strategy_win_rate(self) -> list[dict[str, Any]]:
        return self._aggregate(
            """
            SELECT strategy_name, trading_mode,
                   COUNT(*) AS trade_count,
                   AVG(CASE WHEN win_loss_label = 'win' THEN 1.0 ELSE 0.0 END) AS win_rate,
                   AVG(realized_pnl) AS avg_pnl,
                   AVG(realized_return_pct) AS avg_return_pct
            FROM trade_outcome_features
            GROUP BY strategy_name, trading_mode
            ORDER BY strategy_name, trading_mode
            """
        )

    def token_strategy_performance(self) -> list[dict[str, Any]]:
        return self._aggregate(
            """
            SELECT token_symbol, strategy_name, trading_mode,
                   COUNT(*) AS trade_count,
                   AVG(realized_pnl) AS avg_pnl,
                   AVG(realized_return_pct) AS avg_return_pct,
                   AVG(CASE WHEN win_loss_label = 'win' THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM trade_outcome_features
            GROUP BY token_symbol, strategy_name, trading_mode
            ORDER BY token_symbol, strategy_name, trading_mode
            """
        )

    def rejection_reason_breakdown(self) -> list[dict[str, Any]]:
        return self._aggregate(
            """
            SELECT stage, COALESCE(reason_code, 'unspecified') AS reason_code, COUNT(*) AS rejection_count
            FROM strategy_decision_audits
            WHERE decision = 'rejected'
            GROUP BY stage, COALESCE(reason_code, 'unspecified')
            ORDER BY stage, rejection_count DESC
            """
        )

    def slippage_and_fee_impact(self) -> list[dict[str, Any]]:
        return self._aggregate(
            """
            SELECT strategy_name, trading_mode,
                   AVG(fee_paid) AS avg_fee_paid,
                   SUM(fee_paid) AS total_fee_paid,
                   AVG(slippage_realized) AS avg_slippage_realized
            FROM trade_outcome_features
            GROUP BY strategy_name, trading_mode
            ORDER BY strategy_name, trading_mode
            """
        )

    def paper_vs_live_comparison(self) -> list[dict[str, Any]]:
        return self._aggregate(
            """
            SELECT trading_mode,
                   COUNT(*) AS trade_count,
                   AVG(realized_pnl) AS avg_pnl,
                   AVG(realized_return_pct) AS avg_return_pct,
                   AVG(CASE WHEN win_loss_label = 'win' THEN 1.0 ELSE 0.0 END) AS win_rate
            FROM trade_outcome_features
            GROUP BY trading_mode
            ORDER BY trading_mode
            """
        )

    def _aggregate(self, query: str) -> list[dict[str, Any]]:
        with self._engine.begin() as conn:
            return [dict(row) for row in conn.execute(text(query)).mappings().all()]

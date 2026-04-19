from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine, text

from oziebot_api.services.trade_intelligence import TradeIntelligenceService


def _setup_db(db_path: Path) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    now = datetime.now(UTC).isoformat()
    with eng.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE strategy_signal_snapshots ("
                "id TEXT PRIMARY KEY, user_id TEXT, tenant_id TEXT, trading_mode TEXT, strategy_name TEXT, token_symbol TEXT,"
                "timestamp TEXT, current_price TEXT, best_bid TEXT, best_ask TEXT, spread_pct TEXT, estimated_slippage_pct TEXT,"
                "volume TEXT, volatility TEXT, confidence_score REAL, raw_feature_json TEXT, token_policy_status TEXT, token_policy_multiplier TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE strategy_decision_audits ("
                "id TEXT PRIMARY KEY, signal_snapshot_id TEXT, stage TEXT, decision TEXT, reason_code TEXT, reason_detail TEXT,"
                "size_before TEXT, size_after TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE trade_outcome_features ("
                "id TEXT PRIMARY KEY, trade_id TEXT, signal_snapshot_id TEXT, trading_mode TEXT, strategy_name TEXT, token_symbol TEXT,"
                "entry_price TEXT, exit_price TEXT, filled_size TEXT, fee_paid TEXT, slippage_realized TEXT, hold_seconds INTEGER,"
                "realized_pnl TEXT, realized_return_pct TEXT, max_favorable_excursion_pct TEXT, max_adverse_excursion_pct TEXT,"
                "exit_reason TEXT, win_loss_label TEXT, profitable_after_fees_label TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO strategy_signal_snapshots VALUES "
                "('snap-paper','user-1','tenant-1','paper','momentum','BTC-USD',:now,'50000','49990','50010','0.0004','0.0008','1000','0.01',0.82,:paper_features,'allowed','1'),"
                "('snap-live','user-1','tenant-1','live','momentum','BTC-USD',:now,'50000','49990','50010','0.0004','0.0008','1000','0.01',0.82,:live_features,'allowed','1')"
            ),
            {
                "now": now,
                "paper_features": '{"momentum_value": 0.02}',
                "live_features": '{"momentum_value": 0.03}',
            },
        )
        conn.execute(
            text(
                "INSERT INTO strategy_decision_audits VALUES "
                "('audit-1','snap-paper','strategy','emitted','buy','signal emitted','0.12','0.12',:now),"
                "('audit-2','snap-paper','risk','rejected','max_position_usd','cap reached','0.12','0',:now),"
                "('audit-3','snap-live','strategy','emitted','buy','signal emitted','0.12','0.12',:now)"
            ),
            {"now": now},
        )
        conn.execute(
            text(
                "INSERT INTO trade_outcome_features VALUES "
                "('outcome-1','trade-1','snap-paper','paper','momentum','BTC-USD','50000','51000','0.5','5','0.0002',300,'45','0.018','0.03','-0.01','take_profit','win','profitable',:now),"
                "('outcome-2','trade-2','snap-live','live','momentum','BTC-USD','50000','49500','0.5','5','0.0003',600,'-30','-0.012','0.01','-0.02','stop_loss','loss','not_profitable',:now)"
            ),
            {"now": now},
        )


def test_trade_intelligence_service_exports_training_rows_and_analytics(tmp_path: Path):
    db_path = tmp_path / "trade-intelligence.sqlite"
    _setup_db(db_path)
    svc = TradeIntelligenceService(create_engine(f"sqlite+pysqlite:///{db_path}"))

    exported = svc.export_training_data(limit=10)
    assert len(exported) == 2
    assert exported[0]["decision_audits"]
    assert "momentum_value" in exported[0]["raw_feature_json"]

    win_rates = svc.strategy_win_rate()
    assert len(win_rates) == 2
    breakdown = svc.rejection_reason_breakdown()
    assert any(row["reason_code"] == "max_position_usd" for row in breakdown)
    comparison = svc.paper_vs_live_comparison()
    assert {row["trading_mode"] for row in comparison} == {"paper", "live"}

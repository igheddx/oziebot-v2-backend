from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.execution import ExecutionOrder, ExecutionTradeRecord
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_signal_pipeline import StrategyRun, StrategySignalRecord
from oziebot_api.models.trade_intelligence import TradeOutcomeFeature
from oziebot_api.models.user import User
from oziebot_api.services import trade_review_analytics


def _seed_trade_review_data(db_session: Session, user: User, membership: TenantMembership) -> None:
    now = datetime.now(UTC)

    paper_run = uuid.uuid4()
    live_run = uuid.uuid4()
    blocked_run = uuid.uuid4()
    paper_order_id = uuid.uuid4()
    live_order_id = uuid.uuid4()
    live_failed_order_id = uuid.uuid4()
    paper_trade_id = uuid.uuid4()
    live_trade_id = uuid.uuid4()

    db_session.add_all(
        [
            StrategyRun(
                run_id=paper_run,
                user_id=user.id,
                strategy_name="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                status="completed",
                trace_id="trace-paper-run",
                run_metadata={"confidence": 0.81},
                started_at=now - timedelta(hours=2),
                completed_at=now - timedelta(hours=2),
            ),
            StrategyRun(
                run_id=live_run,
                user_id=user.id,
                strategy_name="momentum",
                symbol="BTC-USD",
                trading_mode="live",
                status="completed",
                trace_id="trace-live-run",
                run_metadata={"confidence": 0.69},
                started_at=now - timedelta(hours=1),
                completed_at=now - timedelta(hours=1),
            ),
            StrategyRun(
                run_id=blocked_run,
                user_id=user.id,
                strategy_name="reversion",
                symbol="ETH-USD",
                trading_mode="paper",
                status="completed",
                trace_id="trace-blocked-run",
                run_metadata={"suppressed": True, "suppression_reason": "max_open_positions"},
                started_at=now - timedelta(minutes=40),
                completed_at=now - timedelta(minutes=40),
            ),
            StrategySignalRecord(
                signal_id=uuid.uuid4(),
                run_id=paper_run,
                user_id=user.id,
                strategy_name="momentum",
                symbol="BTC-USD",
                action="buy",
                confidence=0.81,
                suggested_size="0.20",
                reasoning_metadata={"reason": "paper breakout"},
                trading_mode="paper",
                timestamp=now - timedelta(hours=2),
            ),
            StrategySignalRecord(
                signal_id=uuid.uuid4(),
                run_id=live_run,
                user_id=user.id,
                strategy_name="momentum",
                symbol="BTC-USD",
                action="buy",
                confidence=0.69,
                suggested_size="0.12",
                reasoning_metadata={"reason": "live breakout"},
                trading_mode="live",
                timestamp=now - timedelta(hours=1),
            ),
            RiskEvent(
                id=uuid.uuid4(),
                signal_id=uuid.uuid4(),
                run_id=uuid.uuid4(),
                user_id=user.id,
                strategy_name="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                outcome="reduce_size",
                reason="position_limit",
                detail="Reduce to stay under cap",
                original_size="0.20",
                final_size="0.10",
                trace_id="risk-reduce",
                rules_evaluated={"rules": ["position_limit"]},
                signal_payload={},
                created_at=now - timedelta(hours=2),
            ),
            RiskEvent(
                id=uuid.uuid4(),
                signal_id=uuid.uuid4(),
                run_id=uuid.uuid4(),
                user_id=user.id,
                strategy_name="day_trading",
                symbol="SOL-USD",
                trading_mode="paper",
                outcome="reject",
                reason="fee_economics",
                detail="Expected net edge below threshold",
                original_size="0.15",
                final_size="0",
                trace_id="risk-reject",
                rules_evaluated={"rules": ["fee_economics"]},
                signal_payload={},
                created_at=now - timedelta(minutes=55),
            ),
            ExecutionOrder(
                id=paper_order_id,
                intent_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="BTC-USD",
                side="buy",
                order_type="limit",
                trading_mode="paper",
                venue="coinbase",
                state="filled",
                quantity="0.10",
                requested_notional_cents=5000,
                reserved_cash_cents=0,
                locked_cash_cents=5000,
                filled_quantity="0.10",
                avg_fill_price="50000",
                fees_cents=80,
                expected_gross_edge_bps=120,
                estimated_fee_bps=60,
                estimated_slippage_bps=4,
                estimated_total_cost_bps=64,
                expected_net_edge_bps=56,
                execution_preference="maker_preferred",
                fallback_behavior="convert_to_taker",
                maker_timeout_seconds=10,
                limit_price_offset_bps=2,
                actual_fill_type="maker",
                fallback_triggered=False,
                idempotency_key="analytics-paper-order",
                client_order_id="analytics-paper-order",
                venue_order_id="venue-paper-order",
                failure_code=None,
                failure_detail=None,
                trace_id="execution-paper",
                intent_payload={},
                risk_payload={},
                adapter_payload={},
                created_at=now - timedelta(hours=2),
                updated_at=now - timedelta(hours=2),
                submitted_at=now - timedelta(hours=2),
                completed_at=now - timedelta(hours=2),
                cancelled_at=None,
                failed_at=None,
            ),
            ExecutionOrder(
                id=live_order_id,
                intent_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="BTC-USD",
                side="buy",
                order_type="market",
                trading_mode="live",
                venue="coinbase",
                state="filled",
                quantity="0.10",
                requested_notional_cents=5000,
                reserved_cash_cents=0,
                locked_cash_cents=5000,
                filled_quantity="0.10",
                avg_fill_price="50000",
                fees_cents=120,
                expected_gross_edge_bps=105,
                estimated_fee_bps=75,
                estimated_slippage_bps=8,
                estimated_total_cost_bps=83,
                expected_net_edge_bps=22,
                execution_preference="taker_allowed",
                fallback_behavior="cancel",
                maker_timeout_seconds=0,
                limit_price_offset_bps=0,
                actual_fill_type="taker",
                fallback_triggered=False,
                idempotency_key="analytics-live-order",
                client_order_id="analytics-live-order",
                venue_order_id="venue-live-order",
                failure_code=None,
                failure_detail=None,
                trace_id="execution-live",
                intent_payload={},
                risk_payload={},
                adapter_payload={},
                created_at=now - timedelta(hours=1),
                updated_at=now - timedelta(hours=1),
                submitted_at=now - timedelta(hours=1),
                completed_at=now - timedelta(hours=1),
                cancelled_at=None,
                failed_at=None,
            ),
            ExecutionOrder(
                id=live_failed_order_id,
                intent_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="day_trading",
                symbol="SOL-USD",
                side="buy",
                order_type="market",
                trading_mode="paper",
                venue="coinbase",
                state="failed",
                quantity="1.00",
                requested_notional_cents=2000,
                reserved_cash_cents=0,
                locked_cash_cents=0,
                filled_quantity="0",
                avg_fill_price=None,
                fees_cents=0,
                expected_gross_edge_bps=90,
                estimated_fee_bps=80,
                estimated_slippage_bps=10,
                estimated_total_cost_bps=90,
                expected_net_edge_bps=0,
                execution_preference="taker_allowed",
                fallback_behavior="cancel",
                maker_timeout_seconds=0,
                limit_price_offset_bps=0,
                actual_fill_type=None,
                fallback_triggered=False,
                idempotency_key="analytics-failed-order",
                client_order_id="analytics-failed-order",
                venue_order_id=None,
                failure_code="coinbase_connection",
                failure_detail="Connection is not trade-enabled",
                trace_id="execution-failed",
                intent_payload={},
                risk_payload={},
                adapter_payload={},
                created_at=now - timedelta(minutes=50),
                updated_at=now - timedelta(minutes=50),
                submitted_at=None,
                completed_at=None,
                cancelled_at=None,
                failed_at=now - timedelta(minutes=50),
            ),
            ExecutionTradeRecord(
                id=paper_trade_id,
                order_id=paper_order_id,
                fill_id=None,
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                side="buy",
                quantity="0.10",
                price="50000",
                gross_notional_cents=5000,
                fee_cents=80,
                realized_pnl_cents=150,
                position_quantity_after="0.10",
                avg_entry_price_after="50000",
                executed_at=now - timedelta(hours=2),
                raw_payload={},
            ),
            ExecutionTradeRecord(
                id=live_trade_id,
                order_id=live_order_id,
                fill_id=None,
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="BTC-USD",
                trading_mode="live",
                side="buy",
                quantity="0.10",
                price="50000",
                gross_notional_cents=5000,
                fee_cents=120,
                realized_pnl_cents=-60,
                position_quantity_after="0.10",
                avg_entry_price_after="50000",
                executed_at=now - timedelta(hours=1),
                raw_payload={},
            ),
            TradeOutcomeFeature(
                trade_id=paper_trade_id,
                signal_snapshot_id=None,
                trading_mode="paper",
                strategy_name="momentum",
                token_symbol="BTC-USD",
                entry_price=50000,
                exit_price=50750,
                filled_size=0.10,
                fee_paid=0.8,
                slippage_realized=0.0004,
                hold_seconds=900,
                realized_pnl=15.0,
                realized_return_pct=0.03,
                max_favorable_excursion_pct=0.04,
                max_adverse_excursion_pct=-0.01,
                exit_reason="take_profit",
                win_loss_label="win",
                profitable_after_fees_label="profitable",
                created_at=now - timedelta(hours=2),
            ),
            TradeOutcomeFeature(
                trade_id=live_trade_id,
                signal_snapshot_id=None,
                trading_mode="live",
                strategy_name="momentum",
                token_symbol="BTC-USD",
                entry_price=50000,
                exit_price=49700,
                filled_size=0.10,
                fee_paid=1.2,
                slippage_realized=0.0008,
                hold_seconds=1500,
                realized_pnl=-6.0,
                realized_return_pct=-0.012,
                max_favorable_excursion_pct=0.01,
                max_adverse_excursion_pct=-0.03,
                exit_reason="stop_loss",
                win_loss_label="loss",
                profitable_after_fees_label="not_profitable",
                created_at=now - timedelta(hours=1),
            ),
        ]
    )
    db_session.commit()


def test_trade_review_analytics_overview(client, regular_user_and_token, db_session: Session):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_trade_review_data(db_session, user, membership)

    response = client.get(
        "/v1/me/analytics?trading_mode=paper",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["summary"]["evaluated"] == 2
    assert payload["summary"]["rejected"] == 2
    assert payload["summary"]["executed"] == 1
    assert payload["summary"]["profitable"] == 1
    assert payload["rejectionBreakdown"]["totalRejected"] == 3
    assert {row["strategyName"] for row in payload["strategyPerformance"]} >= {
        "momentum",
        "reversion",
    }
    momentum_rows = [
        row
        for row in payload["strategyPerformance"]
        if row["strategyName"] == "momentum" and row["tradingMode"] == "paper"
    ]
    assert momentum_rows
    assert momentum_rows[0]["winRatePct"] == 100.0
    assert payload["paperLiveComparison"]["overview"]


def test_trade_review_analytics_pair_endpoint_honors_filters(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_trade_review_data(db_session, user, membership)

    response = client.get(
        "/v1/me/analytics/pairs?trading_mode=live&strategy_name=momentum&symbol=BTC-USD",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["filters"]["tradingMode"] == "live"
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["strategyName"] == "momentum"
    assert payload["rows"][0]["symbol"] == "BTC-USD"
    assert payload["rows"][0]["tradingMode"] == "live"


def test_trade_review_analytics_summary_clamps_lookback_window(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_trade_review_data(db_session, user, membership)

    requested_start_at = datetime.now(UTC) - timedelta(days=365)
    requested_start_at_param = requested_start_at.isoformat().replace("+00:00", "Z")
    response = client.get(
        f"/v1/me/analytics/summary?trading_mode=paper&start_at={requested_start_at_param}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    budget = payload["budget"]
    assert budget["windowClamped"] is True
    assert budget["lookbackDaysApplied"] == 90
    assert budget["requestedStartAt"] == requested_start_at.isoformat()
    assert payload["filters"]["startAt"] == budget["startAt"]


def test_trade_review_analytics_reports_budget_degradation(
    client, regular_user_and_token, db_session: Session, monkeypatch
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_trade_review_data(db_session, user, membership)
    monkeypatch.setattr(trade_review_analytics, "ANALYTICS_DATASET_ROW_LIMIT", 1)
    monkeypatch.setattr(trade_review_analytics, "ANALYTICS_GROUP_ROW_LIMIT", 1)

    response = client.get(
        "/v1/me/analytics/summary?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["budget"]["degraded"] is True
    assert payload["budget"]["datasets"]["runs"]["truncated"] is True
    assert "runs" in payload["budget"]["truncatedDatasets"]


def test_trade_review_analytics_summary_does_not_build_full_overview(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_trade_review_data(db_session, user, membership)

    with patch(
        "oziebot_api.api.v1.me.TradeReviewAnalyticsService.build_overview",
        side_effect=AssertionError("summary route should not build full analytics overview"),
    ):
        response = client.get(
            "/v1/me/analytics/summary?trading_mode=paper&force_refresh=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["summary"]["evaluated"] == 2
    assert "momentum" in payload["availableStrategies"]
    assert "BTC-USD" in payload["availableSymbols"]


def test_trade_review_analytics_pair_endpoint_does_not_build_full_overview(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_trade_review_data(db_session, user, membership)

    with patch(
        "oziebot_api.api.v1.me.TradeReviewAnalyticsService.build_overview",
        side_effect=AssertionError("pair route should not build full analytics overview"),
    ):
        response = client.get(
            "/v1/me/analytics/pairs?trading_mode=paper&force_refresh=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["rows"]

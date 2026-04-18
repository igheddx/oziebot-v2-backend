from __future__ import annotations

from datetime import UTC, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.execution import ExecutionOrder, ExecutionTradeRecord
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.user import User


def test_dashboard_reports_available_balance_separately_from_portfolio(
    client,
    regular_user_and_token,
    db_session: Session,
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None

    now = datetime.now(UTC)
    db_session.add_all(
        [
            StrategyCapitalBucket(
                user_id=user.id,
                strategy_id="momentum",
                trading_mode="paper",
                assigned_capital_cents=61_000,
                available_cash_cents=59_000,
                reserved_cash_cents=0,
                locked_capital_cents=2_000,
                realized_pnl_cents=0,
                unrealized_pnl_cents=0,
                available_buying_power_cents=59_000,
                version=1,
                created_at=now,
                updated_at=now,
            ),
            StrategyCapitalBucket(
                user_id=user.id,
                strategy_id="day_trading",
                trading_mode="paper",
                assigned_capital_cents=39_000,
                available_cash_cents=39_000,
                reserved_cash_cents=0,
                locked_capital_cents=0,
                realized_pnl_cents=0,
                unrealized_pnl_cents=0,
                available_buying_power_cents=39_000,
                version=1,
                created_at=now,
                updated_at=now,
            ),
        ]
    )
    db_session.commit()

    summary = client.get(
        "/v1/me/dashboard?trading_mode=paper",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert summary.status_code == 200, summary.text
    payload = summary.json()
    assert payload["availableBalance"] == 980.0
    assert payload["portfolioValue"] == 1000.0


def test_dashboard_includes_fee_analytics(client, regular_user_and_token, db_session: Session):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    now = datetime.now(UTC)
    order_id = uuid.uuid4()
    db_session.add(
        ExecutionOrder(
            id=order_id,
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
            quantity="0.50",
            requested_notional_cents=25_000,
            reserved_cash_cents=0,
            locked_cash_cents=25_000,
            filled_quantity="0.50",
            avg_fill_price="50000",
            fees_cents=125,
            expected_gross_edge_bps=150,
            estimated_fee_bps=100,
            estimated_slippage_bps=8,
            estimated_total_cost_bps=115,
            expected_net_edge_bps=35,
            execution_preference="maker_preferred",
            fallback_behavior="convert_to_taker",
            maker_timeout_seconds=15,
            limit_price_offset_bps=2,
            actual_fill_type="mixed",
            fallback_triggered=True,
            idempotency_key="idem-dashboard-fee",
            client_order_id="client-dashboard-fee",
            venue_order_id="venue-dashboard-fee",
            failure_code=None,
            failure_detail=None,
            trace_id="trace-dashboard-fee",
            intent_payload={},
            risk_payload={},
            adapter_payload={},
            created_at=now,
            updated_at=now,
            submitted_at=now,
            completed_at=now,
            cancelled_at=None,
            failed_at=None,
        )
    )
    db_session.add(
        ExecutionTradeRecord(
            id=uuid.uuid4(),
            order_id=order_id,
            fill_id=None,
            tenant_id=membership.tenant_id,
            user_id=user.id,
            strategy_id="momentum",
            symbol="BTC-USD",
            trading_mode="paper",
            side="buy",
            quantity="0.50",
            price="50000",
            gross_notional_cents=25_000,
            fee_cents=125,
            realized_pnl_cents=600,
            position_quantity_after="0.50",
            avg_entry_price_after="50000",
            executed_at=now,
            raw_payload={},
        )
    )
    db_session.add(
        RiskEvent(
            id=uuid.uuid4(),
            signal_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            user_id=user.id,
            strategy_name="momentum",
            symbol="BTC-USD",
            trading_mode="paper",
            outcome="reject",
            reason="policy",
            detail="fee_economics: Expected net edge below threshold",
            original_size="0.50",
            final_size="0",
            trace_id="risk-dashboard-fee",
            rules_evaluated={"rules": ["fee_economics"]},
            signal_payload={},
            created_at=now,
        )
    )
    db_session.commit()

    summary = client.get(
        "/v1/me/dashboard?trading_mode=paper",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert summary.status_code == 200, summary.text
    analytics = summary.json()["feeAnalytics"]
    assert analytics["totalFeesToday"] == 1.25
    assert analytics["makerCount"] == 0
    assert analytics["mixedCount"] == 1
    assert analytics["avgNetEdgeAtEntryBps"] == 35.0
    assert analytics["skippedTradesDueToFees"] == 1

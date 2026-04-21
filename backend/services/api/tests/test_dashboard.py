from __future__ import annotations

from datetime import UTC, datetime
import os
import uuid
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.market_data import MarketDataBboSnapshot
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.trade_intelligence import (
    StrategyDecisionAudit,
    StrategySignalSnapshot,
)
from oziebot_api.models.user import User
from oziebot_api.services.credential_crypto import CredentialCrypto


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


def test_dashboard_includes_rejection_diagnostics(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    now = datetime.now(UTC)
    snapshot_id = uuid.uuid4()
    failed_order_id = uuid.uuid4()
    db_session.add(
        StrategySignalSnapshot(
            id=snapshot_id,
            user_id=user.id,
            tenant_id=membership.tenant_id,
            trading_mode="paper",
            strategy_name="momentum",
            token_symbol="BTC-USD",
            timestamp=now,
            current_price=65000,
            best_bid=64990,
            best_ask=65010,
            spread_pct=0.0003,
            estimated_slippage_pct=0.0008,
            volume=1000000,
            volatility=0.01,
            confidence_score=0.72,
            raw_feature_json={"momentum_value": 0.014},
            token_policy_status="allowed",
            token_policy_multiplier=1,
        )
    )
    db_session.add(
        StrategyDecisionAudit(
            signal_snapshot_id=snapshot_id,
            stage="suppression",
            decision="rejected",
            reason_code="max_open_positions reached",
            reason_detail="Strategy suppression blocked new buy",
            size_before=0.25,
            size_after=0,
            created_at=now,
        )
    )
    db_session.add(
        RiskEvent(
            id=uuid.uuid4(),
            signal_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            user_id=user.id,
            strategy_name="reversion",
            symbol="ETH-USD",
            trading_mode="paper",
            outcome="reject",
            reason="policy",
            detail="fee_economics: Expected net edge below threshold",
            original_size="0.20",
            final_size="0",
            trace_id="risk-dashboard-rejection",
            rules_evaluated={"rules": ["fee_economics"]},
            signal_payload={},
            created_at=now,
        )
    )
    db_session.add(
        ExecutionOrder(
            id=failed_order_id,
            intent_id=uuid.uuid4(),
            correlation_id=uuid.uuid4(),
            tenant_id=membership.tenant_id,
            user_id=user.id,
            strategy_id="dca",
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
            expected_gross_edge_bps=120,
            estimated_fee_bps=120,
            estimated_slippage_bps=8,
            estimated_total_cost_bps=128,
            expected_net_edge_bps=-8,
            execution_preference="taker_allowed",
            fallback_behavior="cancel",
            maker_timeout_seconds=0,
            limit_price_offset_bps=0,
            actual_fill_type=None,
            fallback_triggered=False,
            idempotency_key="idem-dashboard-rejection",
            client_order_id="client-dashboard-rejection",
            venue_order_id=None,
            failure_code="coinbase_connection",
            failure_detail="Coinbase connection is not trade-enabled",
            trace_id="execution-dashboard-rejection",
            intent_payload={},
            risk_payload={},
            adapter_payload={},
            created_at=now,
            updated_at=now,
            submitted_at=None,
            completed_at=None,
            cancelled_at=None,
            failed_at=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard?trading_mode=paper",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    diagnostics = response.json()["rejectionDiagnostics"]
    assert diagnostics["totalRejected"] == 3
    assert diagnostics["byStage"] == [
        {"stage": "execution", "count": 1},
        {"stage": "risk", "count": 1},
        {"stage": "suppression", "count": 1},
    ]
    assert diagnostics["breakdown"][0]["count"] == 1
    assert {row["stage"] for row in diagnostics["breakdown"]} == {
        "suppression",
        "risk",
        "execution",
    }
    assert diagnostics["recent"][0]["reasonCode"] in {
        "max_open_positions reached",
        "policy",
        "coinbase_connection",
    }


def test_dashboard_summary_does_not_fetch_live_coinbase_balances(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None

    now = datetime.now(UTC)
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="live",
            assigned_capital_cents=100_000,
            available_cash_cents=95_000,
            reserved_cash_cents=0,
            locked_capital_cents=5_000,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=95_000,
            version=1,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()

    with patch(
        "oziebot_api.api.v1.me.load_live_coinbase_accounts",
        side_effect=AssertionError("summary path should not call live Coinbase"),
    ):
        response = client.get(
            "/v1/me/dashboard/summary?trading_mode=live&force_refresh=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["availableBalance"] == 950.0
    assert payload["portfolioValue"] == 1000.0
    assert payload["budget"]["summaryOnly"] is True
    assert payload["totalRejected"] == 0


def test_dashboard_summary_ignores_rejection_diagnostics_history(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    now = datetime.now(UTC)
    snapshot_id = uuid.uuid4()
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="paper",
            assigned_capital_cents=100_000,
            available_cash_cents=95_000,
            reserved_cash_cents=0,
            locked_capital_cents=5_000,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=95_000,
            version=1,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        StrategySignalSnapshot(
            id=snapshot_id,
            user_id=user.id,
            tenant_id=membership.tenant_id,
            trading_mode="paper",
            strategy_name="momentum",
            token_symbol="BTC-USD",
            timestamp=now,
            current_price=65000,
            best_bid=64990,
            best_ask=65010,
            spread_pct=0.0003,
            estimated_slippage_pct=0.0008,
            volume=1000000,
            volatility=0.01,
            confidence_score=0.72,
            raw_feature_json={"momentum_value": 0.014},
            token_policy_status="allowed",
            token_policy_multiplier=1,
        )
    )
    db_session.add(
        StrategyDecisionAudit(
            signal_snapshot_id=snapshot_id,
            stage="suppression",
            decision="rejected",
            reason_code="max_open_positions reached",
            reason_detail="Strategy suppression blocked new buy",
            size_before=0.25,
            size_after=0,
            created_at=now,
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
            original_size="0.25",
            final_size="0",
            trace_id="risk-dashboard-summary",
            rules_evaluated={"rules": ["fee_economics"]},
            signal_payload={},
            created_at=now,
        )
    )
    db_session.add(
        ExecutionOrder(
            id=uuid.uuid4(),
            intent_id=uuid.uuid4(),
            correlation_id=uuid.uuid4(),
            tenant_id=membership.tenant_id,
            user_id=user.id,
            strategy_id="momentum",
            symbol="BTC-USD",
            side="buy",
            order_type="market",
            trading_mode="paper",
            venue="coinbase",
            state="failed",
            quantity="0.25",
            requested_notional_cents=10_000,
            reserved_cash_cents=0,
            locked_cash_cents=0,
            filled_quantity="0",
            avg_fill_price=None,
            fees_cents=0,
            expected_gross_edge_bps=100,
            estimated_fee_bps=90,
            estimated_slippage_bps=8,
            estimated_total_cost_bps=98,
            expected_net_edge_bps=2,
            execution_preference="taker_allowed",
            fallback_behavior="cancel",
            maker_timeout_seconds=0,
            limit_price_offset_bps=0,
            actual_fill_type=None,
            fallback_triggered=False,
            idempotency_key="idem-dashboard-summary",
            client_order_id="client-dashboard-summary",
            venue_order_id=None,
            failure_code="venue_error",
            failure_detail="Synthetic failed order for summary regression",
            trace_id="execution-dashboard-summary",
            intent_payload={},
            risk_payload={},
            adapter_payload={},
            created_at=now,
            updated_at=now,
            submitted_at=None,
            completed_at=None,
            cancelled_at=None,
            failed_at=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/summary?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["availableBalance"] == 950.0
    assert payload["portfolioValue"] == 1000.0
    assert payload["totalRejected"] == 0


def test_dashboard_details_does_not_fetch_live_coinbase_balances(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None

    now = datetime.now(UTC)
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="live",
            assigned_capital_cents=100_000,
            available_cash_cents=95_000,
            reserved_cash_cents=0,
            locked_capital_cents=5_000,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=95_000,
            version=1,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()

    with patch(
        "oziebot_api.api.v1.me.load_live_coinbase_accounts",
        side_effect=AssertionError("details path should not call live Coinbase"),
    ):
        response = client.get(
            "/v1/me/dashboard/details?trading_mode=live&force_refresh=true",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["positions"] == []
    assert payload["budget"]["historyLookbackDaysApplied"] == 30
    assert payload["rejectionDiagnostics"]["totalRejected"] == 0
    assert payload["feeAnalytics"]["skippedTradesDueToFees"] == 0


def test_dashboard_details_ignores_rejection_history(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    now = datetime.now(UTC)
    snapshot_id = uuid.uuid4()
    failed_order_id = uuid.uuid4()
    db_session.add(
        StrategySignalSnapshot(
            id=snapshot_id,
            user_id=user.id,
            tenant_id=membership.tenant_id,
            trading_mode="paper",
            strategy_name="momentum",
            token_symbol="BTC-USD",
            timestamp=now,
            current_price=65000,
            best_bid=64990,
            best_ask=65010,
            spread_pct=0.0003,
            estimated_slippage_pct=0.0008,
            volume=1000000,
            volatility=0.01,
            confidence_score=0.72,
            raw_feature_json={"momentum_value": 0.014},
            token_policy_status="allowed",
            token_policy_multiplier=1,
        )
    )
    db_session.add(
        StrategyDecisionAudit(
            signal_snapshot_id=snapshot_id,
            stage="suppression",
            decision="rejected",
            reason_code="max_open_positions reached",
            reason_detail="Strategy suppression blocked new buy",
            size_before=0.25,
            size_after=0,
            created_at=now,
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
            original_size="0.25",
            final_size="0",
            trace_id="risk-dashboard-details",
            rules_evaluated={"rules": ["fee_economics"]},
            signal_payload={},
            created_at=now,
        )
    )
    db_session.add(
        ExecutionOrder(
            id=failed_order_id,
            intent_id=uuid.uuid4(),
            correlation_id=uuid.uuid4(),
            tenant_id=membership.tenant_id,
            user_id=user.id,
            strategy_id="momentum",
            symbol="BTC-USD",
            side="buy",
            order_type="market",
            trading_mode="paper",
            venue="coinbase",
            state="failed",
            quantity="0.25",
            requested_notional_cents=10_000,
            reserved_cash_cents=0,
            locked_cash_cents=0,
            filled_quantity="0",
            avg_fill_price=None,
            fees_cents=0,
            expected_gross_edge_bps=100,
            estimated_fee_bps=90,
            estimated_slippage_bps=8,
            estimated_total_cost_bps=98,
            expected_net_edge_bps=2,
            execution_preference="taker_allowed",
            fallback_behavior="cancel",
            maker_timeout_seconds=0,
            limit_price_offset_bps=0,
            actual_fill_type=None,
            fallback_triggered=False,
            idempotency_key="idem-dashboard-details",
            client_order_id="client-dashboard-details",
            venue_order_id=None,
            failure_code="venue_error",
            failure_detail="Synthetic failed order for details regression",
            trace_id="execution-dashboard-details",
            intent_payload={},
            risk_payload={},
            adapter_payload={},
            created_at=now,
            updated_at=now,
            submitted_at=None,
            completed_at=None,
            cancelled_at=None,
            failed_at=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["rejectionDiagnostics"] == {
        "totalRejected": 0,
        "byStage": [],
        "breakdown": [],
        "recent": [],
    }
    assert payload["feeAnalytics"]["skippedTradesDueToFees"] == 0


def test_dashboard_rejections_are_loaded_from_bounded_endpoint(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    now = datetime.now(UTC)
    snapshot_id = uuid.uuid4()
    failed_order_id = uuid.uuid4()
    db_session.add(
        StrategySignalSnapshot(
            id=snapshot_id,
            user_id=user.id,
            tenant_id=membership.tenant_id,
            trading_mode="paper",
            strategy_name="momentum",
            token_symbol="BTC-USD",
            timestamp=now,
            current_price=65000,
            best_bid=64990,
            best_ask=65010,
            spread_pct=0.0003,
            estimated_slippage_pct=0.0008,
            volume=1000000,
            volatility=0.01,
            confidence_score=0.72,
            raw_feature_json={"momentum_value": 0.014},
            token_policy_status="allowed",
            token_policy_multiplier=1,
        )
    )
    db_session.add(
        StrategyDecisionAudit(
            signal_snapshot_id=snapshot_id,
            stage="suppression",
            decision="rejected",
            reason_code="max_open_positions reached",
            reason_detail="Strategy suppression blocked new buy",
            size_before=0.25,
            size_after=0,
            created_at=now,
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
            original_size="0.25",
            final_size="0",
            trace_id="risk-dashboard-rejections",
            rules_evaluated={"rules": ["fee_economics"]},
            signal_payload={},
            created_at=now,
        )
    )
    db_session.add(
        ExecutionOrder(
            id=failed_order_id,
            intent_id=uuid.uuid4(),
            correlation_id=uuid.uuid4(),
            tenant_id=membership.tenant_id,
            user_id=user.id,
            strategy_id="momentum",
            symbol="BTC-USD",
            side="buy",
            order_type="market",
            trading_mode="paper",
            venue="coinbase",
            state="failed",
            quantity="0.25",
            requested_notional_cents=10_000,
            reserved_cash_cents=0,
            locked_cash_cents=0,
            filled_quantity="0",
            avg_fill_price=None,
            fees_cents=0,
            expected_gross_edge_bps=100,
            estimated_fee_bps=90,
            estimated_slippage_bps=8,
            estimated_total_cost_bps=98,
            expected_net_edge_bps=2,
            execution_preference="taker_allowed",
            fallback_behavior="cancel",
            maker_timeout_seconds=0,
            limit_price_offset_bps=0,
            actual_fill_type=None,
            fallback_triggered=False,
            idempotency_key="idem-dashboard-rejections",
            client_order_id="client-dashboard-rejections",
            venue_order_id=None,
            failure_code="venue_error",
            failure_detail="Synthetic failed order for rejections regression",
            trace_id="execution-dashboard-rejections",
            intent_payload={},
            risk_payload={},
            adapter_payload={},
            created_at=now,
            updated_at=now,
            submitted_at=None,
            completed_at=None,
            cancelled_at=None,
            failed_at=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/rejections?trading_mode=paper&window_hours=24&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["windowHours"] == 24
    assert payload["skippedTradesDueToFees"] == 1
    assert payload["rejectionDiagnostics"]["totalRejected"] == 3
    assert {row["stage"] for row in payload["rejectionDiagnostics"]["byStage"]} == {
        "suppression",
        "risk",
        "execution",
    }


def test_dashboard_details_use_market_marks_and_hide_dust_positions(
    client, regular_user_and_token, db_session: Session
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    now = datetime.now(UTC)
    db_session.add_all(
        [
            ExecutionPosition(
                id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="OP-USD",
                trading_mode="paper",
                quantity="24",
                avg_entry_price="0.12",
                realized_pnl_cents=0,
                created_at=now,
                updated_at=now,
            ),
            ExecutionPosition(
                id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="ZORA-USD",
                trading_mode="paper",
                quantity="0.01",
                avg_entry_price="0.50",
                realized_pnl_cents=0,
                created_at=now,
                updated_at=now,
            ),
            MarketDataBboSnapshot(
                source="coinbase",
                product_id="OP-USD",
                best_bid_price=0.99,
                best_bid_size=100,
                best_ask_price=1.01,
                best_ask_size=100,
                event_time=now,
                ingest_time=now,
            ),
            MarketDataBboSnapshot(
                source="coinbase",
                product_id="ZORA-USD",
                best_bid_price=0.49,
                best_bid_size=100,
                best_ask_price=0.51,
                best_ask_size=100,
                event_time=now,
                ingest_time=now,
            ),
        ]
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert len(payload["positions"]) == 1
    position = payload["positions"][0]
    assert position["symbol"] == "OP-USD"
    assert position["entryPrice"] == 0.12
    assert position["markPrice"] == 1.0
    assert abs(position["unrealizedPnl"] - 21.12) < 1e-9
    assert position["exposure"] == 24.0


@patch("oziebot_api.api.v1.me.load_live_coinbase_accounts")
def test_live_dashboard_uses_coinbase_balances_for_available_and_portfolio(
    mock_load_live_coinbase_accounts,
    client,
    regular_user_and_token,
    db_session: Session,
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    now = datetime.now(UTC)
    crypto = CredentialCrypto(os.environ["EXCHANGE_CREDENTIALS_ENCRYPTION_KEY"])
    db_session.add(
        ExchangeConnection(
            tenant_id=membership.tenant_id,
            provider="coinbase",
            api_key_name="organizations/test/key",
            encrypted_secret=crypto.encrypt(b"test-private-key"),
            secret_ciphertext_version=1,
            validation_status="valid",
            health_status="healthy",
            can_trade=True,
            can_read_balances=True,
            created_at=now,
            updated_at=now,
            last_validated_at=now,
            last_health_check_at=now,
        )
    )
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="live",
            assigned_capital_cents=999_999,
            available_cash_cents=999_999,
            reserved_cash_cents=0,
            locked_capital_cents=0,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=999_999,
            version=1,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        ExecutionPosition(
            id=uuid.uuid4(),
            tenant_id=membership.tenant_id,
            user_id=user.id,
            strategy_id="momentum",
            symbol="BTC-USD",
            trading_mode="live",
            quantity="0.60",
            avg_entry_price="50000",
            realized_pnl_cents=0,
            updated_at=now,
            created_at=now,
        )
    )
    db_session.commit()

    mock_load_live_coinbase_accounts.return_value = [
        {
            "currency": "USD",
            "available_balance": {"currency": "USD", "value": "120.50"},
            "hold": {"currency": "USD", "value": "10.25"},
        },
        {
            "currency": "USDC",
            "available_balance": {"currency": "USDC", "value": "50.00"},
            "hold": {"currency": "USDC", "value": "5.00"},
        },
        {
            "currency": "BTC",
            "available_balance": {"currency": "BTC", "value": "0.50"},
            "hold": {"currency": "BTC", "value": "0.10"},
        },
    ]

    summary = client.get(
        "/v1/me/dashboard?trading_mode=live",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert summary.status_code == 200, summary.text
    payload = summary.json()
    assert payload["availableBalance"] == 170.5
    assert payload["portfolioValue"] == 30185.75

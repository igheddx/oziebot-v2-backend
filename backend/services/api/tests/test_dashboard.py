from __future__ import annotations

from datetime import UTC, datetime, timedelta
import os
import uuid
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.ai_diagnostics import (
    AiDiagnosticFinding,
    AiDiagnosticReview,
    DiagnosticSnapshot,
)
from oziebot_api.models.market_data import MarketDataBboSnapshot
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.strategy_lifecycle import StrategyLifecycleEvent
from oziebot_api.models.strategy_signal_pipeline import StrategyRun
from oziebot_api.models.trade_intelligence import (
    StrategyDecisionAudit,
    StrategySignalSnapshot,
)
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy
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


def test_dashboard_summary_growth_can_finish_below_prior_peak(
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
    order_id = uuid.uuid4()
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="paper",
            assigned_capital_cents=100_000,
            available_cash_cents=11_000,
            reserved_cash_cents=0,
            locked_capital_cents=2_000,
            realized_pnl_cents=3_000,
            unrealized_pnl_cents=-21_500,
            available_buying_power_cents=11_000,
            version=1,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add_all(
        [
            ExecutionTradeRecord(
                id=uuid.uuid4(),
                order_id=order_id,
                fill_id=None,
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                side="sell",
                quantity="0.10",
                price="65000",
                gross_notional_cents=6_500,
                fee_cents=50,
                realized_pnl_cents=20_000,
                position_quantity_after="0.20",
                avg_entry_price_after="50000",
                executed_at=now - timedelta(hours=4),
                raw_payload={},
            ),
            ExecutionTradeRecord(
                id=uuid.uuid4(),
                order_id=order_id,
                fill_id=None,
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="ETH-USD",
                trading_mode="paper",
                side="sell",
                quantity="1.00",
                price="3200",
                gross_notional_cents=3_200,
                fee_cents=25,
                realized_pnl_cents=10_000,
                position_quantity_after="0.00",
                avg_entry_price_after="0",
                executed_at=now - timedelta(hours=2),
                raw_payload={},
            ),
            ExecutionPosition(
                id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="SOL-USD",
                trading_mode="paper",
                quantity="10",
                avg_entry_price="20",
                realized_pnl_cents=3_000,
                created_at=now - timedelta(hours=5),
                updated_at=now,
                opened_at=now - timedelta(hours=5),
                last_trade_at=now - timedelta(hours=2),
            ),
            MarketDataBboSnapshot(
                source="coinbase",
                product_id="SOL-USD",
                best_bid_price=17.8,
                best_bid_size=100,
                best_ask_price=17.9,
                best_ask_size=100,
                event_time=now,
                ingest_time=now,
            ),
        ]
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/summary?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["portfolioValue"] == 108.5
    assert payload["pnlValue"] == 8.5
    assert payload["realizedPnlValue"] == 30.0
    assert payload["unrealizedPnlValue"] == -21.5
    assert payload["gainLossLabel"] == "Total P&L"
    assert payload["growth"][-1] == 108.5
    assert max(payload["growth"]) > payload["growth"][-1]


def test_dashboard_exposes_capital_utilization_metrics(
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
    order_id = uuid.uuid4()
    db_session.add_all(
        [
            StrategyCapitalBucket(
                user_id=user.id,
                strategy_id="momentum",
                trading_mode="paper",
                assigned_capital_cents=60_000,
                available_cash_cents=30_000,
                reserved_cash_cents=5_000,
                locked_capital_cents=25_000,
                realized_pnl_cents=0,
                unrealized_pnl_cents=0,
                available_buying_power_cents=30_000,
                version=1,
                created_at=now,
                updated_at=now,
            ),
            StrategyCapitalBucket(
                user_id=user.id,
                strategy_id="day_trading",
                trading_mode="paper",
                assigned_capital_cents=40_000,
                available_cash_cents=28_000,
                reserved_cash_cents=2_000,
                locked_capital_cents=10_000,
                realized_pnl_cents=0,
                unrealized_pnl_cents=0,
                available_buying_power_cents=28_000,
                version=1,
                created_at=now,
                updated_at=now,
            ),
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
                fee_cents=100,
                realized_pnl_cents=0,
                position_quantity_after="0.50",
                avg_entry_price_after="50000",
                executed_at=now,
                raw_payload={},
            ),
            ExecutionTradeRecord(
                id=uuid.uuid4(),
                order_id=order_id,
                fill_id=None,
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="day_trading",
                symbol="ETH-USD",
                trading_mode="paper",
                side="buy",
                quantity="1.00",
                price="2500",
                gross_notional_cents=10_000,
                fee_cents=50,
                realized_pnl_cents=0,
                position_quantity_after="1.00",
                avg_entry_price_after="2500",
                executed_at=now,
                raw_payload={},
            ),
        ]
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()["capitalUtilization"]
    assert payload["totalCapital"] == 1000.0
    assert payload["availableCash"] == 580.0
    assert payload["reservedCash"] == 70.0
    assert payload["lockedCapital"] == 350.0
    assert payload["deployedCapital"] == 420.0
    assert payload["totalDeployedPct"] == 42.0
    assert payload["avgTradeSizeByStrategy"] == [
        {"strategy": "day_trading", "avgTradeSize": 100.0},
        {"strategy": "momentum", "avgTradeSize": 250.0},
    ]


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
        UserStrategy(
            user_id=user.id,
            strategy_id="momentum",
            is_enabled=True,
            config={},
            created_at=now,
            updated_at=now,
        )
    )
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


def test_dashboard_details_exposes_bot_health_and_strategy_wait_reasons(
    client,
    root_user_and_token,
    db_session: Session,
):
    email, token = root_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None

    now = datetime.now(UTC)
    next_due = now + timedelta(hours=12)
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="dca",
            trading_mode="paper",
            assigned_capital_cents=25_000,
            available_cash_cents=25_000,
            reserved_cash_cents=0,
            locked_capital_cents=0,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=25_000,
            version=1,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        StrategyRun(
            run_id=uuid.uuid4(),
            user_id=user.id,
            strategy_name="dca",
            symbol="BTC-USD",
            trading_mode="paper",
            status="completed",
            trace_id="trace-dashboard-bot-health",
            run_metadata={
                "suppressed": True,
                "suppression_reason": "skipped_due_to_interval",
                "next_eligible_buy_time": next_due.isoformat(),
            },
            started_at=now,
            completed_at=now,
        )
    )
    db_session.add(
        MarketDataBboSnapshot(
            source="coinbase",
            product_id="BTC-USD",
            best_bid_price=65000,
            best_bid_size=5,
            best_ask_price=65010,
            best_ask_size=5,
            event_time=now,
            ingest_time=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["botHealth"]["marketData"]["status"] == "fresh"
    assert payload["botHealth"]["quietReasonCode"] == "cooldown_active"
    assert payload["botHealth"]["paperLive"]["currentMode"] == "paper"
    assert payload["botHealth"]["paperLive"]["paperWarning"].startswith("Paper results may not")
    assert any(
        item["id"] == "market_data" for item in payload["botHealth"]["paperLive"]["checklist"]
    )
    dca_health = next(row for row in payload["strategyHealth"] if row["id"] == "dca")
    assert dca_health["currentStatus"] == "waiting"
    assert dca_health["blockingReasonCode"] == "cooldown_active"
    assert dca_health["dcaIntervalHours"] == 24
    assert dca_health["lastBuyAt"] is None
    assert dca_health["nextEligibleAt"] == next_due.isoformat()


def test_dashboard_paper_live_validation_uses_recent_failure_window(
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
    for age, key_prefix, trace_id in (
        (timedelta(hours=6), "recent-validation-failure", "trace-recent-validation-failure"),
        (timedelta(days=30), "old-validation-failure", "trace-old-validation-failure"),
    ):
        created_at = now - age
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
                venue="paper",
                state="failed",
                quantity="0",
                requested_notional_cents=0,
                reserved_cash_cents=0,
                locked_cash_cents=0,
                filled_quantity="0",
                avg_fill_price=None,
                fees_cents=0,
                expected_gross_edge_bps=0,
                estimated_fee_bps=0,
                estimated_slippage_bps=0,
                estimated_total_cost_bps=0,
                expected_net_edge_bps=0,
                execution_preference="maker_preferred",
                fallback_behavior="convert_to_taker",
                maker_timeout_seconds=0,
                limit_price_offset_bps=0,
                actual_fill_type=None,
                fallback_triggered=False,
                idempotency_key=f"{key_prefix}-idempotency",
                client_order_id=f"{key_prefix}-client-order",
                venue_order_id=None,
                failure_code="execution_validation_failed",
                failure_detail="quantity must be positive",
                trace_id=trace_id,
                intent_payload={},
                risk_payload={},
                adapter_payload=None,
                created_at=created_at,
                updated_at=created_at,
                submitted_at=None,
                completed_at=None,
                cancelled_at=None,
                failed_at=created_at,
            )
        )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    validation_item = next(
        item
        for item in payload["botHealth"]["paperLive"]["checklist"]
        if item["id"] == "execution_validation"
    )
    assert validation_item["label"] == "No recent execution validation failures (7d)"
    assert validation_item["passed"] is False
    assert "1 validation failure(s) were recorded in the last 7 days" in validation_item["detail"]
    assert "last seen" in validation_item["detail"]


def test_dashboard_details_counts_critical_findings_from_latest_review_only(
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
    older_snapshot = DiagnosticSnapshot(
        tenant_id=membership.tenant_id,
        generated_at=now - timedelta(days=2),
        trading_mode="paper",
        strategy_filter=None,
        token_filter=None,
        days_filter=7,
        raw_json={},
        created_at=now - timedelta(days=2),
    )
    latest_snapshot = DiagnosticSnapshot(
        tenant_id=membership.tenant_id,
        generated_at=now,
        trading_mode="paper",
        strategy_filter=None,
        token_filter=None,
        days_filter=7,
        raw_json={},
        created_at=now,
    )
    db_session.add_all([older_snapshot, latest_snapshot])
    db_session.flush()

    older_review = AiDiagnosticReview(
        tenant_id=membership.tenant_id,
        snapshot_id=older_snapshot.id,
        status="completed",
        overall_health="critical",
        confidence_score=0.8,
        summary="Older review",
        model_name="rule-based",
        prompt_version="ai-diagnostics-v1",
        created_by_admin_id=user.id,
        started_at=now - timedelta(days=2),
        completed_at=now - timedelta(days=2),
        error_message=None,
        created_at=now - timedelta(days=2),
        updated_at=now - timedelta(days=2),
    )
    latest_review = AiDiagnosticReview(
        tenant_id=membership.tenant_id,
        snapshot_id=latest_snapshot.id,
        status="completed",
        overall_health="warning",
        confidence_score=0.8,
        summary="Latest review",
        model_name="rule-based",
        prompt_version="ai-diagnostics-v1",
        created_by_admin_id=user.id,
        started_at=now,
        completed_at=now,
        error_message=None,
        created_at=now,
        updated_at=now,
    )
    db_session.add_all([older_review, latest_review])
    db_session.flush()

    db_session.add_all(
        [
            AiDiagnosticFinding(
                review_id=older_review.id,
                severity="critical",
                category="trading_safety",
                strategy="momentum",
                token="BTC-USD",
                finding_title="Older critical finding",
                finding_detail="Should not be counted once a newer review exists.",
                evidence_json={},
                recommendation="Review it",
                risk_if_ignored="Risk",
                confidence_score=0.9,
                automation_eligibility="not_eligible",
                status="new",
                future_config_change_candidate=False,
                proposed_config_change_json=None,
                approval_required=False,
                eligible_for_auto_tune=False,
                rollback_plan=None,
                expected_impact=None,
                risk_level="high",
                affected_strategy="momentum",
                affected_token="BTC-USD",
                parameter_name=None,
                current_value_json=None,
                proposed_value_json=None,
                created_at=now - timedelta(days=2),
                updated_at=now - timedelta(days=2),
            ),
            AiDiagnosticFinding(
                review_id=older_review.id,
                severity="critical",
                category="trading_safety",
                strategy="day_trading",
                token="ETH-USD",
                finding_title="Older critical finding 2",
                finding_detail="Should not be counted once a newer review exists.",
                evidence_json={},
                recommendation="Review it",
                risk_if_ignored="Risk",
                confidence_score=0.9,
                automation_eligibility="not_eligible",
                status="acknowledged",
                future_config_change_candidate=False,
                proposed_config_change_json=None,
                approval_required=False,
                eligible_for_auto_tune=False,
                rollback_plan=None,
                expected_impact=None,
                risk_level="high",
                affected_strategy="day_trading",
                affected_token="ETH-USD",
                parameter_name=None,
                current_value_json=None,
                proposed_value_json=None,
                created_at=now - timedelta(days=2),
                updated_at=now - timedelta(days=2),
            ),
            AiDiagnosticFinding(
                review_id=latest_review.id,
                severity="critical",
                category="lifecycle_visibility",
                strategy="reversion",
                token="SOL-USD",
                finding_title="Latest critical finding",
                finding_detail="This one should be counted.",
                evidence_json={},
                recommendation="Review it",
                risk_if_ignored="Risk",
                confidence_score=0.9,
                automation_eligibility="not_eligible",
                status="new",
                future_config_change_candidate=False,
                proposed_config_change_json=None,
                approval_required=False,
                eligible_for_auto_tune=False,
                rollback_plan=None,
                expected_impact=None,
                risk_level="high",
                affected_strategy="reversion",
                affected_token="SOL-USD",
                parameter_name=None,
                current_value_json=None,
                proposed_value_json=None,
                created_at=now,
                updated_at=now,
            ),
        ]
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["botHealth"]["criticalDiagnosticsCount"] == 1


def test_dashboard_details_surfaces_reconciliation_mismatches(
    client,
    root_user_and_token,
    db_session: Session,
):
    email, token = root_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None

    now = datetime.now(UTC)
    db_session.add(
        UserStrategy(
            user_id=user.id,
            strategy_id="momentum",
            is_enabled=True,
            config={},
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="paper",
            assigned_capital_cents=50_000,
            available_cash_cents=45_000,
            reserved_cash_cents=0,
            locked_capital_cents=5_000,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=45_000,
            version=1,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    reconciliation = response.json()["botHealth"]["reconciliation"]
    assert reconciliation["status"] == "warning"
    assert reconciliation["mismatchCount"] == 1
    assert reconciliation["topMismatchTypes"] == [
        {"type": "bucket_locked_capital_mismatch", "count": 1}
    ]


def test_dashboard_reconciliation_counts_unique_scope_issues_not_mismatch_types(
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
    order_id = uuid.uuid4()
    db_session.add(
        UserStrategy(
            user_id=user.id,
            strategy_id="momentum",
            is_enabled=True,
            config={},
            created_at=now,
            updated_at=now,
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
            quantity="1.0",
            price="50000",
            gross_notional_cents=50_000,
            fee_cents=0,
            realized_pnl_cents=0,
            position_quantity_after="1.0",
            avg_entry_price_after="50000",
            executed_at=now - timedelta(hours=1),
            raw_payload={},
        )
    )
    db_session.add(
        ExecutionPosition(
            id=uuid.uuid4(),
            tenant_id=membership.tenant_id,
            user_id=user.id,
            strategy_id="momentum",
            symbol="BTC-USD",
            trading_mode="paper",
            quantity="0.5",
            avg_entry_price="50000",
            realized_pnl_cents=0,
            created_at=now - timedelta(hours=1),
            updated_at=now,
            opened_at=now - timedelta(hours=1),
            last_trade_at=now - timedelta(hours=1),
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    reconciliation = response.json()["botHealth"]["reconciliation"]
    assert reconciliation["mismatchCount"] == 1
    assert reconciliation["topMismatchTypes"] == [
        {"type": "position_quantity_mismatch", "count": 1},
        {"type": "latest_trade_quantity_mismatch", "count": 1},
    ]


def test_dashboard_treats_allocation_constraints_as_waiting_not_blocked(
    client,
    regular_user_and_token,
    db_session: Session,
):
    email, token = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None

    now = datetime.now(UTC)
    db_session.add(
        UserStrategy(
            user_id=user.id,
            strategy_id="momentum",
            is_enabled=True,
            config={},
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="paper",
            assigned_capital_cents=25_000,
            available_cash_cents=0,
            reserved_cash_cents=0,
            locked_capital_cents=0,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=0,
            version=1,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        StrategyRun(
            run_id=uuid.uuid4(),
            user_id=user.id,
            strategy_name="momentum",
            symbol="BTC-USD",
            trading_mode="paper",
            status="completed",
            trace_id="trace-allocation-unavailable",
            run_metadata={
                "suppressed": True,
                "suppression_reason": "allocation_unavailable",
            },
            started_at=now,
            completed_at=now,
        )
    )
    db_session.add(
        MarketDataBboSnapshot(
            source="coinbase",
            product_id="BTC-USD",
            best_bid_price=65000,
            best_bid_size=5,
            best_ask_price=65010,
            best_ask_size=5,
            event_time=now,
            ingest_time=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    strategy_health = next(
        row for row in response.json()["strategyHealth"] if row["id"] == "momentum"
    )
    assert strategy_health["blockingReasonCode"] == "allocation_unavailable"
    assert strategy_health["currentStatus"] == "waiting"
    lifecycle_item = next(
        item
        for item in response.json()["botHealth"]["paperLive"]["checklist"]
        if item["id"] == "strategy_lifecycle"
    )
    assert lifecycle_item["passed"] is True


def test_dashboard_details_surfaces_exit_monitoring_and_stalled_positions(
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
    db_session.add(
        UserStrategy(
            user_id=user.id,
            strategy_id="momentum",
            is_enabled=True,
            config={},
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="paper",
            assigned_capital_cents=50_000,
            available_cash_cents=10_000,
            reserved_cash_cents=0,
            locked_capital_cents=40_000,
            realized_pnl_cents=2_000,
            unrealized_pnl_cents=1_500,
            available_buying_power_cents=10_000,
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
            trading_mode="paper",
            quantity="0.5",
            avg_entry_price="50000",
            realized_pnl_cents=0,
            created_at=now - timedelta(hours=4),
            updated_at=now - timedelta(minutes=10),
            opened_at=now - timedelta(hours=4),
            last_trade_at=now - timedelta(minutes=10),
        )
    )
    db_session.add(
        StrategyLifecycleEvent(
            correlation_id="exit-stalled-trace",
            user_id=user.id,
            tenant_id=membership.tenant_id,
            strategy_name="momentum",
            symbol="BTC-USD",
            trading_mode="paper",
            side="sell",
            stage="take_profit_triggered",
            status="observed",
            reason_code="take_profit_triggered",
            reason_detail="Take-profit guard triggered and is waiting to close.",
            event_metadata={},
            occurred_at=now - timedelta(minutes=20),
        )
    )
    db_session.add(
        MarketDataBboSnapshot(
            source="coinbase",
            product_id="BTC-USD",
            best_bid_price=52000,
            best_bid_size=5,
            best_ask_price=52010,
            best_ask_size=5,
            event_time=now,
            ingest_time=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/details?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    strategy_health = next(row for row in payload["strategyHealth"] if row["id"] == "momentum")
    assert strategy_health["currentStatus"] == "exit_monitoring"
    assert strategy_health["exitMonitoredPositions"] == 1
    assert strategy_health["stalledExitCount"] == 1
    assert strategy_health["blockingReasonCode"] == "exit_stalled"
    assert strategy_health["latestExitReasonCode"] == "take_profit_triggered"

    position = next(row for row in payload["positions"] if row["symbol"] == "BTC-USD")
    assert position["exitStatus"] == "stalled"
    assert position["exitStage"] == "take_profit_triggered"
    assert position["exitReasonCode"] == "take_profit_triggered"
    assert position["exitStalled"] is True


def test_dashboard_summary_recomputes_paper_unrealized_from_positions(
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
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="paper",
            assigned_capital_cents=100_000,
            available_cash_cents=50_000,
            reserved_cash_cents=0,
            locked_capital_cents=50_000,
            realized_pnl_cents=0,
            unrealized_pnl_cents=-25_000,
            available_buying_power_cents=50_000,
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
            trading_mode="paper",
            quantity="1",
            avg_entry_price="500",
            realized_pnl_cents=0,
            created_at=now - timedelta(hours=1),
            updated_at=now,
            opened_at=now - timedelta(hours=1),
            last_trade_at=now - timedelta(minutes=5),
        )
    )
    db_session.add(
        MarketDataBboSnapshot(
            source="coinbase",
            product_id="BTC-USD",
            best_bid_price=599,
            best_bid_size=10,
            best_ask_price=601,
            best_ask_size=10,
            event_time=now,
            ingest_time=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard/summary?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["availableBalance"] == 500.0
    assert payload["portfolioValue"] == 1100.0
    assert payload["pnlValue"] == 100.0
    assert payload["realizedPnlValue"] == 0.0
    assert payload["unrealizedPnlValue"] == 100.0


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
                opened_at=now - timedelta(hours=4),
                last_trade_at=now - timedelta(minutes=30),
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
                opened_at=now - timedelta(hours=1),
                last_trade_at=now - timedelta(minutes=10),
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
        "/v1/me/dashboard?trading_mode=paper&force_refresh=true",
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
    assert position["openedAt"] is not None
    assert position["lastTradeAt"] is not None
    assert position["closedAt"] is None
    assert 239 <= position["ageMinutes"] <= 241
    assert 3.98 <= position["ageHours"] <= 4.02


def test_dashboard_details_recomputes_paper_topline_from_position_marks(
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
    db_session.add(
        StrategyCapitalBucket(
            user_id=user.id,
            strategy_id="momentum",
            trading_mode="paper",
            assigned_capital_cents=100_000,
            available_cash_cents=70_000,
            reserved_cash_cents=0,
            locked_capital_cents=30_000,
            realized_pnl_cents=4_000,
            unrealized_pnl_cents=-18_000,
            available_buying_power_cents=70_000,
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
            symbol="ETH-USD",
            trading_mode="paper",
            quantity="3",
            avg_entry_price="100",
            realized_pnl_cents=4_000,
            created_at=now - timedelta(hours=2),
            updated_at=now,
            opened_at=now - timedelta(hours=2),
            last_trade_at=now - timedelta(minutes=15),
        )
    )
    db_session.add(
        MarketDataBboSnapshot(
            source="coinbase",
            product_id="ETH-USD",
            best_bid_price=119,
            best_bid_size=10,
            best_ask_price=121,
            best_ask_size=10,
            event_time=now,
            ingest_time=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/dashboard?trading_mode=paper&force_refresh=true",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["availableBalance"] == 700.0
    assert payload["portfolioValue"] == 1060.0
    assert payload["pnlValue"] == 100.0
    assert payload["realizedPnlValue"] == 40.0
    assert payload["unrealizedPnlValue"] == 60.0
    assert payload["gainLossLabel"] == "Total P&L"
    assert payload["positions"][0]["unrealizedPnl"] == 60.0


def test_dashboard_position_age_uses_opened_at_not_last_trade_at(
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
    db_session.add(
        ExecutionPosition(
            id=uuid.uuid4(),
            tenant_id=membership.tenant_id,
            user_id=user.id,
            strategy_id="day_trading",
            symbol="BTC-USD",
            trading_mode="paper",
            quantity="1",
            avg_entry_price="100",
            realized_pnl_cents=0,
            created_at=now - timedelta(hours=6),
            updated_at=now,
            opened_at=now - timedelta(hours=6),
            last_trade_at=now - timedelta(minutes=5),
        )
    )
    db_session.add(
        MarketDataBboSnapshot(
            source="coinbase",
            product_id="BTC-USD",
            best_bid_price=101,
            best_bid_size=10,
            best_ask_price=101.5,
            best_ask_size=10,
            event_time=now,
            ingest_time=now,
        )
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
    assert position["openedAt"] != position["lastTradeAt"]
    assert 359 <= position["ageMinutes"] <= 361


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

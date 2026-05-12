from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket, StrategyCapitalLedger
from oziebot_api.models.strategy_signal_pipeline import StrategyRun, StrategySignalRecord
from oziebot_api.models.trade_intelligence import StrategySignalSnapshot, TradeOutcomeFeature
from oziebot_api.models.user import User


def _ensure_platform_strategies(db_session: Session) -> None:
    now = datetime.now(UTC)
    configs = {
        "momentum": {
            "strategy_params": {"stop_loss_pct": 0.02, "take_profit_pct": 0.04},
            "risk_caps": {"max_position_usd": 300},
            "signal_rules": {"min_confidence": 0.6, "require_volume_confirmation": True},
        },
        "day_trading": {
            "strategy_params": {"stop_loss_pct": 0.01, "take_profit_pct": 0.025},
            "risk_caps": {"max_position_usd": 200},
            "signal_rules": {"min_confidence": 0.6, "require_volume_confirmation": True},
        },
        "reversion": {
            "strategy_params": {"zscore_entry": 2.0},
            "risk_caps": {"max_position_usd": 100},
            "signal_rules": {"min_confidence": 0.6, "require_volume_confirmation": True},
        },
        "dca": {
            "strategy_params": {"buy_amount_usd": 100},
            "risk_caps": {},
            "signal_rules": {"min_confidence": 0.9, "require_volume_confirmation": True},
        },
    }
    for sort_order, (slug, config_schema) in enumerate(configs.items(), start=1):
        if (
            db_session.scalar(select(PlatformStrategy).where(PlatformStrategy.slug == slug))
            is not None
        ):
            continue
        db_session.add(
            PlatformStrategy(
                id=uuid.uuid4(),
                slug=slug,
                display_name=slug.replace("_", " ").title(),
                description=None,
                is_enabled=True,
                entry_point=None,
                config_schema=config_schema,
                sort_order=sort_order,
                created_at=now,
                updated_at=now,
            )
        )
    db_session.flush()


def _seed_diagnostics_data(db_session: Session, user: User, membership: TenantMembership) -> None:
    _ensure_platform_strategies(db_session)
    now = datetime.now(UTC)

    btc_snapshot_id = uuid.uuid4()
    aero_snapshot_id = uuid.uuid4()
    btc_run_id = uuid.uuid4()
    aero_run_id = uuid.uuid4()
    sol_hold_run_id = uuid.uuid4()
    btc_order_id = uuid.uuid4()
    aero_order_id = uuid.uuid4()
    failed_order_id = uuid.uuid4()
    btc_trade_id = uuid.uuid4()
    aero_trade_id = uuid.uuid4()

    db_session.add_all(
        [
            StrategySignalSnapshot(
                id=btc_snapshot_id,
                user_id=user.id,
                tenant_id=membership.tenant_id,
                trading_mode="paper",
                strategy_name="momentum",
                token_symbol="BTC-USD",
                timestamp=now - timedelta(hours=2),
                current_price=50000,
                best_bid=49990,
                best_ask=50010,
                spread_pct=0.0004,
                estimated_slippage_pct=0.0008,
                volume=1200,
                volatility=0.01,
                confidence_score=0.82,
                raw_feature_json={"volume_confirmation_passed": True},
                token_policy_status="allowed",
                token_policy_multiplier=1.0,
            ),
            StrategySignalSnapshot(
                id=aero_snapshot_id,
                user_id=user.id,
                tenant_id=membership.tenant_id,
                trading_mode="live",
                strategy_name="momentum",
                token_symbol="AERO-USD",
                timestamp=now - timedelta(minutes=50),
                current_price=1.02,
                best_bid=1.01,
                best_ask=1.03,
                spread_pct=0.002,
                estimated_slippage_pct=0.003,
                volume=8000,
                volatility=0.03,
                confidence_score=0.74,
                raw_feature_json={"volume_confirmation_passed": True},
                token_policy_status="allowed",
                token_policy_multiplier=1.0,
            ),
            StrategyRun(
                run_id=btc_run_id,
                user_id=user.id,
                strategy_name="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                status="completed",
                trace_id="btc-run",
                run_metadata={"confidence": 0.82},
                started_at=now - timedelta(hours=2),
                completed_at=now - timedelta(hours=2),
            ),
            StrategyRun(
                run_id=aero_run_id,
                user_id=user.id,
                strategy_name="day_trading",
                symbol="AERO-USD",
                trading_mode="paper",
                status="completed",
                trace_id="aero-suppressed",
                run_metadata={"suppressed": True, "suppression_reason": "token_strategy_policy"},
                started_at=now - timedelta(minutes=45),
                completed_at=now - timedelta(minutes=45),
            ),
            StrategyRun(
                run_id=sol_hold_run_id,
                user_id=user.id,
                strategy_name="day_trading",
                symbol="SOL-USD",
                trading_mode="paper",
                status="completed",
                trace_id="sol-hold",
                run_metadata={"confidence": 0.61},
                started_at=now - timedelta(minutes=30),
                completed_at=now - timedelta(minutes=30),
            ),
            StrategySignalRecord(
                signal_id=uuid.uuid4(),
                run_id=btc_run_id,
                user_id=user.id,
                strategy_name="momentum",
                symbol="BTC-USD",
                action="buy",
                confidence=0.82,
                suggested_size="0.10",
                reasoning_metadata={"reason": "trend"},
                trading_mode="paper",
                timestamp=now - timedelta(hours=2),
            ),
            StrategySignalRecord(
                signal_id=uuid.uuid4(),
                run_id=uuid.uuid4(),
                user_id=user.id,
                strategy_name="momentum",
                symbol="AERO-USD",
                action="buy",
                confidence=0.74,
                suggested_size="20",
                reasoning_metadata={"reason": "breakout"},
                trading_mode="live",
                timestamp=now - timedelta(minutes=50),
            ),
            StrategySignalRecord(
                signal_id=uuid.uuid4(),
                run_id=sol_hold_run_id,
                user_id=user.id,
                strategy_name="day_trading",
                symbol="SOL-USD",
                action="hold",
                confidence=0.61,
                suggested_size="0",
                reasoning_metadata={
                    "reason": "Volume filter blocked entry: latest=80 avg=100 min_multiplier=1.80"
                },
                trading_mode="paper",
                timestamp=now - timedelta(minutes=30),
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
                reason="allocation_cap",
                detail="Reduce to stay within strategy allocation",
                original_size="1.0",
                final_size="0",
                trace_id="risk-allocation",
                rules_evaluated={"rules": ["allocation_cap"]},
                signal_payload={},
                created_at=now - timedelta(minutes=40),
            ),
            ExecutionOrder(
                id=btc_order_id,
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
                expected_gross_edge_bps=110,
                estimated_fee_bps=60,
                estimated_slippage_bps=4,
                estimated_total_cost_bps=64,
                expected_net_edge_bps=46,
                execution_preference="maker_preferred",
                fallback_behavior="convert_to_taker",
                maker_timeout_seconds=10,
                limit_price_offset_bps=2,
                actual_fill_type="maker",
                fallback_triggered=False,
                idempotency_key="btc-filled-order",
                client_order_id="btc-filled-order",
                venue_order_id="venue-btc-order",
                failure_code=None,
                failure_detail=None,
                trace_id="execution-btc",
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
                id=aero_order_id,
                intent_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="AERO-USD",
                side="buy",
                order_type="market",
                trading_mode="live",
                venue="coinbase",
                state="filled",
                quantity="20",
                requested_notional_cents=2000,
                reserved_cash_cents=0,
                locked_cash_cents=2000,
                filled_quantity="20",
                avg_fill_price="1.00",
                fees_cents=30,
                expected_gross_edge_bps=120,
                estimated_fee_bps=70,
                estimated_slippage_bps=10,
                estimated_total_cost_bps=80,
                expected_net_edge_bps=40,
                execution_preference="taker_allowed",
                fallback_behavior="cancel",
                maker_timeout_seconds=0,
                limit_price_offset_bps=0,
                actual_fill_type="taker",
                fallback_triggered=False,
                idempotency_key="aero-filled-order",
                client_order_id="aero-filled-order",
                venue_order_id="venue-aero-order",
                failure_code=None,
                failure_detail=None,
                trace_id="execution-aero",
                intent_payload={},
                risk_payload={},
                adapter_payload={},
                created_at=now - timedelta(minutes=50),
                updated_at=now - timedelta(minutes=50),
                submitted_at=now - timedelta(minutes=50),
                completed_at=now - timedelta(minutes=50),
                cancelled_at=None,
                failed_at=None,
            ),
            ExecutionOrder(
                id=failed_order_id,
                intent_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="DOGE-USD",
                side="buy",
                order_type="market",
                trading_mode="paper",
                venue="coinbase",
                state="failed",
                quantity="100",
                requested_notional_cents=1500,
                reserved_cash_cents=0,
                locked_cash_cents=0,
                filled_quantity="0",
                avg_fill_price=None,
                fees_cents=0,
                expected_gross_edge_bps=80,
                estimated_fee_bps=85,
                estimated_slippage_bps=10,
                estimated_total_cost_bps=95,
                expected_net_edge_bps=-15,
                execution_preference="taker_allowed",
                fallback_behavior="cancel",
                maker_timeout_seconds=0,
                limit_price_offset_bps=0,
                actual_fill_type=None,
                fallback_triggered=False,
                idempotency_key="doge-failed-order",
                client_order_id="doge-failed-order",
                venue_order_id=None,
                failure_code="confidence_gate",
                failure_detail="Signal confidence fell below threshold",
                trace_id="execution-doge",
                intent_payload={},
                risk_payload={},
                adapter_payload={},
                created_at=now - timedelta(minutes=25),
                updated_at=now - timedelta(minutes=25),
                submitted_at=None,
                completed_at=None,
                cancelled_at=None,
                failed_at=now - timedelta(minutes=25),
            ),
            ExecutionTradeRecord(
                id=btc_trade_id,
                order_id=btc_order_id,
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
                realized_pnl_cents=1200,
                position_quantity_after="0.00",
                avg_entry_price_after="50000",
                executed_at=now - timedelta(hours=2),
                raw_payload={},
            ),
            ExecutionTradeRecord(
                id=aero_trade_id,
                order_id=aero_order_id,
                fill_id=None,
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="AERO-USD",
                trading_mode="live",
                side="buy",
                quantity="20",
                price="1.00",
                gross_notional_cents=2000,
                fee_cents=30,
                realized_pnl_cents=-350,
                position_quantity_after="0.00",
                avg_entry_price_after="1.00",
                executed_at=now - timedelta(minutes=50),
                raw_payload={},
            ),
            ExecutionPosition(
                id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="dca",
                symbol="ETH-USD",
                trading_mode="paper",
                quantity="0.45",
                avg_entry_price="2400",
                realized_pnl_cents=320,
                created_at=now - timedelta(days=2),
                updated_at=now - timedelta(minutes=5),
                opened_at=now - timedelta(days=2),
                last_trade_at=now - timedelta(minutes=5),
                closed_at=None,
            ),
            TradeOutcomeFeature(
                trade_id=btc_trade_id,
                signal_snapshot_id=btc_snapshot_id,
                trading_mode="paper",
                strategy_name="momentum",
                token_symbol="BTC-USD",
                entry_price=50000,
                exit_price=50600,
                filled_size=0.10,
                fee_paid=0.8,
                slippage_realized=0.0004,
                hold_seconds=900,
                realized_pnl=12.0,
                realized_return_pct=0.024,
                max_favorable_excursion_pct=0.03,
                max_adverse_excursion_pct=-0.004,
                profit_giveback_pct=0.006,
                partial_profit_taken=False,
                remaining_position_outcome=None,
                exit_reason="take_profit",
                win_loss_label="win",
                profitable_after_fees_label="profitable",
                created_at=now - timedelta(hours=2),
            ),
            TradeOutcomeFeature(
                trade_id=aero_trade_id,
                signal_snapshot_id=aero_snapshot_id,
                trading_mode="live",
                strategy_name="momentum",
                token_symbol="AERO-USD",
                entry_price=1.00,
                exit_price=0.975,
                filled_size=20,
                fee_paid=0.3,
                slippage_realized=0.0008,
                hold_seconds=45,
                realized_pnl=-0.5,
                realized_return_pct=-0.025,
                max_favorable_excursion_pct=0.004,
                max_adverse_excursion_pct=-0.028,
                profit_giveback_pct=0.012,
                partial_profit_taken=False,
                remaining_position_outcome=None,
                exit_reason="stop_loss",
                win_loss_label="loss",
                profitable_after_fees_label="not_profitable",
                created_at=now - timedelta(minutes=48),
            ),
            StrategyCapitalBucket(
                id=uuid.uuid4(),
                user_id=user.id,
                strategy_id="momentum",
                trading_mode="paper",
                assigned_capital_cents=200000,
                available_cash_cents=120000,
                reserved_cash_cents=10000,
                locked_capital_cents=70000,
                realized_pnl_cents=1500,
                unrealized_pnl_cents=0,
                available_buying_power_cents=120000,
                version=1,
                created_at=now,
                updated_at=now,
            ),
            StrategyCapitalBucket(
                id=uuid.uuid4(),
                user_id=user.id,
                strategy_id="momentum",
                trading_mode="live",
                assigned_capital_cents=150000,
                available_cash_cents=110000,
                reserved_cash_cents=5000,
                locked_capital_cents=35000,
                realized_pnl_cents=-500,
                unrealized_pnl_cents=250,
                available_buying_power_cents=110000,
                version=1,
                created_at=now,
                updated_at=now,
            ),
            StrategyCapitalLedger(
                id=uuid.uuid4(),
                user_id=user.id,
                strategy_id="momentum",
                trading_mode="live",
                event_type="trade_close",
                amount_cents=-350,
                before_available_cash_cents=108000,
                after_available_cash_cents=110000,
                before_reserved_cash_cents=5000,
                after_reserved_cash_cents=5000,
                before_locked_capital_cents=38000,
                after_locked_capital_cents=35000,
                before_realized_pnl_cents=-150,
                after_realized_pnl_cents=-500,
                before_unrealized_pnl_cents=300,
                after_unrealized_pnl_cents=250,
                reference_id=str(aero_trade_id),
                metadata_json={},
                created_at=now - timedelta(minutes=48),
            ),
        ]
    )
    db_session.commit()


def _seed_outcome_only_data(db_session: Session, user: User, membership: TenantMembership) -> None:
    _ensure_platform_strategies(db_session)
    now = datetime.now(UTC)
    order_id = uuid.uuid4()
    trade_id = uuid.uuid4()
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
            order_type="market",
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
            expected_gross_edge_bps=100,
            estimated_fee_bps=60,
            estimated_slippage_bps=4,
            estimated_total_cost_bps=64,
            expected_net_edge_bps=36,
            execution_preference="taker_allowed",
            fallback_behavior="cancel",
            maker_timeout_seconds=0,
            limit_price_offset_bps=0,
            actual_fill_type="taker",
            fallback_triggered=False,
            idempotency_key="outcome-only-order",
            client_order_id="outcome-only-order",
            venue_order_id="venue-outcome-only-order",
            failure_code=None,
            failure_detail=None,
            trace_id="outcome-only-trace",
            intent_payload={},
            risk_payload={},
            adapter_payload={},
            created_at=now - timedelta(minutes=20),
            updated_at=now - timedelta(minutes=20),
            submitted_at=now - timedelta(minutes=20),
            completed_at=now - timedelta(minutes=20),
            cancelled_at=None,
            failed_at=None,
        )
    )
    db_session.add(
        ExecutionTradeRecord(
            id=trade_id,
            order_id=order_id,
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
            realized_pnl_cents=500,
            position_quantity_after="0.00",
            avg_entry_price_after="50000",
            executed_at=now - timedelta(minutes=20),
            raw_payload={},
        )
    )
    db_session.add(
        TradeOutcomeFeature(
            trade_id=trade_id,
            signal_snapshot_id=None,
            trading_mode="paper",
            strategy_name="momentum",
            token_symbol="BTC-USD",
            entry_price=50000,
            exit_price=50300,
            filled_size=0.10,
            fee_paid=0.8,
            slippage_realized=0.0004,
            hold_seconds=300,
            realized_pnl=5.0,
            realized_return_pct=0.01,
            max_favorable_excursion_pct=0.015,
            max_adverse_excursion_pct=-0.002,
            profit_giveback_pct=0.005,
            partial_profit_taken=False,
            remaining_position_outcome=None,
            exit_reason="take_profit",
            win_loss_label="win",
            profitable_after_fees_label="profitable",
            created_at=now - timedelta(minutes=18),
        )
    )
    db_session.commit()


def test_admin_trading_diagnostics_requires_root(client):
    response = client.get("/v1/admin/trading-diagnostics")
    assert response.status_code == 401


def test_admin_trading_diagnostics_returns_consistent_funnel_and_respects_filters(
    client,
    root_user_and_token,
    regular_user_and_token,
    db_session: Session,
):
    _, admin_token = root_user_and_token
    user_email, _ = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == user_email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_diagnostics_data(db_session, user, membership)

    response = client.get(
        "/v1/admin/trading-diagnostics?days=7&limit=10",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["trade_count"] == 2
    assert payload["execution_activity"]["execution_count"] == 2
    assert payload["execution_activity"]["flattened_trade_count"] == 2
    assert payload["open_positions"]["position_count"] == 1
    assert payload["open_positions"]["positions"][0]["token"] == "ETH-USD"
    assert payload["signal_funnel"]["trades_executed"] == payload["trade_count"]
    assert payload["signal_funnel"]["signals_evaluated"] == 3
    assert payload["signal_funnel"]["signals_emitted"] == 3
    assert payload["signal_funnel"]["non_hold_signals_emitted"] == 2
    assert payload["signal_funnel"]["signals_rejected"] == 3
    assert payload["signal_funnel"]["signal_actions"]["buy"] == 2
    assert payload["signal_funnel"]["signal_actions"]["hold"] == 1
    assert payload["signal_funnel"]["rejection_reasons"]["token_strategy_policy"] == 1
    assert payload["signal_funnel"]["rejection_reasons"]["allocation"] == 1
    assert payload["signal_funnel"]["rejection_reasons"]["confidence"] == 1
    assert (
        payload["signal_funnel"]["strategy_breakdown"]["day_trading"]["signal_actions"]["hold"] == 1
    )
    assert (
        payload["signal_funnel"]["strategy_breakdown"]["day_trading"]["top_hold_reasons"][0][
            "reason"
        ]
        == "Volume filter blocked entry: latest=80 avg=100 min_multiplier=1.80"
    )
    assert payload["capital_utilization"]["total_account_value"] is not None
    assert (
        payload["active_strategy_config"]["momentum_config"]["risk_caps"]["max_position_usd"] == 300
    )

    filtered = client.get(
        "/v1/admin/trading-diagnostics?days=7&token=AERO-USD&strategy=momentum&trading_mode=live&limit=10",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert filtered.status_code == 200, filtered.text
    filtered_payload = filtered.json()
    assert filtered_payload["trade_count"] == 1
    assert filtered_payload["trade_details"][0]["token"] == "AERO-USD"
    assert filtered_payload["trade_details"][0]["strategy"] == "momentum"
    assert filtered_payload["trade_details"][0]["trading_mode"] == "live"
    assert filtered_payload["execution_activity"]["execution_count"] == 1
    assert filtered_payload["open_positions"]["position_count"] == 0

    json_export = client.get(
        "/v1/admin/trading-diagnostics/export?format=json&days=7&token=AERO-USD",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert json_export.status_code == 200, json_export.text
    assert json_export.json()["trade_count"] == 1

    csv_export = client.get(
        "/v1/admin/trading-diagnostics/export?format=csv&days=7&token=AERO-USD",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert csv_export.status_code == 200, csv_export.text
    csv_text = csv_export.text
    assert "AERO-USD" in csv_text
    assert "BTC-USD" not in csv_text
    assert "execution_detail" in csv_text


def test_admin_trading_diagnostics_marks_missing_signal_stage_data_unavailable(
    client,
    root_user_and_token,
    regular_user_and_token,
    db_session: Session,
):
    _, admin_token = root_user_and_token
    user_email, _ = regular_user_and_token
    user = db_session.scalar(select(User).where(User.email == user_email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_outcome_only_data(db_session, user, membership)

    response = client.get(
        "/v1/admin/trading-diagnostics?days=7",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["trade_count"] == 1
    assert payload["execution_activity"]["execution_count"] == 1
    assert payload["open_positions"]["position_count"] == 0
    assert payload["signal_funnel"]["trades_executed"] == 1
    assert payload["signal_funnel"]["signals_evaluated"] is None
    assert payload["signal_funnel"]["signals_emitted"] is None
    assert payload["signal_funnel"]["signals_rejected"] is None
    assert payload["signal_funnel"]["unavailable_metrics"] == [
        "signals_evaluated",
        "signals_emitted",
        "signals_rejected",
    ]

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.execution import ExecutionPosition
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.strategy_lifecycle import StrategyLifecycleEvent
from oziebot_api.models.user import User


def _seed_lifecycle_data(db_session: Session, user: User, membership: TenantMembership) -> None:
    now = datetime.now(UTC)
    db_session.add_all(
        [
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-policy",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="momentum",
                symbol="AERO-USD",
                trading_mode="paper",
                side=None,
                stage="validation_started",
                status="observed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=None,
                order_id=None,
                trade_id=None,
                reason_code=None,
                reason_detail=None,
                event_metadata={},
                occurred_at=now - timedelta(hours=2),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-policy",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="momentum",
                symbol="AERO-USD",
                trading_mode="paper",
                side=None,
                stage="policy_validation",
                status="failed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=None,
                order_id=None,
                trade_id=None,
                reason_code="token_strategy_policy",
                reason_detail="blocked by admin policy",
                event_metadata={"policy": "blocked"},
                occurred_at=now - timedelta(hours=2) + timedelta(seconds=1),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-risk",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="reversion",
                symbol="SOL-USD",
                trading_mode="paper",
                side="buy",
                stage="signal_generated",
                status="observed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=None,
                order_id=None,
                trade_id=None,
                reason_code="buy",
                reason_detail="mean reversion setup",
                event_metadata={},
                occurred_at=now - timedelta(hours=1, minutes=30),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-risk",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="reversion",
                symbol="SOL-USD",
                trading_mode="paper",
                side="buy",
                stage="signal_emitted",
                status="observed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=None,
                order_id=None,
                trade_id=None,
                reason_code="buy",
                reason_detail="mean reversion setup",
                event_metadata={},
                occurred_at=now - timedelta(hours=1, minutes=29),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-risk",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="reversion",
                symbol="SOL-USD",
                trading_mode="paper",
                side="buy",
                stage="risk_validation",
                status="observed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=None,
                order_id=None,
                trade_id=None,
                reason_code=None,
                reason_detail=None,
                event_metadata={},
                occurred_at=now - timedelta(hours=1, minutes=28),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-risk",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="reversion",
                symbol="SOL-USD",
                trading_mode="paper",
                side="buy",
                stage="risk_validation",
                status="failed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=None,
                order_id=None,
                trade_id=None,
                reason_code="allocation_cap",
                reason_detail="allocation exceeded",
                event_metadata={},
                occurred_at=now - timedelta(hours=1, minutes=27),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-open",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                side="buy",
                stage="position_opened",
                status="observed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=uuid.uuid4(),
                order_id=uuid.uuid4(),
                trade_id=uuid.uuid4(),
                reason_code=None,
                reason_detail=None,
                event_metadata={},
                occurred_at=now - timedelta(minutes=40),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-open",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                side="buy",
                stage="exit_monitoring_started",
                status="observed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=uuid.uuid4(),
                order_id=uuid.uuid4(),
                trade_id=uuid.uuid4(),
                reason_code=None,
                reason_detail=None,
                event_metadata={},
                occurred_at=now - timedelta(minutes=39),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-stuck-exit",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="day_trading",
                symbol="ETH-USD",
                trading_mode="paper",
                side="buy",
                stage="position_opened",
                status="observed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=uuid.uuid4(),
                order_id=uuid.uuid4(),
                trade_id=uuid.uuid4(),
                reason_code=None,
                reason_detail=None,
                event_metadata={},
                occurred_at=now - timedelta(minutes=20),
            ),
            StrategyLifecycleEvent(
                id=uuid.uuid4(),
                correlation_id="trace-stuck-exit",
                user_id=user.id,
                tenant_id=membership.tenant_id,
                strategy_name="day_trading",
                symbol="ETH-USD",
                trading_mode="paper",
                side="sell",
                stage="exit_execution_requested",
                status="observed",
                signal_snapshot_id=None,
                run_id=uuid.uuid4(),
                signal_id=None,
                intent_id=uuid.uuid4(),
                order_id=uuid.uuid4(),
                trade_id=None,
                reason_code="trailing_stop",
                reason_detail="exit requested",
                event_metadata={},
                occurred_at=now - timedelta(minutes=5),
            ),
        ]
    )
    db_session.add_all(
        [
            ExecutionPosition(
                id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                quantity="0.10",
                avg_entry_price="50000",
                realized_pnl_cents=0,
                created_at=now - timedelta(minutes=40),
                updated_at=now - timedelta(minutes=1),
                opened_at=now - timedelta(minutes=40),
                last_trade_at=now - timedelta(minutes=40),
                closed_at=None,
            ),
            ExecutionPosition(
                id=uuid.uuid4(),
                tenant_id=membership.tenant_id,
                user_id=user.id,
                strategy_id="day_trading",
                symbol="ETH-USD",
                trading_mode="paper",
                quantity="1.25",
                avg_entry_price="2500",
                realized_pnl_cents=0,
                created_at=now - timedelta(minutes=20),
                updated_at=now - timedelta(minutes=1),
                opened_at=now - timedelta(minutes=20),
                last_trade_at=now - timedelta(minutes=20),
                closed_at=None,
            ),
        ]
    )
    db_session.commit()


def test_admin_strategy_lifecycle_requires_root(client):
    response = client.get("/v1/admin/strategy-lifecycle-diagnostics")

    assert response.status_code == 401


def test_admin_strategy_lifecycle_returns_summary_and_traces(
    client,
    db_session: Session,
    tenant_root_user_and_token: tuple[str, str],
):
    email, token = tenant_root_user_and_token
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    _seed_lifecycle_data(db_session, user, membership)

    response = client.get(
        "/v1/admin/strategy-lifecycle-diagnostics?days=7&limit=10",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["summary"]["blocked_by_policy"] == 1
    assert payload["summary"]["blocked_by_risk"] == 1
    assert payload["summary"]["positions_without_exits"] == 1
    assert payload["summary"]["stuck_open_positions"] == 1
    funnel = {row["stage"]: row for row in payload["funnel"]}
    assert funnel["policy_validation"]["failed_count"] == 1
    assert funnel["risk_validation"]["failed_count"] == 1
    assert {row["token"] for row in payload["open_positions"]} == {"BTC-USD", "ETH-USD"}

    traces_response = client.get(
        "/v1/admin/strategy-lifecycle-diagnostics/traces?days=7&limit=10",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert traces_response.status_code == 200, traces_response.text
    traces_payload = traces_response.json()
    assert traces_payload["trace_count"] >= 4
    trace_ids = {row["correlation_id"] for row in traces_payload["traces"]}
    assert {"trace-policy", "trace-risk", "trace-open", "trace-stuck-exit"} <= trace_ids

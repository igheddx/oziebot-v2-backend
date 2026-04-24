from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.execution import ExecutionOrder, ExecutionTradeRecord
from oziebot_api.models.market_data import MarketDataBboSnapshot, MarketDataTradeSnapshot
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.trade_intelligence import StrategySignalSnapshot
from oziebot_api.models.user import User
from oziebot_common.runtime_status import runtime_status_key


class FakeRedis:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def mget(self, keys: list[str]) -> list[str | None]:
        return [self.values.get(key) for key in keys]


def test_admin_runtime_requires_root(client, regular_user_and_token: tuple[str, str]):
    _, token = regular_user_and_token

    response = client.get(
        "/v1/admin/platform/runtime",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 403


def test_admin_runtime_reports_service_and_pipeline_activity(
    client,
    root_user_and_token: tuple[str, str],
    db_session: Session,
    monkeypatch,
):
    _, token = root_user_and_token
    root_user = db_session.scalar(select(User).where(User.email == "root@example.com"))
    assert root_user is not None

    tenant_id = uuid.uuid4()
    now = datetime.now(UTC)
    signal_id = uuid.uuid4()
    run_id = uuid.uuid4()
    order_id = uuid.uuid4()

    db_session.add_all(
        [
            Tenant(
                id=tenant_id,
                name="Runtime Admin Tenant",
                created_at=now,
                default_trading_mode="paper",
                trial_started_at=None,
                trial_ends_at=None,
            ),
            StrategySignalSnapshot(
                user_id=root_user.id,
                tenant_id=tenant_id,
                trading_mode="paper",
                strategy_name="momentum",
                token_symbol="BTC-USD",
                timestamp=now,
                current_price=62000,
                best_bid=61995,
                best_ask=62005,
                spread_pct=0.01,
                estimated_slippage_pct=0.02,
                volume=1000,
                volatility=0.3,
                confidence_score=0.8,
                raw_feature_json={},
                token_policy_status="preferred",
                token_policy_multiplier=1,
            ),
            RiskEvent(
                signal_id=signal_id,
                run_id=run_id,
                user_id=root_user.id,
                strategy_name="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                outcome="approved",
                reason=None,
                detail=None,
                original_size="100",
                final_size="100",
                trace_id="trace-1",
                rules_evaluated={},
                signal_payload={},
                created_at=now,
            ),
            ExecutionOrder(
                id=order_id,
                intent_id=uuid.uuid4(),
                correlation_id=uuid.uuid4(),
                tenant_id=tenant_id,
                user_id=root_user.id,
                strategy_id="momentum",
                symbol="BTC-USD",
                side="buy",
                order_type="market",
                trading_mode="paper",
                venue="paper",
                state="filled",
                quantity="0.01",
                requested_notional_cents=10000,
                reserved_cash_cents=0,
                locked_cash_cents=0,
                filled_quantity="0.01",
                avg_fill_price="62000",
                fees_cents=25,
                expected_gross_edge_bps=50,
                estimated_fee_bps=25,
                estimated_slippage_bps=10,
                estimated_total_cost_bps=35,
                expected_net_edge_bps=15,
                execution_preference="maker_preferred",
                fallback_behavior="convert_to_taker",
                maker_timeout_seconds=15,
                limit_price_offset_bps=2,
                actual_fill_type="taker",
                fallback_triggered=False,
                idempotency_key="idem-1",
                client_order_id="client-1",
                venue_order_id="venue-1",
                failure_code=None,
                failure_detail=None,
                trace_id="trace-1",
                intent_payload={},
                risk_payload={},
                adapter_payload={},
                created_at=now,
                updated_at=now,
                submitted_at=now,
                completed_at=now,
                cancelled_at=None,
                failed_at=None,
            ),
            ExecutionTradeRecord(
                order_id=order_id,
                fill_id=None,
                tenant_id=tenant_id,
                user_id=root_user.id,
                strategy_id="momentum",
                symbol="BTC-USD",
                trading_mode="paper",
                side="buy",
                quantity="0.01",
                price="62000",
                gross_notional_cents=10000,
                fee_cents=25,
                realized_pnl_cents=0,
                position_quantity_after="0.01",
                avg_entry_price_after="62000",
                executed_at=now,
                raw_payload={},
            ),
            MarketDataTradeSnapshot(
                source="coinbase",
                product_id="BTC-USD",
                trade_id="trade-1",
                side="buy",
                price=62000,
                size=0.01,
                event_time=now,
                ingest_time=now,
            ),
            MarketDataBboSnapshot(
                source="coinbase",
                product_id="BTC-USD",
                best_bid_price=61995,
                best_bid_size=1,
                best_ask_price=62005,
                best_ask_size=1,
                event_time=now,
                ingest_time=now,
            ),
        ]
    )
    db_session.commit()

    redis_values = {
        runtime_status_key("strategy-engine"): json.dumps(
            {
                "service": "strategy-engine",
                "status": "ok",
                "ready": True,
                "degraded": False,
                "degraded_reason": None,
                "started_at": now.isoformat(),
                "last_heartbeat_at": now.isoformat(),
                "heartbeat_age_seconds": 1.2,
                "stale_after_seconds": 90,
                "details": {},
            }
        ),
        runtime_status_key("execution-engine"): json.dumps(
            {
                "service": "execution-engine",
                "status": "degraded",
                "ready": False,
                "degraded": True,
                "degraded_reason": "redis_receive_failed",
                "started_at": now.isoformat(),
                "last_heartbeat_at": now.isoformat(),
                "heartbeat_age_seconds": 4.5,
                "stale_after_seconds": 90,
                "details": {"workerRuntime": {"redisReceiveFailuresTotal": 2}},
            }
        ),
    }
    monkeypatch.setattr(
        "oziebot_api.services.runtime_status.redis_from_url",
        lambda *args, **kwargs: FakeRedis(redis_values),
    )
    monkeypatch.setattr(
        "oziebot_api.services.runtime_status.disconnect_redis",
        lambda client: None,
    )

    response = client.get(
        "/v1/admin/platform/runtime",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["overall_status"] == "warning"
    assert payload["pipeline_status"] == "active"
    assert payload["summary"]["paper_orders_recent"] == 1
    assert payload["summary"]["paper_fills_recent"] == 1
    assert payload["activity"]["market_data"]["trade_ticks"] == 1
    assert payload["activity"]["strategy"]["paper"]["count"] == 1
    services = {service["service"]: service for service in payload["services"]}
    assert services["strategy-engine"]["level"] == "healthy"
    assert services["execution-engine"]["level"] == "warning"
    assert services["alerts-worker"]["level"] == "unknown"

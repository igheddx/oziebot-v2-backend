from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.execution import ExecutionOrder
from oziebot_api.models.market_data import (
    MarketDataBboSnapshot,
    MarketDataCandle,
    MarketDataTradeSnapshot,
)
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.strategy_signal_pipeline import StrategyRun, StrategySignalRecord
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.user import User


def _seed_market_data(db_session: Session, symbol: str) -> None:
    now = datetime.now(UTC)
    existing_slugs = set(db_session.scalars(select(PlatformStrategy.slug)).all())
    strategies = [
        ("momentum", "Momentum", 1),
        ("reversion", "Mean Reversion", 2),
        ("day_trading", "Day Trading", 3),
        ("dca", "DCA", 4),
    ]
    for slug, display_name, sort_order in strategies:
        if slug in existing_slugs:
            continue
        db_session.add(
            PlatformStrategy(
                id=uuid.uuid4(),
                slug=slug,
                display_name=display_name,
                description=None,
                is_enabled=True,
                entry_point=None,
                config_schema={},
                sort_order=sort_order,
                created_at=now,
                updated_at=now,
            )
        )
    for idx in range(30):
        bucket_start = now - timedelta(minutes=30 - idx)
        close = 100 + idx
        db_session.add(
            MarketDataCandle(
                id=uuid.uuid4(),
                source="coinbase",
                product_id=symbol,
                granularity_sec=60,
                bucket_start=bucket_start,
                open=close - 0.5,
                high=close + 1.0,
                low=close - 1.0,
                close=close,
                volume=10 + idx,
                event_time=bucket_start,
                ingest_time=bucket_start,
            )
        )
    for idx in range(30):
        event_time = now - timedelta(seconds=idx * 10)
        db_session.add(
            MarketDataBboSnapshot(
                id=uuid.uuid4(),
                source="coinbase",
                product_id=symbol,
                best_bid_price=129.9,
                best_bid_size=25,
                best_ask_price=130.1,
                best_ask_size=24,
                event_time=event_time,
                ingest_time=event_time,
            )
        )
        db_session.add(
            MarketDataTradeSnapshot(
                id=uuid.uuid4(),
                source="coinbase",
                product_id=symbol,
                trade_id=f"trade-{idx}",
                side="buy",
                price=130,
                size=4 + (idx / 10),
                event_time=event_time,
                ingest_time=event_time,
            )
        )
    db_session.commit()


def test_admin_token_policy_recalculated_on_create(
    client, root_user_and_token, db_session: Session
):
    _, token = root_user_and_token
    symbol = "BTC-USD"
    _seed_market_data(db_session, symbol)

    create = client.post(
        "/v1/admin/platform/tokens",
        headers={"Authorization": f"Bearer {token}"},
        json={"symbol": symbol, "display_name": "Bitcoin"},
    )
    assert create.status_code == 201, create.text
    token_id = create.json()["id"]

    policy = client.get(
        f"/v1/admin/platform/tokens/{token_id}/strategy-policies",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert policy.status_code == 200, policy.text
    data = policy.json()
    assert data["market_profile"]["liquidity_score"] > 0
    assert len(data["strategy_policies"]) == 4
    dca_policy = next(item for item in data["strategy_policies"] if item["strategy_id"] == "dca")
    assert dca_policy["computed_recommendation_status"] in {"discouraged", "blocked"}


def test_admin_can_override_effective_token_policy(
    client, root_user_and_token, db_session: Session
):
    _, token = root_user_and_token
    symbol = "ETH-USD"
    _seed_market_data(db_session, symbol)

    create = client.post(
        "/v1/admin/platform/tokens",
        headers={"Authorization": f"Bearer {token}"},
        json={"symbol": symbol, "display_name": "Ethereum", "extra": {"core_token": True}},
    )
    assert create.status_code == 201, create.text
    token_id = create.json()["id"]

    update = client.patch(
        f"/v1/admin/platform/tokens/{token_id}/strategy-policies/day_trading",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "recommendation_status": "blocked",
            "recommendation_reason": "manual review required",
            "max_position_pct_override": 0.25,
            "notes": "cap this token",
        },
    )
    assert update.status_code == 200, update.text
    data = update.json()
    assert data["computed_recommendation_status"] in {
        "preferred",
        "allowed",
        "discouraged",
        "blocked",
    }
    assert data["recommendation_status"] == "blocked"
    assert data["recommendation_reason"] == "manual review required"
    assert data["max_position_pct_override"] == 0.25
    assert data["notes"] == "cap this token"


def test_admin_token_policy_matrix_exposes_effective_and_override_values(
    client, root_user_and_token, db_session: Session
):
    _, token = root_user_and_token
    symbol = "SOL-USD"
    _seed_market_data(db_session, symbol)

    create = client.post(
        "/v1/admin/platform/tokens",
        headers={"Authorization": f"Bearer {token}"},
        json={"symbol": symbol, "display_name": "Solana"},
    )
    assert create.status_code == 201, create.text
    token_id = create.json()["id"]

    update = client.patch(
        f"/v1/admin/platform/tokens/{token_id}/strategy-policies/momentum",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "admin_enabled": False,
            "recommendation_status": "blocked",
            "recommendation_reason": "admin override",
            "max_position_pct_override": 0.15,
            "notes": "watch liquidity",
        },
    )
    assert update.status_code == 200, update.text

    profiles = client.get(
        "/v1/admin/platform/token-policy/market-profiles",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert profiles.status_code == 200, profiles.text
    profile_entry = next(item for item in profiles.json() if item["token"]["symbol"] == symbol)
    assert profile_entry["market_profile"]["spread_score"] > 0

    matrix = client.get(
        "/v1/admin/platform/token-policy/matrix",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert matrix.status_code == 200, matrix.text
    token_entry = next(item for item in matrix.json() if item["token"]["id"] == token_id)
    policy = next(
        item for item in token_entry["strategy_policies"] if item["strategy_id"] == "momentum"
    )
    assert policy["computed_recommendation_status"] in {
        "preferred",
        "allowed",
        "discouraged",
        "blocked",
    }
    assert policy["effective_recommendation_status"] == "blocked"
    assert policy["recommendation_status_override"] == "blocked"
    assert policy["admin_enabled"] is False
    assert policy["max_position_pct_override"] == 0.15
    assert policy["notes"] == "watch liquidity"

    detail = client.get(
        f"/v1/admin/platform/token-policy/tokens/{token_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["token"]["symbol"] == symbol


def test_admin_token_policy_decisions_show_live_enforcement_stages(
    client,
    root_user_and_token,
    regular_user_and_token,
    db_session: Session,
):
    _, admin_token = root_user_and_token
    user_email, _ = regular_user_and_token
    symbol = "ADA-USD"
    _seed_market_data(db_session, symbol)

    create = client.post(
        "/v1/admin/platform/tokens",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"symbol": symbol, "display_name": "Cardano"},
    )
    assert create.status_code == 201, create.text

    user = db_session.scalar(select(User).where(User.email == user_email))
    assert user is not None
    tenant = db_session.scalar(
        select(Tenant).join(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert tenant is not None

    now = datetime.now(UTC)
    blocked_policy = {
        "admin_enabled": True,
        "recommendation_status": "allowed",
        "recommendation_status_override": "blocked",
        "recommendation_reason": "computed safe",
        "recommendation_reason_override": "manual block",
        "max_position_pct_override": 0.20,
    }
    discouraged_policy = {
        "admin_enabled": True,
        "computed_recommendation_status": "preferred",
        "recommendation_status": "discouraged",
        "recommendation_reason": "manual caution",
        "size_multiplier": "0.5",
        "max_position_pct_override": "0.20",
    }
    run_id = uuid.uuid4()
    signal_id = uuid.uuid4()
    db_session.add(
        StrategyRun(
            id=uuid.uuid4(),
            run_id=run_id,
            user_id=user.id,
            strategy_name="momentum",
            symbol=symbol,
            trading_mode="paper",
            status="completed",
            trace_id="trace-strategy",
            run_metadata={
                "suppressed": True,
                "suppression_reason": "token policy blocked signal emission",
                "token_policy": blocked_policy,
            },
            started_at=now - timedelta(minutes=3),
            completed_at=now - timedelta(minutes=3),
        )
    )
    db_session.add(
        StrategySignalRecord(
            id=uuid.uuid4(),
            signal_id=signal_id,
            run_id=run_id,
            user_id=user.id,
            strategy_name="day_trading",
            symbol=symbol,
            action="buy",
            confidence=0.82,
            suggested_size="100",
            reasoning_metadata={
                "reason": "volume breakout",
                "token_policy": discouraged_policy,
            },
            trading_mode="paper",
            timestamp=now - timedelta(minutes=2),
        )
    )
    db_session.add(
        RiskEvent(
            id=uuid.uuid4(),
            signal_id=signal_id,
            run_id=run_id,
            user_id=user.id,
            strategy_name="day_trading",
            symbol=symbol,
            trading_mode="paper",
            outcome="reduce_size",
            reason=None,
            detail="token_strategy_policy_size: discouraged token reduced size",
            original_size="100",
            final_size="50",
            trace_id="trace-risk",
            rules_evaluated=["token_policy_size"],
            signal_payload={
                "confidence": 0.82,
                "reasoning_metadata": {"token_policy": discouraged_policy},
            },
            created_at=now - timedelta(minutes=1),
        )
    )
    db_session.add(
        ExecutionOrder(
            id=uuid.uuid4(),
            intent_id=uuid.uuid4(),
            correlation_id=run_id,
            tenant_id=tenant.id,
            user_id=user.id,
            strategy_id="day_trading",
            symbol=symbol,
            side="buy",
            order_type="market",
            trading_mode="paper",
            venue="coinbase",
            state="created",
            quantity="40",
            requested_notional_cents=4000,
            reserved_cash_cents=0,
            locked_cash_cents=0,
            filled_quantity="0",
            avg_fill_price=None,
            fees_cents=0,
            idempotency_key="token-policy-test-order",
            client_order_id="token-policy-test-order",
            venue_order_id=None,
            failure_code=None,
            failure_detail=None,
            trace_id="trace-execution",
            intent_payload={
                "metadata": {
                    "token_policy_execution": {
                        "admin_enabled": True,
                        "computed_recommendation_status": "preferred",
                        "effective_recommendation_status": "discouraged",
                        "effective_recommendation_reason": "manual caution",
                        "size_multiplier": "0.5",
                        "max_position_pct_override": "0.20",
                    },
                    "adjusted_quantity": "40",
                    "policy_adjustment_reason": "max_position_pct_override applied",
                }
            },
            risk_payload={"signal_id": str(signal_id), "final_size": "50"},
            adapter_payload={},
            created_at=now,
            updated_at=now,
            submitted_at=None,
            completed_at=None,
            cancelled_at=None,
            failed_at=None,
        )
    )
    db_session.commit()

    response = client.get(
        f"/v1/admin/platform/token-policy/decisions?symbol={symbol}",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert any(
        item["enforced_in"] == "strategy-engine"
        and item["decision_outcome"] == "rejected"
        and item["strategy_name"] == "momentum"
        for item in data
    )
    assert any(
        item["enforced_in"] == "risk-engine"
        and item["decision_outcome"] == "reduced"
        and item["final_sizing_impact"]["final_size"] == "50"
        for item in data
    )
    assert any(
        item["enforced_in"] == "execution/sizing"
        and item["decision_outcome"] == "reduced"
        and item["final_sizing_impact"]["final_size"] == "40"
        for item in data
    )

    filtered = client.get(
        "/v1/admin/platform/token-policy/decisions?outcome=rejected&strategy_id=momentum",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert filtered.status_code == 200, filtered.text
    rows = filtered.json()
    assert len(rows) == 1
    assert rows[0]["enforced_in"] == "strategy-engine"

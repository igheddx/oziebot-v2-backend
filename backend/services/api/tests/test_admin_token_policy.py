from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from oziebot_api.models.market_data import MarketDataBboSnapshot, MarketDataCandle, MarketDataTradeSnapshot
from oziebot_api.models.platform_strategy import PlatformStrategy


def _seed_market_data(db_session: Session, symbol: str) -> None:
    now = datetime.now(UTC)
    db_session.add_all(
        [
            PlatformStrategy(
                id=uuid.uuid4(),
                slug="momentum",
                display_name="Momentum",
                description=None,
                is_enabled=True,
                entry_point=None,
                config_schema={},
                sort_order=1,
                created_at=now,
                updated_at=now,
            ),
            PlatformStrategy(
                id=uuid.uuid4(),
                slug="reversion",
                display_name="Mean Reversion",
                description=None,
                is_enabled=True,
                entry_point=None,
                config_schema={},
                sort_order=2,
                created_at=now,
                updated_at=now,
            ),
            PlatformStrategy(
                id=uuid.uuid4(),
                slug="day_trading",
                display_name="Day Trading",
                description=None,
                is_enabled=True,
                entry_point=None,
                config_schema={},
                sort_order=3,
                created_at=now,
                updated_at=now,
            ),
            PlatformStrategy(
                id=uuid.uuid4(),
                slug="dca",
                display_name="DCA",
                description=None,
                is_enabled=True,
                entry_point=None,
                config_schema={},
                sort_order=4,
                created_at=now,
                updated_at=now,
            ),
        ]
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


def test_admin_token_policy_recalculated_on_create(client, root_user_and_token, db_session: Session):
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


def test_admin_can_override_effective_token_policy(client, root_user_and_token, db_session: Session):
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
    assert data["computed_recommendation_status"] in {"preferred", "allowed", "discouraged", "blocked"}
    assert data["recommendation_status"] == "blocked"
    assert data["recommendation_reason"] == "manual review required"
    assert data["max_position_pct_override"] == 0.25
    assert data["notes"] == "cap this token"

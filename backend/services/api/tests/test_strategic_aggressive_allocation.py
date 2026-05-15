from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select, text

from oziebot_api.models.market_data import MarketDataBboSnapshot, MarketDataCandle
from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.strategy_signal_pipeline import StrategySignalRecord
from oziebot_api.models.token_market_profile import TokenMarketProfile
from oziebot_api.models.token_strategy_policy import TokenStrategyPolicy
from oziebot_api.models.user import User
from oziebot_api.models.user_token_permission import UserTokenPermission
from oziebot_strategy_engine.strategies.strategic_aggressive_allocation import (
    HIGH_CONVICTION_BUCKET,
    STRATEGY_ID,
    default_strategy_config,
)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _user(db_session, email: str) -> User:
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    return user


def _base_payload(*, trading_mode: str = "paper") -> dict:
    payload = default_strategy_config()
    payload["trading_mode"] = trading_mode
    payload["enabled"] = True
    return payload


def _seed_token(
    db_session,
    *,
    user: User,
    symbol: str,
    blocked: bool = False,
) -> PlatformTokenAllowlist:
    now = datetime.now(UTC)
    token = PlatformTokenAllowlist(
        id=uuid.uuid4(),
        symbol=symbol,
        quote_currency="USD",
        network="mainnet",
        display_name=symbol,
        is_enabled=True,
        sort_order=0,
        extra={"ecosystem": "base" if symbol == "AERO-USD" else "mainnet"},
        created_at=now,
        updated_at=now,
    )
    db_session.add(token)
    db_session.add(
        UserTokenPermission(
            id=uuid.uuid4(),
            user_id=user.id,
            platform_token_id=token.id,
            is_enabled=True,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        TokenMarketProfile(
            id=uuid.uuid4(),
            token_id=token.id,
            liquidity_score=Decimal("0.7"),
            spread_score=Decimal("0.7"),
            volatility_score=Decimal("0.7"),
            trend_score=Decimal("0.8"),
            reversion_score=Decimal("0.2"),
            slippage_score=Decimal("0.6"),
            avg_daily_volume_usd=Decimal("1000000"),
            avg_spread_pct=Decimal("0.002"),
            avg_intraday_volatility_pct=Decimal("0.03"),
            last_computed_at=now,
            raw_metrics_json={"tag": "test"},
        )
    )
    db_session.add(
        TokenStrategyPolicy(
            id=uuid.uuid4(),
            token_id=token.id,
            strategy_id=STRATEGY_ID,
            admin_enabled=not blocked,
            suitability_score=Decimal("0.8"),
            recommendation_status="blocked" if blocked else "preferred",
            recommendation_reason="blocked for test" if blocked else "allowed for test",
            recommendation_status_override=None,
            recommendation_reason_override=None,
            size_multiplier=Decimal("1"),
            max_position_usd_override=None,
            max_position_pct_override=None,
            notes=None,
            created_at=now,
            computed_at=now,
            updated_at=now,
        )
    )
    db_session.commit()
    return token


def _seed_market_data(db_session, *, symbol: str) -> None:
    now = datetime.now(UTC)
    db_session.add(
        MarketDataBboSnapshot(
            id=uuid.uuid4(),
            source="test",
            product_id=symbol,
            best_bid_price=Decimal("1.99"),
            best_bid_size=Decimal("100"),
            best_ask_price=Decimal("2.01"),
            best_ask_size=Decimal("100"),
            event_time=now,
            ingest_time=now,
        )
    )
    for index in range(30):
        bucket = now - timedelta(hours=30 - index)
        close = Decimal("1") + (Decimal(index) * Decimal("0.03"))
        db_session.add(
            MarketDataCandle(
                id=uuid.uuid4(),
                source="test",
                product_id=symbol,
                granularity_sec=3600,
                bucket_start=bucket,
                open=close - Decimal("0.01"),
                high=close + Decimal("0.03"),
                low=close - Decimal("0.03"),
                close=close,
                volume=Decimal("1000") + (Decimal(index) * Decimal("10")),
                event_time=bucket,
                ingest_time=bucket,
            )
        )
    db_session.commit()


def test_config_rejects_blocked_token(client, db_session, regular_user_and_token):
    email, token = regular_user_and_token
    user = _user(db_session, email)
    _seed_token(db_session, user=user, symbol="BTC-USD", blocked=True)

    payload = _base_payload()
    payload["selected_tokens"][HIGH_CONVICTION_BUCKET] = ["BTC-USD"]

    response = client.post(
        "/v1/me/strategies/strategic-aggressive-allocation/config",
        headers=_auth(token),
        json=payload,
    )

    assert response.status_code == 400
    assert "not enabled" in response.json()["detail"]


def test_config_rejects_over_allocation(client, regular_user_and_token):
    _email, token = regular_user_and_token
    payload = _base_payload()
    payload["total_allocated_amount_usd"] = {"target": 5000, "source": "manual"}

    response = client.post(
        "/v1/me/strategies/strategic-aggressive-allocation/config",
        headers=_auth(token),
        json=payload,
    )

    assert response.status_code == 400
    assert "available cash" in response.json()["detail"]


def test_rebalance_preview_does_not_queue_and_execute_queues_signal(
    client, db_session, regular_user_and_token
):
    email, token = regular_user_and_token
    user = _user(db_session, email)
    _seed_token(db_session, user=user, symbol="BTC-USD", blocked=False)
    _seed_market_data(db_session, symbol="BTC-USD")
    now = datetime.now(UTC)
    db_session.add(
        StrategyCapitalBucket(
            id=uuid.uuid4(),
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode="paper",
            assigned_capital_cents=100_000,
            available_cash_cents=100_000,
            reserved_cash_cents=0,
            locked_capital_cents=0,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=100_000,
            version=0,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()
    db_session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS worker_message_outbox (
              id TEXT PRIMARY KEY,
              queue_name TEXT NOT NULL,
              payload TEXT NOT NULL,
              status TEXT NOT NULL,
              attempt_count INTEGER NOT NULL,
              retry_after TEXT NULL,
              lease_expires_at TEXT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )
    )
    db_session.commit()

    payload = _base_payload()
    payload["selected_tokens"][HIGH_CONVICTION_BUCKET] = ["BTC-USD"]
    config_response = client.post(
        "/v1/me/strategies/strategic-aggressive-allocation/config",
        headers=_auth(token),
        json=payload,
    )
    assert config_response.status_code == 200, config_response.text

    preview_response = client.post(
        "/v1/me/strategies/strategic-aggressive-allocation/rebalance-preview",
        headers=_auth(token),
        json={"trading_mode": "paper", "execute": False},
    )
    assert preview_response.status_code == 200, preview_response.text
    preview_body = preview_response.json()
    assert preview_body["actions"]
    outbox_count = db_session.execute(
        text("SELECT COUNT(1) FROM worker_message_outbox")
    ).scalar_one()
    assert outbox_count == 0

    execute_response = client.post(
        "/v1/me/strategies/strategic-aggressive-allocation/rebalance-execute",
        headers=_auth(token),
        json={"trading_mode": "paper", "execute": True},
    )
    assert execute_response.status_code == 200, execute_response.text
    body = execute_response.json()
    assert body["queued_count"] == 1
    queued_records = db_session.scalars(select(StrategySignalRecord)).all()
    assert len(queued_records) == 1
    outbox_count = db_session.execute(
        text("SELECT COUNT(1) FROM worker_message_outbox")
    ).scalar_one()
    assert outbox_count == 1

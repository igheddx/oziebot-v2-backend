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
from oziebot_api.models.user_strategy import UserStrategyState
from oziebot_api.models.user_token_permission import UserTokenPermission
from oziebot_strategy_engine.strategies.volatility_harvest import (
    STRATEGY_ID,
    default_strategy_config,
)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _user(db_session, email: str) -> User:
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    return user


def _seed_token(db_session, *, user: User, symbol: str = "AERO-USD") -> None:
    now = datetime.now(UTC)
    token = PlatformTokenAllowlist(
        id=uuid.uuid4(),
        symbol=symbol,
        quote_currency="USD",
        network="mainnet",
        display_name=symbol,
        is_enabled=True,
        sort_order=0,
        extra={"ecosystem": "base"},
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
            liquidity_score=Decimal("72"),
            spread_score=Decimal("70"),
            volatility_score=Decimal("82"),
            trend_score=Decimal("68"),
            reversion_score=Decimal("30"),
            slippage_score=Decimal("66"),
            avg_daily_volume_usd=Decimal("2500000"),
            avg_spread_pct=Decimal("0.003"),
            avg_intraday_volatility_pct=Decimal("0.025"),
            last_computed_at=now,
            raw_metrics_json={"has_minimum_data": True},
        )
    )
    db_session.add(
        TokenStrategyPolicy(
            id=uuid.uuid4(),
            token_id=token.id,
            strategy_id=STRATEGY_ID,
            admin_enabled=True,
            suitability_score=Decimal("84"),
            recommendation_status="preferred",
            recommendation_reason="volatility harvest fit",
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


def _seed_market_data(db_session, *, symbol: str = "AERO-USD") -> None:
    now = datetime.now(UTC)
    db_session.add(
        MarketDataBboSnapshot(
            id=uuid.uuid4(),
            source="test",
            product_id=symbol,
            best_bid_price=Decimal("1.999"),
            best_bid_size=Decimal("100"),
            best_ask_price=Decimal("2.001"),
            best_ask_size=Decimal("100"),
            event_time=now,
            ingest_time=now,
        )
    )
    db_session.add(
        MarketDataBboSnapshot(
            id=uuid.uuid4(),
            source="test",
            product_id="BTC-USD",
            best_bid_price=Decimal("65000"),
            best_bid_size=Decimal("10"),
            best_ask_price=Decimal("65010"),
            best_ask_size=Decimal("10"),
            event_time=now,
            ingest_time=now,
        )
    )
    for idx in range(48):
        bucket = now - timedelta(hours=48 - idx)
        close = Decimal("1.2") + (Decimal(idx) * Decimal("0.02"))
        for product_id, base_close in (
            (symbol, close),
            ("BTC-USD", Decimal("64000") + (Decimal(idx) * Decimal("20"))),
        ):
            db_session.add(
                MarketDataCandle(
                    id=uuid.uuid4(),
                    source="test",
                    product_id=product_id,
                    granularity_sec=3600,
                    bucket_start=bucket,
                    open=base_close - Decimal("0.01"),
                    high=base_close + Decimal("0.03"),
                    low=base_close - Decimal("0.03"),
                    close=base_close,
                    volume=Decimal("1000") + (Decimal(idx) * Decimal("10")),
                    event_time=bucket,
                    ingest_time=bucket,
                )
            )
    db_session.commit()


def _base_payload(*, trading_mode: str = "paper") -> dict:
    payload = default_strategy_config()
    payload["trading_mode"] = trading_mode
    payload["enabled"] = True
    payload["selected_tokens"] = ["AERO-USD"]
    payload["total_allocated_amount_usd"] = {"target": 500, "source": "manual"}
    return payload


def test_volatility_harvest_config_and_preview(client, db_session, regular_user_and_token):
    email, token = regular_user_and_token
    user = _user(db_session, email)
    _seed_token(db_session, user=user)
    _seed_market_data(db_session)
    now = datetime.now(UTC)
    db_session.add(
        StrategyCapitalBucket(
            id=uuid.uuid4(),
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode="paper",
            assigned_capital_cents=75_000,
            available_cash_cents=75_000,
            reserved_cash_cents=0,
            locked_capital_cents=0,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=75_000,
            version=0,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()

    response = client.post(
        "/v1/me/strategies/volatility-harvest/config",
        headers=_auth(token),
        json=_base_payload(),
    )
    assert response.status_code == 200, response.text
    preview = client.post(
        "/v1/me/strategies/volatility-harvest/cycle-preview",
        headers=_auth(token),
        json={"trading_mode": "paper", "execute": False},
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["actions"]


def test_volatility_harvest_cycle_execute_queues_signal(client, db_session, regular_user_and_token):
    email, token = regular_user_and_token
    user = _user(db_session, email)
    _seed_token(db_session, user=user)
    _seed_market_data(db_session)
    now = datetime.now(UTC)
    db_session.add(
        StrategyCapitalBucket(
            id=uuid.uuid4(),
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode="paper",
            assigned_capital_cents=75_000,
            available_cash_cents=75_000,
            reserved_cash_cents=0,
            locked_capital_cents=0,
            realized_pnl_cents=0,
            unrealized_pnl_cents=0,
            available_buying_power_cents=75_000,
            version=0,
            created_at=now,
            updated_at=now,
        )
    )
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

    client.post(
        "/v1/me/strategies/volatility-harvest/config",
        headers=_auth(token),
        json=_base_payload(),
    )
    execute = client.post(
        "/v1/me/strategies/volatility-harvest/cycle-execute",
        headers=_auth(token),
        json={"trading_mode": "paper", "execute": True},
    )
    assert execute.status_code == 200, execute.text
    assert execute.json()["queued_count"] == 1
    assert len(db_session.scalars(select(StrategySignalRecord)).all()) == 1


def test_volatility_harvest_overview_reads_runtime_state(
    client, db_session, regular_user_and_token
):
    email, token = regular_user_and_token
    user = _user(db_session, email)
    _seed_token(db_session, user=user)
    _seed_market_data(db_session)
    now = datetime.now(UTC)
    config_response = client.post(
        "/v1/me/strategies/volatility-harvest/config",
        headers=_auth(token),
        json=_base_payload(),
    )
    assert config_response.status_code == 200, config_response.text
    db_session.add(
        UserStrategyState(
            id=uuid.uuid4(),
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode="paper",
            state={
                "symbols": {
                    "AERO-USD": {
                        "core_quantity": "7",
                        "trading_quantity": "3",
                        "avg_core_entry_price": "1.50",
                        "avg_trading_entry_price": "1.40",
                        "harvested_cash_cents": 12500,
                        "total_harvested_gains_cents": 3200,
                        "token_accumulation_quantity": "0.35",
                        "token_accumulation_pct": "5.0",
                        "completed_harvest_bands": ["harvest_1"],
                        "completed_rebuy_bands": ["rebuy_1"],
                    }
                }
            },
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()

    response = client.get(
        "/v1/me/strategies/volatility-harvest/overview?trading_mode=paper",
        headers=_auth(token),
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["harvested_cash_cents"] >= 12_500
    assert body["token_accumulation_quantity"] != "0"

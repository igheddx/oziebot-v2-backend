from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

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

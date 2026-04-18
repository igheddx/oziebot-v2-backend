"""Example catalog rows: tokens, strategies, subscription plans, trial policy, global settings."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import get_settings
from oziebot_api.db.session import make_session_factory
from oziebot_api.models.platform_setting import PlatformSetting
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.subscription_plan import SubscriptionPlan
from oziebot_api.services.platform_management import get_or_create_trial_policy


def run() -> None:
    settings = get_settings()
    if not settings.database_url:
        raise SystemExit("DATABASE_URL is required")
    factory = make_session_factory(settings)
    if factory is None:
        raise SystemExit("Could not create session factory")
    session: Session = factory()
    try:
        now = datetime.now(UTC)
        if session.scalars(select(PlatformTokenAllowlist).limit(1)).first() is None:
            session.add_all(
                [
                    PlatformTokenAllowlist(
                        id=uuid.uuid4(),
                        symbol="BTC-USD",
                        quote_currency="USD",
                        network="coinbase",
                        contract_address=None,
                        display_name="Bitcoin / USD",
                        is_enabled=True,
                        sort_order=10,
                        extra=None,
                        created_at=now,
                        updated_at=now,
                    ),
                    PlatformTokenAllowlist(
                        id=uuid.uuid4(),
                        symbol="ETH-USD",
                        quote_currency="USD",
                        network="coinbase",
                        contract_address=None,
                        display_name="Ethereum / USD",
                        is_enabled=True,
                        sort_order=20,
                        extra=None,
                        created_at=now,
                        updated_at=now,
                    ),
                ]
            )
            print("Seeded platform_token_allowlist examples.")

        if session.scalars(select(PlatformStrategy).limit(1)).first() is None:
            session.add(
                PlatformStrategy(
                    id=uuid.uuid4(),
                    slug="demo.momentum",
                    display_name="Demo Momentum",
                    description="Example strategy entry for catalog",
                    is_enabled=True,
                    entry_point="oziebot.strategies",
                    config_schema={"lookback": {"type": "integer", "default": 14}},
                    sort_order=10,
                    created_at=now,
                    updated_at=now,
                )
            )
            print("Seeded platform_strategies example.")

        if session.scalars(select(SubscriptionPlan).limit(1)).first() is None:
            price_all = os.environ.get("SEED_STRIPE_PRICE_ID_ALL", "price_seed_all_strategies")
            price_each = os.environ.get("SEED_STRIPE_PRICE_ID_EACH", "price_seed_per_strategy")
            prod_all = os.environ.get("SEED_STRIPE_PRODUCT_ID_ALL")
            prod_each = os.environ.get("SEED_STRIPE_PRODUCT_ID_EACH")
            session.add_all(
                [
                    SubscriptionPlan(
                        id=uuid.uuid4(),
                        slug="all-strategies-monthly",
                        display_name="All strategies — $19.99/mo",
                        description="Unlimited catalog strategies; replace Stripe IDs for production",
                        plan_kind="all_strategies",
                        stripe_price_id=price_all,
                        stripe_product_id=prod_all,
                        billing_interval="month",
                        amount_cents=1999,
                        currency="usd",
                        is_active=True,
                        features={"all_strategies": True},
                        trial_days_override=None,
                        sort_order=10,
                        created_at=now,
                        updated_at=now,
                    ),
                    SubscriptionPlan(
                        id=uuid.uuid4(),
                        slug="per-strategy-monthly",
                        display_name="Single strategy — $6.99/mo each",
                        description="Per-strategy line items; replace Stripe IDs for production",
                        plan_kind="per_strategy",
                        stripe_price_id=price_each,
                        stripe_product_id=prod_each,
                        billing_interval="month",
                        amount_cents=699,
                        currency="usd",
                        is_active=True,
                        features={"per_strategy": True},
                        trial_days_override=None,
                        sort_order=20,
                        created_at=now,
                        updated_at=now,
                    ),
                ]
            )
            print("Seeded subscription_plans examples (all-strategies + per-strategy).")

        get_or_create_trial_policy(session)
        print("Trial policy row ensured.")

        if session.get(PlatformSetting, "maintenance") is None:
            session.add(
                PlatformSetting(
                    key="maintenance",
                    value={"enabled": False, "message": None},
                    updated_at=now,
                    updated_by_user_id=None,
                )
            )
            print("Seeded platform_settings maintenance key.")

        if session.get(PlatformSetting, "billing.allow_paper_without_subscription") is None:
            session.add(
                PlatformSetting(
                    key="billing.allow_paper_without_subscription",
                    value={"enabled": True},
                    updated_at=now,
                    updated_by_user_id=None,
                )
            )
            print("Seeded billing.allow_paper_without_subscription (default: paper without sub).")

        session.commit()
    finally:
        session.close()


if __name__ == "__main__":
    run()

"""Billing, trial, and entitlement enforcement."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm import Session

from oziebot_api.models.platform_setting import PlatformSetting
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.stripe_subscription import StripeSubscription
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_entitlement import TenantEntitlement
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_api.services.entitlements import (
    allow_paper_without_subscription,
    has_strategy_entitlement,
    is_trial_active,
)
from oziebot_domain.trading_mode import TradingMode


def test_register_starts_trial_and_billing_summary(client):
    r = client.post(
        "/v1/auth/register",
        json={
            "email": "trialuser@example.com",
            "password": "password-123",
            "tenant_name": "Trial Co",
        },
    )
    assert r.status_code == 201
    token = r.json()["access_token"]
    s = client.get("/v1/billing/summary", headers={"Authorization": f"Bearer {token}"})
    assert s.status_code == 200
    body = s.json()
    assert body["trial_active"] is True
    assert body["trial_ends_at"] is not None


def test_live_blocked_after_trial_expires_even_with_coinbase(
    client,
    root_user_and_token: tuple[str, str],
    db_session: Session,
):
    client.post(
        "/v1/auth/register",
        json={
            "email": "expired@example.com",
            "password": "password-123",
            "tenant_name": "Expired",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "expired@example.com", "password": "password-123"},
    )
    token = r.json()["access_token"]
    me = client.get("/v1/me", headers={"Authorization": f"Bearer {token}"}).json()
    tenant_id = me["tenants"][0]["id"]
    from uuid import UUID

    tid = UUID(tenant_id)
    tenant = db_session.get(Tenant, tid)
    assert tenant is not None
    past = datetime.now(UTC) - timedelta(days=1)
    tenant.trial_started_at = past - timedelta(days=30)
    tenant.trial_ends_at = past
    db_session.commit()

    _, admin_token = root_user_and_token
    r = client.put(
        f"/v1/admin/tenants/{tenant_id}/integrations/coinbase",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"connected": True},
    )
    assert r.status_code == 200

    r = client.patch(
        "/v1/me/trading-mode",
        headers={"Authorization": f"Bearer {token}"},
        json={"trading_mode": "live"},
    )
    assert r.status_code == 403
    assert "subscription" in r.json()["detail"].lower() or "trial" in r.json()["detail"].lower()


def test_paper_requires_billing_when_config_disables_free_paper(client, db_session: Session):
    client.post(
        "/v1/auth/register",
        json={
            "email": "paperonly@example.com",
            "password": "password-123",
            "tenant_name": "Paper",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "paperonly@example.com", "password": "password-123"},
    )
    token = r.json()["access_token"]
    me = client.get("/v1/me", headers={"Authorization": f"Bearer {token}"}).json()
    tenant_id = me["tenants"][0]["id"]
    from uuid import UUID

    tid = UUID(tenant_id)
    tenant = db_session.get(Tenant, tid)
    assert tenant is not None
    tenant.trial_ends_at = datetime.now(UTC) - timedelta(days=1)
    tenant.trial_started_at = tenant.trial_ends_at - timedelta(days=30)
    db_session.add(
        PlatformSetting(
            key="billing.allow_paper_without_subscription",
            value={"enabled": False},
            updated_at=datetime.now(UTC),
            updated_by_user_id=None,
        )
    )
    db_session.commit()

    r = client.patch(
        "/v1/me/trading-mode",
        headers={"Authorization": f"Bearer {token}"},
        json={"trading_mode": "paper"},
    )
    assert r.status_code == 403
    assert "paper" in r.json()["detail"].lower() or "subscription" in r.json()["detail"].lower()


def test_has_strategy_entitlement_respects_trial_and_subscription(
    db_session: Session,
):
    from uuid import uuid4

    now = datetime.now(UTC)
    tid = uuid4()
    sid = uuid4()
    db_session.add(
        Tenant(
            id=tid,
            name="t",
            created_at=now,
            trial_started_at=now - timedelta(days=1),
            trial_ends_at=now + timedelta(days=20),
        )
    )
    db_session.add(
        PlatformStrategy(
            id=sid,
            slug="alpha",
            display_name="Alpha",
            is_enabled=True,
            sort_order=0,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.add(
        TenantEntitlement(
            id=uuid4(),
            tenant_id=tid,
            platform_strategy_id=None,
            source="trial",
            valid_from=now - timedelta(hours=1),
            valid_until=now + timedelta(days=20),
            is_active=True,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()
    assert is_trial_active(db_session, tid) is True
    assert has_strategy_entitlement(db_session, tid, "alpha") is True
    assert allow_paper_without_subscription(db_session) is True


def test_active_subscription_grants_live_without_trial(
    client,
    root_user_and_token: tuple[str, str],
    db_session: Session,
):
    client.post(
        "/v1/auth/register",
        json={
            "email": "subuser@example.com",
            "password": "password-123",
            "tenant_name": "Subbed",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "subuser@example.com", "password": "password-123"},
    )
    token = r.json()["access_token"]
    me = client.get("/v1/me", headers={"Authorization": f"Bearer {token}"}).json()
    tenant_id = me["tenants"][0]["id"]
    from uuid import UUID, uuid4

    tid = UUID(tenant_id)
    tenant = db_session.get(Tenant, tid)
    assert tenant is not None
    tenant.trial_ends_at = datetime.now(UTC) - timedelta(days=2)
    tenant.trial_started_at = tenant.trial_ends_at - timedelta(days=30)
    db_session.add(
        StripeSubscription(
            id=uuid4(),
            tenant_id=tid,
            stripe_subscription_id="sub_test_123",
            stripe_customer_id="cus_test",
            status="active",
            subscription_plan_id=None,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
    )
    ti = db_session.get(TenantIntegration, tid)
    assert ti is not None
    ti.coinbase_connected = True
    ti.updated_at = datetime.now(UTC)
    db_session.commit()

    r = client.patch(
        "/v1/me/trading-mode",
        headers={"Authorization": f"Bearer {token}"},
        json={"trading_mode": "live"},
    )
    assert r.status_code == 200
    assert r.json()["current_trading_mode"] == "live"


@pytest.mark.parametrize("mode", [TradingMode.PAPER, TradingMode.LIVE])
def test_entitlements_unit_trading_mode_checks(
    db_session: Session,
    mode: TradingMode,
) -> None:
    from oziebot_api.services.entitlements import can_use_trading_for_mode
    from uuid import uuid4

    now = datetime.now(UTC)
    tid = uuid4()
    db_session.add(
        Tenant(
            id=tid,
            name="x",
            created_at=now,
            trial_started_at=now,
            trial_ends_at=now + timedelta(days=7),
        )
    )
    db_session.add(
        TenantIntegration(
            tenant_id=tid,
            coinbase_connected=False,
            updated_at=now,
        )
    )
    db_session.commit()
    ok, err = can_use_trading_for_mode(db_session, tenant_id=tid, trading_mode=mode)
    if mode == TradingMode.PAPER:
        assert ok is True
    else:
        assert ok is False
        assert err is not None

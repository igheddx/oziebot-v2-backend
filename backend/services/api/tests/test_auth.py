from __future__ import annotations

from sqlalchemy import select

from oziebot_api.models.tenant_entitlement import TenantEntitlement
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy


def test_register_login_me(client):
    r = client.post(
        "/v1/auth/register",
        json={
            "email": "User@Example.com",
            "full_name": "User Example",
            "password": "password-123",
            "tenant_name": "Acme Trading",
        },
    )
    assert r.status_code == 201, r.text
    data = r.json()
    assert "access_token" in data and "refresh_token" in data
    assert data["role"] == "user"
    token = data["access_token"]
    me = client.get("/v1/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200
    body = me.json()
    assert body["email"] == "user@example.com"
    assert body["full_name"] == "User Example"
    assert body["role"] == "user"
    assert body["is_root_admin"] is False
    assert body["current_trading_mode"] == "paper"
    assert len(body["tenants"]) == 1
    assert body["tenants"][0]["name"] == "Acme Trading"


def test_admin_platform_settings_requires_root(client):
    r = client.get("/v1/admin/platform/overview")
    assert r.status_code == 401


def test_admin_platform_settings_ok(client, root_user_and_token: tuple[str, str]):
    _, token = root_user_and_token
    r = client.get(
        "/v1/admin/platform/overview",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert "environment" in r.json()


def test_admin_fee_settings_round_trip(client, root_user_and_token: tuple[str, str]):
    _, token = root_user_and_token
    get_response = client.get(
        "/v1/admin/platform/fee-settings",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert get_response.status_code == 200, get_response.text
    current = get_response.json()
    assert "defaults" in current

    updated = client.put(
        "/v1/admin/platform/fee-settings",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "value": {
                **current,
                "defaults": {
                    **current["defaults"],
                    "min_expected_edge_bps": 42,
                },
            }
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["value"]["defaults"]["min_expected_edge_bps"] == 42


def test_tenants_list_requires_root(client):
    r = client.get("/v1/tenants")
    assert r.status_code == 401


def test_login_includes_role_and_protected_requires_auth(client):
    client.post(
        "/v1/auth/register",
        json={
            "email": "authz@example.com",
            "password": "password-123",
            "tenant_name": "AuthZ",
        },
    )
    login = client.post(
        "/v1/auth/login",
        json={"email": "authz@example.com", "password": "password-123"},
    )
    assert login.status_code == 200, login.text
    payload = login.json()
    assert payload["role"] == "user"

    no_auth = client.get("/v1/me")
    assert no_auth.status_code == 401


def test_live_trading_requires_coinbase(client):
    client.post(
        "/v1/auth/register",
        json={
            "email": "trader@example.com",
            "password": "password-123",
            "tenant_name": "T",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "trader@example.com", "password": "password-123"},
    )
    token = r.json()["access_token"]
    r = client.patch(
        "/v1/me/trading-mode",
        headers={"Authorization": f"Bearer {token}"},
        json={"trading_mode": "live"},
    )
    assert r.status_code == 403
    assert "Coinbase" in r.json()["detail"]


def test_live_allowed_when_coinbase_connected(
    client,
    root_user_and_token: tuple[str, str],
):
    client.post(
        "/v1/auth/register",
        json={
            "email": "trader2@example.com",
            "password": "password-123",
            "tenant_name": "T2",
        },
    )
    r = client.post(
        "/v1/auth/login",
        json={"email": "trader2@example.com", "password": "password-123"},
    )
    user_token = r.json()["access_token"]
    me = client.get("/v1/me", headers={"Authorization": f"Bearer {user_token}"}).json()
    tenant_id = me["tenants"][0]["id"]
    _, admin_token = root_user_and_token
    r = client.put(
        f"/v1/admin/tenants/{tenant_id}/integrations/coinbase",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"connected": True},
    )
    assert r.status_code == 200, r.text
    assert r.json()["coinbase_connected"] is True
    r = client.patch(
        "/v1/me/trading-mode",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"trading_mode": "live"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["current_trading_mode"] == "live"


def test_root_admin_with_tenant_membership_can_switch_trading_mode(
    client,
    tenant_root_user_and_token: tuple[str, str],
    root_user_and_token: tuple[str, str],
):
    _, user_token = tenant_root_user_and_token
    me = client.get("/v1/me", headers={"Authorization": f"Bearer {user_token}"}).json()
    tenant_id = me["tenants"][0]["id"]
    _, admin_token = root_user_and_token
    r = client.put(
        f"/v1/admin/tenants/{tenant_id}/integrations/coinbase",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"connected": True},
    )
    assert r.status_code == 200, r.text

    r = client.patch(
        "/v1/me/trading-mode",
        headers={"Authorization": f"Bearer {user_token}"},
        json={"trading_mode": "live"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["current_trading_mode"] == "live"


def test_root_admin_login_bootstraps_strategy_access(client, db_session):
    email = "dominic@oziebot.com"
    password = "password-123"
    register = client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "full_name": "Dominic Ighedosa",
            "password": password,
            "tenant_name": "Oziebot Admin",
        },
    )
    assert register.status_code == 201, register.text

    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    db_session.add(user)
    db_session.commit()

    login = client.post("/v1/auth/login", json={"email": email, "password": password})
    assert login.status_code == 200, login.text
    token = login.json()["access_token"]

    catalog = client.get(
        "/v1/me/strategies/catalog",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert catalog.status_code == 200, catalog.text
    strategy_rows = {
        row["strategy_id"]: row
        for row in catalog.json()["strategies"]
        if row["strategy_id"] in {"momentum", "day_trading", "dca", "reversion"}
    }
    assert set(strategy_rows) == {"momentum", "day_trading", "dca", "reversion"}
    assert all(row["is_assigned"] is True for row in strategy_rows.values())
    assert strategy_rows["momentum"]["config_schema"]["risk_caps"]["max_position_usd"] == 300
    assert strategy_rows["day_trading"]["config_schema"]["signal_rules"]["cooldown_seconds"] == 20
    assert (
        strategy_rows["reversion"]["config_schema"]["strategy_params"]["use_trend_filter"] is True
    )

    configured = client.get(
        "/v1/me/strategies",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert configured.status_code == 200, configured.text
    configured_ids = {row["strategy_id"] for row in configured.json()["strategies"]}
    assert {"momentum", "day_trading", "dca", "reversion"}.issubset(configured_ids)

    entitlement = db_session.scalar(
        select(TenantEntitlement).where(
            TenantEntitlement.source == "root_admin",
        )
    )
    assert entitlement is not None

    bootstrapped = db_session.scalars(
        select(UserStrategy).where(
            UserStrategy.user_id == user.id,
            UserStrategy.strategy_id.in_(("momentum", "day_trading", "dca", "reversion")),
        )
    ).all()
    assert len(bootstrapped) == 4
    assert all(row.is_enabled is True for row in bootstrapped)


def test_refresh_and_logout(client):
    r = client.post(
        "/v1/auth/register",
        json={
            "email": "refresh@example.com",
            "password": "password-123",
            "tenant_name": "R",
        },
    )
    refresh = r.json()["refresh_token"]
    r2 = client.post("/v1/auth/refresh", json={"refresh_token": refresh})
    assert r2.status_code == 200
    new_refresh = r2.json()["refresh_token"]
    r3 = client.post("/v1/auth/logout", json={"refresh_token": new_refresh})
    assert r3.status_code == 204
    r4 = client.post("/v1/auth/refresh", json={"refresh_token": new_refresh})
    assert r4.status_code == 401

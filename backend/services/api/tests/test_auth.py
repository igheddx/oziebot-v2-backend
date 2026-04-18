from __future__ import annotations


def test_register_login_me(client):
    r = client.post(
        "/v1/auth/register",
        json={
            "email": "User@Example.com",
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

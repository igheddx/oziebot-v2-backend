from __future__ import annotations


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_alert_channel_and_preference_crud(client, regular_user_and_token):
    _, token = regular_user_and_token

    r = client.put(
        "/v1/me/alerts/channels/slack",
        headers=_auth(token),
        json={"destination": "#trading-desk", "is_enabled": True, "settings": {"thread": "alerts"}},
    )
    assert r.status_code == 200, r.text
    assert r.json()["channel"] == "slack"
    assert r.json()["destination"] == "#trading-desk"

    r = client.put(
        "/v1/me/alerts/preferences/trade_opened",
        headers=_auth(token),
        json={"trading_mode": "paper", "is_enabled": True},
    )
    assert r.status_code == 200, r.text
    assert r.json()["event_type"] == "trade_opened"
    assert r.json()["trading_mode"] == "paper"
    assert r.json()["is_enabled"] is True

    r = client.get("/v1/me/alerts/config", headers=_auth(token))
    assert r.status_code == 200, r.text
    payload = r.json()
    assert "slack" in payload["supported_channels"]
    assert "trade_opened" in payload["supported_event_types"]
    assert any(ch["channel"] == "slack" for ch in payload["channels"])
    assert any(p["event_type"] == "trade_opened" and p["trading_mode"] == "paper" for p in payload["preferences"])


def test_alert_rejects_unsupported_channel_or_event(client, regular_user_and_token):
    _, token = regular_user_and_token

    r = client.put(
        "/v1/me/alerts/channels/email",
        headers=_auth(token),
        json={"destination": "x", "is_enabled": True},
    )
    assert r.status_code == 400

    r = client.put(
        "/v1/me/alerts/preferences/unknown_event",
        headers=_auth(token),
        json={"trading_mode": "live", "is_enabled": True},
    )
    assert r.status_code == 400
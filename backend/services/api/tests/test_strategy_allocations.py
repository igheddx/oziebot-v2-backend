from __future__ import annotations

import os
from datetime import UTC, datetime
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.strategy_allocation import StrategyCapitalLedger
from oziebot_api.models.user import User
from oziebot_api.services.credential_crypto import CredentialCrypto


def _create_enabled_strategy(client, token: str, strategy_id: str) -> None:
    r = client.post(
        "/v1/me/strategies",
        headers={"Authorization": f"Bearer {token}"},
        json={"strategy_id": strategy_id, "is_enabled": True},
    )
    assert r.status_code in (200, 409), r.text


def _seed_enabled_strategies(client, token: str) -> None:
    _create_enabled_strategy(client, token, "momentum")
    _create_enabled_strategy(client, token, "day_trading")
    _create_enabled_strategy(client, token, "dca")
    _create_enabled_strategy(client, token, "reversion")


def test_manual_allocation_creates_mode_buckets(client, regular_user_and_token):
    _, token = regular_user_and_token
    _seed_enabled_strategies(client, token)

    r = client.put(
        "/v1/me/allocations/paper/manual",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "total_capital_cents": 100_000,
            "allocations": [
                {"strategy_id": "dca", "allocation_bps": 5000},
                {"strategy_id": "momentum", "allocation_bps": 3000},
                {"strategy_id": "day_trading", "allocation_bps": 2000},
            ],
        },
    )
    assert r.status_code == 200, r.text

    buckets = client.get(
        "/v1/me/allocations/paper/buckets",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert buckets.status_code == 200
    data = buckets.json()
    assert data["trading_mode"] == "paper"
    assert len(data["buckets"]) == 3

    by_strategy = {b["strategy_id"]: b for b in data["buckets"]}
    assert by_strategy["dca"]["assigned_capital_cents"] == 50_000
    assert by_strategy["momentum"]["assigned_capital_cents"] == 30_000
    assert by_strategy["day_trading"]["assigned_capital_cents"] == 20_000


def test_guided_preset_allocations(client, regular_user_and_token):
    _, token = regular_user_and_token
    _seed_enabled_strategies(client, token)

    r = client.put(
        "/v1/me/allocations/live/guided",
        headers={"Authorization": f"Bearer {token}"},
        json={"total_capital_cents": 200_000, "preset_name": "balanced"},
    )
    assert r.status_code == 200, r.text
    assert r.json()["allocation_mode"] == "guided"
    assert r.json()["preset_name"] == "balanced"
    by_strategy = {item["strategy_id"]: item for item in r.json()["items"]}
    assert by_strategy["momentum"]["allocation_bps"] == 4500
    assert by_strategy["day_trading"]["allocation_bps"] == 2000
    assert by_strategy["reversion"]["allocation_bps"] == 1000
    assert by_strategy["dca"]["allocation_bps"] == 2500


def test_starvation_prevention_between_strategies(client, regular_user_and_token):
    _, token = regular_user_and_token
    _seed_enabled_strategies(client, token)

    setup = client.put(
        "/v1/me/allocations/live/manual",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "total_capital_cents": 100_000,
            "allocations": [
                {"strategy_id": "momentum", "allocation_bps": 6000},
                {"strategy_id": "day_trading", "allocation_bps": 4000},
            ],
        },
    )
    assert setup.status_code == 200, setup.text

    reserve_a = client.post(
        "/v1/me/allocations/live/reserve",
        headers={"Authorization": f"Bearer {token}"},
        json={"strategy_id": "momentum", "amount_cents": 55_000, "reference_id": "ord-1"},
    )
    assert reserve_a.status_code == 200, reserve_a.text

    # day_trading still has its own bucket funds; momentum reservation must not consume them.
    reserve_b = client.post(
        "/v1/me/allocations/live/reserve",
        headers={"Authorization": f"Bearer {token}"},
        json={"strategy_id": "day_trading", "amount_cents": 39_000, "reference_id": "ord-2"},
    )
    assert reserve_b.status_code == 200, reserve_b.text

    over_reserve = client.post(
        "/v1/me/allocations/live/reserve",
        headers={"Authorization": f"Bearer {token}"},
        json={"strategy_id": "momentum", "amount_cents": 6_000, "reference_id": "ord-3"},
    )
    assert over_reserve.status_code == 409


def test_bucket_accounting_reserve_release_lock_settle(client, regular_user_and_token):
    _, token = regular_user_and_token
    _seed_enabled_strategies(client, token)

    setup = client.put(
        "/v1/me/allocations/paper/manual",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "total_capital_cents": 50_000,
            "allocations": [{"strategy_id": "momentum", "allocation_bps": 10_000}],
        },
    )
    assert setup.status_code == 200, setup.text

    reserve = client.post(
        "/v1/me/allocations/paper/reserve",
        headers={"Authorization": f"Bearer {token}"},
        json={"strategy_id": "momentum", "amount_cents": 10_000, "reference_id": "r-1"},
    )
    assert reserve.status_code == 200
    assert reserve.json()["available_cash_cents"] == 40_000
    assert reserve.json()["reserved_cash_cents"] == 10_000

    lock = client.post(
        "/v1/me/allocations/paper/lock",
        headers={"Authorization": f"Bearer {token}"},
        json={"strategy_id": "momentum", "amount_cents": 8_000, "reference_id": "l-1"},
    )
    assert lock.status_code == 200
    assert lock.json()["reserved_cash_cents"] == 2_000
    assert lock.json()["locked_capital_cents"] == 8_000

    settle = client.post(
        "/v1/me/allocations/paper/settle",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "strategy_id": "momentum",
            "released_locked_cents": 8_000,
            "realized_pnl_delta_cents": 1_500,
            "reference_id": "s-1",
        },
    )
    assert settle.status_code == 200
    assert settle.json()["locked_capital_cents"] == 0
    assert settle.json()["realized_pnl_cents"] == 1_500
    assert settle.json()["available_cash_cents"] == 49_500

    release = client.post(
        "/v1/me/allocations/paper/release",
        headers={"Authorization": f"Bearer {token}"},
        json={"strategy_id": "momentum", "amount_cents": 2_000, "reference_id": "rel-1"},
    )
    assert release.status_code == 200
    assert release.json()["reserved_cash_cents"] == 0
    assert release.json()["available_cash_cents"] == 51_500


def test_mode_isolation_no_transfer_between_paper_and_live(client, regular_user_and_token):
    _, token = regular_user_and_token
    _seed_enabled_strategies(client, token)

    r1 = client.put(
        "/v1/me/allocations/paper/manual",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "total_capital_cents": 120_000,
            "allocations": [{"strategy_id": "dca", "allocation_bps": 10_000}],
        },
    )
    r2 = client.put(
        "/v1/me/allocations/live/manual",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "total_capital_cents": 80_000,
            "allocations": [{"strategy_id": "dca", "allocation_bps": 10_000}],
        },
    )
    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text

    b1 = client.get(
        "/v1/me/allocations/paper/buckets",
        headers={"Authorization": f"Bearer {token}"},
    )
    b2 = client.get(
        "/v1/me/allocations/live/buckets",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert b1.status_code == 200
    assert b2.status_code == 200
    assert b1.json()["buckets"][0]["assigned_capital_cents"] == 120_000
    assert b2.json()["buckets"][0]["assigned_capital_cents"] == 80_000


def test_ledger_audit_entries_written(client, regular_user_and_token, db_session: Session):
    _, token = regular_user_and_token
    _seed_enabled_strategies(client, token)

    setup = client.put(
        "/v1/me/allocations/live/manual",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "total_capital_cents": 20_000,
            "allocations": [{"strategy_id": "momentum", "allocation_bps": 10_000}],
        },
    )
    assert setup.status_code == 200

    reserve = client.post(
        "/v1/me/allocations/live/reserve",
        headers={"Authorization": f"Bearer {token}"},
        json={"strategy_id": "momentum", "amount_cents": 2_000, "reference_id": "aud-1"},
    )
    assert reserve.status_code == 200

    rows = (
        db_session.query(StrategyCapitalLedger)
        .filter(StrategyCapitalLedger.reference_id == "aud-1")
        .all()
    )
    assert len(rows) == 1
    assert rows[0].event_type == "reserve"
    assert rows[0].amount_cents == 2_000


def _seed_valid_coinbase_connection(db_session: Session, *, email: str) -> None:
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    now = datetime.now(UTC)
    crypto = CredentialCrypto(os.environ["EXCHANGE_CREDENTIALS_ENCRYPTION_KEY"])
    db_session.add(
        ExchangeConnection(
            tenant_id=membership.tenant_id,
            provider="coinbase",
            api_key_name="organizations/test/key",
            encrypted_secret=crypto.encrypt(b"test-private-key"),
            secret_ciphertext_version=1,
            validation_status="valid",
            health_status="healthy",
            can_trade=True,
            can_read_balances=True,
            created_at=now,
            updated_at=now,
            last_validated_at=now,
            last_health_check_at=now,
        )
    )
    db_session.commit()


@patch("oziebot_api.services.live_coinbase.list_coinbase_accounts")
def test_live_plan_auto_syncs_from_coinbase_available_cash(
    mock_list_coinbase_accounts,
    client,
    regular_user_and_token,
    db_session: Session,
):
    email, token = regular_user_and_token
    _seed_enabled_strategies(client, token)
    _seed_valid_coinbase_connection(db_session, email=email)
    mock_list_coinbase_accounts.return_value = [
        {
            "currency": "USD",
            "available_balance": {"currency": "USD", "value": "120.50"},
            "hold": {"currency": "USD", "value": "10.25"},
        },
        {
            "currency": "USDC",
            "available_balance": {"currency": "USDC", "value": "50.00"},
            "hold": {"currency": "USDC", "value": "5.00"},
        },
    ]

    plan = client.get(
        "/v1/me/allocations/live",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert plan.status_code == 200, plan.text
    payload = plan.json()
    assert payload["total_capital_cents"] == 17_050
    assert len(payload["items"]) == 4
    assert sum(item["allocation_bps"] for item in payload["items"]) == 10_000

    buckets = client.get(
        "/v1/me/allocations/live/buckets",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert buckets.status_code == 200, buckets.text
    bucket_rows = buckets.json()["buckets"]
    assert len(bucket_rows) == 4
    assert sum(item["assigned_capital_cents"] for item in bucket_rows) == 17_050
    assert sum(item["available_cash_cents"] for item in bucket_rows) == 17_050


@patch("oziebot_api.services.live_coinbase.list_coinbase_accounts")
def test_live_manual_allocations_ignore_submitted_total_when_coinbase_is_available(
    mock_list_coinbase_accounts,
    client,
    regular_user_and_token,
    db_session: Session,
):
    email, token = regular_user_and_token
    _seed_enabled_strategies(client, token)
    _seed_valid_coinbase_connection(db_session, email=email)
    mock_list_coinbase_accounts.return_value = [
        {
            "currency": "USD",
            "available_balance": {"currency": "USD", "value": "80.00"},
            "hold": {"currency": "USD", "value": "0"},
        },
        {
            "currency": "USDC",
            "available_balance": {"currency": "USDC", "value": "20.00"},
            "hold": {"currency": "USDC", "value": "50.00"},
        },
    ]

    response = client.put(
        "/v1/me/allocations/live/manual",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "total_capital_cents": 999_999,
            "allocations": [
                {"strategy_id": "momentum", "allocation_bps": 6000},
                {"strategy_id": "day_trading", "allocation_bps": 4000},
            ],
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["total_capital_cents"] == 10_000
    by_strategy = {item["strategy_id"]: item for item in payload["items"]}
    assert by_strategy["momentum"]["assigned_capital_cents"] == 6_000
    assert by_strategy["day_trading"]["assigned_capital_cents"] == 4_000

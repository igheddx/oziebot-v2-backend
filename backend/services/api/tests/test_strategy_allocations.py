from __future__ import annotations

from sqlalchemy.orm import Session

from oziebot_api.models.strategy_allocation import StrategyCapitalLedger


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

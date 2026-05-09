from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from oziebot_api.models.ai_diagnostics import DiagnosticSnapshot
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.user import User
from oziebot_api.services.admin_ai_diagnostics import RuleBasedDiagnosticProvider


def _raw_snapshot() -> dict:
    return {
        "generated_at": "2026-05-09T12:00:00+00:00",
        "trade_count": 1,
        "trade_details": [],
        "strategy_summary": [],
        "token_summary": [],
        "execution_activity": {
            "execution_count": 4,
            "flattened_trade_count": 0,
            "buy_count": 3,
            "sell_count": 1,
            "unique_tokens": 2,
            "total_notional_usd": 150.0,
            "total_fees_usd": 1.2,
            "total_realized_pnl_usd": 4.0,
            "data_source": "execution_trades",
            "note": None,
            "strategy_summary": [
                {
                    "strategy": "dca",
                    "trading_mode": "paper",
                    "total_executions": 2,
                    "buy_executions": 2,
                    "sell_executions": 0,
                    "flattened_executions": 0,
                    "total_notional_usd": 100.0,
                    "total_fees_usd": 0.5,
                    "total_realized_pnl_usd": 0.0,
                    "last_executed_at": "2026-05-09T06:00:00+00:00",
                },
                {
                    "strategy": "day_trading",
                    "trading_mode": "paper",
                    "total_executions": 1,
                    "buy_executions": 1,
                    "sell_executions": 0,
                    "flattened_executions": 0,
                    "total_notional_usd": 50.0,
                    "total_fees_usd": 0.2,
                    "total_realized_pnl_usd": 0.0,
                    "last_executed_at": "2026-05-09T03:00:00+00:00",
                },
            ],
            "token_summary": [],
            "execution_details": [
                {
                    "execution_trade_id": "trade-1",
                    "order_id": "order-1",
                    "strategy": "dca",
                    "token": "BTC-USD",
                    "trading_mode": "paper",
                    "side": "buy",
                    "executed_at": "2026-05-08T12:00:00+00:00",
                    "quantity": 0.002,
                    "price_usd": 50000.0,
                    "notional_usd": 100.0,
                    "fees_usd": 0.2,
                    "realized_pnl_usd": 0.0,
                    "position_quantity_after": 0.002,
                    "position_closed": False,
                },
                {
                    "execution_trade_id": "trade-2",
                    "order_id": "order-2",
                    "strategy": "dca",
                    "token": "BTC-USD",
                    "trading_mode": "paper",
                    "side": "buy",
                    "executed_at": "2026-05-09T06:00:00+00:00",
                    "quantity": 0.002,
                    "price_usd": 50500.0,
                    "notional_usd": 101.0,
                    "fees_usd": 0.2,
                    "realized_pnl_usd": 0.0,
                    "position_quantity_after": 0.004,
                    "position_closed": False,
                },
                {
                    "execution_trade_id": "trade-3",
                    "order_id": "order-3",
                    "strategy": "momentum",
                    "token": "ETH-USD",
                    "trading_mode": "paper",
                    "side": "buy",
                    "executed_at": "2026-05-09T08:00:00+00:00",
                    "quantity": 0.0,
                    "price_usd": 2500.0,
                    "notional_usd": 0.0,
                    "fees_usd": 0.1,
                    "realized_pnl_usd": 0.0,
                    "position_quantity_after": 0.5,
                    "position_closed": False,
                },
                {
                    "execution_trade_id": "trade-4",
                    "order_id": "order-4",
                    "strategy": "day_trading",
                    "token": "AERO-USD",
                    "trading_mode": "paper",
                    "side": "buy",
                    "executed_at": "2026-05-09T03:00:00+00:00",
                    "quantity": 45.0,
                    "price_usd": 1.11,
                    "notional_usd": 49.95,
                    "fees_usd": 0.1,
                    "realized_pnl_usd": 0.0,
                    "position_quantity_after": 45.0,
                    "position_closed": False,
                },
            ],
        },
        "open_positions": {
            "position_count": 2,
            "unique_tokens": 2,
            "total_position_notional_usd": 320.0,
            "total_realized_pnl_usd": 0.0,
            "exposure_by_strategy": {"dca": 202.0, "momentum": 118.0},
            "data_source": "execution_positions",
            "note": None,
            "positions": [
                {
                    "position_id": "position-1",
                    "strategy": "dca",
                    "token": "BTC-USD",
                    "trading_mode": "paper",
                    "quantity": 0.005,
                    "avg_entry_price": 50250.0,
                    "position_notional_usd": 251.25,
                    "realized_pnl_usd": 0.0,
                    "opened_at": "2026-05-08T12:00:00+00:00",
                    "last_trade_at": "2026-05-09T06:00:00+00:00",
                    "updated_at": "2026-05-09T06:00:00+00:00",
                    "closed_at": None,
                },
                {
                    "position_id": "position-2",
                    "strategy": "reversion",
                    "token": "SOL-USD",
                    "trading_mode": "paper",
                    "quantity": 2.0,
                    "avg_entry_price": 34.0,
                    "position_notional_usd": 68.0,
                    "realized_pnl_usd": 0.0,
                    "opened_at": "2026-05-09T01:00:00+00:00",
                    "last_trade_at": "2026-05-09T01:00:00+00:00",
                    "updated_at": "2026-05-09T01:00:00+00:00",
                    "closed_at": None,
                },
            ],
        },
        "signal_funnel": {
            "signals_evaluated": 20,
            "signals_emitted": 12,
            "signals_rejected": 10,
            "trades_executed": 1,
            "rejection_reasons": {
                "confidence": 1,
                "volume": 1,
                "allocation": 1,
                "risk_engine": 1,
                "token_strategy_policy": 1,
                "cooldown": 1,
                "liquidity_hours": 0,
                "other": 5,
            },
            "data_sources": {},
            "unavailable_metrics": [],
            "note": None,
        },
        "capital_utilization": {
            "total_account_value": 1000.0,
            "avg_capital_deployed_pct": 82.0,
            "peak_capital_deployed_pct": 61.0,
            "avg_cash_idle_pct": 75.0,
            "capital_by_strategy": {"dca": 500.0, "momentum": 300.0},
            "note": None,
        },
        "exit_analysis": {
            "most_common_exit_reason": None,
            "stop_loss_rate_pct": None,
            "avg_profit_before_trailing_exit_pct": None,
            "avg_profit_before_reversal_pct": None,
            "partial_take_profit_effectiveness_pct": None,
            "trades_that_were_positive_before_loss_pct": None,
        },
        "active_strategy_config": {
            "momentum_config": {"strategy_params": {"min_trade_usd": 75}},
            "day_trading_config": {"strategy_params": {"min_trade_usd": 50}},
            "reversion_config": {"strategy_params": {"min_trade_usd": 30}},
            "dca_config": {"strategy_params": {"buy_amount_usd": 100, "buy_interval_hours": 24}},
            "signal_rules": {
                "momentum": {"min_confidence": 0.6},
                "day_trading": {"min_confidence": 0.6},
                "reversion": {"min_confidence": 0.6},
                "dca": {"min_confidence": 0.9},
            },
            "token_strategy_policy_matrix": {
                "BTC-USD": {
                    "dca": {"effective_recommendation_status": "preferred"},
                    "momentum": {"effective_recommendation_status": "allowed"},
                },
                "AERO-USD": {
                    "day_trading": {"effective_recommendation_status": "blocked"},
                    "momentum": {"effective_recommendation_status": "allowed"},
                },
                "SOL-USD": {
                    "reversion": {"effective_recommendation_status": "allowed"},
                },
            },
            "default_missing_policy_behavior": "allowed",
        },
    }


def _create_snapshot(db_session, user: User) -> DiagnosticSnapshot:
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    now = datetime.now(UTC)
    snapshot = DiagnosticSnapshot(
        id=uuid.uuid4(),
        tenant_id=membership.tenant_id if membership is not None else None,
        generated_at=now,
        trading_mode=None,
        strategy_filter=None,
        token_filter=None,
        days_filter=7,
        raw_json=_raw_snapshot(),
        created_at=now,
    )
    db_session.add(snapshot)
    db_session.commit()
    return snapshot


def _auth_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_rule_based_provider_generates_expected_findings():
    provider = RuleBasedDiagnosticProvider()
    snapshot = DiagnosticSnapshot(
        id=uuid.uuid4(),
        tenant_id=None,
        generated_at=datetime.now(UTC),
        trading_mode=None,
        strategy_filter=None,
        token_filter=None,
        days_filter=7,
        raw_json=_raw_snapshot(),
        created_at=datetime.now(UTC),
    )

    result = provider.review_diagnostics(snapshot, context={})

    categories = {finding["category"] for finding in result["findings"]}
    assert result["overall_health"] == "critical"
    assert "dca_interval" in categories
    assert "execution_accounting" in categories
    assert "token_policy" in categories
    assert "position_reconciliation" in categories
    assert "capital_utilization" in categories


def test_rule_based_provider_downgrades_stale_dca_interval_violations():
    provider = RuleBasedDiagnosticProvider()
    snapshot_payload = _raw_snapshot()
    snapshot_payload["generated_at"] = "2026-05-09T12:00:00+00:00"
    snapshot_payload["execution_activity"]["execution_details"][0]["executed_at"] = (
        "2026-05-07T08:00:00+00:00"
    )
    snapshot_payload["execution_activity"]["execution_details"][1]["executed_at"] = (
        "2026-05-07T09:00:00+00:00"
    )
    snapshot = DiagnosticSnapshot(
        id=uuid.uuid4(),
        tenant_id=None,
        generated_at=datetime.now(UTC),
        trading_mode=None,
        token_filter=None,
        strategy_filter=None,
        days_filter=7,
        raw_json=snapshot_payload,
        created_at=datetime.now(UTC),
    )

    result = provider.review_diagnostics(snapshot, context={})

    dca_finding = next(
        finding for finding in result["findings"] if finding["category"] == "dca_interval"
    )
    assert dca_finding["severity"] == "warning"
    assert dca_finding["finding_title"] == "Historical DCA interval violations detected"
    assert dca_finding["evidence_json"]["violation_is_active"] is False


def test_rule_based_provider_downgrades_stale_zero_value_executions():
    provider = RuleBasedDiagnosticProvider()
    snapshot_payload = _raw_snapshot()
    snapshot_payload["execution_activity"]["execution_details"][2]["executed_at"] = (
        "2026-05-07T08:00:00+00:00"
    )
    snapshot = DiagnosticSnapshot(
        id=uuid.uuid4(),
        tenant_id=None,
        generated_at=datetime.now(UTC),
        trading_mode=None,
        token_filter=None,
        strategy_filter=None,
        days_filter=7,
        raw_json=snapshot_payload,
        created_at=datetime.now(UTC),
    )

    result = provider.review_diagnostics(snapshot, context={})

    finding = next(
        item for item in result["findings"] if item["category"] == "execution_accounting"
    )
    assert finding["severity"] == "warning"
    assert finding["evidence_json"]["offender_is_recent"] is False


def test_rule_based_provider_downgrades_stale_token_policy_conflicts():
    provider = RuleBasedDiagnosticProvider()
    snapshot_payload = _raw_snapshot()
    snapshot_payload["execution_activity"]["execution_details"][3]["executed_at"] = (
        "2026-05-07T03:00:00+00:00"
    )
    snapshot = DiagnosticSnapshot(
        id=uuid.uuid4(),
        tenant_id=None,
        generated_at=datetime.now(UTC),
        trading_mode=None,
        token_filter=None,
        strategy_filter=None,
        days_filter=7,
        raw_json=snapshot_payload,
        created_at=datetime.now(UTC),
    )

    result = provider.review_diagnostics(snapshot, context={})

    finding = next(item for item in result["findings"] if item["category"] == "token_policy")
    assert finding["severity"] == "warning"
    assert finding["evidence_json"]["conflict_is_recent"] is False


def test_rule_based_provider_downgrades_positions_outside_review_window():
    provider = RuleBasedDiagnosticProvider()
    snapshot_payload = _raw_snapshot()
    snapshot_payload["open_positions"]["positions"][0]["quantity"] = 0.004
    snapshot_payload["open_positions"]["positions"][1]["opened_at"] = "2026-05-01T01:00:00+00:00"
    snapshot_payload["open_positions"]["positions"][1]["last_trade_at"] = (
        "2026-05-01T01:00:00+00:00"
    )
    snapshot_payload["open_positions"]["positions"][1]["updated_at"] = "2026-05-01T01:00:00+00:00"
    snapshot = DiagnosticSnapshot(
        id=uuid.uuid4(),
        tenant_id=None,
        generated_at=datetime.now(UTC),
        trading_mode=None,
        token_filter=None,
        strategy_filter=None,
        days_filter=7,
        raw_json=snapshot_payload,
        created_at=datetime.now(UTC),
    )

    result = provider.review_diagnostics(snapshot, context={})

    finding = next(
        item for item in result["findings"] if item["category"] == "position_reconciliation"
    )
    assert finding["severity"] == "warning"
    assert finding["evidence_json"]["active_mismatch_count"] == 0


def test_create_review_endpoint_persists_findings(client, db_session, tenant_root_user_and_token):
    _, token = tenant_root_user_and_token
    user = db_session.scalar(select(User).where(User.email == "tenant-root@example.com"))
    assert user is not None
    snapshot = _create_snapshot(db_session, user)

    list_response = client.get(
        "/v1/admin/ai-diagnostics/snapshots",
        headers=_auth_header(token),
    )
    assert list_response.status_code == 200, list_response.text
    assert list_response.json()["snapshots"][0]["id"] == str(snapshot.id)

    response = client.post(
        "/v1/admin/ai-diagnostics/reviews",
        headers=_auth_header(token),
        json={
            "snapshot_id": str(snapshot.id),
            "trading_mode": "all",
            "strategy": "all",
            "token": None,
            "days": 7,
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["status"] == "completed"

    history = client.get("/v1/admin/ai-diagnostics/reviews", headers=_auth_header(token))
    assert history.status_code == 200, history.text
    assert history.json()["reviews"][0]["id"] == payload["review_id"]
    assert history.json()["reviews"][0]["critical_count"] >= 1

    detail = client.get(
        f"/v1/admin/ai-diagnostics/reviews/{payload['review_id']}",
        headers=_auth_header(token),
    )
    assert detail.status_code == 200, detail.text
    detail_payload = detail.json()
    assert detail_payload["snapshot"]["id"] == str(snapshot.id)
    assert len(detail_payload["findings"]) >= 4
    assert {finding["category"] for finding in detail_payload["findings"]} >= {
        "dca_interval",
        "execution_accounting",
        "token_policy",
    }


def test_patch_finding_status_updates_finding(client, db_session, tenant_root_user_and_token):
    _, token = tenant_root_user_and_token
    user = db_session.scalar(select(User).where(User.email == "tenant-root@example.com"))
    assert user is not None
    snapshot = _create_snapshot(db_session, user)

    response = client.post(
        "/v1/admin/ai-diagnostics/reviews",
        headers=_auth_header(token),
        json={"snapshot_id": str(snapshot.id), "trading_mode": "all", "strategy": "all", "days": 7},
    )
    review_id = response.json()["review_id"]
    detail = client.get(
        f"/v1/admin/ai-diagnostics/reviews/{review_id}",
        headers=_auth_header(token),
    ).json()
    finding_id = detail["findings"][0]["id"]

    patch_response = client.patch(
        f"/v1/admin/ai-diagnostics/findings/{finding_id}",
        headers=_auth_header(token),
        json={"status": "acknowledged", "note": "Investigating."},
    )
    assert patch_response.status_code == 200, patch_response.text
    assert patch_response.json()["status"] == "acknowledged"


def test_ai_diagnostics_endpoints_require_root_admin(client, regular_user_and_token):
    _, token = regular_user_and_token

    response = client.get(
        "/v1/admin/ai-diagnostics/snapshots",
        headers=_auth_header(token),
    )
    assert response.status_code == 403, response.text

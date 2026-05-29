from __future__ import annotations

from sqlalchemy.orm import Session

from tests.test_education_catalog import _root_token
from tests.test_pacing_guide_foundation import _teacher_token


def test_pilot_readiness_endpoints(client, db_session: Session):
    _ = db_session
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Pilot Readiness Tenant")

    review = client.get(
        "/v1/teacher-assist/pilot/completion-review",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert review.status_code == 200, review.text
    assert review.json()["summary"]["total_features"] >= 20

    metrics = client.get(
        "/v1/teacher-assist/pilot/usage-metrics",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert metrics.status_code == 200, metrics.text
    assert "metrics" in metrics.json()

    login_metric = client.post(
        "/v1/teacher-assist/pilot/usage-metrics/login",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert login_metric.status_code == 204, login_metric.text

    created = client.post(
        "/v1/teacher-assist/pilot/feedback",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "category": "usability",
            "severity": "medium",
            "feature_area": "Copilot",
            "description": "Starter prompts could be grouped by intent.",
            "requested_improvement": "Group prompts under Week, Students, Objectives.",
        },
    )
    assert created.status_code == 201, created.text
    feedback_id = created.json()["id"]

    listed = client.get(
        "/v1/teacher-assist/pilot/feedback",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert listed.status_code == 200, listed.text
    assert any(row["id"] == feedback_id for row in listed.json())

    health = client.get(
        "/v1/teacher-assist/pilot/system-health",
        headers={"Authorization": f"Bearer {root_token}"},
    )
    assert health.status_code == 200, health.text
    assert "storage" in health.json()

    seed = client.get(
        "/v1/teacher-assist/pilot/seed-validation",
        headers={"Authorization": f"Bearer {root_token}"},
    )
    assert seed.status_code == 200, seed.text
    assert "checks" in seed.json()

    forbidden = client.get(
        "/v1/teacher-assist/pilot/system-health",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert forbidden.status_code == 403, forbidden.text

from __future__ import annotations

from sqlalchemy.orm import Session

from tests.test_current_week_foundation import _align_teacher_tenant, _create_week_guide
from tests.test_education_catalog import _root_token
from tests.test_pacing_guide_foundation import _catalog_scope, _school_year, _teacher_token


def test_teacher_copilot_endpoints(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Teacher Copilot Tenant")
    _align_teacher_tenant(db_session, teacher_email="pacing-guide-teacher@example.com")
    scope = _catalog_scope(client, root_token)
    school_year = _school_year(client, root_token)
    guide, period_id = _create_week_guide(client, root_token, scope, school_year)

    client.patch(
        "/v1/teacher-assist/pacing-guides/active-selection",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"active_pacing_guide_id": guide["id"]},
    )

    created = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/instructional-weeks",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"status": "ACTIVE"},
    )
    assert created.status_code == 201, created.text
    week_id = created.json()["id"]

    questions = client.get(
        "/v1/teacher-assist/copilot/suggested-questions",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert questions.status_code == 200, questions.text
    assert len(questions.json()["questions"]) >= 5

    context = client.get(
        "/v1/teacher-assist/copilot/context",
        headers={"Authorization": f"Bearer {teacher_token}"},
        params={"instructional_week_id": week_id},
    )
    assert context.status_code == 200, context.text
    assert "context_packets" in context.json()

    session = client.post(
        "/v1/teacher-assist/copilot/sessions",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"title": "Week review"},
    )
    assert session.status_code == 201, session.text
    session_id = session.json()["id"]

    message = client.post(
        f"/v1/teacher-assist/copilot/sessions/{session_id}/messages",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "question": "Summarize this week.",
            "provider_mode": "mock",
            "instructional_week_id": week_id,
        },
    )
    assert message.status_code == 200, message.text
    payload = message.json()
    assert payload["requires_teacher_review"] is True
    assert payload["analysis"]["intent"] == "week_summarizer"
    assert payload["assistant_message"]["role"] == "assistant"
    assert (
        payload["assistant_message"]["context_snapshot"]["audit"]["prompt"]
        == "Summarize this week."
    )

    history = client.get(
        f"/v1/teacher-assist/copilot/sessions/{session_id}/messages",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert history.status_code == 200, history.text
    assert len(history.json()) == 2

    objective = client.post(
        f"/v1/teacher-assist/copilot/sessions/{session_id}/messages",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "question": "Which students need support?",
            "provider_mode": "mock",
            "instructional_week_id": week_id,
        },
    )
    assert objective.status_code == 200, objective.text
    assert objective.json()["analysis"]["intent"] == "student_support"

    real_provider = client.post(
        f"/v1/teacher-assist/copilot/sessions/{session_id}/messages",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"question": "What should I reteach?", "provider_mode": "real"},
    )
    assert real_provider.status_code == 400, real_provider.text

    home = client.get(
        "/v1/teacher-assist/home",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert home.status_code == 200, home.text
    assert home.json()["copilot"]["href"] == "/teacher-assist/copilot"

    admin = client.post(
        "/v1/teacher-assist/copilot/admin/query",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"question": "Which objectives lack resources?"},
    )
    assert admin.status_code == 200, admin.text
    assert admin.json()["requires_teacher_review"] is True

    forbidden = client.post(
        "/v1/teacher-assist/copilot/admin/query",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"question": "Which objectives lack resources?"},
    )
    assert forbidden.status_code == 403, forbidden.text

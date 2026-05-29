from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy.orm import Session

from tests.test_current_week_foundation import _align_teacher_tenant, _create_week_guide
from tests.test_education_catalog import _root_token
from tests.test_pacing_guide_foundation import _catalog_scope, _school_year, _teacher_token
from tests.test_time_savings_foundation import _add_week_period


def test_active_selection_auto_creates_instructional_week(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Instructional Week Auto Tenant")
    _align_teacher_tenant(db_session, teacher_email="pacing-guide-teacher@example.com")
    scope = _catalog_scope(client, root_token)
    school_year = _school_year(client, root_token)
    guide, period_id = _create_week_guide(client, root_token, scope, school_year)

    active = client.patch(
        "/v1/teacher-assist/pacing-guides/active-selection",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"active_pacing_guide_id": guide["id"]},
    )
    assert active.status_code == 200, active.text

    by_period = client.get(
        f"/v1/teacher-assist/instructional-weeks/by-period/{period_id}",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert by_period.status_code == 200, by_period.text
    week_id = by_period.json()["id"]
    assert by_period.json()["status"] == "DRAFT"

    home = client.get("/v1/teacher-assist/home", headers={"Authorization": f"Bearer {teacher_token}"})
    assert home.status_code == 200, home.text
    home_payload = home.json()
    assert home_payload["instructional_week_id"] == week_id
    assert home_payload["continue_planning"]["instructional_week_href"] == f"/teacher-assist/week/{week_id}"

    workspace = client.get(
        "/v1/teacher-assist/pacing-guide-workspace",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert workspace.status_code == 200, workspace.text
    assert workspace.json()["instructional_week"]["instructional_week_id"] == week_id


def test_generated_artifact_links_instructional_week(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Instructional Week Artifact Tenant")
    _align_teacher_tenant(db_session, teacher_email="pacing-guide-teacher@example.com")
    scope = _catalog_scope(client, root_token)
    school_year = _school_year(client, root_token)
    guide, period_id = _create_week_guide(client, root_token, scope, school_year)

    created = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/instructional-weeks",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"status": "ACTIVE"},
    )
    assert created.status_code == 201, created.text
    week_id = created.json()["id"]

    client.patch(
        "/v1/teacher-assist/pacing-guides/active-selection",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"active_pacing_guide_id": guide["id"]},
    )

    school_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"name": "Instructional Week Class", "grade_level": "5", "student_count": 20, "school_year_id": school_year["id"]},
    )
    assert school_class.status_code == 201, school_class.text

    subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"code": "MATH", "name": "Math"},
    )
    assert subject.status_code == 201, subject.text

    client.post(
        f"/v1/teacher-assist/classes/{school_class.json()['id']}/subjects",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"subject_id": subject.json()["id"]},
    )

    generated = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/generate",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"artifact_type": "ASSIGNMENT", "class_id": school_class.json()["id"]},
    )
    assert generated.status_code == 201, generated.text
    assert generated.json()["instructional_week_id"] == week_id

    artifacts = client.get(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/artifacts",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert artifacts.status_code == 200, artifacts.text
    assert artifacts.json()[0]["instructional_week_id"] == week_id


def test_instructional_week_create_workspace_snapshot_and_next_week(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Instructional Week Tenant")
    _align_teacher_tenant(db_session, teacher_email="pacing-guide-teacher@example.com")
    scope = _catalog_scope(client, root_token)
    school_year = _school_year(client, root_token)
    guide, period_id = _create_week_guide(client, root_token, scope, school_year)
    next_period_id = _add_week_period(client, root_token, guide["id"], title="Week 2", sequence_number=2)

    preview = client.get(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/instructional-week-preview",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["requires_review"] is True

    created = client.post(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/instructional-weeks",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"status": "ACTIVE"},
    )
    assert created.status_code == 201, created.text
    week_id = created.json()["id"]

    by_period = client.get(
        f"/v1/teacher-assist/instructional-weeks/by-period/{period_id}",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert by_period.status_code == 200, by_period.text
    assert by_period.json()["id"] == week_id

    workspace = client.get(
        f"/v1/teacher-assist/instructional-weeks/{week_id}/workspace",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert workspace.status_code == 200, workspace.text
    payload = workspace.json()
    assert payload["instructional_week"]["id"] == week_id
    assert "tabs" in payload
    assert "action_center" in payload
    assert "health_indicators" in payload
    assert "overview" in payload["tabs"]

    snapshot = client.post(
        f"/v1/teacher-assist/instructional-weeks/{week_id}/snapshots",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"name": "Week 1 Snapshot"},
    )
    assert snapshot.status_code == 201, snapshot.text

    snapshots = client.get(
        f"/v1/teacher-assist/instructional-weeks/{week_id}/snapshots",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert snapshots.status_code == 200, snapshots.text
    assert len(snapshots.json()) >= 1

    next_week = client.post(
        f"/v1/teacher-assist/instructional-weeks/{week_id}/generate-next-week",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert next_week.status_code == 200, next_week.text
    assert next_week.json()["next_period_id"] == next_period_id
    assert next_week.json()["instructional_week_id"]

    home = client.get("/v1/teacher-assist/home", headers={"Authorization": f"Bearer {teacher_token}"})
    assert home.status_code == 200, home.text
    assert home.json().get("instructional_week_id") == week_id

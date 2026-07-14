from __future__ import annotations

from sqlalchemy.orm import Session

from tests.test_current_week_foundation import _align_teacher_tenant, _create_week_guide
from tests.test_education_catalog import _root_token
from tests.test_pacing_guide_foundation import _catalog_scope, _school_year, _teacher_token


def test_instructional_loop_endpoints(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Instructional Loop Tenant")
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

    evidence = client.post(
        "/v1/teacher-assist/instructional-evidence",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "student_identifier": "12",
            "source_type": "ASSIGNMENT",
            "source_id": period_id,
            "instructional_week_id": week_id,
            "score": 72.5,
            "mastery_level": "developing",
            "teacher_confirmed": True,
        },
    )
    assert evidence.status_code == 201, evidence.text
    evidence_id = evidence.json()["id"]

    confirm = client.post(
        f"/v1/teacher-assist/instructional-evidence/{evidence_id}/confirm",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"mastery_level": "developing", "teacher_notes": "Reviewed in gradebook."},
    )
    assert confirm.status_code == 200, confirm.text
    assert confirm.json()["teacher_confirmed"] is True

    performance = client.get(
        "/v1/teacher-assist/objective-performance",
        headers={"Authorization": f"Bearer {teacher_token}"},
        params={"instructional_week_id": week_id},
    )
    assert performance.status_code == 200, performance.text
    assert "objectives" in performance.json()

    dashboard = client.get(
        "/v1/teacher-assist/mastery-dashboard/v2",
        headers={"Authorization": f"Bearer {teacher_token}"},
        params={"instructional_week_id": week_id},
    )
    assert dashboard.status_code == 200, dashboard.text
    assert "v2" in dashboard.json()

    reteach = client.get(
        "/v1/teacher-assist/reteach-workspace",
        headers={"Authorization": f"Bearer {teacher_token}"},
        params={"instructional_week_id": week_id},
    )
    assert reteach.status_code == 200, reteach.text
    assert "objectives_requiring_reteach" in reteach.json()

    reflection = client.put(
        "/v1/teacher-assist/instructional-reflections",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "instructional_week_id": week_id,
            "what_worked": "Guided practice",
            "what_didnt_work": "Timing",
            "status": "draft",
        },
    )
    assert reflection.status_code == 200, reflection.text

    closure = client.get(
        f"/v1/teacher-assist/instructional-weeks/{week_id}/closure",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert closure.status_code == 200, closure.text
    assert closure.json()["checklist"]["lessons_completed"] is False

    updated = client.patch(
        f"/v1/teacher-assist/instructional-weeks/{week_id}/closure",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "checklist": {
                "lessons_completed": True,
                "assessments_reviewed": True,
                "grades_confirmed": True,
                "mastery_reviewed": True,
                "reteach_identified": True,
                "reflection_completed": True,
            }
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["status"] == "completed"

    summary = client.post(
        f"/v1/teacher-assist/instructional-weeks/{week_id}/summary",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={},
    )
    assert summary.status_code == 201, summary.text
    assert summary.json()["summary"]["week_title"]

    report = client.get(
        "/v1/teacher-assist/instructional-health-report",
        headers={"Authorization": f"Bearer {teacher_token}"},
        params={"instructional_week_id": week_id},
    )
    assert report.status_code == 200, report.text
    assert report.json()["exportable"] is True

    home = client.get(
        "/v1/teacher-assist/home", headers={"Authorization": f"Bearer {teacher_token}"}
    )
    assert home.status_code == 200, home.text
    assert "instructional_loop" in home.json()

    workspace = client.get(
        f"/v1/teacher-assist/instructional-weeks/{week_id}/workspace",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert workspace.status_code == 200, workspace.text
    assert "instructional_loop" in workspace.json()["tabs"]

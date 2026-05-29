from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.user import User
from tests.test_education_catalog import _root_token
from tests.test_pacing_guide_foundation import _catalog_scope, _school_year, _teacher_token
from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user


def _align_teacher_tenant(db_session: Session, *, teacher_email: str) -> None:
    root_user = db_session.scalar(select(User).where(User.email == "catalog-root@example.com"))
    teacher_user = db_session.scalar(select(User).where(User.email == teacher_email))
    root_membership = db_session.scalar(select(TenantMembership).where(TenantMembership.user_id == root_user.id))
    teacher_membership = db_session.scalar(select(TenantMembership).where(TenantMembership.user_id == teacher_user.id))
    assert root_membership is not None and teacher_membership is not None
    teacher_membership.tenant_id = root_membership.tenant_id
    db_session.commit()


def _create_week_guide(client, root_token: str, scope: dict[str, str], school_year: dict) -> tuple[dict, str]:
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=4)
    create = client.post(
        "/v1/teacher-assist/pacing-guides",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "school_year_id": school_year["id"],
            "guide_type": "DISTRICT",
            "title": "Grade 5 Math Current Week Guide",
            "description": "District pacing",
            "catalog_state_id": scope["state_id"],
            "catalog_district_id": scope["district_id"],
            "catalog_school_id": scope["school_id"],
            "catalog_grade_id": scope["grade_id"],
            "catalog_subject_id": scope["subject_id"],
            "is_shared": True,
        },
    )
    assert create.status_code == 201, create.text
    guide = create.json()
    period = client.post(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}/periods",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "period_type": "WEEK",
            "title": "Current Week",
            "description": "Decimal review",
            "sequence_number": 1,
            "start_date": week_start.isoformat(),
            "end_date": week_end.isoformat(),
        },
    )
    assert period.status_code == 201, period.text
    return guide, period.json()["id"]


def test_current_week_active_selection_workspace_and_launch_context(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Current Week Tenant")
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
    active_payload = active.json()
    assert active_payload["has_active_guide"] is True
    assert active_payload["active_pacing_guide_id"] == guide["id"]
    assert active_payload["current_week"]["id"] == period_id

    home = client.get(
        "/v1/teacher-assist/home",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert home.status_code == 200, home.text
    assert home.json()["current_week"]["has_active_guide"] is True
    assert any("period_id=" in action["navigation_href"] for action in home.json()["quick_actions"])

    workspace = client.get(
        "/v1/teacher-assist/pacing-guide-workspace",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert workspace.status_code == 200, workspace.text
    workspace_payload = workspace.json()
    assert workspace_payload["selected_period"]["id"] == period_id
    assert len(workspace_payload["timeline"]) >= 1
    assert workspace_payload["objective_coverage"] is not None

    launch = client.get(
        f"/v1/teacher-assist/pacing-guide-periods/{period_id}/launch-context",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert launch.status_code == 200, launch.text
    launch_payload = launch.json()
    assert launch_payload["pacing_guide_period_id"] == period_id
    assert launch_payload["planning_draft"]["pacing_guide_period_id"] == period_id
    assert launch_payload["assignment"]["title"].endswith("Assignment")

    draft = client.post(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"pacing_guide_period_id": period_id},
    )
    assert draft.status_code == 201, draft.text
    assert draft.json()["title"] == "Current Week"

    note = client.put(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}/periods/{period_id}/notes",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"notes": "Focus on small groups."},
    )
    assert note.status_code == 200, note.text
    assert note.json()["notes"] == "Focus on small groups."

    manual = client.patch(
        "/v1/teacher-assist/pacing-guides/active-selection",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"manual_pacing_period_id": period_id},
    )
    assert manual.status_code == 200, manual.text
    assert manual.json()["manual_pacing_period_id"] == period_id

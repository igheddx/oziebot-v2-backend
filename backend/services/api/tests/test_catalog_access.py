from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_activity_event import TeacherAssistActivityEvent
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.access_seed import ensure_user_teacher_assist_access
from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user


def _root_token(client, db_session: Session) -> str:
    email = "catalog-browse-root@example.com"
    token = _register_user(client, email=email, tenant_name="Catalog Browse Root Tenant")
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    db_session.commit()
    _grant_teacher_assist_access(db_session, email=email)
    return token


def _seed_assignment_context(
    client, root_token: str, teacher_token: str, db_session: Session, teacher_email: str
):
    state = client.post(
        "/v1/teacher-assist/education-catalog/states",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"name": "Browse Texas", "abbreviation": "BTX", "active": True},
    ).json()
    district = client.post(
        "/v1/teacher-assist/education-catalog/districts",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"state_id": state["id"], "name": "Browse ISD", "active": True},
    ).json()
    school = client.post(
        "/v1/teacher-assist/education-catalog/schools",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "district_id": district["id"],
            "name": "Browse Elementary",
            "school_type": "elementary",
            "active": True,
        },
    ).json()
    grade = client.post(
        "/v1/teacher-assist/education-catalog/grades",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"school_id": school["id"], "grade_code": "5", "display_name": "5", "active": True},
    ).json()
    client.post(
        "/v1/teacher-assist/education-catalog/subjects",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "grade_id": grade["id"],
            "subject_code": "Math",
            "display_name": "Math",
            "active": True,
        },
    )
    client.post(
        "/v1/teacher-assist/education-catalog/objectives",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "state_id": state["id"],
            "grade_level": "5",
            "subject_code": "Math",
            "objective_type": "TEKS",
            "objective_id": "BTX.5.MATH.1",
            "description": "Browse objective.",
            "coverage_type": "required",
            "active": True,
        },
    )
    client.post(
        "/v1/teacher-assist/education-catalog/curriculum-resources",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "school_id": school["id"],
            "grade_level": "5",
            "subject_code": "Math",
            "resource_type": "curriculum",
            "title": "Browse Math Guide",
            "description": "Placeholder",
            "active": True,
        },
    )
    teacher_user = db_session.scalar(select(User).where(User.email == teacher_email))
    assert teacher_user is not None
    client.post(
        "/v1/teacher-assist/education-catalog/teacher-assignments",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "user_id": str(teacher_user.id),
            "state_id": state["id"],
            "district_id": district["id"],
            "school_id": school["id"],
            "active": True,
        },
    )
    return state, school


def test_catalog_browse_inherits_assignment_and_blocks_edits(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_email = "catalog-browse-teacher@example.com"
    teacher_token = _register_user(
        client, email=teacher_email, tenant_name="Catalog Browse Teacher Tenant"
    )
    ensure_user_teacher_assist_access(
        db_session,
        email=teacher_email,
        full_name="Catalog Browse Teacher",
        tenant_name="Catalog Browse Teacher Tenant",
    )
    db_session.commit()

    _seed_assignment_context(client, root_token, teacher_token, db_session, teacher_email)

    context = client.get(
        "/v1/teacher-assist/catalog/context",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert context.status_code == 200, context.text
    body = context.json()
    assert body["can_browse"] is True
    assert body["assignment"]["school"]["name"] == "Browse Elementary"

    grades = client.get(
        "/v1/teacher-assist/catalog/grades",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert grades.status_code == 200, grades.text
    assert grades.json()["meta"]["total"] >= 1

    objectives = client.get(
        "/v1/teacher-assist/catalog/objectives",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert objectives.status_code == 200, objectives.text
    assert any(row["objective_id"] == "BTX.5.MATH.1" for row in objectives.json()["items"])

    resources = client.get(
        "/v1/teacher-assist/catalog/resources",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert resources.status_code == 200, resources.text
    assert any(row["title"] == "Browse Math Guide" for row in resources.json()["items"])

    forbidden = client.post(
        "/v1/teacher-assist/education-catalog/states",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"name": "Blocked", "abbreviation": "BLK", "active": True},
    )
    assert forbidden.status_code == 403


def test_catalog_browse_missing_assignment_is_audited(client, db_session: Session):
    teacher_email = "catalog-browse-unassigned@example.com"
    teacher_token = _register_user(
        client, email=teacher_email, tenant_name="Catalog Browse Unassigned Tenant"
    )
    ensure_user_teacher_assist_access(
        db_session,
        email=teacher_email,
        full_name="Catalog Browse Unassigned",
        tenant_name="Catalog Browse Unassigned Tenant",
    )
    db_session.commit()

    context = client.get(
        "/v1/teacher-assist/catalog/context",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert context.status_code == 200, context.text
    assert context.json()["can_browse"] is False
    assert context.json()["missing_assignment"] is True

    grades = client.get(
        "/v1/teacher-assist/catalog/grades",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert grades.status_code == 403

    teacher_user = db_session.scalar(select(User).where(User.email == teacher_email))
    assert teacher_user is not None
    events = db_session.scalars(
        select(TeacherAssistActivityEvent).where(
            TeacherAssistActivityEvent.user_id == teacher_user.id,
            TeacherAssistActivityEvent.event_type.in_(
                ("catalog_assignment_missing", "catalog_access_failed")
            ),
        )
    ).all()
    assert len(events) >= 1


def test_root_admin_can_browse_unscoped_catalog(client, db_session: Session):
    root_token = _root_token(client, db_session)
    context = client.get(
        "/v1/teacher-assist/catalog/context",
        headers={"Authorization": f"Bearer {root_token}"},
    )
    assert context.status_code == 200, context.text
    assert context.json()["can_browse"] is True
    assert context.json()["is_root_unscoped"] is True

    grades = client.get(
        "/v1/teacher-assist/catalog/grades",
        headers={"Authorization": f"Bearer {root_token}"},
    )
    assert grades.status_code == 200, grades.text
    assert "items" in grades.json()
    assert "meta" in grades.json()

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.access_seed import ensure_user_teacher_assist_access
from tests.test_education_catalog import _root_token
from tests.test_teacher_assist_setup import _register_user


def _seed_catalog(client, root_token: str) -> dict[str, str]:
    state = client.post(
        "/v1/teacher-assist/education-catalog/states",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"name": "Texas", "abbreviation": "TX", "active": True},
    )
    assert state.status_code == 201, state.text
    state_id = state.json()["id"]
    district = client.post(
        "/v1/teacher-assist/education-catalog/districts",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"state_id": state_id, "name": "Leander Independent School District", "active": True},
    ).json()
    school = client.post(
        "/v1/teacher-assist/education-catalog/schools",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"district_id": district["id"], "name": "Mason Elementary", "school_type": "elementary", "active": True},
    ).json()
    grade = client.post(
        "/v1/teacher-assist/education-catalog/grades",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"school_id": school["id"], "grade_code": "5", "display_name": "Grade 5", "active": True},
    ).json()
    client.post(
        "/v1/teacher-assist/education-catalog/subjects",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"grade_id": grade["id"], "subject_code": "Math", "display_name": "Math", "active": True},
    )
    return {
        "state_id": state_id,
        "district_id": district["id"],
        "school_id": school["id"],
        "grade_id": grade["id"],
    }


def test_available_teachers_excludes_assigned_school(client, db_session: Session):
    root_token = _root_token(client, db_session)
    catalog = _seed_catalog(client, root_token)

    assigned_email = "assigned-teacher@example.com"
    unassigned_email = "unassigned-teacher@example.com"
    _register_user(client, email=assigned_email, tenant_name="Assigned Tenant")
    _register_user(client, email=unassigned_email, tenant_name="Unassigned Tenant")
    ensure_user_teacher_assist_access(
        db_session,
        email=assigned_email,
        full_name="Assigned Teacher",
        tenant_name="Assigned Tenant",
        password="password-123",
    )
    ensure_user_teacher_assist_access(
        db_session,
        email=unassigned_email,
        full_name="Unassigned Teacher",
        tenant_name="Unassigned Tenant",
        password="password-123",
    )
    db_session.commit()

    assigned_user = db_session.scalar(select(User).where(User.email == assigned_email))
    assert assigned_user is not None
    create = client.post(
        "/v1/teacher-assist/education-catalog/teacher-assignments/provision",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "user_id": str(assigned_user.id),
            "state_id": catalog["state_id"],
            "district_id": catalog["district_id"],
            "school_id": catalog["school_id"],
            "active": True,
        },
    )
    assert create.status_code == 201, create.text

    available = client.get(
        "/v1/teacher-assist/education-catalog/teacher-assignments/available-teachers",
        headers={"Authorization": f"Bearer {root_token}"},
        params={"school_id": catalog["school_id"], "q": "teacher"},
    )
    assert available.status_code == 200, available.text
    emails = {row["email"] for row in available.json()}
    assert assigned_email not in emails
    assert unassigned_email in emails


def test_provision_new_teacher_generates_temporary_password(client, db_session: Session):
    root_token = _root_token(client, db_session)
    catalog = _seed_catalog(client, root_token)

    response = client.post(
        "/v1/teacher-assist/education-catalog/teacher-assignments/provision",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "email": "new-mason-teacher@example.com",
            "full_name": "New Mason Teacher",
            "state_id": catalog["state_id"],
            "district_id": catalog["district_id"],
            "school_id": catalog["school_id"],
            "catalog_grade_id": catalog["grade_id"],
            "active": True,
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["created_user"] is True
    assert payload["temporary_password"]
    assert payload["grade_setup_applied"] is True
    assert payload["assignment"]["school_id"] == catalog["school_id"]

    user = db_session.scalar(select(User).where(User.email == "new-mason-teacher@example.com"))
    assert user is not None
    assert user.full_name == "New Mason Teacher"

    login = client.post(
        "/v1/auth/login",
        json={"email": "new-mason-teacher@example.com", "password": payload["temporary_password"]},
    )
    assert login.status_code == 200, login.text
    teacher_token = login.json()["access_token"]

    setup = client.get(
        "/v1/teacher-assist/education-catalog/my-school-setup",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert setup.status_code == 200, setup.text
    assert setup.json()["catalog_grade_code"] == "5"
    assert setup.json()["synced_subjects"]

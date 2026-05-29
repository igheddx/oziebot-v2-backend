from __future__ import annotations

from sqlalchemy.orm import Session

from oziebot_api.services.teacher_assist.access_seed import ensure_user_teacher_assist_access
from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user


def _root_token(client, db_session: Session) -> str:
    email = "catalog-root@example.com"
    token = _register_user(client, email=email, tenant_name="Catalog Root Tenant")
    from sqlalchemy import select

    from oziebot_api.models.user import User

    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    db_session.commit()
    _grant_teacher_assist_access(db_session, email=email)
    return token


def test_education_catalog_root_admin_crud_and_teacher_read_only(client, db_session: Session):
    root_token = _root_token(client, db_session)

    teacher_email = "catalog-teacher@example.com"
    teacher_token = _register_user(client, email=teacher_email, tenant_name="Catalog Teacher Tenant")
    ensure_user_teacher_assist_access(
        db_session,
        email=teacher_email,
        full_name="Catalog Teacher",
        tenant_name="Catalog Teacher Tenant",
    )
    db_session.commit()

    state = client.post(
        "/v1/teacher-assist/education-catalog/states",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"name": "Texas", "abbreviation": "TX", "active": True},
    )
    assert state.status_code == 201, state.text
    state_id = state.json()["id"]

    forbidden = client.post(
        "/v1/teacher-assist/education-catalog/states",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"name": "Florida", "abbreviation": "FL", "active": True},
    )
    assert forbidden.status_code == 403

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
        json={"school_id": school["id"], "grade_code": "5", "display_name": "5", "active": True},
    ).json()
    client.post(
        "/v1/teacher-assist/education-catalog/subjects",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"grade_id": grade["id"], "subject_code": "Math", "display_name": "Math", "active": True},
    )
    objective = client.post(
        "/v1/teacher-assist/education-catalog/objectives",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "state_id": state_id,
            "grade_level": "5",
            "subject_code": "Math",
            "objective_type": "TEKS",
            "objective_id": "5.MATH.1",
            "description": "Students perform operations with decimals.",
            "coverage_type": "required",
            "active": True,
        },
    )
    assert objective.status_code == 201, objective.text

    from sqlalchemy import select

    from oziebot_api.models.user import User

    teacher_user = db_session.scalar(select(User).where(User.email == teacher_email))
    assert teacher_user is not None
    assignment = client.post(
        "/v1/teacher-assist/education-catalog/teacher-assignments",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "user_id": str(teacher_user.id),
            "state_id": state_id,
            "district_id": district["id"],
            "school_id": school["id"],
            "active": True,
        },
    )
    assert assignment.status_code == 201, assignment.text

    teacher_states = client.get(
        "/v1/teacher-assist/education-catalog/states",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert teacher_states.status_code == 200, teacher_states.text
    assert teacher_states.json()[0]["abbreviation"] == "TX"

    context = client.get(
        "/v1/teacher-assist/education-catalog/my-context",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert context.status_code == 200, context.text
    assert context.json()["assignment"]["school"]["name"] == "Mason Elementary"
    assert any(row["objective_id"] == "5.MATH.1" for row in context.json()["objectives"])

    csv_content = "\n".join(
        [
            "state_abbreviation,grade_level,subject_code,objective_type,objective_id,description,coverage_type",
            "TX,5,Math,TEKS,5.MATH.2,Students solve multi-step mathematical problems.,required",
        ]
    )
    preview = client.post(
        "/v1/teacher-assist/education-catalog/objectives/import/preview",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"csv_content": csv_content},
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["valid_count"] == 1

    commit = client.post(
        "/v1/teacher-assist/education-catalog/objectives/import/commit",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "rows": [
                {
                    "state_abbreviation": "TX",
                    "grade_level": "5",
                    "subject_code": "Math",
                    "objective_type": "TEKS",
                    "objective_id": "5.MATH.2",
                    "description": "Students solve multi-step mathematical problems.",
                    "coverage_type": "required",
                }
            ]
        },
    )
    assert commit.status_code == 200, commit.text
    assert commit.json()["created_count"] == 1

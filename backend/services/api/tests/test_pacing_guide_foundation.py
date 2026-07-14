from __future__ import annotations

from sqlalchemy.orm import Session

from sqlalchemy import select

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.user import User
from oziebot_api.scripts.seed_pacing_guides import seed_pacing_guides
from tests.test_education_catalog import _root_token
from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user


def _teacher_token(client, db_session: Session, *, tenant_name: str) -> str:
    email = "pacing-guide-teacher@example.com"
    token = _register_user(client, email=email, tenant_name=tenant_name)
    _grant_teacher_assist_access(db_session, email=email)
    return token


def _catalog_scope(client, root_token: str) -> dict[str, str]:
    state = client.post(
        "/v1/teacher-assist/education-catalog/states",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"name": "Texas", "abbreviation": "TX", "active": True},
    ).json()
    district = client.post(
        "/v1/teacher-assist/education-catalog/districts",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "state_id": state["id"],
            "name": "Leander Independent School District",
            "active": True,
        },
    ).json()
    school = client.post(
        "/v1/teacher-assist/education-catalog/schools",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "district_id": district["id"],
            "name": "Mason Elementary",
            "school_type": "elementary",
            "active": True,
        },
    ).json()
    grade = client.post(
        "/v1/teacher-assist/education-catalog/grades",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"school_id": school["id"], "grade_code": "5", "display_name": "5", "active": True},
    ).json()
    subject = client.post(
        "/v1/teacher-assist/education-catalog/subjects",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "grade_id": grade["id"],
            "subject_code": "Math",
            "display_name": "Math",
            "active": True,
        },
    ).json()
    objective = client.post(
        "/v1/teacher-assist/education-catalog/objectives",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "state_id": state["id"],
            "grade_level": "5",
            "subject_code": "Math",
            "objective_type": "TEKS",
            "objective_id": "5.MATH.1",
            "description": "Students perform operations with decimals.",
            "coverage_type": "required",
            "active": True,
        },
    ).json()
    resource = client.post(
        "/v1/teacher-assist/education-catalog/curriculum-resources",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "state_id": state["id"],
            "district_id": district["id"],
            "school_id": school["id"],
            "grade_level": "5",
            "subject_code": "Math",
            "resource_type": "curriculum",
            "title": "5th Grade Math Curriculum Guide",
            "description": "Curriculum guide",
            "storage_key": "education-catalog/placeholders/math-curriculum.pdf",
            "active": True,
        },
    ).json()
    return {
        "state_id": state["id"],
        "district_id": district["id"],
        "school_id": school["id"],
        "grade_id": grade["id"],
        "subject_id": subject["id"],
        "objective_id": objective["id"],
        "resource_id": resource["id"],
    }


def _school_year(client, token: str) -> dict:
    response = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_catalog_pacing_guide_crud_periods_copy_and_rollover(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _teacher_token(client, db_session, tenant_name="Teacher Copy Tenant")
    root_user = db_session.scalar(select(User).where(User.email == "catalog-root@example.com"))
    teacher_user = db_session.scalar(
        select(User).where(User.email == "pacing-guide-teacher@example.com")
    )
    root_membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == root_user.id)
    )
    teacher_membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == teacher_user.id)
    )
    assert root_membership is not None and teacher_membership is not None
    teacher_membership.tenant_id = root_membership.tenant_id
    db_session.commit()
    scope = _catalog_scope(client, root_token)
    school_year = _school_year(client, root_token)
    target_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "title": "2027-2028",
            "start_date": "2027-08-09",
            "end_date": "2028-05-26",
            "is_active": False,
        },
    ).json()

    create = client.post(
        "/v1/teacher-assist/pacing-guides",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "school_year_id": school_year["id"],
            "guide_type": "DISTRICT",
            "title": "Grade 5 Math District Guide",
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
    assert guide["guide_type"] == "DISTRICT"
    assert guide["period_count"] == 0

    forbidden = client.post(
        "/v1/teacher-assist/pacing-guides",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "school_year_id": school_year["id"],
            "guide_type": "DISTRICT",
            "title": "Should fail",
            "description": None,
            "catalog_state_id": scope["state_id"],
            "catalog_district_id": scope["district_id"],
            "catalog_school_id": scope["school_id"],
            "catalog_grade_id": scope["grade_id"],
            "catalog_subject_id": scope["subject_id"],
        },
    )
    assert forbidden.status_code == 403

    period = client.post(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}/periods",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "period_type": "WEEK",
            "title": "Week 1",
            "description": "Decimal review",
            "sequence_number": 1,
            "start_date": "2026-08-17",
            "end_date": "2026-08-21",
        },
    )
    assert period.status_code == 201, period.text
    period_id = period.json()["id"]

    objective = client.post(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}/periods/{period_id}/objectives",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "objective_id": scope["objective_id"],
            "is_required": True,
            "notes": "Required focus",
        },
    )
    assert objective.status_code == 201, objective.text

    resource = client.post(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}/periods/{period_id}/resources",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "catalog_resource_id": scope["resource_id"],
            "is_primary": True,
            "notes": "Primary guide",
        },
    )
    assert resource.status_code == 201, resource.text

    detail = client.get(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert detail.status_code == 200, detail.text
    detail_payload = detail.json()
    assert detail_payload["period_count"] == 1
    assert detail_payload["periods"][0]["objectives"][0]["objective_code"] == "5.MATH.1"
    assert (
        detail_payload["periods"][0]["resources"][0]["resource_title"]
        == "5th Grade Math Curriculum Guide"
    )

    teacher_copy = client.post(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}/copy",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"target_guide_type": "TEACHER", "title": "My Math Guide"},
    )
    assert teacher_copy.status_code == 201, teacher_copy.text
    copied = teacher_copy.json()
    assert copied["guide_type"] == "TEACHER"
    assert copied["period_count"] == 1
    assert copied["periods"][0]["objectives"]

    rollover = client.post(
        "/v1/teacher-assist/pacing-guides/rollover",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "source_school_year_id": school_year["id"],
            "target_school_year_id": target_year["id"],
            "guide_ids": [guide["id"]],
        },
    )
    assert rollover.status_code == 200, rollover.text
    rolled = rollover.json()
    assert len(rolled) == 1
    assert rolled[0]["school_year_id"] == target_year["id"]
    assert rolled[0]["period_count"] == 1

    delete = client.delete(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}",
        headers={"Authorization": f"Bearer {root_token}"},
    )
    assert delete.status_code == 200, delete.text
    assert delete.json()["is_active"] is False


def test_seed_pacing_guides_idempotent(client, db_session: Session):
    _register_user(client, email="pacing-seed@example.com", tenant_name="Pacing Seed Tenant")
    _grant_teacher_assist_access(db_session, email="pacing-seed@example.com")
    db_session.commit()
    first = seed_pacing_guides(db_session)
    db_session.commit()
    second = seed_pacing_guides(db_session)
    assert first["guides"] >= 1
    assert second["guides"] == 0

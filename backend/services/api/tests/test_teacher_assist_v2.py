from __future__ import annotations

from sqlalchemy import select

from oziebot_api.models.education_catalog import EducationSchool, EducationSchoolYear, EducationState
from oziebot_api.models.user import User
from oziebot_api.scripts.seed_teacher_assist_v2 import seed_teacher_assist_v2
from oziebot_api.services.teacher_assist_v2.roles import ensure_v2_root_admin_role
from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user


def test_v2_context_root_admin_landing(client, db_session):
    email = "v2-root@example.com"
    token = _register_user(client, email=email, tenant_name="V2 Root Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()

    response = client.get("/v1/teacher-assist-v2/context", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["has_access"] is True
    assert payload["role"] == "root_admin"
    assert payload["landing_route"] == "/teacher-assist-v2/admin"


def test_v2_context_teacher_onboarding_landing(client, db_session):
    email = "v2-teacher@example.com"
    token = _register_user(client, email=email, tenant_name="V2 Teacher Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    response = client.get("/v1/teacher-assist-v2/context", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["has_access"] is True
    assert payload["role"] == "teacher"
    assert payload["landing_route"] == "/teacher-assist-v2/onboarding"


def test_v2_context_access_denied_without_product(client):
    token = _register_user(client, email="v2-no-access@example.com", tenant_name="No TA Tenant")
    response = client.get("/v1/teacher-assist-v2/context", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 403


def test_v2_archive_state_blocked_with_district(client, db_session):
    email = "v2-archive@example.com"
    token = _register_user(client, email=email, tenant_name="V2 Archive Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()

    seed_teacher_assist_v2(db_session)
    db_session.commit()
    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    assert state is not None

    response = client.post(
        f"/v1/teacher-assist-v2/catalog/states/{state.id}/archive",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "district" in detail["message"].lower() or "district" in str(detail["dependencies"]).lower()


def test_v2_hierarchy_explorer(client, db_session):
    email = "v2-hierarchy@example.com"
    token = _register_user(client, email=email, tenant_name="V2 Hierarchy Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()

    seed_teacher_assist_v2(db_session)
    db_session.commit()

    response = client.get("/v1/teacher-assist-v2/admin/hierarchy", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200, response.text
    tree = response.json()
    assert len(tree) >= 1
    texas = next(row for row in tree if row["abbreviation"] == "TX")
    assert len(texas["districts"]) >= 1
    district = texas["districts"][0]
    assert district["district_code"] == "LISD"
    assert len(district["schools"]) >= 1


def test_v2_grade_requires_school(client, db_session):
    email = "v2-grade-val@example.com"
    token = _register_user(client, email=email, tenant_name="V2 Grade Val Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()

    response = client.post(
        "/v1/teacher-assist-v2/catalog/grades",
        headers={"Authorization": f"Bearer {token}"},
        json={"school_id": None, "grade_code": "5", "display_name": "Grade 5"},
    )
    assert response.status_code == 400


def test_v2_archive_school_blocked_with_grades(client, db_session):
    email = "v2-school-archive@example.com"
    token = _register_user(client, email=email, tenant_name="V2 School Archive Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()

    seed_teacher_assist_v2(db_session)
    db_session.commit()

    from oziebot_api.models.education_catalog import EducationSchool

    school = db_session.scalar(
        select(EducationSchool).where(EducationSchool.name == "Mason Elementary")
    )
    assert school is not None

    response = client.post(
        f"/v1/teacher-assist-v2/catalog/schools/{school.id}/archive",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 409
    assert "grade" in response.json()["detail"]["message"].lower()


def test_v2_root_admin_email_allowlist(db_session):
    assert ensure_v2_root_admin_role(db_session, email="random@example.com") is False


def test_v2_platform_school_year_single_active(client, db_session):
    email = "v2-sy@example.com"
    token = _register_user(client, email=email, tenant_name="V2 SY Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()

    seed_teacher_assist_v2(db_session)
    db_session.commit()
    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    assert state is not None

    from datetime import date
    from oziebot_api.models.education_catalog import EducationSchoolYear

    second = client.post(
        "/v1/teacher-assist-v2/instructional/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "state_id": str(state.id),
            "title": "2027-2028",
            "start_date": "2027-08-01",
            "end_date": "2028-06-30",
            "active": True,
        },
    )
    assert second.status_code == 201, second.text
    active_rows = db_session.scalars(
        select(EducationSchoolYear).where(EducationSchoolYear.active.is_(True))
    ).all()
    assert len(active_rows) == 1
    assert active_rows[0].title == "2027-2028"


def test_v2_objectives_require_hierarchy(client, db_session):
    email = "v2-obj@example.com"
    token = _register_user(client, email=email, tenant_name="V2 Obj Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()

    seed_teacher_assist_v2(db_session)
    db_session.commit()

    school_year = db_session.scalar(
        select(EducationSchoolYear).where(EducationSchoolYear.title == "2026-2027")
    )
    assert school_year is not None

    response = client.get(
        f"/v1/teacher-assist-v2/instructional/objectives?school_year_id={school_year.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert len(response.json()) >= 12


def test_v2_pacing_guides_list(client, db_session):
    email = "v2-pacing@example.com"
    token = _register_user(client, email=email, tenant_name="V2 Pacing Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()

    seed_teacher_assist_v2(db_session)
    db_session.commit()

    response = client.get(
        "/v1/teacher-assist-v2/instructional/pacing-guides",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    guides = response.json()
    assert len(guides) >= 4
    assert all(row["guide_type"] == "DISTRICT" for row in guides)


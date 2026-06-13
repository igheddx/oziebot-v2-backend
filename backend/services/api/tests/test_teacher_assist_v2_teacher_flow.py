from __future__ import annotations

from sqlalchemy import select

from oziebot_api.models.education_catalog import EducationGrade, EducationSchool, EducationSchoolYear, EducationState
from oziebot_api.models.user import User
from oziebot_api.scripts.seed_teacher_assist_v2 import seed_teacher_assist_v2
from oziebot_api.services.teacher_assist.teacher_assignment_provisioning import provision_teacher_school_assignment
from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user


def _root_token(client, db_session):
    email = "v2-flow-root@example.com"
    token = _register_user(client, email=email, tenant_name="V2 Flow Root")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()
    return token


def test_v2_teacher_temp_password_and_onboarding_flow(client, db_session):
    _root_token(client, db_session)
    seed_teacher_assist_v2(db_session)
    db_session.commit()

    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    school = db_session.scalar(select(EducationSchool).where(EducationSchool.name == "Mason Elementary"))
    school_year = db_session.scalar(select(EducationSchoolYear).where(EducationSchoolYear.title == "2026-2027"))
    grade = db_session.scalar(
        select(EducationGrade).where(EducationGrade.school_id == school.id, EducationGrade.grade_code == "5")
    )
    assert state and school and school_year and grade

    result = provision_teacher_school_assignment(
        db_session,
        state_id=state.id,
        district_id=school.district_id,
        school_id=school.id,
        email="v2-new-teacher@example.com",
        full_name="V2 New Teacher",
        catalog_grade_id=grade.id,
    )
    db_session.commit()
    assert result.temporary_password

    teacher = db_session.scalar(select(User).where(User.email == "v2-new-teacher@example.com"))
    assert teacher is not None
    assert teacher.must_change_password is True
    assert teacher.teacher_assist_role == "teacher"

    login = client.post(
        "/v1/auth/login",
        json={"email": "v2-new-teacher@example.com", "password": result.temporary_password},
    )
    assert login.status_code == 200, login.text
    token = login.json()["access_token"]

    context = client.get("/v1/teacher-assist-v2/context", headers={"Authorization": f"Bearer {token}"})
    assert context.status_code == 200, context.text
    assert context.json()["landing_route"] == "/teacher-assist-v2/reset-password"
    assert context.json()["requires_password_change"] is True

    changed = client.post(
        "/v1/auth/change-password",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "current_password": result.temporary_password,
            "new_password": "new-password-123",
            "confirm_password": "new-password-123",
        },
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["landing_route"] == "/teacher-assist-v2/onboarding"

    db_session.refresh(teacher)
    assert teacher.must_change_password is False

    onboarding_form = client.get(
        "/v1/teacher-assist-v2/teacher/onboarding",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert onboarding_form.status_code == 200, onboarding_form.text
    subjects = onboarding_form.json()["subjects"]
    assert len(subjects) >= 1

    saved = client.post(
        "/v1/teacher-assist-v2/teacher/onboarding",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": str(school_year.id),
            "grade_id": str(grade.id),
            "student_count": 24,
            "selected_subject_ids": [subjects[0]["id"]],
        },
    )
    assert saved.status_code == 200, saved.text
    assert saved.json()["landing_route"] == "/teacher-assist-v2/pacing-guide-setup"

    setup_form = client.get(
        "/v1/teacher-assist-v2/teacher/pacing-guide-setup",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert setup_form.status_code == 200, setup_form.text
    subject = setup_form.json()["subjects"][0]
    guide_id = subject["available_guides"][0]["id"]

    setup_saved = client.post(
        "/v1/teacher-assist-v2/teacher/pacing-guide-setup",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "selections": [
                {
                    "subject_id": subject["id"],
                    "source_guide_id": guide_id,
                    "mode": "district",
                }
            ]
        },
    )
    assert setup_saved.status_code == 200, setup_saved.text
    assert setup_saved.json()["landing_route"] == "/teacher-assist-v2/planning"

    resaved = client.post(
        "/v1/teacher-assist-v2/teacher/pacing-guide-setup",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "selections": [
                {
                    "subject_id": subject["id"],
                    "source_guide_id": guide_id,
                    "mode": "district",
                }
            ]
        },
    )
    assert resaved.status_code == 200, resaved.text

    home = client.get("/v1/teacher-assist-v2/teacher/home", headers={"Authorization": f"Bearer {token}"})
    assert home.status_code == 200, home.text
    assert home.json()["ready_to_plan"] is True


def test_v2_admin_provision_teacher(client, db_session):
    root_token = _root_token(client, db_session)
    seed_teacher_assist_v2(db_session)
    db_session.commit()

    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    school = db_session.scalar(select(EducationSchool).where(EducationSchool.name == "Mason Elementary"))
    grade = db_session.scalar(
        select(EducationGrade).where(EducationGrade.school_id == school.id, EducationGrade.grade_code == "5")
    )
    assert state and school and grade

    response = client.post(
        "/v1/teacher-assist-v2/admin/teachers/provision",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "email": "v2-admin-created@example.com",
            "full_name": "Admin Created Teacher",
            "state_id": str(state.id),
            "district_id": str(school.district_id),
            "school_id": str(school.id),
            "catalog_grade_id": str(grade.id),
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    assert payload["temporary_password"]
    assert payload["created_user"] is True

    listed = client.get(
        "/v1/teacher-assist-v2/admin/teachers",
        headers={"Authorization": f"Bearer {root_token}"},
    )
    assert listed.status_code == 200, listed.text
    assert any(row["email"] == "v2-admin-created@example.com" for row in listed.json())

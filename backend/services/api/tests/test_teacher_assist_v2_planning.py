from __future__ import annotations

import io
import uuid

from sqlalchemy import select

from oziebot_api.models.education_catalog import (
    EducationGrade,
    EducationSchool,
    EducationSchoolYear,
    EducationState,
)
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
)
from oziebot_api.scripts.seed_teacher_assist_v2 import seed_teacher_assist_v2
from oziebot_api.services.teacher_assist.teacher_assignment_provisioning import (
    provision_teacher_school_assignment,
)
from tests.test_teacher_assist_v2_supporting_materials import _make_root_admin


def _ready_teacher_token(client, db_session) -> str:
    _make_root_admin(db_session, client, "v2-planning-root@example.com")
    seed_teacher_assist_v2(db_session)
    db_session.commit()

    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    school = db_session.scalar(
        select(EducationSchool).where(EducationSchool.name == "Mason Elementary")
    )
    school_year = db_session.scalar(
        select(EducationSchoolYear).where(EducationSchoolYear.title == "2026-2027")
    )
    grade = db_session.scalar(
        select(EducationGrade).where(
            EducationGrade.school_id == school.id, EducationGrade.grade_code == "5"
        )
    )
    assert state and school and school_year and grade

    result = provision_teacher_school_assignment(
        db_session,
        state_id=state.id,
        district_id=school.district_id,
        school_id=school.id,
        email="v2-planning-teacher@example.com",
        full_name="Planning Teacher",
        catalog_grade_id=grade.id,
    )
    db_session.commit()

    login = client.post(
        "/v1/auth/login",
        json={"email": "v2-planning-teacher@example.com", "password": result.temporary_password},
    )
    token = login.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    client.post(
        "/v1/auth/change-password",
        headers=headers,
        json={
            "current_password": result.temporary_password,
            "new_password": "new-password-123",
            "confirm_password": "new-password-123",
        },
    )
    onboarding = client.get("/v1/teacher-assist-v2/teacher/onboarding", headers=headers)
    subjects = onboarding.json()["subjects"]
    client.post(
        "/v1/teacher-assist-v2/teacher/onboarding",
        headers=headers,
        json={
            "school_year_id": str(school_year.id),
            "grade_id": str(grade.id),
            "student_count": 24,
            "selected_subject_ids": [subject["id"] for subject in subjects[:4]],
        },
    )
    setup = client.get("/v1/teacher-assist-v2/teacher/pacing-guide-setup", headers=headers)
    selections = [
        {
            "subject_id": subject["id"],
            "source_guide_id": subject["available_guides"][0]["id"],
            "mode": "teacher_copy",
        }
        for subject in setup.json()["subjects"]
        if subject["available_guides"]
    ]
    client.post(
        "/v1/teacher-assist-v2/teacher/pacing-guide-setup",
        headers=headers,
        json={"selections": selections},
    )
    db_session.commit()
    return token


def test_v2_teacher_planning_form_and_generate(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}

    context = client.get("/v1/teacher-assist-v2/context", headers=headers)
    assert context.status_code == 200, context.text
    assert context.json()["landing_route"] == "/teacher-assist-v2/today"

    form = client.get("/v1/teacher-assist-v2/teacher/planning/form", headers=headers)
    assert form.status_code == 200, form.text
    form_payload = form.json()
    assert form_payload["school_year"]["title"]
    assert len(form_payload["subjects"]) >= 1
    teaching_order = form_payload["default_teaching_order"]

    review = client.get(
        "/v1/teacher-assist-v2/teacher/planning/review?week_start=1&week_end=1",
        headers=headers,
    )
    assert review.status_code == 200, review.text
    assert review.json()["weeks"]

    upload = client.post(
        "/v1/teacher-assist-v2/teacher/planning/supplemental-materials/upload",
        headers=headers,
        files={"file": ("supplement.txt", io.BytesIO(b"Teacher supplemental"), "text/plain")},
        data={
            "week_start": "1",
            "week_end": "1",
            "title": "My supplement",
            "resource_type": "worksheet",
        },
    )
    assert upload.status_code == 201, upload.text

    package = client.post(
        "/v1/teacher-assist-v2/teacher/planning/packages/generate",
        headers=headers,
        json={
            "week_start": 1,
            "week_end": 1,
            "teaching_order": teaching_order,
            "selected_outputs": [
                "daily_lesson_plan",
                "subject_slide_deck",
                "quiz",
                "parent_newsletter_summary",
            ],
        },
    )
    assert package.status_code == 201, package.text
    package_payload = package.json()
    assert package_payload["artifact_groups"]["daily_teaching_plans"]
    assert package_payload["artifact_groups"]["subject_slide_decks"]
    assert package_payload["teaching_mode_available"] is True
    assert package_payload["teaching_presentations"]["daily_plans"]
    assert package_payload["teaching_presentations"]["subject_decks"]
    daily_plan = package_payload["artifact_groups"]["daily_teaching_plans"][0]
    assert daily_plan["content_json"]["subjects"]
    slide_deck = package_payload["artifact_groups"]["subject_slide_decks"][0]
    assert slide_deck["content_json"]["slides"]

    row = db_session.scalar(
        select(TeacherAssistV2InstructionalPackage).where(
            TeacherAssistV2InstructionalPackage.id == uuid.UUID(package_payload["id"])
        )
    )
    assert row is not None
    assert row.week_start == 1


def test_v2_planning_supplemental_materials_scoped_to_current_session(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}

    form = client.get("/v1/teacher-assist-v2/teacher/planning/form", headers=headers)
    teaching_order = form.json()["default_teaching_order"]

    upload = client.post(
        "/v1/teacher-assist-v2/teacher/planning/supplemental-materials/upload",
        headers=headers,
        files={"file": ("prior-plan.txt", io.BytesIO(b"Prior plan supplement"), "text/plain")},
        data={
            "week_start": "1",
            "week_end": "1",
            "title": "Prior plan file",
            "resource_type": "worksheet",
        },
    )
    assert upload.status_code == 201, upload.text
    prior_material_id = upload.json()["id"]

    link = client.post(
        "/v1/teacher-assist-v2/teacher/planning/supplemental-materials/links",
        headers=headers,
        json={
            "week_start": 1,
            "week_end": 1,
            "title": "Prior plan link",
            "external_url": "https://example.com/prior-plan",
            "resource_type": "link",
        },
    )
    assert link.status_code == 201, link.text

    package = client.post(
        "/v1/teacher-assist-v2/teacher/planning/packages/generate",
        headers=headers,
        json={
            "week_start": 1,
            "week_end": 1,
            "teaching_order": teaching_order,
            "selected_outputs": ["daily_lesson_plan"],
        },
    )
    assert package.status_code == 201, package.text
    package_id = package.json()["id"]

    package_detail = client.get(
        f"/v1/teacher-assist-v2/teacher/packages/{package_id}",
        headers=headers,
    )
    assert package_detail.status_code == 200, package_detail.text
    linked_titles = {
        item["title"] for item in package_detail.json()["teacher_supplemental_materials"]
    }
    assert linked_titles == {"Prior plan file", "Prior plan link"}

    review = client.get(
        "/v1/teacher-assist-v2/teacher/planning/review?week_start=1&week_end=1",
        headers=headers,
    )
    assert review.status_code == 200, review.text
    assert review.json()["teacher_supplemental_materials"] == []

    supplemental = client.get(
        "/v1/teacher-assist-v2/teacher/planning/supplemental-materials?week_start=1&week_end=1",
        headers=headers,
    )
    assert supplemental.status_code == 200, supplemental.text
    assert supplemental.json() == []

    new_upload = client.post(
        "/v1/teacher-assist-v2/teacher/planning/supplemental-materials/upload",
        headers=headers,
        files={"file": ("new-plan.txt", io.BytesIO(b"New plan supplement"), "text/plain")},
        data={
            "week_start": "1",
            "week_end": "1",
            "title": "New plan file",
            "resource_type": "worksheet",
        },
    )
    assert new_upload.status_code == 201, new_upload.text
    assert new_upload.json()["id"] != prior_material_id


def _teacher_onboarded_without_pacing_setup(client, db_session) -> tuple[str, dict[str, str]]:
    _make_root_admin(db_session, client, "v2-pacing-no-option-root@example.com")
    seed_teacher_assist_v2(db_session)
    db_session.commit()

    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    school = db_session.scalar(
        select(EducationSchool).where(EducationSchool.name == "Mason Elementary")
    )
    school_year = db_session.scalar(
        select(EducationSchoolYear).where(EducationSchoolYear.title == "2026-2027")
    )
    grade = db_session.scalar(
        select(EducationGrade).where(
            EducationGrade.school_id == school.id, EducationGrade.grade_code == "5"
        )
    )
    assert state and school and school_year and grade

    result = provision_teacher_school_assignment(
        db_session,
        state_id=state.id,
        district_id=school.district_id,
        school_id=school.id,
        email="v2-pacing-no-option@example.com",
        full_name="Pacing No Option Teacher",
        catalog_grade_id=grade.id,
    )
    db_session.commit()

    login = client.post(
        "/v1/auth/login",
        json={"email": "v2-pacing-no-option@example.com", "password": result.temporary_password},
    )
    token = login.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    client.post(
        "/v1/auth/change-password",
        headers=headers,
        json={
            "current_password": result.temporary_password,
            "new_password": "new-password-123",
            "confirm_password": "new-password-123",
        },
    )
    onboarding = client.get("/v1/teacher-assist-v2/teacher/onboarding", headers=headers)
    subjects = onboarding.json()["subjects"]
    assert len(subjects) >= 2
    client.post(
        "/v1/teacher-assist-v2/teacher/onboarding",
        headers=headers,
        json={
            "school_year_id": str(school_year.id),
            "grade_id": str(grade.id),
            "student_count": 24,
            "selected_subject_ids": [subject["id"] for subject in subjects[:2]],
        },
    )
    return token, headers


def test_v2_pacing_guide_setup_allows_no_option_and_excludes_from_planning(client, db_session):
    token, headers = _teacher_onboarded_without_pacing_setup(client, db_session)

    setup = client.get("/v1/teacher-assist-v2/teacher/pacing-guide-setup", headers=headers)
    assert setup.status_code == 200, setup.text
    subjects = setup.json()["subjects"]
    assert len(subjects) >= 2
    first, second = subjects[0], subjects[1]
    assert first["available_guides"]
    assert second["available_guides"]

    empty_save = client.post(
        "/v1/teacher-assist-v2/teacher/pacing-guide-setup",
        headers=headers,
        json={"selections": []},
    )
    assert empty_save.status_code == 400, empty_save.text

    partial_save = client.post(
        "/v1/teacher-assist-v2/teacher/pacing-guide-setup",
        headers=headers,
        json={
            "selections": [
                {
                    "subject_id": first["id"],
                    "source_guide_id": first["available_guides"][0]["id"],
                    "mode": "district",
                }
            ]
        },
    )
    assert partial_save.status_code == 200, partial_save.text

    form = client.get("/v1/teacher-assist-v2/teacher/planning/form", headers=headers)
    assert form.status_code == 200, form.text
    subject_ids = {row["subject_id"] for row in form.json()["subjects"]}
    assert subject_ids == {first["id"]}

    swapped_save = client.post(
        "/v1/teacher-assist-v2/teacher/pacing-guide-setup",
        headers=headers,
        json={
            "selections": [
                {
                    "subject_id": second["id"],
                    "source_guide_id": second["available_guides"][0]["id"],
                    "mode": "district",
                }
            ]
        },
    )
    assert swapped_save.status_code == 200, swapped_save.text

    form = client.get("/v1/teacher-assist-v2/teacher/planning/form", headers=headers)
    assert form.status_code == 200, form.text
    subject_ids = {row["subject_id"] for row in form.json()["subjects"]}
    assert subject_ids == {second["id"]}


def test_v2_teacher_planning_locked_before_setup(client, db_session):
    from tests.test_teacher_assist_v2_supporting_materials import _make_teacher

    token = _make_teacher(db_session, client, "v2-planning-locked@example.com")
    headers = {"Authorization": f"Bearer {token}"}
    response = client.get("/v1/teacher-assist-v2/teacher/planning/form", headers=headers)
    assert response.status_code == 403

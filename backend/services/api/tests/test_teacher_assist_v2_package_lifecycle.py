from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta

from sqlalchemy import select

from oziebot_api.models.education_catalog import EducationGrade, EducationSchool, EducationSchoolYear, EducationState
from oziebot_api.models.teacher_assist_v2_instructional_package import TeacherAssistV2InstructionalPackage
from oziebot_api.models.user import User
from oziebot_api.scripts.seed_teacher_assist_v2 import seed_teacher_assist_v2
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user
from oziebot_api.services.teacher_assist.teacher_assignment_provisioning import provision_teacher_school_assignment
from oziebot_api.services.teacher_assist_v2.package_dashboard import (
    build_package_dashboard,
    close_out_instructional_package,
    list_teacher_instructional_packages,
    serialize_package_summary,
)
from oziebot_api.services.teacher_assist_v2.package_lifecycle import (
    ENDING_SOON_DAYS,
    can_close_out_package,
    package_status_message,
    resolve_effective_package_status,
)
from tests.test_teacher_assist_v2_supporting_materials import _make_root_admin


def _planning_ready_teacher(db_session, client) -> tuple[User, uuid.UUID]:
    _make_root_admin(db_session, client, "v2-lifecycle-root@example.com")
    seed_teacher_assist_v2(db_session)
    db_session.commit()

    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    school = db_session.scalar(select(EducationSchool).where(EducationSchool.name == "Mason Elementary"))
    school_year = db_session.scalar(select(EducationSchoolYear).where(EducationSchoolYear.title == "2026-2027"))
    grade = db_session.scalar(
        select(EducationGrade).where(EducationGrade.school_id == school.id, EducationGrade.grade_code == "5")
    )
    assert state and school and school_year and grade

    provision_teacher_school_assignment(
        db_session,
        state_id=state.id,
        district_id=school.district_id,
        school_id=school.id,
        email="v2-lifecycle-teacher@example.com",
        full_name="Lifecycle Teacher",
        catalog_grade_id=grade.id,
    )
    user = db_session.scalar(select(User).where(User.email == "v2-lifecycle-teacher@example.com"))
    assert user is not None
    tenant_id = teacher_assist_context_for_user(db_session, user).tenant_id

    from oziebot_api.models.teacher_assist_v2_onboarding import TeacherAssistV2Onboarding

    onboarding = TeacherAssistV2Onboarding(
        id=uuid.uuid4(),
        user_id=user.id,
        tenant_id=tenant_id,
        state_id=state.id,
        district_id=school.district_id,
        school_id=school.id,
        school_year_id=school_year.id,
        grade_id=grade.id,
        selected_subject_ids=[str(grade.id)],
        student_count=24,
        onboarding_completed_at=datetime.now(UTC),
        pacing_guide_setup_completed_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    db_session.add(onboarding)
    db_session.commit()
    return user, tenant_id


def _insert_package(
    db_session,
    *,
    user: User,
    tenant_id: uuid.UUID,
    week_start: int,
    week_end: int,
    plan_start: date,
    plan_end: date,
    status: str = "active",
) -> TeacherAssistV2InstructionalPackage:
    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    school = db_session.scalar(select(EducationSchool).where(EducationSchool.name == "Mason Elementary"))
    school_year = db_session.scalar(select(EducationSchoolYear).where(EducationSchoolYear.title == "2026-2027"))
    grade = db_session.scalar(
        select(EducationGrade).where(EducationGrade.school_id == school.id, EducationGrade.grade_code == "5")
    )
    assert state and school and school_year and grade
    now = datetime.now(UTC)
    row = TeacherAssistV2InstructionalPackage(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        teacher_user_id=user.id,
        platform_school_year_id=school_year.id,
        catalog_state_id=state.id,
        catalog_district_id=school.district_id,
        catalog_school_id=school.id,
        catalog_grade_id=grade.id,
        subject_ids_json=[],
        pacing_guide_ids_json=[],
        title=f"Week {week_start} Instructional Package",
        week_start=week_start,
        week_end=week_end,
        plan_start_date=plan_start,
        plan_end_date=plan_end,
        teaching_order_json=[],
        selected_outputs_json=["daily_lesson_plan", "subject_slide_deck"],
        status=status,
        created_at=now,
        updated_at=now,
    )
    db_session.add(row)
    db_session.flush()
    return row


def test_package_lifecycle_status_rules():
    today = date(2026, 5, 28)
    start = today - timedelta(days=7)
    end = today + timedelta(days=2)

    assert resolve_effective_package_status(
        stored_status="active",
        plan_start_date=start,
        plan_end_date=end,
        today=today,
    ) == "ending_soon"
    assert package_status_message("ending_soon") == (
        "This plan is ending soon. Review and close it out when teaching is complete."
    )

    expired_end = today - timedelta(days=1)
    assert resolve_effective_package_status(
        stored_status="active",
        plan_start_date=start,
        plan_end_date=expired_end,
        today=today,
    ) == "expired"
    assert package_status_message("expired") == (
        "This plan has passed its end date. Review it and mark it done when complete."
    )

    assert resolve_effective_package_status(
        stored_status="completed",
        plan_start_date=start,
        plan_end_date=expired_end,
        today=today,
    ) == "completed"

    future_start = today + timedelta(days=5)
    future_end = today + timedelta(days=12)
    assert resolve_effective_package_status(
        stored_status="generated",
        plan_start_date=future_start,
        plan_end_date=future_end,
        today=today,
    ) == "generated"

    assert ENDING_SOON_DAYS == 3
    assert can_close_out_package(stored_status="active", effective_status="expired") is True
    assert can_close_out_package(stored_status="completed", effective_status="completed") is False


def test_package_dashboard_close_out_and_filters(db_session, client):
    user, tenant_id = _planning_ready_teacher(db_session, client)
    today = date.today()

    ending_row = _insert_package(
        db_session,
        user=user,
        tenant_id=tenant_id,
        week_start=1,
        week_end=1,
        plan_start=today - timedelta(days=5),
        plan_end=today + timedelta(days=2),
    )
    expired_row = _insert_package(
        db_session,
        user=user,
        tenant_id=tenant_id,
        week_start=2,
        week_end=2,
        plan_start=today - timedelta(days=14),
        plan_end=today - timedelta(days=1),
    )
    db_session.commit()

    ending_summary = serialize_package_summary(ending_row, subject_names=["Math"], today=today)
    assert ending_summary["status"] == "ending_soon"

    expired_summary = serialize_package_summary(expired_row, subject_names=["ELA"], today=today)
    assert expired_summary["status"] == "expired"

    dashboard = build_package_dashboard(db_session, user=user)
    assert any(row["id"] == str(ending_row.id) for row in dashboard["ending_soon_packages"])
    assert any(row["id"] == str(expired_row.id) for row in dashboard["expired_packages"])

    expired_only = list_teacher_instructional_packages(db_session, user=user, status="expired")
    assert any(row["id"] == str(expired_row.id) for row in expired_only)

    close_out_instructional_package(
        db_session,
        user=user,
        package_id=expired_row.id,
        close_out_notes="Teaching finished for week 2.",
    )
    db_session.commit()

    closed_summary = serialize_package_summary(expired_row, subject_names=["ELA"], today=today)
    assert closed_summary["status"] == "completed"
    assert expired_row.close_out_notes == "Teaching finished for week 2."

    expired_after = list_teacher_instructional_packages(db_session, user=user, status="expired")
    assert all(row["id"] != str(expired_row.id) for row in expired_after)

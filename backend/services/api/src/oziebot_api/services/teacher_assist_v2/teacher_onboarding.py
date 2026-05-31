"""TeacherAssist v2 teacher onboarding workflow."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationGrade, EducationSchoolYear, EducationSubject
from oziebot_api.models.teacher_assist_v2_onboarding import TeacherAssistV2Onboarding
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.education_catalog import (
    get_active_teacher_assignment,
    get_grade_or_404,
    get_school_or_404,
)
from oziebot_api.services.teacher_assist.setup import get_teacher_profile, teacher_assist_context_for_user, upsert_teacher_profile
from oziebot_api.services.teacher_assist.teacher_classroom import upsert_my_classroom
from oziebot_api.services.teacher_assist.teacher_school_setup import (
    build_my_school_setup,
    sync_my_teaching_subjects,
)
from oziebot_api.services.teacher_assist_v2.pacing_guides import ensure_tenant_school_year
from oziebot_api.services.teacher_assist_v2.school_years import get_platform_school_year_or_404, list_platform_school_years


def _now() -> datetime:
    return datetime.now(UTC)


def get_v2_onboarding(db: Session, *, user_id: uuid.UUID) -> TeacherAssistV2Onboarding | None:
    return db.scalars(
        select(TeacherAssistV2Onboarding).where(TeacherAssistV2Onboarding.user_id == user_id)
    ).one_or_none()


def get_or_create_v2_onboarding(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> TeacherAssistV2Onboarding:
    row = get_v2_onboarding(db, user_id=user_id)
    if row is not None:
        return row
    now = _now()
    row = TeacherAssistV2Onboarding(
        id=uuid.uuid4(),
        user_id=user_id,
        tenant_id=tenant_id,
        selected_subject_ids=[],
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    try:
        with db.begin_nested():
            db.flush()
    except IntegrityError:
        db.expunge(row)
        existing = get_v2_onboarding(db, user_id=user_id)
        if existing is None:
            raise
        return existing
    return row


def is_v2_onboarding_complete(row: TeacherAssistV2Onboarding | None) -> bool:
    return row is not None and row.onboarding_completed_at is not None


def is_v2_pacing_setup_complete(row: TeacherAssistV2Onboarding | None) -> bool:
    return row is not None and row.pacing_guide_setup_completed_at is not None


def build_teacher_onboarding_form(db: Session, *, user: User, grade_id: uuid.UUID | None = None) -> dict[str, Any]:
    ctx = teacher_assist_context_for_user(db, user)
    assignment = get_active_teacher_assignment(db, user_id=user.id)
    if assignment is None:
        raise ValueError("Your school placement is not ready yet. Contact your administrator.")

    school = get_school_or_404(db, assignment.school_id)
    school_setup = build_my_school_setup(db, tenant_id=ctx.tenant_id, user_id=user.id)
    onboarding = get_or_create_v2_onboarding(db, tenant_id=ctx.tenant_id, user_id=user.id)

    school_years = list_platform_school_years(
        db,
        state_id=assignment.state_id,
        active_only=False,
    )
    active_school_year = next((row for row in school_years if row.active), None)
    if active_school_year is None and school_years:
        active_school_year = school_years[0]

    grades = db.scalars(
        select(EducationGrade)
        .where(EducationGrade.school_id == school.id, EducationGrade.active.is_(True))
        .order_by(EducationGrade.grade_code.asc())
    ).all()

    assigned_grade_id = school_setup.get("catalog_grade_id")
    selected_grade_id = str(grade_id) if grade_id else (str(onboarding.grade_id) if onboarding.grade_id else assigned_grade_id)
    if selected_grade_id is None and assigned_grade_id:
        selected_grade_id = assigned_grade_id

    selected_subject_ids = [str(subject_id) for subject_id in (onboarding.selected_subject_ids or [])]
    if not selected_subject_ids:
        selected_subject_ids = school_setup.get("selected_catalog_subject_ids") or []

    grade_row = None
    if selected_grade_id:
        grade_row = db.get(EducationGrade, uuid.UUID(selected_grade_id))

    subjects: list[EducationSubject] = []
    if grade_row is not None:
        subjects = db.scalars(
            select(EducationSubject)
            .where(EducationSubject.grade_id == grade_row.id, EducationSubject.active.is_(True))
            .order_by(EducationSubject.display_name.asc())
        ).all()

    profile = get_teacher_profile(db, user_id=user.id)
    student_count = onboarding.student_count
    if student_count is None and profile is not None:
        student_count = profile.default_student_count

    return {
        "assignment": {
            "state_id": str(assignment.state_id),
            "district_id": str(assignment.district_id),
            "school_id": str(assignment.school_id),
            "school_name": school.name,
        },
        "school_years": [
            {
                "id": str(row.id),
                "title": row.title,
                "active": row.active,
            }
            for row in school_years
        ],
        "default_school_year_id": str(active_school_year.id) if active_school_year else None,
        "grades": [
            {
                "id": str(row.id),
                "grade_code": row.grade_code,
                "display_name": row.display_name,
            }
            for row in grades
        ],
        "assigned_grade_id": assigned_grade_id,
        "selected_school_year_id": str(onboarding.school_year_id) if onboarding.school_year_id else None,
        "selected_grade_id": selected_grade_id,
        "selected_subject_ids": selected_subject_ids,
        "subjects": [
            {
                "id": str(row.id),
                "subject_code": row.subject_code,
                "display_name": row.display_name,
            }
            for row in subjects
        ],
        "student_count": student_count,
        "onboarding_complete": is_v2_onboarding_complete(onboarding),
    }


def save_teacher_onboarding(
    db: Session,
    *,
    user: User,
    school_year_id: uuid.UUID,
    grade_id: uuid.UUID,
    student_count: int,
    selected_subject_ids: list[uuid.UUID],
) -> TeacherAssistV2Onboarding:
    field_errors: dict[str, str] = {}
    if student_count < 1 or student_count > 100:
        field_errors["student_count"] = "Enter a class size between 1 and 100."
    if not selected_subject_ids:
        field_errors["selected_subject_ids"] = "Select at least one subject you teach."

    ctx = teacher_assist_context_for_user(db, user)
    assignment = get_active_teacher_assignment(db, user_id=user.id)
    if assignment is None:
        raise ValueError("Your school placement is not ready yet. Contact your administrator.")

    platform_year = None
    try:
        platform_year = get_platform_school_year_or_404(db, school_year_id=school_year_id)
    except LookupError:
        field_errors["school_year_id"] = "Choose a school year."

    if platform_year is not None and platform_year.state_id != assignment.state_id:
        field_errors["school_year_id"] = "Choose a school year for your state."

    try:
        grade = get_grade_or_404(db, grade_id)
    except LookupError:
        field_errors["grade_id"] = "Choose your grade."
        grade = None

    if grade is not None and grade.school_id != assignment.school_id:
        field_errors["grade_id"] = "Choose a grade at your assigned school."

    if field_errors:
        raise ValueError(field_errors)

    existing = get_v2_onboarding(db, user_id=user.id)
    if is_v2_onboarding_complete(existing):
        raise ValueError("Onboarding is already complete.")

    assert grade is not None
    assert platform_year is not None

    sync_my_teaching_subjects(
        db,
        tenant_id=ctx.tenant_id,
        user_id=user.id,
        catalog_grade_id=grade.id,
        catalog_subject_ids=selected_subject_ids,
    )

    tenant_year = ensure_tenant_school_year(db, tenant_id=ctx.tenant_id, platform_year=platform_year)
    profile = get_teacher_profile(db, user_id=user.id)
    upsert_teacher_profile(
        db,
        user=user,
        preferred_grade_level=grade.grade_code,
        default_student_count=student_count,
        preferred_grading_period_type=profile.preferred_grading_period_type if profile else None,
        timezone=profile.timezone if profile else None,
    )

    upsert_my_classroom(
        db,
        tenant_id=ctx.tenant_id,
        user=user,
        homeroom_name=f"Grade {grade.display_name} Homeroom",
        student_count=student_count,
        timezone=profile.timezone if profile else None,
    )

    onboarding = get_or_create_v2_onboarding(db, tenant_id=ctx.tenant_id, user_id=user.id)
    now = _now()
    onboarding.school_year_id = platform_year.id
    onboarding.state_id = assignment.state_id
    onboarding.district_id = assignment.district_id
    onboarding.school_id = assignment.school_id
    onboarding.grade_id = grade.id
    onboarding.selected_subject_ids = [str(subject_id) for subject_id in selected_subject_ids]
    onboarding.student_count = student_count
    onboarding.onboarding_completed_at = now
    onboarding.updated_at = now
    db.flush()
    return onboarding


def serialize_teacher_onboarding_status(row: TeacherAssistV2Onboarding | None) -> dict[str, Any]:
    if row is None:
        return {
            "onboarding_complete": False,
            "pacing_guide_setup_complete": False,
            "school_year_id": None,
            "grade_id": None,
            "selected_subject_ids": [],
            "student_count": None,
        }
    return {
        "onboarding_complete": is_v2_onboarding_complete(row),
        "pacing_guide_setup_complete": is_v2_pacing_setup_complete(row),
        "school_year_id": str(row.school_year_id) if row.school_year_id else None,
        "grade_id": str(row.grade_id) if row.grade_id else None,
        "selected_subject_ids": [str(subject_id) for subject_id in (row.selected_subject_ids or [])],
        "student_count": row.student_count,
        "onboarding_completed_at": row.onboarding_completed_at.isoformat() if row.onboarding_completed_at else None,
        "pacing_guide_setup_completed_at": (
            row.pacing_guide_setup_completed_at.isoformat() if row.pacing_guide_setup_completed_at else None
        ),
    }

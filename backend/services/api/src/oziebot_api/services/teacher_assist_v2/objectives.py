from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationGrade,
    EducationObjective,
    EducationSubject,
)
from oziebot_api.services.teacher_assist.education_catalog import (
    create_objective,
    get_grade_or_404,
    get_objective_or_404,
    get_subject_or_404,
    list_objectives,
    update_objective,
)
from oziebot_api.services.teacher_assist_v2.school_years import get_platform_school_year_or_404


def validate_objective_hierarchy(
    db: Session,
    *,
    state_id: uuid.UUID,
    district_id: uuid.UUID | None,
    school_id: uuid.UUID | None,
    grade_id: uuid.UUID,
    subject_id: uuid.UUID,
    school_year_id: uuid.UUID,
) -> tuple[EducationGrade, EducationSubject]:
    school_year = get_platform_school_year_or_404(db, school_year_id=school_year_id)
    if school_year.state_id != state_id:
        raise ValueError("School year does not belong to the selected state")
    if district_id is not None and school_year.district_id not in {None, district_id}:
        raise ValueError("School year district scope does not match")
    if school_id is not None and school_year.school_id not in {None, school_id}:
        raise ValueError("School year school scope does not match")
    grade = get_grade_or_404(db, grade_id)
    if grade.school_id is None:
        raise ValueError("Grade must belong to a school")
    subject = get_subject_or_404(db, subject_id)
    if subject.grade_id != grade.id:
        raise ValueError("Subject does not belong to the selected grade")
    return grade, subject


def list_v2_objectives(
    db: Session,
    *,
    state_id: uuid.UUID | None = None,
    grade_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    school_year_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationObjective]:
    return list_objectives(
        db,
        state_id=state_id,
        grade_id=grade_id,
        subject_id=subject_id,
        school_year_id=school_year_id,
        active_only=active_only,
    )


def create_v2_objective(
    db: Session,
    *,
    state_id: uuid.UUID,
    district_id: uuid.UUID | None,
    school_id: uuid.UUID | None,
    grade_id: uuid.UUID,
    subject_id: uuid.UUID,
    school_year_id: uuid.UUID,
    objective_type: str,
    objective_id: str,
    description: str,
    is_required: bool,
    active: bool = True,
) -> EducationObjective:
    grade, subject = validate_objective_hierarchy(
        db,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        grade_id=grade_id,
        subject_id=subject_id,
        school_year_id=school_year_id,
    )
    return create_objective(
        db,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        grade_id=grade.id,
        subject_id=subject.id,
        school_year_id=school_year_id,
        grade_level=grade.grade_code,
        subject_code=subject.subject_code,
        objective_type=objective_type,
        objective_id=objective_id,
        description=description,
        coverage_type="required" if is_required else "optional",
        active=active,
    )


def update_v2_objective(
    db: Session,
    *,
    row_id: uuid.UUID,
    state_id: uuid.UUID,
    district_id: uuid.UUID | None,
    school_id: uuid.UUID | None,
    grade_id: uuid.UUID,
    subject_id: uuid.UUID,
    school_year_id: uuid.UUID,
    objective_type: str,
    objective_id: str,
    description: str,
    is_required: bool,
    active: bool,
) -> EducationObjective:
    get_objective_or_404(db, row_id)
    grade, subject = validate_objective_hierarchy(
        db,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        grade_id=grade_id,
        subject_id=subject_id,
        school_year_id=school_year_id,
    )
    return update_objective(
        db,
        row_id=row_id,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        grade_id=grade.id,
        subject_id=subject.id,
        school_year_id=school_year_id,
        grade_level=grade.grade_code,
        subject_code=subject.subject_code,
        objective_type=objective_type,
        objective_id=objective_id,
        description=description,
        coverage_type="required" if is_required else "optional",
        active=active,
    )


def archive_v2_objective(db: Session, *, row_id: uuid.UUID) -> EducationObjective:
    row = get_objective_or_404(db, row_id)
    return update_objective(
        db,
        row_id=row.id,
        state_id=row.state_id,
        district_id=row.district_id,
        school_id=row.school_id,
        grade_id=row.grade_id,
        subject_id=row.subject_id,
        school_year_id=row.school_year_id,
        grade_level=row.grade_level,
        subject_code=row.subject_code,
        objective_type=row.objective_type,
        objective_id=row.objective_id,
        description=row.description,
        coverage_type=row.coverage_type,
        active=False,
    )

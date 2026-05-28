from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_class_subject import TeacherAssistClassSubject
from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
from oziebot_api.models.teacher_assist_profile import TeacherAssistProfile
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.user import User
from oziebot_api.services.product_access import (
    TEACHER_ASSIST_PRODUCT_KEY,
    resolve_tenant_id_for_product,
)
from oziebot_api.services.teacher_assist.constants import (
    validate_grade_level,
    validate_grading_period_type,
    validate_standard_type,
    validate_timezone,
)


@dataclass(frozen=True)
class TeacherAssistContext:
    tenant_id: uuid.UUID


def teacher_assist_context_for_user(db: Session, user: User) -> TeacherAssistContext:
    tenant_id = resolve_tenant_id_for_product(db, user=user, product_key=TEACHER_ASSIST_PRODUCT_KEY)
    if tenant_id is None:
        raise PermissionError("TeacherAssist is not enabled for this user")
    return TeacherAssistContext(tenant_id=tenant_id)


def list_school_years(db: Session, *, tenant_id: uuid.UUID) -> list[TeacherAssistSchoolYear]:
    return db.scalars(
        select(TeacherAssistSchoolYear)
        .where(TeacherAssistSchoolYear.tenant_id == tenant_id)
        .order_by(TeacherAssistSchoolYear.start_date.desc(), TeacherAssistSchoolYear.created_at.desc())
    ).all()


def list_grading_periods(
    db: Session, *, tenant_id: uuid.UUID, school_year_id: uuid.UUID | None = None
) -> list[TeacherAssistGradingPeriod]:
    stmt = (
        select(TeacherAssistGradingPeriod)
        .join(
            TeacherAssistSchoolYear,
            TeacherAssistSchoolYear.id == TeacherAssistGradingPeriod.school_year_id,
        )
        .where(TeacherAssistSchoolYear.tenant_id == tenant_id)
        .order_by(
            TeacherAssistSchoolYear.start_date.desc(),
            TeacherAssistGradingPeriod.sort_order.asc(),
            TeacherAssistGradingPeriod.start_date.asc(),
        )
    )
    if school_year_id is not None:
        stmt = stmt.where(TeacherAssistGradingPeriod.school_year_id == school_year_id)
    return db.scalars(stmt).all()


def list_subjects(db: Session, *, tenant_id: uuid.UUID) -> list[TeacherAssistSubject]:
    return db.scalars(
        select(TeacherAssistSubject)
        .where(TeacherAssistSubject.tenant_id == tenant_id)
        .order_by(TeacherAssistSubject.name.asc(), TeacherAssistSubject.created_at.asc())
    ).all()


def list_classes(db: Session, *, tenant_id: uuid.UUID) -> list[TeacherAssistClass]:
    return db.scalars(
        select(TeacherAssistClass)
        .where(TeacherAssistClass.tenant_id == tenant_id)
        .order_by(TeacherAssistClass.created_at.desc())
    ).all()


def list_standards(db: Session, *, tenant_id: uuid.UUID) -> list[TeacherAssistStandard]:
    return db.scalars(
        select(TeacherAssistStandard)
        .where(TeacherAssistStandard.tenant_id == tenant_id)
        .order_by(TeacherAssistStandard.standard_type.asc(), TeacherAssistStandard.code.asc())
    ).all()


def list_class_subjects(db: Session, *, tenant_id: uuid.UUID) -> list[TeacherAssistClassSubject]:
    return db.scalars(
        select(TeacherAssistClassSubject)
        .join(TeacherAssistClass, TeacherAssistClass.id == TeacherAssistClassSubject.class_id)
        .where(TeacherAssistClass.tenant_id == tenant_id)
        .order_by(TeacherAssistClassSubject.created_at.asc())
    ).all()


def get_teacher_profile(db: Session, *, user_id: uuid.UUID) -> TeacherAssistProfile | None:
    return db.scalars(
        select(TeacherAssistProfile).where(TeacherAssistProfile.user_id == user_id)
    ).one_or_none()


def upsert_teacher_profile(
    db: Session,
    *,
    user: User,
    preferred_grade_level: str | None,
    default_student_count: int | None,
    preferred_grading_period_type: str | None,
    timezone: str | None,
) -> TeacherAssistProfile:
    normalized_grade_level = validate_grade_level(preferred_grade_level)
    normalized_grading_period_type = validate_grading_period_type(preferred_grading_period_type)
    normalized_timezone = validate_timezone(timezone)
    if default_student_count is not None and default_student_count <= 0:
        raise ValueError("Default student count must be greater than zero")

    row = get_teacher_profile(db, user_id=user.id)
    now = datetime.now(UTC)
    if row is None:
        row = TeacherAssistProfile(
            user_id=user.id,
            preferred_grade_level=normalized_grade_level,
            default_student_count=default_student_count,
            preferred_grading_period_type=normalized_grading_period_type,
            timezone=normalized_timezone,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.preferred_grade_level = normalized_grade_level
        row.default_student_count = default_student_count
        row.preferred_grading_period_type = normalized_grading_period_type
        row.timezone = normalized_timezone
        row.updated_at = now
    db.flush()
    return row


def _validate_date_window(start_date: date, end_date: date, *, label: str) -> None:
    if start_date > end_date:
        raise ValueError(f"{label} start date must be on or before end date")


def _enforce_active_school_year(db: Session, *, tenant_id: uuid.UUID, school_year_id: uuid.UUID) -> None:
    for row in db.scalars(
        select(TeacherAssistSchoolYear).where(TeacherAssistSchoolYear.tenant_id == tenant_id)
    ).all():
        row.is_active = row.id == school_year_id
        row.updated_at = datetime.now(UTC)


def create_school_year(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    title: str,
    start_date: date,
    end_date: date,
    is_active: bool,
) -> TeacherAssistSchoolYear:
    _validate_date_window(start_date, end_date, label="School year")
    now = datetime.now(UTC)
    row = TeacherAssistSchoolYear(
        tenant_id=tenant_id,
        title=title.strip(),
        start_date=start_date,
        end_date=end_date,
        is_active=is_active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    if is_active:
        _enforce_active_school_year(db, tenant_id=tenant_id, school_year_id=row.id)
    return row


def get_school_year_or_404(
    db: Session, *, tenant_id: uuid.UUID, school_year_id: uuid.UUID
) -> TeacherAssistSchoolYear:
    row = db.scalars(
        select(TeacherAssistSchoolYear).where(
            TeacherAssistSchoolYear.id == school_year_id,
            TeacherAssistSchoolYear.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("School year not found")
    return row


def update_school_year(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID,
    title: str,
    start_date: date,
    end_date: date,
    is_active: bool,
) -> TeacherAssistSchoolYear:
    _validate_date_window(start_date, end_date, label="School year")
    row = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    row.title = title.strip()
    row.start_date = start_date
    row.end_date = end_date
    row.is_active = is_active
    row.updated_at = datetime.now(UTC)
    _validate_school_year_grading_periods(row)
    db.flush()
    if is_active:
        _enforce_active_school_year(db, tenant_id=tenant_id, school_year_id=row.id)
    return row


def _validate_grading_period_window(
    school_year: TeacherAssistSchoolYear,
    *,
    grading_period_id: uuid.UUID | None,
    start_date: date,
    end_date: date,
) -> None:
    _validate_date_window(start_date, end_date, label="Grading period")
    if start_date < school_year.start_date or end_date > school_year.end_date:
        raise ValueError("Grading period dates must fall within the school year")
    rows = school_year.grading_periods
    for row in rows:
        if grading_period_id is not None and row.id == grading_period_id:
            continue
        if start_date <= row.end_date and end_date >= row.start_date:
            raise ValueError("Grading period dates overlap an existing grading period")


def _validate_school_year_grading_periods(school_year: TeacherAssistSchoolYear) -> None:
    rows = sorted(school_year.grading_periods, key=lambda item: (item.start_date, item.sort_order, item.created_at))
    for index, row in enumerate(rows):
        if row.start_date < school_year.start_date or row.end_date > school_year.end_date:
            raise ValueError("Existing grading period falls outside the updated school year dates")
        if row.start_date > row.end_date:
            raise ValueError("Existing grading period has invalid dates")
        if index == 0:
            continue
        previous = rows[index - 1]
        if row.start_date <= previous.end_date:
            raise ValueError("Existing grading periods overlap inside the updated school year")


def create_grading_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID,
    title: str,
    grading_period_type: str,
    start_date: date,
    end_date: date,
    sort_order: int,
) -> TeacherAssistGradingPeriod:
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    normalized_type = validate_grading_period_type(grading_period_type)
    if normalized_type is None:
        raise ValueError("Grading period type is required")
    _validate_grading_period_window(
        school_year,
        grading_period_id=None,
        start_date=start_date,
        end_date=end_date,
    )
    now = datetime.now(UTC)
    row = TeacherAssistGradingPeriod(
        school_year_id=school_year.id,
        title=title.strip(),
        grading_period_type=normalized_type,
        start_date=start_date,
        end_date=end_date,
        sort_order=sort_order,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def get_grading_period_or_404(
    db: Session, *, tenant_id: uuid.UUID, grading_period_id: uuid.UUID
) -> TeacherAssistGradingPeriod:
    row = db.scalars(
        select(TeacherAssistGradingPeriod)
        .join(
            TeacherAssistSchoolYear,
            TeacherAssistSchoolYear.id == TeacherAssistGradingPeriod.school_year_id,
        )
        .where(
            TeacherAssistGradingPeriod.id == grading_period_id,
            TeacherAssistSchoolYear.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Grading period not found")
    return row


def update_grading_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    grading_period_id: uuid.UUID,
    school_year_id: uuid.UUID,
    title: str,
    grading_period_type: str,
    start_date: date,
    end_date: date,
    sort_order: int,
) -> TeacherAssistGradingPeriod:
    row = get_grading_period_or_404(db, tenant_id=tenant_id, grading_period_id=grading_period_id)
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    normalized_type = validate_grading_period_type(grading_period_type)
    if normalized_type is None:
        raise ValueError("Grading period type is required")
    _validate_grading_period_window(
        school_year,
        grading_period_id=row.id,
        start_date=start_date,
        end_date=end_date,
    )
    row.school_year_id = school_year.id
    row.title = title.strip()
    row.grading_period_type = normalized_type
    row.start_date = start_date
    row.end_date = end_date
    row.sort_order = sort_order
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row


def create_subject(
    db: Session, *, tenant_id: uuid.UUID, code: str | None, name: str
) -> TeacherAssistSubject:
    now = datetime.now(UTC)
    row = TeacherAssistSubject(
        tenant_id=tenant_id,
        code=code.strip() or None if code is not None else None,
        name=name.strip(),
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def get_subject_or_404(db: Session, *, tenant_id: uuid.UUID, subject_id: uuid.UUID) -> TeacherAssistSubject:
    row = db.scalars(
        select(TeacherAssistSubject).where(
            TeacherAssistSubject.id == subject_id,
            TeacherAssistSubject.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Subject not found")
    return row


def create_class(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID,
    name: str,
    grade_level: str,
    student_count: int,
) -> TeacherAssistClass:
    if student_count <= 0:
        raise ValueError("Student count must be greater than zero")
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    now = datetime.now(UTC)
    row = TeacherAssistClass(
        tenant_id=tenant_id,
        school_year_id=school_year.id,
        name=name.strip(),
        grade_level=validate_grade_level(grade_level, required=True) or "",
        student_count=student_count,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def get_class_or_404(db: Session, *, tenant_id: uuid.UUID, class_id: uuid.UUID) -> TeacherAssistClass:
    row = db.scalars(
        select(TeacherAssistClass).where(
            TeacherAssistClass.id == class_id,
            TeacherAssistClass.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Class not found")
    return row


def update_class(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    class_id: uuid.UUID,
    school_year_id: uuid.UUID,
    name: str,
    grade_level: str,
    student_count: int,
) -> TeacherAssistClass:
    if student_count <= 0:
        raise ValueError("Student count must be greater than zero")
    row = get_class_or_404(db, tenant_id=tenant_id, class_id=class_id)
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    row.school_year_id = school_year.id
    row.name = name.strip()
    row.grade_level = validate_grade_level(grade_level, required=True) or ""
    row.student_count = student_count
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row


def attach_class_subject(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
) -> TeacherAssistClassSubject:
    teacher_class = get_class_or_404(db, tenant_id=tenant_id, class_id=class_id)
    subject = get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    existing = db.scalars(
        select(TeacherAssistClassSubject).where(
            TeacherAssistClassSubject.class_id == teacher_class.id,
            TeacherAssistClassSubject.subject_id == subject.id,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    row = TeacherAssistClassSubject(
        class_id=teacher_class.id,
        subject_id=subject.id,
        created_at=datetime.now(UTC),
    )
    db.add(row)
    db.flush()
    return row


def create_standard(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    subject_id: uuid.UUID | None,
    standard_type: str,
    code: str,
    description: str,
    grade_level: str | None,
    school_year_id: uuid.UUID | None,
) -> TeacherAssistStandard:
    normalized_grade_level = validate_grade_level(grade_level)
    normalized_standard_type = validate_standard_type(standard_type)
    if subject_id is not None:
        get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    if school_year_id is not None:
        get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    now = datetime.now(UTC)
    row = TeacherAssistStandard(
        tenant_id=tenant_id,
        subject_id=subject_id,
        standard_type=normalized_standard_type,
        code=code.strip(),
        description=description.strip(),
        grade_level=normalized_grade_level,
        school_year_id=school_year_id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row

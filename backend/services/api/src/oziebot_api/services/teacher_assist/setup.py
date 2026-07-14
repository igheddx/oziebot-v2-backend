from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, date, datetime
import io
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


def list_school_years(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    include_templates: bool = True,
) -> list[TeacherAssistSchoolYear]:
    stmt = (
        select(TeacherAssistSchoolYear)
        .where(TeacherAssistSchoolYear.tenant_id == tenant_id)
        .order_by(
            TeacherAssistSchoolYear.start_date.desc(), TeacherAssistSchoolYear.created_at.desc()
        )
    )
    if not include_templates:
        stmt = stmt.where(TeacherAssistSchoolYear.is_template.is_(False))
    return db.scalars(stmt).all()


def list_teacher_school_years(
    db: Session, *, tenant_id: uuid.UUID
) -> list[TeacherAssistSchoolYear]:
    return list_school_years(db, tenant_id=tenant_id, include_templates=False)


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


def _enforce_active_school_year(
    db: Session, *, tenant_id: uuid.UUID, school_year_id: uuid.UUID
) -> None:
    for row in db.scalars(
        select(TeacherAssistSchoolYear).where(TeacherAssistSchoolYear.tenant_id == tenant_id)
    ).all():
        if row.is_template:
            row.is_active = False
            continue
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
    is_template: bool = False,
) -> TeacherAssistSchoolYear:
    _validate_date_window(start_date, end_date, label="School year")
    now = datetime.now(UTC)
    row = TeacherAssistSchoolYear(
        tenant_id=tenant_id,
        title=title.strip(),
        start_date=start_date,
        end_date=end_date,
        is_active=is_active and not is_template,
        is_template=is_template,
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
    if row.is_template:
        raise ValueError("Template school years cannot be modified")
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
    rows = sorted(
        school_year.grading_periods,
        key=lambda item: (item.start_date, item.sort_order, item.created_at),
    )
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


def get_subject_or_404(
    db: Session, *, tenant_id: uuid.UUID, subject_id: uuid.UUID
) -> TeacherAssistSubject:
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


def get_class_or_404(
    db: Session, *, tenant_id: uuid.UUID, class_id: uuid.UUID
) -> TeacherAssistClass:
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


def get_standard_or_404(
    db: Session, *, tenant_id: uuid.UUID, standard_id: uuid.UUID
) -> TeacherAssistStandard:
    row = db.scalars(
        select(TeacherAssistStandard).where(
            TeacherAssistStandard.id == standard_id,
            TeacherAssistStandard.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Standard not found")
    return row


def _standard_code_exists(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    code: str,
    exclude_standard_id: uuid.UUID | None = None,
) -> bool:
    normalized_code = code.strip().lower()
    for row in list_standards(db, tenant_id=tenant_id):
        if exclude_standard_id is not None and row.id == exclude_standard_id:
            continue
        if row.code.strip().lower() == normalized_code:
            return True
    return False


def _resolve_subject_label(
    db: Session, *, tenant_id: uuid.UUID, subject_label: str
) -> TeacherAssistSubject | None:
    normalized_label = subject_label.strip().lower()
    if not normalized_label:
        return None
    for subject in list_subjects(db, tenant_id=tenant_id):
        if subject.name.strip().lower() == normalized_label:
            return subject
        if subject.code is not None and subject.code.strip().lower() == normalized_label:
            return subject
    return None


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
    if subject_id is None:
        raise ValueError("Subject is required")
    normalized_code = code.strip()
    normalized_description = description.strip()
    if not normalized_code:
        raise ValueError("Standard code is required")
    if not normalized_description:
        raise ValueError("Standard description is required")
    normalized_grade_level = validate_grade_level(grade_level)
    normalized_standard_type = validate_standard_type(standard_type)
    get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    if school_year_id is not None:
        get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    if _standard_code_exists(db, tenant_id=tenant_id, code=normalized_code):
        raise ValueError("A standard with this code already exists")
    now = datetime.now(UTC)
    row = TeacherAssistStandard(
        tenant_id=tenant_id,
        subject_id=subject_id,
        standard_type=normalized_standard_type,
        code=normalized_code,
        description=normalized_description,
        grade_level=normalized_grade_level,
        school_year_id=school_year_id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_standard(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    standard_id: uuid.UUID,
    subject_id: uuid.UUID | None,
    standard_type: str,
    code: str,
    description: str,
    grade_level: str | None,
    school_year_id: uuid.UUID | None,
) -> TeacherAssistStandard:
    if subject_id is None:
        raise ValueError("Subject is required")
    normalized_code = code.strip()
    normalized_description = description.strip()
    if not normalized_code:
        raise ValueError("Standard code is required")
    if not normalized_description:
        raise ValueError("Standard description is required")
    row = get_standard_or_404(db, tenant_id=tenant_id, standard_id=standard_id)
    normalized_grade_level = validate_grade_level(grade_level)
    normalized_standard_type = validate_standard_type(standard_type)
    get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    if school_year_id is not None:
        get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    if _standard_code_exists(
        db,
        tenant_id=tenant_id,
        code=normalized_code,
        exclude_standard_id=row.id,
    ):
        raise ValueError("A standard with this code already exists")
    row.subject_id = subject_id
    row.standard_type = normalized_standard_type
    row.code = normalized_code
    row.description = normalized_description
    row.grade_level = normalized_grade_level
    row.school_year_id = school_year_id
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row


@dataclass(frozen=True)
class StandardImportPreviewRow:
    row_number: int
    code: str
    standard_type: str
    subject_label: str
    description: str
    subject_id: uuid.UUID | None
    status: str


@dataclass(frozen=True)
class StandardImportRowError:
    row_number: int
    message: str
    field: str | None = None


@dataclass(frozen=True)
class StandardImportPreviewResult:
    total_rows: int
    valid_count: int
    invalid_count: int
    duplicate_count: int
    rows: list[StandardImportPreviewRow]
    errors: list[StandardImportRowError]


@dataclass(frozen=True)
class StandardImportCommitRow:
    code: str
    standard_type: str
    subject_id: uuid.UUID
    description: str


@dataclass(frozen=True)
class StandardImportCommitResult:
    created_count: int
    skipped_duplicate_count: int
    errors: list[StandardImportRowError]


STANDARD_IMPORT_REQUIRED_HEADERS = ("code", "type", "subject", "description")


def _parse_standards_csv_rows(
    csv_content: str,
) -> tuple[list[dict[str, str]], list[StandardImportRowError]]:
    reader = csv.DictReader(io.StringIO(csv_content.strip()))
    if reader.fieldnames is None:
        raise ValueError("CSV file is empty or missing a header row")
    normalized_headers = {header.strip().lower(): header for header in reader.fieldnames if header}
    missing_headers = [
        header for header in STANDARD_IMPORT_REQUIRED_HEADERS if header not in normalized_headers
    ]
    if missing_headers:
        raise ValueError(
            "CSV headers must include code, type, subject, and description. "
            f"Missing: {', '.join(missing_headers)}"
        )
    parsed_rows: list[dict[str, str]] = []
    errors: list[StandardImportRowError] = []
    for row_number, row in enumerate(reader, start=2):
        parsed_rows.append(
            {
                "row_number": str(row_number),
                "code": (row.get(normalized_headers["code"]) or "").strip(),
                "standard_type": (row.get(normalized_headers["type"]) or "").strip(),
                "subject_label": (row.get(normalized_headers["subject"]) or "").strip(),
                "description": (row.get(normalized_headers["description"]) or "").strip(),
            }
        )
    if not parsed_rows:
        raise ValueError("CSV file does not contain any data rows")
    return parsed_rows, errors


def preview_standards_import(
    db: Session, *, tenant_id: uuid.UUID, csv_content: str
) -> StandardImportPreviewResult:
    parsed_rows, _ = _parse_standards_csv_rows(csv_content)
    preview_rows: list[StandardImportPreviewRow] = []
    errors: list[StandardImportRowError] = []
    valid_count = 0
    invalid_count = 0
    duplicate_count = 0
    for raw_row in parsed_rows:
        row_number = int(raw_row["row_number"])
        code = raw_row["code"]
        standard_type = raw_row["standard_type"]
        subject_label = raw_row["subject_label"]
        description = raw_row["description"]
        row_errors: list[StandardImportRowError] = []
        if not code:
            row_errors.append(
                StandardImportRowError(
                    row_number=row_number, field="code", message="Code is required."
                )
            )
        if not standard_type:
            row_errors.append(
                StandardImportRowError(
                    row_number=row_number, field="type", message="Type is required."
                )
            )
        else:
            try:
                validate_standard_type(standard_type)
            except ValueError as exc:
                row_errors.append(
                    StandardImportRowError(row_number=row_number, field="type", message=str(exc))
                )
        if not subject_label:
            row_errors.append(
                StandardImportRowError(
                    row_number=row_number,
                    field="subject",
                    message="Subject is required.",
                )
            )
        if not description:
            row_errors.append(
                StandardImportRowError(
                    row_number=row_number,
                    field="description",
                    message="Description is required.",
                )
            )
        subject = (
            _resolve_subject_label(db, tenant_id=tenant_id, subject_label=subject_label)
            if subject_label
            else None
        )
        if subject_label and subject is None:
            row_errors.append(
                StandardImportRowError(
                    row_number=row_number,
                    field="subject",
                    message=(
                        f"Subject '{subject_label}' was not found. Create the subject first, "
                        "then re-import this row."
                    ),
                )
            )
        status = "valid"
        if row_errors:
            status = "invalid"
            invalid_count += 1
            errors.extend(row_errors)
        elif _standard_code_exists(db, tenant_id=tenant_id, code=code):
            status = "duplicate"
            duplicate_count += 1
        else:
            valid_count += 1
        preview_rows.append(
            StandardImportPreviewRow(
                row_number=row_number,
                code=code,
                standard_type=standard_type,
                subject_label=subject_label,
                description=description,
                subject_id=subject.id if subject is not None else None,
                status=status,
            )
        )
    return StandardImportPreviewResult(
        total_rows=len(preview_rows),
        valid_count=valid_count,
        invalid_count=invalid_count,
        duplicate_count=duplicate_count,
        rows=preview_rows,
        errors=errors,
    )


def commit_standards_import(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    rows: list[StandardImportCommitRow],
) -> StandardImportCommitResult:
    created_count = 0
    skipped_duplicate_count = 0
    errors: list[StandardImportRowError] = []
    for index, row in enumerate(rows, start=1):
        try:
            if _standard_code_exists(db, tenant_id=tenant_id, code=row.code):
                skipped_duplicate_count += 1
                continue
            create_standard(
                db,
                tenant_id=tenant_id,
                subject_id=row.subject_id,
                standard_type=row.standard_type,
                code=row.code,
                description=row.description,
                grade_level=None,
                school_year_id=None,
            )
            created_count += 1
        except (LookupError, ValueError) as exc:
            errors.append(StandardImportRowError(row_number=index, message=str(exc)))
    return StandardImportCommitResult(
        created_count=created_count,
        skipped_duplicate_count=skipped_duplicate_count,
        errors=errors,
    )

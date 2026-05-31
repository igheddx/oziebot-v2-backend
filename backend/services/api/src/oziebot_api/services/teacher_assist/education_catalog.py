from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import csv
import io
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationCurriculumResource,
    EducationDistrict,
    EducationGrade,
    EducationObjective,
    EducationResourceLink,
    EducationSchool,
    EducationState,
    EducationSubject,
    TeacherSchoolAssignment,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.education_catalog_constants import (
    validate_catalog_resource_type,
    validate_coverage_type,
    validate_objective_type,
)


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class CatalogImportRowError:
    row_number: int
    message: str
    field: str | None = None


@dataclass(frozen=True)
class CatalogImportPreviewResult:
    total_rows: int
    valid_count: int
    invalid_count: int
    duplicate_count: int
    errors: list[CatalogImportRowError]


@dataclass(frozen=True)
class CatalogImportCommitResult:
    created_count: int
    skipped_duplicate_count: int
    errors: list[CatalogImportRowError]


def _parse_csv(csv_content: str) -> tuple[list[dict[str, str]], list[str]]:
    reader = csv.DictReader(io.StringIO(csv_content.strip()))
    if reader.fieldnames is None:
        raise ValueError("CSV file is empty or missing a header row")
    normalized_headers = {header.strip().lower(): header for header in reader.fieldnames if header}
    rows: list[dict[str, str]] = []
    for row_number, row in enumerate(reader, start=2):
        parsed = {
            key: (row.get(original) or "").strip() for key, original in normalized_headers.items()
        }
        parsed["_row_number"] = str(row_number)
        rows.append(parsed)
    if not rows:
        raise ValueError("CSV file does not contain any data rows")
    return rows, list(normalized_headers.keys())


def list_states(db: Session, *, q: str | None = None, active_only: bool = False) -> list[EducationState]:
    stmt = select(EducationState).order_by(EducationState.name.asc())
    if active_only:
        stmt = stmt.where(EducationState.active.is_(True))
    if q:
        stmt = stmt.where(
            func.lower(EducationState.name).contains(q.strip().lower())
            | func.lower(EducationState.abbreviation).contains(q.strip().lower())
        )
    return db.scalars(stmt).all()


def get_state_or_404(db: Session, state_id: uuid.UUID) -> EducationState:
    row = db.get(EducationState, state_id)
    if row is None:
        raise LookupError("State not found")
    return row


def create_state(db: Session, *, name: str, abbreviation: str, active: bool = True) -> EducationState:
    now = _now()
    row = EducationState(
        name=name.strip(),
        abbreviation=abbreviation.strip().upper(),
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_state(
    db: Session, *, state_id: uuid.UUID, name: str, abbreviation: str, active: bool
) -> EducationState:
    row = get_state_or_404(db, state_id)
    row.name = name.strip()
    row.abbreviation = abbreviation.strip().upper()
    row.active = active
    row.updated_at = _now()
    db.flush()
    return row


def list_districts(
    db: Session, *, state_id: uuid.UUID | None = None, q: str | None = None, active_only: bool = False
) -> list[EducationDistrict]:
    stmt = select(EducationDistrict).order_by(EducationDistrict.name.asc())
    if state_id is not None:
        stmt = stmt.where(EducationDistrict.state_id == state_id)
    if active_only:
        stmt = stmt.where(EducationDistrict.active.is_(True))
    if q:
        stmt = stmt.where(func.lower(EducationDistrict.name).contains(q.strip().lower()))
    return db.scalars(stmt).all()


def get_district_or_404(db: Session, district_id: uuid.UUID) -> EducationDistrict:
    row = db.get(EducationDistrict, district_id)
    if row is None:
        raise LookupError("District not found")
    return row


def create_district(
    db: Session,
    *,
    state_id: uuid.UUID,
    name: str,
    district_code: str | None = None,
    active: bool = True,
) -> EducationDistrict:
    get_state_or_404(db, state_id)
    now = _now()
    row = EducationDistrict(
        state_id=state_id,
        name=name.strip(),
        district_code=district_code.strip().upper() if district_code else None,
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_district(
    db: Session,
    *,
    district_id: uuid.UUID,
    state_id: uuid.UUID,
    name: str,
    district_code: str | None,
    active: bool,
) -> EducationDistrict:
    row = get_district_or_404(db, district_id)
    get_state_or_404(db, state_id)
    row.state_id = state_id
    row.name = name.strip()
    row.district_code = district_code.strip().upper() if district_code else None
    row.active = active
    row.updated_at = _now()
    db.flush()
    return row


def list_schools(
    db: Session,
    *,
    district_id: uuid.UUID | None = None,
    q: str | None = None,
    active_only: bool = False,
) -> list[EducationSchool]:
    stmt = select(EducationSchool).order_by(EducationSchool.name.asc())
    if district_id is not None:
        stmt = stmt.where(EducationSchool.district_id == district_id)
    if active_only:
        stmt = stmt.where(EducationSchool.active.is_(True))
    if q:
        stmt = stmt.where(func.lower(EducationSchool.name).contains(q.strip().lower()))
    return db.scalars(stmt).all()


def get_school_or_404(db: Session, school_id: uuid.UUID) -> EducationSchool:
    row = db.get(EducationSchool, school_id)
    if row is None:
        raise LookupError("School not found")
    return row


def create_school(
    db: Session,
    *,
    district_id: uuid.UUID,
    name: str,
    school_type: str | None,
    active: bool = True,
) -> EducationSchool:
    get_district_or_404(db, district_id)
    now = _now()
    row = EducationSchool(
        district_id=district_id,
        name=name.strip(),
        school_type=school_type.strip() if school_type else None,
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_school(
    db: Session,
    *,
    school_id: uuid.UUID,
    district_id: uuid.UUID,
    name: str,
    school_type: str | None,
    active: bool,
) -> EducationSchool:
    row = get_school_or_404(db, school_id)
    get_district_or_404(db, district_id)
    row.district_id = district_id
    row.name = name.strip()
    row.school_type = school_type.strip() if school_type else None
    row.active = active
    row.updated_at = _now()
    db.flush()
    return row


def list_grades(
    db: Session, *, school_id: uuid.UUID | None = None, active_only: bool = False
) -> list[EducationGrade]:
    stmt = select(EducationGrade).order_by(EducationGrade.grade_code.asc())
    if school_id is not None:
        stmt = stmt.where(EducationGrade.school_id == school_id)
    if active_only:
        stmt = stmt.where(EducationGrade.active.is_(True))
    return db.scalars(stmt).all()


def get_grade_or_404(db: Session, grade_id: uuid.UUID) -> EducationGrade:
    row = db.get(EducationGrade, grade_id)
    if row is None:
        raise LookupError("Grade not found")
    return row


def create_grade(
    db: Session,
    *,
    school_id: uuid.UUID | None,
    grade_code: str,
    display_name: str,
    active: bool = True,
) -> EducationGrade:
    if school_id is not None:
        get_school_or_404(db, school_id)
    row = EducationGrade(
        school_id=school_id,
        grade_code=grade_code.strip(),
        display_name=display_name.strip(),
        active=active,
    )
    db.add(row)
    db.flush()
    return row


def update_grade(
    db: Session,
    *,
    grade_id: uuid.UUID,
    school_id: uuid.UUID | None,
    grade_code: str,
    display_name: str,
    active: bool,
) -> EducationGrade:
    row = get_grade_or_404(db, grade_id)
    if school_id is not None:
        get_school_or_404(db, school_id)
    row.school_id = school_id
    row.grade_code = grade_code.strip()
    row.display_name = display_name.strip()
    row.active = active
    db.flush()
    return row


def list_subjects(
    db: Session, *, grade_id: uuid.UUID | None = None, active_only: bool = False
) -> list[EducationSubject]:
    stmt = select(EducationSubject).order_by(EducationSubject.subject_code.asc())
    if grade_id is not None:
        stmt = stmt.where(EducationSubject.grade_id == grade_id)
    if active_only:
        stmt = stmt.where(EducationSubject.active.is_(True))
    return db.scalars(stmt).all()


def get_subject_or_404(db: Session, subject_id: uuid.UUID) -> EducationSubject:
    row = db.get(EducationSubject, subject_id)
    if row is None:
        raise LookupError("Subject not found")
    return row


def create_subject(
    db: Session,
    *,
    grade_id: uuid.UUID | None,
    subject_code: str,
    display_name: str,
    active: bool = True,
) -> EducationSubject:
    if grade_id is not None:
        get_grade_or_404(db, grade_id)
    row = EducationSubject(
        grade_id=grade_id,
        subject_code=subject_code.strip(),
        display_name=display_name.strip(),
        active=active,
    )
    db.add(row)
    db.flush()
    return row


def update_subject(
    db: Session,
    *,
    subject_id: uuid.UUID,
    grade_id: uuid.UUID | None,
    subject_code: str,
    display_name: str,
    active: bool,
) -> EducationSubject:
    row = get_subject_or_404(db, subject_id)
    if grade_id is not None:
        get_grade_or_404(db, grade_id)
    row.grade_id = grade_id
    row.subject_code = subject_code.strip()
    row.display_name = display_name.strip()
    row.active = active
    db.flush()
    return row


def list_objectives(
    db: Session,
    *,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
    grade_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    school_year_id: uuid.UUID | None = None,
    grade_level: str | None = None,
    subject_code: str | None = None,
    q: str | None = None,
    active_only: bool = False,
) -> list[EducationObjective]:
    stmt = select(EducationObjective).order_by(
        EducationObjective.grade_level.asc(), EducationObjective.objective_id.asc()
    )
    if state_id is not None:
        stmt = stmt.where(EducationObjective.state_id == state_id)
    if district_id is not None:
        stmt = stmt.where(EducationObjective.district_id == district_id)
    if school_id is not None:
        stmt = stmt.where(EducationObjective.school_id == school_id)
    if grade_id is not None:
        stmt = stmt.where(EducationObjective.grade_id == grade_id)
    if subject_id is not None:
        stmt = stmt.where(EducationObjective.subject_id == subject_id)
    if school_year_id is not None:
        stmt = stmt.where(EducationObjective.school_year_id == school_year_id)
    if grade_level:
        stmt = stmt.where(EducationObjective.grade_level == grade_level.strip())
    if subject_code:
        stmt = stmt.where(EducationObjective.subject_code == subject_code.strip())
    if active_only:
        stmt = stmt.where(EducationObjective.active.is_(True))
    if q:
        lowered = q.strip().lower()
        stmt = stmt.where(
            func.lower(EducationObjective.objective_id).contains(lowered)
            | func.lower(EducationObjective.description).contains(lowered)
        )
    return db.scalars(stmt).all()


def get_objective_or_404(db: Session, objective_id: uuid.UUID) -> EducationObjective:
    row = db.get(EducationObjective, objective_id)
    if row is None:
        raise LookupError("Objective not found")
    return row


def create_objective(
    db: Session,
    *,
    state_id: uuid.UUID,
    grade_level: str,
    subject_code: str,
    objective_type: str,
    objective_id: str,
    description: str,
    coverage_type: str,
    active: bool = True,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
    grade_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    school_year_id: uuid.UUID | None = None,
) -> EducationObjective:
    get_state_or_404(db, state_id)
    if district_id is not None:
        get_district_or_404(db, district_id)
    if school_id is not None:
        get_school_or_404(db, school_id)
    if grade_id is not None:
        get_grade_or_404(db, grade_id)
    if subject_id is not None:
        get_subject_or_404(db, subject_id)
    if school_year_id is not None:
        from oziebot_api.models.education_catalog import EducationSchoolYear

        row = db.get(EducationSchoolYear, school_year_id)
        if row is None:
            raise LookupError("School year not found")
    now = _now()
    row = EducationObjective(
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        grade_id=grade_id,
        subject_id=subject_id,
        school_year_id=school_year_id,
        grade_level=grade_level.strip(),
        subject_code=subject_code.strip(),
        objective_type=validate_objective_type(objective_type),
        objective_id=objective_id.strip(),
        description=description.strip(),
        coverage_type=validate_coverage_type(coverage_type),
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_objective(
    db: Session,
    *,
    row_id: uuid.UUID,
    state_id: uuid.UUID,
    grade_level: str,
    subject_code: str,
    objective_type: str,
    objective_id: str,
    description: str,
    coverage_type: str,
    active: bool,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
    grade_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    school_year_id: uuid.UUID | None = None,
) -> EducationObjective:
    row = get_objective_or_404(db, row_id)
    get_state_or_404(db, state_id)
    if district_id is not None:
        get_district_or_404(db, district_id)
    if school_id is not None:
        get_school_or_404(db, school_id)
    if grade_id is not None:
        get_grade_or_404(db, grade_id)
    if subject_id is not None:
        get_subject_or_404(db, subject_id)
    if school_year_id is not None:
        from oziebot_api.models.education_catalog import EducationSchoolYear

        school_year = db.get(EducationSchoolYear, school_year_id)
        if school_year is None:
            raise LookupError("School year not found")
    row.state_id = state_id
    row.district_id = district_id
    row.school_id = school_id
    row.grade_id = grade_id
    row.subject_id = subject_id
    row.school_year_id = school_year_id
    row.grade_level = grade_level.strip()
    row.subject_code = subject_code.strip()
    row.objective_type = validate_objective_type(objective_type)
    row.objective_id = objective_id.strip()
    row.description = description.strip()
    row.coverage_type = validate_coverage_type(coverage_type)
    row.active = active
    row.updated_at = _now()
    db.flush()
    return row


def list_curriculum_resources(
    db: Session,
    *,
    school_id: uuid.UUID | None = None,
    grade_level: str | None = None,
    subject_code: str | None = None,
    resource_type: str | None = None,
    active_only: bool = False,
) -> list[EducationCurriculumResource]:
    stmt = select(EducationCurriculumResource).order_by(EducationCurriculumResource.title.asc())
    if school_id is not None:
        stmt = stmt.where(EducationCurriculumResource.school_id == school_id)
    if grade_level:
        stmt = stmt.where(EducationCurriculumResource.grade_level == grade_level.strip())
    if subject_code:
        stmt = stmt.where(EducationCurriculumResource.subject_code == subject_code.strip())
    if resource_type:
        stmt = stmt.where(EducationCurriculumResource.resource_type == resource_type.strip())
    if active_only:
        stmt = stmt.where(EducationCurriculumResource.active.is_(True))
    return db.scalars(stmt).all()


def get_curriculum_resource_or_404(db: Session, resource_id: uuid.UUID) -> EducationCurriculumResource:
    row = db.get(EducationCurriculumResource, resource_id)
    if row is None:
        raise LookupError("Curriculum resource not found")
    return row


def create_curriculum_resource(
    db: Session,
    *,
    state_id: uuid.UUID | None,
    district_id: uuid.UUID | None,
    school_id: uuid.UUID | None,
    grade_level: str,
    subject_code: str,
    resource_type: str,
    title: str,
    description: str | None,
    storage_key: str | None,
    active: bool = True,
) -> EducationCurriculumResource:
    if state_id is not None:
        get_state_or_404(db, state_id)
    if district_id is not None:
        get_district_or_404(db, district_id)
    if school_id is not None:
        get_school_or_404(db, school_id)
    now = _now()
    row = EducationCurriculumResource(
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        grade_level=grade_level.strip(),
        subject_code=subject_code.strip(),
        resource_type=validate_catalog_resource_type(resource_type),
        title=title.strip(),
        description=description.strip() if description else None,
        storage_key=storage_key.strip() if storage_key else None,
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_curriculum_resource(
    db: Session,
    *,
    resource_id: uuid.UUID,
    state_id: uuid.UUID | None,
    district_id: uuid.UUID | None,
    school_id: uuid.UUID | None,
    grade_level: str,
    subject_code: str,
    resource_type: str,
    title: str,
    description: str | None,
    storage_key: str | None,
    active: bool,
) -> EducationCurriculumResource:
    row = get_curriculum_resource_or_404(db, resource_id)
    if state_id is not None:
        get_state_or_404(db, state_id)
    if district_id is not None:
        get_district_or_404(db, district_id)
    if school_id is not None:
        get_school_or_404(db, school_id)
    row.state_id = state_id
    row.district_id = district_id
    row.school_id = school_id
    row.grade_level = grade_level.strip()
    row.subject_code = subject_code.strip()
    row.resource_type = validate_catalog_resource_type(resource_type)
    row.title = title.strip()
    row.description = description.strip() if description else None
    row.storage_key = storage_key.strip() if storage_key else None
    row.active = active
    row.updated_at = _now()
    db.flush()
    return row


def list_resource_links(
    db: Session, *, curriculum_resource_id: uuid.UUID | None = None, active_only: bool = False
) -> list[EducationResourceLink]:
    stmt = select(EducationResourceLink).order_by(EducationResourceLink.link_title.asc())
    if curriculum_resource_id is not None:
        stmt = stmt.where(EducationResourceLink.curriculum_resource_id == curriculum_resource_id)
    if active_only:
        stmt = stmt.where(EducationResourceLink.active.is_(True))
    return db.scalars(stmt).all()


def create_resource_link(
    db: Session,
    *,
    curriculum_resource_id: uuid.UUID,
    link_title: str,
    url: str,
    active: bool = True,
) -> EducationResourceLink:
    get_curriculum_resource_or_404(db, curriculum_resource_id)
    now = _now()
    row = EducationResourceLink(
        curriculum_resource_id=curriculum_resource_id,
        link_title=link_title.strip(),
        url=url.strip(),
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_resource_link(
    db: Session,
    *,
    link_id: uuid.UUID,
    curriculum_resource_id: uuid.UUID,
    link_title: str,
    url: str,
    active: bool,
) -> EducationResourceLink:
    row = db.get(EducationResourceLink, link_id)
    if row is None:
        raise LookupError("Resource link not found")
    get_curriculum_resource_or_404(db, curriculum_resource_id)
    row.curriculum_resource_id = curriculum_resource_id
    row.link_title = link_title.strip()
    row.url = url.strip()
    row.active = active
    row.updated_at = _now()
    db.flush()
    return row


def list_teacher_assignments(
    db: Session, *, user_id: uuid.UUID | None = None, school_id: uuid.UUID | None = None, active_only: bool = False
) -> list[TeacherSchoolAssignment]:
    stmt = select(TeacherSchoolAssignment).order_by(TeacherSchoolAssignment.created_at.desc())
    if user_id is not None:
        stmt = stmt.where(TeacherSchoolAssignment.user_id == user_id)
    if school_id is not None:
        stmt = stmt.where(TeacherSchoolAssignment.school_id == school_id)
    if active_only:
        stmt = stmt.where(TeacherSchoolAssignment.active.is_(True))
    return db.scalars(stmt).all()


def get_teacher_assignment_or_404(db: Session, assignment_id: uuid.UUID) -> TeacherSchoolAssignment:
    row = db.get(TeacherSchoolAssignment, assignment_id)
    if row is None:
        raise LookupError("Teacher assignment not found")
    return row


def create_teacher_assignment(
    db: Session,
    *,
    user_id: uuid.UUID,
    state_id: uuid.UUID,
    district_id: uuid.UUID,
    school_id: uuid.UUID,
    active: bool = True,
) -> TeacherSchoolAssignment:
    user = db.get(User, user_id)
    if user is None:
        raise LookupError("User not found")
    get_state_or_404(db, state_id)
    district = get_district_or_404(db, district_id)
    if district.state_id != state_id:
        raise ValueError("District does not belong to the selected state")
    school = get_school_or_404(db, school_id)
    if school.district_id != district_id:
        raise ValueError("School does not belong to the selected district")
    now = _now()
    row = TeacherSchoolAssignment(
        user_id=user_id,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_teacher_assignment(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    user_id: uuid.UUID,
    state_id: uuid.UUID,
    district_id: uuid.UUID,
    school_id: uuid.UUID,
    active: bool,
) -> TeacherSchoolAssignment:
    row = get_teacher_assignment_or_404(db, assignment_id)
    user = db.get(User, user_id)
    if user is None:
        raise LookupError("User not found")
    get_state_or_404(db, state_id)
    district = get_district_or_404(db, district_id)
    if district.state_id != state_id:
        raise ValueError("District does not belong to the selected state")
    school = get_school_or_404(db, school_id)
    if school.district_id != district_id:
        raise ValueError("School does not belong to the selected district")
    row.user_id = user_id
    row.state_id = state_id
    row.district_id = district_id
    row.school_id = school_id
    row.active = active
    row.updated_at = _now()
    db.flush()
    return row


def get_active_teacher_assignment(db: Session, *, user_id: uuid.UUID) -> TeacherSchoolAssignment | None:
    return db.scalars(
        select(TeacherSchoolAssignment)
        .where(TeacherSchoolAssignment.user_id == user_id, TeacherSchoolAssignment.active.is_(True))
        .order_by(TeacherSchoolAssignment.created_at.desc())
        .limit(1)
    ).first()


def _state_by_abbreviation(db: Session, abbreviation: str) -> EducationState | None:
    return db.scalars(
        select(EducationState).where(func.upper(EducationState.abbreviation) == abbreviation.strip().upper())
    ).one_or_none()


def preview_objectives_import(db: Session, *, csv_content: str) -> CatalogImportPreviewResult:
    rows, headers = _parse_csv(csv_content)
    required = {"state_abbreviation", "grade_level", "subject_code", "objective_type", "objective_id", "description", "coverage_type"}
    if not required.issubset(set(headers)):
        missing = ", ".join(sorted(required - set(headers)))
        raise ValueError(f"CSV headers must include: {missing}")
    errors: list[CatalogImportRowError] = []
    valid_count = 0
    duplicate_count = 0
    for row in rows:
        row_number = int(row["_row_number"])
        state = _state_by_abbreviation(db, row["state_abbreviation"]) if row["state_abbreviation"] else None
        row_errors: list[CatalogImportRowError] = []
        if not row["state_abbreviation"]:
            row_errors.append(CatalogImportRowError(row_number, "State abbreviation is required.", "state_abbreviation"))
        elif state is None:
            row_errors.append(
                CatalogImportRowError(row_number, f"State '{row['state_abbreviation']}' not found.", "state_abbreviation")
            )
        for field in ("grade_level", "subject_code", "objective_id", "description"):
            if not row[field]:
                row_errors.append(CatalogImportRowError(row_number, f"{field} is required.", field))
        try:
            if row["objective_type"]:
                validate_objective_type(row["objective_type"])
        except ValueError as exc:
            row_errors.append(CatalogImportRowError(row_number, str(exc), "objective_type"))
        try:
            if row["coverage_type"]:
                validate_coverage_type(row["coverage_type"])
        except ValueError as exc:
            row_errors.append(CatalogImportRowError(row_number, str(exc), "coverage_type"))
        if row_errors:
            errors.extend(row_errors)
            continue
        assert state is not None
        existing = db.scalars(
            select(EducationObjective).where(
                EducationObjective.state_id == state.id,
                EducationObjective.objective_id == row["objective_id"].strip(),
            )
        ).one_or_none()
        if existing is not None:
            duplicate_count += 1
        else:
            valid_count += 1
    return CatalogImportPreviewResult(
        total_rows=len(rows),
        valid_count=valid_count,
        invalid_count=len({error.row_number for error in errors}),
        duplicate_count=duplicate_count,
        errors=errors,
    )


def commit_objectives_import(db: Session, *, rows: list[dict[str, str]]) -> CatalogImportCommitResult:
    created_count = 0
    skipped_duplicate_count = 0
    errors: list[CatalogImportRowError] = []
    for index, row in enumerate(rows, start=1):
        try:
            state = _state_by_abbreviation(db, row["state_abbreviation"])
            if state is None:
                raise ValueError(f"State '{row['state_abbreviation']}' not found.")
            existing = db.scalars(
                select(EducationObjective).where(
                    EducationObjective.state_id == state.id,
                    EducationObjective.objective_id == row["objective_id"].strip(),
                )
            ).one_or_none()
            if existing is not None:
                skipped_duplicate_count += 1
                continue
            create_objective(
                db,
                state_id=state.id,
                grade_level=row["grade_level"],
                subject_code=row["subject_code"],
                objective_type=row["objective_type"],
                objective_id=row["objective_id"],
                description=row["description"],
                coverage_type=row["coverage_type"],
            )
            created_count += 1
        except (LookupError, ValueError) as exc:
            errors.append(CatalogImportRowError(row_number=index, message=str(exc)))
    return CatalogImportCommitResult(
        created_count=created_count,
        skipped_duplicate_count=skipped_duplicate_count,
        errors=errors,
    )


def build_teacher_catalog_context(db: Session, *, user_id: uuid.UUID) -> dict:
    """Legacy flat snapshot used by ``/education-catalog/my-context``.

    Prefer ``catalog_access.build_catalog_context`` and ``/teacher-assist/catalog/*``
    for new browse and planning integration work.
    """
    assignment = get_active_teacher_assignment(db, user_id=user_id)
    if assignment is None:
        return {"assignment": None, "grades": [], "subjects": [], "objectives": [], "resources": []}
    state = get_state_or_404(db, assignment.state_id)
    district = get_district_or_404(db, assignment.district_id)
    school = get_school_or_404(db, assignment.school_id)
    grades = list_grades(db, school_id=school.id, active_only=True)
    grade_ids = {grade.id for grade in grades}
    if grade_ids:
        school_subjects = db.scalars(
            select(EducationSubject)
            .where(EducationSubject.grade_id.in_(grade_ids), EducationSubject.active.is_(True))
            .order_by(EducationSubject.subject_code.asc())
        ).all()
    else:
        school_subjects = []
    objectives = list_objectives(db, state_id=state.id, active_only=True)
    resources = list_curriculum_resources(db, school_id=school.id, active_only=True)
    return {
        "assignment": {
            "id": str(assignment.id),
            "state": {"id": str(state.id), "name": state.name, "abbreviation": state.abbreviation},
            "district": {"id": str(district.id), "name": district.name},
            "school": {"id": str(school.id), "name": school.name, "school_type": school.school_type},
        },
        "grades": [
            {"id": str(grade.id), "grade_code": grade.grade_code, "display_name": grade.display_name}
            for grade in grades
        ],
        "subjects": [
            {
                "id": str(subject.id),
                "grade_id": str(subject.grade_id) if subject.grade_id else None,
                "subject_code": subject.subject_code,
                "display_name": subject.display_name,
            }
            for subject in school_subjects
        ],
        "objectives": [
            {
                "id": str(objective.id),
                "objective_id": objective.objective_id,
                "grade_level": objective.grade_level,
                "subject_code": objective.subject_code,
                "description": objective.description,
                "coverage_type": objective.coverage_type,
                "objective_type": objective.objective_type,
            }
            for objective in objectives
        ],
        "resources": [
            {
                "id": str(resource.id),
                "title": resource.title,
                "resource_type": resource.resource_type,
                "grade_level": resource.grade_level,
                "subject_code": resource.subject_code,
                "description": resource.description,
            }
            for resource in resources
        ],
    }

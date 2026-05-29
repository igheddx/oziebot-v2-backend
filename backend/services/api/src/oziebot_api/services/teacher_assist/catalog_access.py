"""Centralized TeacherAssist education catalog access and inheritance."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import math
import uuid

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationCurriculumResource,
    EducationGrade,
    EducationObjective,
    EducationObjectiveResourceMapping,
    EducationResourceLink,
    EducationSubject,
    TeacherSchoolAssignment,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.education_catalog import (
    get_district_or_404,
    get_school_or_404,
    get_state_or_404,
    list_grades,
)

logger = logging.getLogger(__name__)

CATALOG_ACCESS_EVENT_TYPES = (
    "catalog_access_failed",
    "catalog_assignment_missing",
    "catalog_multiple_assignments_detected",
)

DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 100


@dataclass(frozen=True)
class CatalogPageMeta:
    page: int
    page_size: int
    total: int
    total_pages: int


@dataclass(frozen=True)
class CatalogAccessScope:
    assignment_id: uuid.UUID | None
    state_id: uuid.UUID | None
    district_id: uuid.UUID | None
    school_id: uuid.UUID | None
    grade_ids: tuple[uuid.UUID, ...]
    grade_codes: tuple[str, ...]
    subject_codes: tuple[str, ...]
    missing_assignment: bool
    multiple_assignments_detected: bool
    is_root_unscoped: bool


class CatalogAccessError(Exception):
    def __init__(self, message: str, *, code: str = "catalog_access_failed") -> None:
        super().__init__(message)
        self.code = code


def _clamp_page_size(page_size: int) -> int:
    return max(1, min(page_size, MAX_PAGE_SIZE))


def _page_meta(*, page: int, page_size: int, total: int) -> CatalogPageMeta:
    safe_page = max(1, page)
    safe_page_size = _clamp_page_size(page_size)
    total_pages = max(1, math.ceil(total / safe_page_size)) if total else 0
    return CatalogPageMeta(
        page=safe_page,
        page_size=safe_page_size,
        total=total,
        total_pages=total_pages if total else 0,
    )


def _list_active_assignments(db: Session, *, user_id: uuid.UUID) -> list[TeacherSchoolAssignment]:
    return db.scalars(
        select(TeacherSchoolAssignment)
        .where(TeacherSchoolAssignment.user_id == user_id, TeacherSchoolAssignment.active.is_(True))
        .order_by(TeacherSchoolAssignment.created_at.desc())
    ).all()


def _record_catalog_audit(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    event_type: str,
    summary_text: str,
    details_json: dict | None = None,
) -> None:
    if event_type not in CATALOG_ACCESS_EVENT_TYPES:
        raise ValueError(f"Unsupported catalog audit event type '{event_type}'")
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type=event_type,
        event_category="system",
        entity_type="education_catalog",
        entity_id=user_id,
        summary_text=summary_text,
        details_json=details_json,
    )


def resolve_catalog_access_scope(
    db: Session,
    *,
    user: User,
    tenant_id: uuid.UUID,
    is_root_admin: bool,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
    audit: bool = True,
) -> CatalogAccessScope:
    if is_root_admin and state_id is None and district_id is None and school_id is None:
        return CatalogAccessScope(
            assignment_id=None,
            state_id=None,
            district_id=None,
            school_id=None,
            grade_ids=(),
            grade_codes=(),
            subject_codes=(),
            missing_assignment=False,
            multiple_assignments_detected=False,
            is_root_unscoped=True,
        )

    if is_root_admin:
        resolved_state_id = state_id
        resolved_district_id = district_id
        resolved_school_id = school_id
        if resolved_school_id is not None:
            school = get_school_or_404(db, resolved_school_id)
            resolved_district_id = school.district_id
            district = get_district_or_404(db, school.district_id)
            resolved_state_id = district.state_id
        elif resolved_district_id is not None:
            district = get_district_or_404(db, resolved_district_id)
            resolved_state_id = district.state_id
        grades = list_grades(db, school_id=resolved_school_id, active_only=True) if resolved_school_id else []
        grade_ids = tuple(grade.id for grade in grades)
        grade_codes = tuple(grade.grade_code for grade in grades)
        subject_codes: tuple[str, ...] = ()
        if grade_ids:
            subjects = db.scalars(
                select(EducationSubject.subject_code)
                .where(EducationSubject.grade_id.in_(grade_ids), EducationSubject.active.is_(True))
                .distinct()
                .order_by(EducationSubject.subject_code.asc())
            ).all()
            subject_codes = tuple(subjects)
        return CatalogAccessScope(
            assignment_id=None,
            state_id=resolved_state_id,
            district_id=resolved_district_id,
            school_id=resolved_school_id,
            grade_ids=grade_ids,
            grade_codes=grade_codes,
            subject_codes=subject_codes,
            missing_assignment=False,
            multiple_assignments_detected=False,
            is_root_unscoped=False,
        )

    assignments = _list_active_assignments(db, user_id=user.id)
    multiple_assignments = len(assignments) > 1
    assignment = assignments[0] if assignments else None

    if multiple_assignments and audit:
        assignment_ids = [str(row.id) for row in assignments]
        logger.warning(
            "Multiple active teacher school assignments detected for user_id=%s assignment_ids=%s",
            user.id,
            assignment_ids,
        )
        _record_catalog_audit(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            event_type="catalog_multiple_assignments_detected",
            summary_text="Multiple active school assignments detected for teacher catalog access.",
            details_json={"assignment_ids": assignment_ids, "selected_assignment_id": str(assignment.id)},
        )

    if assignment is None:
        if audit:
            _record_catalog_audit(
                db,
                tenant_id=tenant_id,
                user_id=user.id,
                event_type="catalog_assignment_missing",
                summary_text="Teacher catalog access attempted without an active school assignment.",
                details_json={"user_id": str(user.id)},
            )
        return CatalogAccessScope(
            assignment_id=None,
            state_id=None,
            district_id=None,
            school_id=None,
            grade_ids=(),
            grade_codes=(),
            subject_codes=(),
            missing_assignment=True,
            multiple_assignments_detected=False,
            is_root_unscoped=False,
        )

    grades = list_grades(db, school_id=assignment.school_id, active_only=True)
    grade_ids = tuple(grade.id for grade in grades)
    grade_codes = tuple(grade.grade_code for grade in grades)
    subject_codes: tuple[str, ...] = ()
    if grade_ids:
        subjects = db.scalars(
            select(EducationSubject.subject_code)
            .where(EducationSubject.grade_id.in_(grade_ids), EducationSubject.active.is_(True))
            .distinct()
            .order_by(EducationSubject.subject_code.asc())
        ).all()
        subject_codes = tuple(subjects)

    return CatalogAccessScope(
        assignment_id=assignment.id,
        state_id=assignment.state_id,
        district_id=assignment.district_id,
        school_id=assignment.school_id,
        grade_ids=grade_ids,
        grade_codes=grade_codes,
        subject_codes=subject_codes,
        missing_assignment=False,
        multiple_assignments_detected=multiple_assignments,
        is_root_unscoped=False,
    )


def require_catalog_browse_scope(
    db: Session,
    *,
    user: User,
    tenant_id: uuid.UUID,
    is_root_admin: bool,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
    audit: bool = True,
) -> CatalogAccessScope:
    scope = resolve_catalog_access_scope(
        db,
        user=user,
        tenant_id=tenant_id,
        is_root_admin=is_root_admin,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        audit=audit,
    )
    if scope.missing_assignment:
        if audit:
            _record_catalog_audit(
                db,
                tenant_id=tenant_id,
                user_id=user.id,
                event_type="catalog_access_failed",
                summary_text="Catalog browse denied because no active school assignment exists.",
                details_json={"reason": "missing_assignment"},
            )
        raise CatalogAccessError(
            "No school assignment found. Please contact an administrator.",
            code="missing_assignment",
        )
    return scope


def _scope_labels(db: Session, scope: CatalogAccessScope) -> dict[str, str | None]:
    state_name = None
    district_name = None
    school_name = None
    if scope.state_id is not None:
        state_name = get_state_or_404(db, scope.state_id).name
    if scope.district_id is not None:
        district_name = get_district_or_404(db, scope.district_id).name
    if scope.school_id is not None:
        school_name = get_school_or_404(db, scope.school_id).name
    return {
        "state_name": state_name,
        "district_name": district_name,
        "school_name": school_name,
    }


def _scope_banner(*, is_root_admin: bool, scope: CatalogAccessScope, labels: dict[str, str | None]) -> str | None:
    if is_root_admin and scope.is_root_unscoped:
        return "Root admin view: all catalog data"
    parts = [labels["state_name"], labels["district_name"], labels["school_name"]]
    parts = [part for part in parts if part]
    if not parts:
        return None
    prefix = "Root admin view" if is_root_admin else "Catalog scope"
    return f"{prefix}: {' → '.join(parts)}"


def build_catalog_context(
    db: Session,
    *,
    user: User,
    tenant_id: uuid.UUID,
    is_root_admin: bool,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
) -> dict:
    scope = resolve_catalog_access_scope(
        db,
        user=user,
        tenant_id=tenant_id,
        is_root_admin=is_root_admin,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
    )

    assignment_payload = None
    if scope.is_root_unscoped:
        can_browse = True
    elif scope.missing_assignment:
        can_browse = False
    else:
        can_browse = True
        if scope.state_id is not None and scope.district_id is not None and scope.school_id is not None:
            state = get_state_or_404(db, scope.state_id)
            district = get_district_or_404(db, scope.district_id)
            school = get_school_or_404(db, scope.school_id)
            assignment_payload = {
                "id": str(scope.assignment_id) if scope.assignment_id else None,
                "state": {"id": str(state.id), "name": state.name, "abbreviation": state.abbreviation},
                "district": {"id": str(district.id), "name": district.name},
                "school": {"id": str(school.id), "name": school.name, "school_type": school.school_type},
            }

    labels = _scope_labels(db, scope)
    scope_banner = _scope_banner(is_root_admin=is_root_admin, scope=scope, labels=labels)

    return {
        "assignment": assignment_payload,
        "missing_assignment": scope.missing_assignment,
        "multiple_assignments_detected": scope.multiple_assignments_detected,
        "can_browse": can_browse,
        "is_root_unscoped": scope.is_root_unscoped,
        "scope_filters": {
            "state_id": str(scope.state_id) if scope.state_id else None,
            "district_id": str(scope.district_id) if scope.district_id else None,
            "school_id": str(scope.school_id) if scope.school_id else None,
        },
        "scope_labels": labels,
        "scope_banner": scope_banner,
    }


def _resource_scope_filter(scope: CatalogAccessScope):
    if scope.is_root_unscoped:
        return None
    options = []
    if scope.school_id is not None:
        options.append(EducationCurriculumResource.school_id == scope.school_id)
    if scope.district_id is not None:
        options.append(
            and_(
                EducationCurriculumResource.school_id.is_(None),
                EducationCurriculumResource.district_id == scope.district_id,
            )
        )
    if scope.state_id is not None:
        options.append(
            and_(
                EducationCurriculumResource.school_id.is_(None),
                EducationCurriculumResource.district_id.is_(None),
                EducationCurriculumResource.state_id == scope.state_id,
            )
        )
    if not options:
        return None
    return or_(*options)


def list_catalog_grades(
    db: Session,
    *,
    scope: CatalogAccessScope,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> tuple[list[dict], CatalogPageMeta]:
    stmt = select(EducationGrade).where(EducationGrade.active.is_(True))
    if not scope.is_root_unscoped:
        if scope.school_id is not None:
            stmt = stmt.where(EducationGrade.school_id == scope.school_id)
        elif scope.grade_ids:
            stmt = stmt.where(EducationGrade.id.in_(scope.grade_ids))
        else:
            return [], _page_meta(page=page, page_size=page_size, total=0)

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = db.scalar(count_stmt) or 0
    meta = _page_meta(page=page, page_size=page_size, total=total)
    rows = db.scalars(
        stmt.order_by(EducationGrade.grade_code.asc())
        .offset((meta.page - 1) * meta.page_size)
        .limit(meta.page_size)
    ).all()

    grade_ids = [row.id for row in rows]
    subject_counts: dict[uuid.UUID, int] = {}
    if grade_ids:
        counts = db.execute(
            select(EducationSubject.grade_id, func.count())
            .where(EducationSubject.grade_id.in_(grade_ids), EducationSubject.active.is_(True))
            .group_by(EducationSubject.grade_id)
        ).all()
        subject_counts = {grade_id: count for grade_id, count in counts}

    items = [
        {
            "id": str(row.id),
            "grade_code": row.grade_code,
            "display_name": row.display_name,
            "active": row.active,
            "subject_count": subject_counts.get(row.id, 0),
        }
        for row in rows
    ]
    return items, meta


def list_catalog_subjects(
    db: Session,
    *,
    scope: CatalogAccessScope,
    grade_id: uuid.UUID | None = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> tuple[list[dict], CatalogPageMeta]:
    stmt = select(EducationSubject).where(EducationSubject.active.is_(True))
    if grade_id is not None:
        stmt = stmt.where(EducationSubject.grade_id == grade_id)
    elif not scope.is_root_unscoped:
        if scope.grade_ids:
            stmt = stmt.where(EducationSubject.grade_id.in_(scope.grade_ids))
        else:
            return [], _page_meta(page=page, page_size=page_size, total=0)

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = db.scalar(count_stmt) or 0
    meta = _page_meta(page=page, page_size=page_size, total=total)
    rows = db.scalars(
        stmt.order_by(EducationSubject.subject_code.asc())
        .offset((meta.page - 1) * meta.page_size)
        .limit(meta.page_size)
    ).all()

    grade_code_by_id: dict[uuid.UUID, str] = {}
    grade_ids = {row.grade_id for row in rows if row.grade_id is not None}
    if grade_ids:
        grades = db.scalars(select(EducationGrade).where(EducationGrade.id.in_(grade_ids))).all()
        grade_code_by_id = {grade.id: grade.grade_code for grade in grades}

    items: list[dict] = []
    for row in rows:
        grade_code = grade_code_by_id.get(row.grade_id) if row.grade_id else None
        objective_count = 0
        resource_count = 0
        if scope.state_id and grade_code:
            objective_count = (
                db.scalar(
                    select(func.count())
                    .select_from(EducationObjective)
                    .where(
                        EducationObjective.state_id == scope.state_id,
                        EducationObjective.active.is_(True),
                        EducationObjective.grade_level == grade_code,
                        EducationObjective.subject_code == row.subject_code,
                    )
                )
                or 0
            )
        resource_stmt = select(func.count()).select_from(EducationCurriculumResource).where(
            EducationCurriculumResource.active.is_(True),
            EducationCurriculumResource.grade_level == (grade_code or ""),
            EducationCurriculumResource.subject_code == row.subject_code,
        )
        resource_filter = _resource_scope_filter(scope)
        if resource_filter is not None:
            resource_stmt = resource_stmt.where(resource_filter)
        if grade_code:
            resource_count = db.scalar(resource_stmt) or 0

        items.append(
            {
                "id": str(row.id),
                "grade_id": str(row.grade_id) if row.grade_id else None,
                "grade_code": grade_code,
                "subject_code": row.subject_code,
                "display_name": row.display_name,
                "active": row.active,
                "objective_count": objective_count,
                "resource_count": resource_count,
            }
        )
    return items, meta


def list_catalog_objectives(
    db: Session,
    *,
    scope: CatalogAccessScope,
    grade_level: str | None = None,
    subject_code: str | None = None,
    objective_type: str | None = None,
    coverage_type: str | None = None,
    q: str | None = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> tuple[list[dict], CatalogPageMeta]:
    stmt = select(EducationObjective).where(EducationObjective.active.is_(True))
    if not scope.is_root_unscoped:
        if scope.state_id is None:
            return [], _page_meta(page=page, page_size=page_size, total=0)
        stmt = stmt.where(EducationObjective.state_id == scope.state_id)
        if scope.grade_codes:
            stmt = stmt.where(EducationObjective.grade_level.in_(scope.grade_codes))
        if scope.subject_codes:
            stmt = stmt.where(EducationObjective.subject_code.in_(scope.subject_codes))
    if grade_level:
        stmt = stmt.where(EducationObjective.grade_level == grade_level.strip())
    if subject_code:
        stmt = stmt.where(EducationObjective.subject_code == subject_code.strip())
    if objective_type:
        stmt = stmt.where(EducationObjective.objective_type == objective_type.strip())
    if coverage_type:
        stmt = stmt.where(EducationObjective.coverage_type == coverage_type.strip())
    if q:
        lowered = q.strip().lower()
        stmt = stmt.where(
            func.lower(EducationObjective.objective_id).contains(lowered)
            | func.lower(EducationObjective.description).contains(lowered)
        )

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = db.scalar(count_stmt) or 0
    meta = _page_meta(page=page, page_size=page_size, total=total)
    rows = db.scalars(
        stmt.order_by(EducationObjective.grade_level.asc(), EducationObjective.objective_id.asc())
        .offset((meta.page - 1) * meta.page_size)
        .limit(meta.page_size)
    ).all()

    objective_ids = [row.id for row in rows]
    resources_by_objective: dict[uuid.UUID, list[dict]] = {objective_id: [] for objective_id in objective_ids}
    if objective_ids:
        mappings = db.scalars(
            select(EducationObjectiveResourceMapping).where(
                EducationObjectiveResourceMapping.objective_id.in_(objective_ids)
            )
        ).all()
        resource_ids = {mapping.resource_id for mapping in mappings}
        resources_by_id: dict[uuid.UUID, EducationCurriculumResource] = {}
        links_by_resource: dict[uuid.UUID, list[dict]] = {}
        if resource_ids:
            resources = db.scalars(
                select(EducationCurriculumResource).where(
                    EducationCurriculumResource.id.in_(resource_ids),
                    EducationCurriculumResource.active.is_(True),
                )
            ).all()
            resources_by_id = {resource.id: resource for resource in resources}
            links = db.scalars(
                select(EducationResourceLink).where(
                    EducationResourceLink.curriculum_resource_id.in_(resource_ids),
                    EducationResourceLink.active.is_(True),
                )
            ).all()
            for link in links:
                links_by_resource.setdefault(link.curriculum_resource_id, []).append(
                    {"id": str(link.id), "link_title": link.link_title, "url": link.url, "active": link.active}
                )
        for mapping in mappings:
            resource = resources_by_id.get(mapping.resource_id)
            if resource is None:
                continue
            resources_by_objective[mapping.objective_id].append(
                {
                    "id": str(resource.id),
                    "title": resource.title,
                    "resource_type": resource.resource_type,
                    "reference_links": links_by_resource.get(resource.id, []),
                }
            )

    items = [
        {
            "id": str(row.id),
            "objective_id": row.objective_id,
            "objective_type": row.objective_type,
            "description": row.description,
            "coverage_type": row.coverage_type,
            "grade_level": row.grade_level,
            "subject_code": row.subject_code,
            "active": row.active,
            "linked_resources": resources_by_objective.get(row.id, []),
        }
        for row in rows
    ]
    return items, meta


def list_catalog_resources(
    db: Session,
    *,
    scope: CatalogAccessScope,
    grade_level: str | None = None,
    subject_code: str | None = None,
    resource_type: str | None = None,
    q: str | None = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> tuple[list[dict], CatalogPageMeta]:
    stmt = select(EducationCurriculumResource).where(EducationCurriculumResource.active.is_(True))
    resource_filter = _resource_scope_filter(scope)
    if resource_filter is not None:
        stmt = stmt.where(resource_filter)
    if grade_level:
        stmt = stmt.where(EducationCurriculumResource.grade_level == grade_level.strip())
    if subject_code:
        stmt = stmt.where(EducationCurriculumResource.subject_code == subject_code.strip())
    if resource_type:
        stmt = stmt.where(EducationCurriculumResource.resource_type == resource_type.strip())
    if q:
        lowered = q.strip().lower()
        stmt = stmt.where(
            func.lower(EducationCurriculumResource.title).contains(lowered)
            | func.lower(func.coalesce(EducationCurriculumResource.description, "")).contains(lowered)
        )

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = db.scalar(count_stmt) or 0
    meta = _page_meta(page=page, page_size=page_size, total=total)
    rows = db.scalars(
        stmt.order_by(EducationCurriculumResource.title.asc())
        .offset((meta.page - 1) * meta.page_size)
        .limit(meta.page_size)
    ).all()

    resource_ids = [row.id for row in rows]
    links_by_resource: dict[uuid.UUID, list[dict]] = {resource_id: [] for resource_id in resource_ids}
    objectives_by_resource: dict[uuid.UUID, list[dict]] = {resource_id: [] for resource_id in resource_ids}

    if resource_ids:
        links = db.scalars(
            select(EducationResourceLink).where(
                EducationResourceLink.curriculum_resource_id.in_(resource_ids),
                EducationResourceLink.active.is_(True),
            )
        ).all()
        for link in links:
            links_by_resource[link.curriculum_resource_id].append(
                {"id": str(link.id), "link_title": link.link_title, "url": link.url, "active": link.active}
            )

        mappings = db.scalars(
            select(EducationObjectiveResourceMapping).where(
                EducationObjectiveResourceMapping.resource_id.in_(resource_ids)
            )
        ).all()
        objective_ids = {mapping.objective_id for mapping in mappings}
        objectives_by_id: dict[uuid.UUID, EducationObjective] = {}
        if objective_ids:
            objectives = db.scalars(select(EducationObjective).where(EducationObjective.id.in_(objective_ids))).all()
            objectives_by_id = {objective.id: objective for objective in objectives}
        for mapping in mappings:
            objective = objectives_by_id.get(mapping.objective_id)
            if objective is None:
                continue
            objectives_by_resource[mapping.resource_id].append(
                {
                    "id": str(objective.id),
                    "objective_id": objective.objective_id,
                    "objective_type": objective.objective_type,
                    "coverage_type": objective.coverage_type,
                    "grade_level": objective.grade_level,
                    "subject_code": objective.subject_code,
                }
            )

    items = [
        {
            "id": str(row.id),
            "title": row.title,
            "resource_type": row.resource_type,
            "description": row.description,
            "grade_level": row.grade_level,
            "subject_code": row.subject_code,
            "storage_key": row.storage_key,
            "active": row.active,
            "reference_links": links_by_resource.get(row.id, []),
            "associated_objectives": objectives_by_resource.get(row.id, []),
        }
        for row in rows
    ]
    return items, meta

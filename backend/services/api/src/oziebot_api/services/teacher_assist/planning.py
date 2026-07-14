from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
import uuid
from urllib.parse import urlparse

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_class_subject import TeacherAssistClassSubject
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_item import TeacherAssistPacingItem
from oziebot_api.models.teacher_assist_pacing_item_resource import TeacherAssistPacingItemResource
from oziebot_api.models.teacher_assist_pacing_item_standard import TeacherAssistPacingItemStandard
from oziebot_api.models.teacher_assist_planning_input_draft import TeacherAssistPlanningInputDraft
from oziebot_api.models.teacher_assist_planning_input_draft_pacing_item import (
    TeacherAssistPlanningInputDraftPacingItem,
)
from oziebot_api.models.teacher_assist_planning_input_draft_resource import (
    TeacherAssistPlanningInputDraftResource,
)
from oziebot_api.models.teacher_assist_planning_input_draft_standard import (
    TeacherAssistPlanningInputDraftStandard,
)
from oziebot_api.models.teacher_assist_planning_input_draft_subject import (
    TeacherAssistPlanningInputDraftSubject,
)
from oziebot_api.models.teacher_assist_resource_library_item import TeacherAssistResourceLibraryItem
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.constants import (
    validate_planning_scope,
    validate_grade_level,
    validate_planning_draft_status,
    validate_resource_type,
)
from oziebot_api.services.teacher_assist.setup import (
    get_class_or_404,
    get_grading_period_or_404,
    get_school_year_or_404,
    get_subject_or_404,
)


@dataclass(frozen=True)
class ResourceLinkCounts:
    pacing_items: dict[uuid.UUID, int]
    planning_drafts: dict[uuid.UUID, int]


@dataclass(frozen=True)
class PlanningDraftReadiness:
    is_ready: bool
    missing_items: list[str]
    warnings: list[str]


@dataclass(frozen=True)
class PlanningDurationSummary:
    start_date: date | None
    end_date: date | None
    estimated_weeks: int | None
    instructional_days_count: int | None
    summary: str


@dataclass(frozen=True)
class PlanningPacingGroup:
    group_key: str
    label: str
    pacing_items: list[TeacherAssistPacingItem]


@dataclass(frozen=True)
class PlanningDraftContextPreview:
    draft: TeacherAssistPlanningInputDraft
    subjects: list[TeacherAssistSubject]
    pacing_items: list[TeacherAssistPacingItem]
    standards: list[TeacherAssistStandard]
    resources: list[TeacherAssistResourceLibraryItem]
    duration_summary: PlanningDurationSummary
    pacing_groups: list[PlanningPacingGroup]
    readiness: PlanningDraftReadiness


def list_pacing_guides(db: Session, *, tenant_id: uuid.UUID) -> list[TeacherAssistPacingGuide]:
    return db.scalars(
        select(TeacherAssistPacingGuide)
        .where(TeacherAssistPacingGuide.tenant_id == tenant_id)
        .order_by(
            TeacherAssistPacingGuide.updated_at.desc(), TeacherAssistPacingGuide.created_at.desc()
        )
    ).all()


def get_pacing_guide_or_404(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID
) -> TeacherAssistPacingGuide:
    row = db.scalars(
        select(TeacherAssistPacingGuide).where(
            TeacherAssistPacingGuide.id == pacing_guide_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Pacing guide not found")
    return row


def create_pacing_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    created_by_user: User,
    school_year_id: uuid.UUID,
    title: str,
    description: str | None,
    grade_level: str | None,
    subject_id: uuid.UUID | None,
    is_shared: bool,
) -> TeacherAssistPacingGuide:
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    normalized_grade_level = validate_grade_level(grade_level)
    if subject_id is not None:
        get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    now = datetime.now(UTC)
    row = TeacherAssistPacingGuide(
        tenant_id=tenant_id,
        school_year_id=school_year.id,
        title=title.strip(),
        description=description.strip() if description else None,
        grade_level=normalized_grade_level,
        subject_id=subject_id,
        is_shared=is_shared,
        created_by_user_id=created_by_user.id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_pacing_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
    school_year_id: uuid.UUID,
    title: str,
    description: str | None,
    grade_level: str | None,
    subject_id: uuid.UUID | None,
    is_shared: bool,
) -> TeacherAssistPacingGuide:
    row = get_pacing_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    normalized_grade_level = validate_grade_level(grade_level)
    if subject_id is not None:
        get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    row.school_year_id = school_year.id
    row.title = title.strip()
    row.description = description.strip() if description else None
    row.grade_level = normalized_grade_level
    row.subject_id = subject_id
    row.is_shared = is_shared
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row


def list_pacing_items(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID
) -> list[TeacherAssistPacingItem]:
    guide = get_pacing_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    rows = db.scalars(
        select(TeacherAssistPacingItem).where(TeacherAssistPacingItem.pacing_guide_id == guide.id)
    ).all()
    return sorted(
        rows,
        key=lambda row: (
            row.sort_order if row.sort_order is not None else 1_000_000,
            row.week_number if row.week_number is not None else 1_000_000,
            row.day_number if row.day_number is not None else 1_000_000,
            row.instructional_date or date.max,
            row.created_at,
        ),
    )


def get_pacing_item_or_404(
    db: Session, *, tenant_id: uuid.UUID, pacing_item_id: uuid.UUID
) -> TeacherAssistPacingItem:
    row = db.scalars(
        select(TeacherAssistPacingItem)
        .join(
            TeacherAssistPacingGuide,
            TeacherAssistPacingGuide.id == TeacherAssistPacingItem.pacing_guide_id,
        )
        .where(
            TeacherAssistPacingItem.id == pacing_item_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Pacing item not found")
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


def _next_pacing_item_sort_order(db: Session, *, pacing_guide_id: uuid.UUID) -> int:
    existing = db.scalars(
        select(TeacherAssistPacingItem).where(
            TeacherAssistPacingItem.pacing_guide_id == pacing_guide_id
        )
    ).all()
    if not existing:
        return 0
    current = [row.sort_order for row in existing if row.sort_order is not None]
    return (max(current) + 1) if current else len(existing)


def _validate_instructional_date_within_school_year(
    school_year_id: uuid.UUID, *, tenant_id: uuid.UUID, db: Session, instructional_date: date | None
) -> None:
    if instructional_date is None:
        return
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    if instructional_date < school_year.start_date or instructional_date > school_year.end_date:
        raise ValueError("Instructional date must fall within the school year")


def _validate_pacing_item_relations(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    guide: TeacherAssistPacingGuide,
    grading_period_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
    instructional_date: date | None,
) -> None:
    _validate_instructional_date_within_school_year(
        guide.school_year_id,
        tenant_id=tenant_id,
        db=db,
        instructional_date=instructional_date,
    )
    if subject_id is not None:
        get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    if grading_period_id is None:
        return
    grading_period = get_grading_period_or_404(
        db, tenant_id=tenant_id, grading_period_id=grading_period_id
    )
    if grading_period.school_year_id != guide.school_year_id:
        raise ValueError("Grading period must belong to the same school year as the pacing guide")


def create_pacing_item(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
    week_number: int | None,
    day_number: int | None,
    instructional_date: date | None,
    title: str,
    instructional_focus: str | None,
    objectives: str | None,
    notes: str | None,
    sort_order: int | None,
) -> TeacherAssistPacingItem:
    guide = get_pacing_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    _validate_pacing_item_relations(
        db,
        tenant_id=tenant_id,
        guide=guide,
        grading_period_id=grading_period_id,
        subject_id=subject_id,
        instructional_date=instructional_date,
    )
    now = datetime.now(UTC)
    row = TeacherAssistPacingItem(
        pacing_guide_id=guide.id,
        grading_period_id=grading_period_id,
        subject_id=subject_id,
        week_number=week_number,
        day_number=day_number,
        instructional_date=instructional_date,
        title=title.strip(),
        instructional_focus=instructional_focus.strip() if instructional_focus else None,
        objectives=objectives.strip() if objectives else None,
        notes=notes.strip() if notes else None,
        sort_order=sort_order
        if sort_order is not None
        else _next_pacing_item_sort_order(db, pacing_guide_id=guide.id),
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_pacing_item(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_item_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
    week_number: int | None,
    day_number: int | None,
    instructional_date: date | None,
    title: str,
    instructional_focus: str | None,
    objectives: str | None,
    notes: str | None,
    sort_order: int | None,
) -> TeacherAssistPacingItem:
    row = get_pacing_item_or_404(db, tenant_id=tenant_id, pacing_item_id=pacing_item_id)
    guide = get_pacing_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=row.pacing_guide_id)
    _validate_pacing_item_relations(
        db,
        tenant_id=tenant_id,
        guide=guide,
        grading_period_id=grading_period_id,
        subject_id=subject_id,
        instructional_date=instructional_date,
    )
    row.grading_period_id = grading_period_id
    row.subject_id = subject_id
    row.week_number = week_number
    row.day_number = day_number
    row.instructional_date = instructional_date
    row.title = title.strip()
    row.instructional_focus = instructional_focus.strip() if instructional_focus else None
    row.objectives = objectives.strip() if objectives else None
    row.notes = notes.strip() if notes else None
    row.sort_order = sort_order
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row


def list_pacing_item_standards(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID | None = None
) -> list[TeacherAssistPacingItemStandard]:
    stmt = (
        select(TeacherAssistPacingItemStandard)
        .join(
            TeacherAssistPacingItem,
            TeacherAssistPacingItem.id == TeacherAssistPacingItemStandard.pacing_item_id,
        )
        .join(
            TeacherAssistPacingGuide,
            TeacherAssistPacingGuide.id == TeacherAssistPacingItem.pacing_guide_id,
        )
        .where(TeacherAssistPacingGuide.tenant_id == tenant_id)
        .order_by(TeacherAssistPacingItemStandard.created_at.asc())
    )
    if pacing_guide_id is not None:
        stmt = stmt.where(TeacherAssistPacingItem.pacing_guide_id == pacing_guide_id)
    return db.scalars(stmt).all()


def attach_pacing_item_standard(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_item_id: uuid.UUID,
    standard_id: uuid.UUID,
) -> TeacherAssistPacingItemStandard:
    pacing_item = get_pacing_item_or_404(db, tenant_id=tenant_id, pacing_item_id=pacing_item_id)
    standard = db.scalars(
        select(TeacherAssistStandard).where(
            TeacherAssistStandard.id == standard_id,
            TeacherAssistStandard.tenant_id == tenant_id,
        )
    ).one_or_none()
    if standard is None:
        raise LookupError("Standard not found")
    guide = get_pacing_guide_or_404(
        db, tenant_id=tenant_id, pacing_guide_id=pacing_item.pacing_guide_id
    )
    if standard.school_year_id is not None and standard.school_year_id != guide.school_year_id:
        raise ValueError("Standard must belong to the same school year as the pacing guide")
    if (
        pacing_item.subject_id is not None
        and standard.subject_id is not None
        and standard.subject_id != pacing_item.subject_id
    ):
        raise ValueError("Standard subject must match the pacing item subject")
    existing = db.scalars(
        select(TeacherAssistPacingItemStandard).where(
            TeacherAssistPacingItemStandard.pacing_item_id == pacing_item.id,
            TeacherAssistPacingItemStandard.standard_id == standard.id,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    row = TeacherAssistPacingItemStandard(
        pacing_item_id=pacing_item.id,
        standard_id=standard.id,
        created_at=datetime.now(UTC),
    )
    db.add(row)
    db.flush()
    return row


def list_resources(db: Session, *, tenant_id: uuid.UUID) -> list[TeacherAssistResourceLibraryItem]:
    return db.scalars(
        select(TeacherAssistResourceLibraryItem)
        .where(TeacherAssistResourceLibraryItem.tenant_id == tenant_id)
        .order_by(
            TeacherAssistResourceLibraryItem.uploaded_at.desc(),
            TeacherAssistResourceLibraryItem.created_at.desc(),
        )
    ).all()


def get_resource_or_404(
    db: Session, *, tenant_id: uuid.UUID, resource_id: uuid.UUID
) -> TeacherAssistResourceLibraryItem:
    row = db.scalars(
        select(TeacherAssistResourceLibraryItem).where(
            TeacherAssistResourceLibraryItem.id == resource_id,
            TeacherAssistResourceLibraryItem.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Resource not found")
    return row


def create_link_resource(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    uploaded_by_user: User,
    title: str,
    description: str | None,
    external_url: str,
) -> TeacherAssistResourceLibraryItem:
    parsed = urlparse(external_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("External URL must be a valid http or https URL")
    now = datetime.now(UTC)
    row = TeacherAssistResourceLibraryItem(
        tenant_id=tenant_id,
        uploaded_by_user_id=uploaded_by_user.id,
        title=title.strip(),
        description=description.strip() if description else None,
        resource_type="link",
        storage_key=None,
        original_filename=None,
        mime_type=None,
        file_size=None,
        external_url=external_url.strip(),
        uploaded_at=now,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def create_uploaded_resource(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    uploaded_by_user: User,
    title: str | None,
    description: str | None,
    resource_type: str,
    storage_key: str,
    original_filename: str,
    mime_type: str,
    file_size: int,
) -> TeacherAssistResourceLibraryItem:
    normalized_resource_type = validate_resource_type(resource_type, required=True) or "other"
    now = datetime.now(UTC)
    row = TeacherAssistResourceLibraryItem(
        tenant_id=tenant_id,
        uploaded_by_user_id=uploaded_by_user.id,
        title=(title.strip() if title else original_filename),
        description=description.strip() if description else None,
        resource_type=normalized_resource_type,
        storage_key=storage_key,
        original_filename=original_filename,
        mime_type=mime_type,
        file_size=file_size,
        external_url=None,
        uploaded_at=now,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def list_pacing_item_resources(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID | None = None
) -> list[TeacherAssistPacingItemResource]:
    stmt = (
        select(TeacherAssistPacingItemResource)
        .join(
            TeacherAssistPacingItem,
            TeacherAssistPacingItem.id == TeacherAssistPacingItemResource.pacing_item_id,
        )
        .join(
            TeacherAssistPacingGuide,
            TeacherAssistPacingGuide.id == TeacherAssistPacingItem.pacing_guide_id,
        )
        .where(TeacherAssistPacingGuide.tenant_id == tenant_id)
        .order_by(TeacherAssistPacingItemResource.created_at.asc())
    )
    if pacing_guide_id is not None:
        stmt = stmt.where(TeacherAssistPacingItem.pacing_guide_id == pacing_guide_id)
    return db.scalars(stmt).all()


def attach_pacing_item_resource(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_item_id: uuid.UUID,
    resource_library_item_id: uuid.UUID,
) -> TeacherAssistPacingItemResource:
    pacing_item = get_pacing_item_or_404(db, tenant_id=tenant_id, pacing_item_id=pacing_item_id)
    resource = get_resource_or_404(db, tenant_id=tenant_id, resource_id=resource_library_item_id)
    existing = db.scalars(
        select(TeacherAssistPacingItemResource).where(
            TeacherAssistPacingItemResource.pacing_item_id == pacing_item.id,
            TeacherAssistPacingItemResource.resource_library_item_id == resource.id,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    row = TeacherAssistPacingItemResource(
        pacing_item_id=pacing_item.id,
        resource_library_item_id=resource.id,
        created_at=datetime.now(UTC),
    )
    db.add(row)
    db.flush()
    return row


def list_resource_link_counts(db: Session, *, tenant_id: uuid.UUID) -> ResourceLinkCounts:
    pacing_counts = dict(
        db.execute(
            select(
                TeacherAssistPacingItemResource.resource_library_item_id,
                func.count(TeacherAssistPacingItemResource.id),
            )
            .join(
                TeacherAssistPacingItem,
                TeacherAssistPacingItem.id == TeacherAssistPacingItemResource.pacing_item_id,
            )
            .join(
                TeacherAssistPacingGuide,
                TeacherAssistPacingGuide.id == TeacherAssistPacingItem.pacing_guide_id,
            )
            .where(TeacherAssistPacingGuide.tenant_id == tenant_id)
            .group_by(TeacherAssistPacingItemResource.resource_library_item_id)
        ).all()
    )
    planning_counts = dict(
        db.execute(
            select(
                TeacherAssistPlanningInputDraftResource.resource_library_item_id,
                func.count(TeacherAssistPlanningInputDraftResource.id),
            )
            .join(
                TeacherAssistPlanningInputDraft,
                TeacherAssistPlanningInputDraft.id
                == TeacherAssistPlanningInputDraftResource.planning_input_draft_id,
            )
            .where(TeacherAssistPlanningInputDraft.tenant_id == tenant_id)
            .group_by(TeacherAssistPlanningInputDraftResource.resource_library_item_id)
        ).all()
    )
    return ResourceLinkCounts(pacing_items=pacing_counts, planning_drafts=planning_counts)


def _dedupe_uuid_values(values: list[uuid.UUID | None]) -> list[uuid.UUID]:
    seen: set[uuid.UUID] = set()
    normalized: list[uuid.UUID] = []
    for value in values:
        if value is None or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _normalize_subject_ids(
    *, subject_id: uuid.UUID | None, subject_ids: list[uuid.UUID] | None
) -> list[uuid.UUID]:
    normalized = _dedupe_uuid_values(
        (subject_ids or []) + ([subject_id] if subject_id is not None else [])
    )
    return normalized


def _sync_mapping_rows(
    db: Session,
    *,
    existing_rows: list[object],
    target_ids: list[uuid.UUID],
    value_attr: str,
    row_factory: callable,
) -> None:
    existing_by_id = {getattr(row, value_attr): row for row in existing_rows}
    target_set = set(target_ids)
    for value_id, row in existing_by_id.items():
        if value_id not in target_set:
            db.delete(row)
    for value_id in target_ids:
        if value_id not in existing_by_id:
            db.add(row_factory(value_id))


def list_planning_drafts(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> list[TeacherAssistPlanningInputDraft]:
    return db.scalars(
        select(TeacherAssistPlanningInputDraft)
        .where(
            TeacherAssistPlanningInputDraft.tenant_id == tenant_id,
            TeacherAssistPlanningInputDraft.user_id == user_id,
        )
        .order_by(
            TeacherAssistPlanningInputDraft.updated_at.desc(),
            TeacherAssistPlanningInputDraft.created_at.desc(),
        )
    ).all()


def get_planning_draft_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
) -> TeacherAssistPlanningInputDraft:
    row = db.scalars(
        select(TeacherAssistPlanningInputDraft).where(
            TeacherAssistPlanningInputDraft.id == planning_draft_id,
            TeacherAssistPlanningInputDraft.tenant_id == tenant_id,
            TeacherAssistPlanningInputDraft.user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Planning draft not found")
    return row


def list_planning_draft_subject_links(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID | None = None,
) -> list[TeacherAssistPlanningInputDraftSubject]:
    stmt = (
        select(TeacherAssistPlanningInputDraftSubject)
        .join(
            TeacherAssistPlanningInputDraft,
            TeacherAssistPlanningInputDraft.id
            == TeacherAssistPlanningInputDraftSubject.planning_input_draft_id,
        )
        .where(
            TeacherAssistPlanningInputDraft.tenant_id == tenant_id,
            TeacherAssistPlanningInputDraft.user_id == user_id,
        )
        .order_by(TeacherAssistPlanningInputDraftSubject.created_at.asc())
    )
    if planning_draft_id is not None:
        stmt = stmt.where(TeacherAssistPlanningInputDraft.id == planning_draft_id)
    return db.scalars(stmt).all()


def list_planning_draft_pacing_item_links(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID | None = None,
) -> list[TeacherAssistPlanningInputDraftPacingItem]:
    stmt = (
        select(TeacherAssistPlanningInputDraftPacingItem)
        .join(
            TeacherAssistPlanningInputDraft,
            TeacherAssistPlanningInputDraft.id
            == TeacherAssistPlanningInputDraftPacingItem.planning_input_draft_id,
        )
        .where(
            TeacherAssistPlanningInputDraft.tenant_id == tenant_id,
            TeacherAssistPlanningInputDraft.user_id == user_id,
        )
        .order_by(TeacherAssistPlanningInputDraftPacingItem.created_at.asc())
    )
    if planning_draft_id is not None:
        stmt = stmt.where(TeacherAssistPlanningInputDraft.id == planning_draft_id)
    return db.scalars(stmt).all()


def list_planning_draft_standard_links(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID | None = None,
) -> list[TeacherAssistPlanningInputDraftStandard]:
    stmt = (
        select(TeacherAssistPlanningInputDraftStandard)
        .join(
            TeacherAssistPlanningInputDraft,
            TeacherAssistPlanningInputDraft.id
            == TeacherAssistPlanningInputDraftStandard.planning_input_draft_id,
        )
        .where(
            TeacherAssistPlanningInputDraft.tenant_id == tenant_id,
            TeacherAssistPlanningInputDraft.user_id == user_id,
        )
        .order_by(TeacherAssistPlanningInputDraftStandard.created_at.asc())
    )
    if planning_draft_id is not None:
        stmt = stmt.where(TeacherAssistPlanningInputDraft.id == planning_draft_id)
    return db.scalars(stmt).all()


def _get_draft_subject_ids(
    db: Session, *, draft: TeacherAssistPlanningInputDraft
) -> list[uuid.UUID]:
    subject_ids = [
        row.subject_id
        for row in db.scalars(
            select(TeacherAssistPlanningInputDraftSubject).where(
                TeacherAssistPlanningInputDraftSubject.planning_input_draft_id == draft.id
            )
        ).all()
    ]
    if not subject_ids and draft.subject_id is not None:
        subject_ids = [draft.subject_id]
    return _dedupe_uuid_values(subject_ids)


def _get_draft_pacing_item_ids(
    db: Session, *, draft: TeacherAssistPlanningInputDraft
) -> list[uuid.UUID]:
    return _dedupe_uuid_values(
        [
            row.pacing_item_id
            for row in db.scalars(
                select(TeacherAssistPlanningInputDraftPacingItem).where(
                    TeacherAssistPlanningInputDraftPacingItem.planning_input_draft_id == draft.id
                )
            ).all()
        ]
    )


def _get_draft_standard_ids(
    db: Session, *, draft: TeacherAssistPlanningInputDraft
) -> list[uuid.UUID]:
    return _dedupe_uuid_values(
        [
            row.standard_id
            for row in db.scalars(
                select(TeacherAssistPlanningInputDraftStandard).where(
                    TeacherAssistPlanningInputDraftStandard.planning_input_draft_id == draft.id
                )
            ).all()
        ]
    )


def _get_draft_resource_ids(
    db: Session, *, draft: TeacherAssistPlanningInputDraft
) -> list[uuid.UUID]:
    return _dedupe_uuid_values(
        [
            row.resource_library_item_id
            for row in db.scalars(
                select(TeacherAssistPlanningInputDraftResource).where(
                    TeacherAssistPlanningInputDraftResource.planning_input_draft_id == draft.id
                )
            ).all()
        ]
    )


def _validate_planning_reference_set(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID | None,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID | None,
    subject_ids: list[uuid.UUID],
    pacing_item_ids: list[uuid.UUID],
    standard_ids: list[uuid.UUID],
) -> None:
    school_year = (
        get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
        if school_year_id is not None
        else None
    )
    grading_period = (
        get_grading_period_or_404(db, tenant_id=tenant_id, grading_period_id=grading_period_id)
        if grading_period_id is not None
        else None
    )
    teacher_class = (
        get_class_or_404(db, tenant_id=tenant_id, class_id=class_id)
        if class_id is not None
        else None
    )
    subjects = [
        get_subject_or_404(db, tenant_id=tenant_id, subject_id=current_subject_id)
        for current_subject_id in subject_ids
    ]
    pacing_items = [
        get_pacing_item_or_404(db, tenant_id=tenant_id, pacing_item_id=pacing_item_id)
        for pacing_item_id in pacing_item_ids
    ]
    standards = [
        get_standard_or_404(db, tenant_id=tenant_id, standard_id=standard_id)
        for standard_id in standard_ids
    ]

    resolved_school_year_ids = {
        value
        for value in (
            school_year.id if school_year is not None else None,
            grading_period.school_year_id if grading_period is not None else None,
            teacher_class.school_year_id if teacher_class is not None else None,
            *[
                pacing_item.pacing_guide.school_year_id
                for pacing_item in pacing_items
                if pacing_item.pacing_guide.school_year_id is not None
            ],
            *[
                standard.school_year_id
                for standard in standards
                if standard.school_year_id is not None
            ],
        )
        if value is not None
    }
    if len(resolved_school_year_ids) > 1:
        raise ValueError("Planning draft references must belong to the same school year")

    if subjects and teacher_class is not None:
        class_subject_ids = {
            row.subject_id
            for row in db.scalars(
                select(TeacherAssistClassSubject).where(
                    TeacherAssistClassSubject.class_id == teacher_class.id
                )
            ).all()
        }
        if class_subject_ids:
            invalid_subject_ids = [
                subject.id for subject in subjects if subject.id not in class_subject_ids
            ]
            if invalid_subject_ids:
                raise ValueError("Selected subjects must be attached to the selected class")


def _validate_planning_references(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID | None,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
) -> None:
    _validate_planning_reference_set(
        db,
        tenant_id=tenant_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_ids=_normalize_subject_ids(subject_id=subject_id, subject_ids=None),
        pacing_item_ids=[],
        standard_ids=[],
    )


def _sync_planning_draft_links(
    db: Session,
    *,
    draft: TeacherAssistPlanningInputDraft,
    subject_ids: list[uuid.UUID],
    pacing_item_ids: list[uuid.UUID],
    standard_ids: list[uuid.UUID],
) -> None:
    now = datetime.now(UTC)
    _sync_mapping_rows(
        db,
        existing_rows=db.scalars(
            select(TeacherAssistPlanningInputDraftSubject).where(
                TeacherAssistPlanningInputDraftSubject.planning_input_draft_id == draft.id
            )
        ).all(),
        target_ids=subject_ids,
        value_attr="subject_id",
        row_factory=lambda current_subject_id: TeacherAssistPlanningInputDraftSubject(
            planning_input_draft_id=draft.id,
            subject_id=current_subject_id,
            created_at=now,
        ),
    )
    _sync_mapping_rows(
        db,
        existing_rows=db.scalars(
            select(TeacherAssistPlanningInputDraftPacingItem).where(
                TeacherAssistPlanningInputDraftPacingItem.planning_input_draft_id == draft.id
            )
        ).all(),
        target_ids=pacing_item_ids,
        value_attr="pacing_item_id",
        row_factory=lambda current_pacing_item_id: TeacherAssistPlanningInputDraftPacingItem(
            planning_input_draft_id=draft.id,
            pacing_item_id=current_pacing_item_id,
            created_at=now,
        ),
    )
    _sync_mapping_rows(
        db,
        existing_rows=db.scalars(
            select(TeacherAssistPlanningInputDraftStandard).where(
                TeacherAssistPlanningInputDraftStandard.planning_input_draft_id == draft.id
            )
        ).all(),
        target_ids=standard_ids,
        value_attr="standard_id",
        row_factory=lambda current_standard_id: TeacherAssistPlanningInputDraftStandard(
            planning_input_draft_id=draft.id,
            standard_id=current_standard_id,
            created_at=now,
        ),
    )


def _validate_planning_duration_inputs(
    *,
    planning_scope: str,
    start_date: date | None,
    end_date: date | None,
    estimated_weeks: int | None,
    instructional_days_count: int | None,
) -> None:
    if (start_date is None) != (end_date is None):
        raise ValueError("Start date and end date must be provided together")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("Start date must be on or before end date")
    if estimated_weeks is not None and estimated_weeks < 1:
        raise ValueError("Estimated weeks must be at least 1")
    if instructional_days_count is not None and instructional_days_count < 1:
        raise ValueError("Instructional days count must be at least 1")
    if planning_scope == "weekly" and estimated_weeks is not None and estimated_weeks < 1:
        raise ValueError("Weekly planning requires at least one estimated week")


def _sorted_pacing_items(
    pacing_items: list[TeacherAssistPacingItem],
) -> list[TeacherAssistPacingItem]:
    return sorted(
        pacing_items,
        key=lambda row: (
            row.week_number if row.week_number is not None else 1_000_000,
            row.instructional_date or date.max,
            row.day_number if row.day_number is not None else 1_000_000,
            row.sort_order if row.sort_order is not None else 1_000_000,
            row.created_at,
        ),
    )


def _derived_estimated_weeks(
    draft: TeacherAssistPlanningInputDraft, pacing_items: list[TeacherAssistPacingItem]
) -> int | None:
    if draft.estimated_weeks is not None:
        return draft.estimated_weeks
    week_numbers = {item.week_number for item in pacing_items if item.week_number is not None}
    if week_numbers:
        return max(week_numbers)
    if draft.start_date is not None and draft.end_date is not None:
        day_span = (draft.end_date - draft.start_date).days + 1
        return max(1, (day_span + 6) // 7)
    if draft.planning_scope == "weekly":
        return 1
    return None


def _derived_instructional_days_count(
    draft: TeacherAssistPlanningInputDraft, pacing_items: list[TeacherAssistPacingItem]
) -> int | None:
    if draft.instructional_days_count is not None:
        return draft.instructional_days_count
    instructional_dates = {
        item.instructional_date for item in pacing_items if item.instructional_date is not None
    }
    if instructional_dates:
        return len(instructional_dates)
    day_numbers = {item.day_number for item in pacing_items if item.day_number is not None}
    if day_numbers:
        return len(day_numbers)
    estimated_weeks = _derived_estimated_weeks(draft, pacing_items)
    if estimated_weeks is not None:
        return estimated_weeks * 5
    return None


def _build_duration_summary(
    *, draft: TeacherAssistPlanningInputDraft, pacing_items: list[TeacherAssistPacingItem]
) -> PlanningDurationSummary:
    estimated_weeks = _derived_estimated_weeks(draft, pacing_items)
    instructional_days_count = _derived_instructional_days_count(draft, pacing_items)
    summary_parts = [validate_planning_scope(draft.planning_scope).replace("_", " ")]
    if estimated_weeks is not None:
        summary_parts.append(f"{estimated_weeks} week{'s' if estimated_weeks != 1 else ''}")
    if instructional_days_count is not None:
        summary_parts.append(
            f"{instructional_days_count} instructional day{'s' if instructional_days_count != 1 else ''}"
        )
    if draft.start_date is not None and draft.end_date is not None:
        summary_parts.append(f"{draft.start_date.isoformat()} to {draft.end_date.isoformat()}")
    return PlanningDurationSummary(
        start_date=draft.start_date,
        end_date=draft.end_date,
        estimated_weeks=estimated_weeks,
        instructional_days_count=instructional_days_count,
        summary=" · ".join(summary_parts),
    )


def _build_pacing_groups(pacing_items: list[TeacherAssistPacingItem]) -> list[PlanningPacingGroup]:
    grouped: dict[str, list[TeacherAssistPacingItem]] = {}
    labels: dict[str, str] = {}
    for item in _sorted_pacing_items(pacing_items):
        if item.week_number is not None:
            key = f"week:{item.week_number}"
            labels[key] = f"Week {item.week_number}"
        elif item.instructional_date is not None:
            key = f"date:{item.instructional_date.isoformat()}"
            labels[key] = item.instructional_date.isoformat()
        else:
            key = "ungrouped"
            labels[key] = "Ungrouped pacing"
        grouped.setdefault(key, []).append(item)
    return [
        PlanningPacingGroup(group_key=group_key, label=labels[group_key], pacing_items=items)
        for group_key, items in grouped.items()
    ]


def _collect_planning_draft_context(
    db: Session, *, draft: TeacherAssistPlanningInputDraft
) -> PlanningDraftContextPreview:
    subject_ids = _get_draft_subject_ids(db, draft=draft)
    pacing_item_ids = _get_draft_pacing_item_ids(db, draft=draft)
    standard_ids = _get_draft_standard_ids(db, draft=draft)
    resource_ids = _get_draft_resource_ids(db, draft=draft)

    subjects = [
        get_subject_or_404(db, tenant_id=draft.tenant_id, subject_id=subject_id)
        for subject_id in subject_ids
    ]
    pacing_items = [
        get_pacing_item_or_404(db, tenant_id=draft.tenant_id, pacing_item_id=pacing_item_id)
        for pacing_item_id in pacing_item_ids
    ]
    standards = [
        get_standard_or_404(db, tenant_id=draft.tenant_id, standard_id=standard_id)
        for standard_id in standard_ids
    ]
    resources = [
        get_resource_or_404(db, tenant_id=draft.tenant_id, resource_id=resource_id)
        for resource_id in resource_ids
    ]

    warnings: list[str] = []
    missing_items: list[str] = []
    planning_scope = validate_planning_scope(draft.planning_scope)
    duration_summary = _build_duration_summary(draft=draft, pacing_items=pacing_items)
    pacing_groups = _build_pacing_groups(pacing_items)
    if draft.school_year_id is None:
        missing_items.append("Select a school year.")
    if draft.grading_period_id is None:
        missing_items.append("Select a grading period.")
    if draft.class_id is None:
        missing_items.append("Select a class.")
    if not subjects:
        missing_items.append("Add at least one subject.")
    if not pacing_items and not (draft.notes or "").strip() and not resources:
        missing_items.append("Add at least one pacing item, teacher note, or attached resource.")
    if draft.title is None or not draft.title.strip():
        missing_items.append("Add a plan title.")
    if planning_scope != "weekly" and not (
        (draft.start_date is not None and draft.end_date is not None)
        or draft.estimated_weeks is not None
        or draft.instructional_days_count is not None
    ):
        missing_items.append("Add a date range or estimated duration for non-weekly planning.")
    if (draft.start_date is None) != (draft.end_date is None):
        missing_items.append("Provide both a start date and end date.")
    if not standards:
        warnings.append("No standards are attached yet.")
    if not pacing_items:
        warnings.append("No pacing items are attached yet.")
    if not resources:
        warnings.append("No resources are attached yet.")
    if not (draft.notes or "").strip():
        warnings.append("Teacher notes are empty.")
    if planning_scope == "weekly" and duration_summary.estimated_weeks not in {None, 1}:
        warnings.append("Weekly view is active, but the saved duration spans multiple weeks.")
    if not pacing_groups and pacing_items:
        warnings.append("Pacing items could not be grouped into week/date segments.")

    return PlanningDraftContextPreview(
        draft=draft,
        subjects=subjects,
        pacing_items=pacing_items,
        standards=standards,
        resources=resources,
        duration_summary=duration_summary,
        pacing_groups=pacing_groups,
        readiness=PlanningDraftReadiness(
            is_ready=not missing_items,
            missing_items=missing_items,
            warnings=warnings,
        ),
    )


def validate_planning_draft_readiness(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
) -> PlanningDraftReadiness:
    draft = get_planning_draft_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    return _collect_planning_draft_context(db, draft=draft).readiness


def get_planning_draft_context_preview(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
) -> PlanningDraftContextPreview:
    draft = get_planning_draft_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    return _collect_planning_draft_context(db, draft=draft)


def _apply_planning_draft_changes(
    db: Session,
    *,
    row: TeacherAssistPlanningInputDraft,
    planning_scope: str,
    school_year_id: uuid.UUID | None,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
    subject_ids: list[uuid.UUID] | None,
    pacing_item_ids: list[uuid.UUID] | None,
    standard_ids: list[uuid.UUID] | None,
    title: str | None,
    module_title: str | None,
    start_date: date | None,
    end_date: date | None,
    estimated_weeks: int | None,
    instructional_days_count: int | None,
    notes: str | None,
    status: str,
) -> TeacherAssistPlanningInputDraft:
    normalized_subject_ids = _normalize_subject_ids(subject_id=subject_id, subject_ids=subject_ids)
    normalized_pacing_item_ids = _dedupe_uuid_values((pacing_item_ids or []).copy())
    normalized_standard_ids = _dedupe_uuid_values((standard_ids or []).copy())
    normalized_status = validate_planning_draft_status(status)
    normalized_scope = validate_planning_scope(planning_scope)
    _validate_planning_duration_inputs(
        planning_scope=normalized_scope,
        start_date=start_date,
        end_date=end_date,
        estimated_weeks=estimated_weeks,
        instructional_days_count=instructional_days_count,
    )

    _validate_planning_reference_set(
        db,
        tenant_id=row.tenant_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_ids=normalized_subject_ids,
        pacing_item_ids=normalized_pacing_item_ids,
        standard_ids=normalized_standard_ids,
    )

    row.school_year_id = school_year_id
    row.grading_period_id = grading_period_id
    row.class_id = class_id
    row.subject_id = normalized_subject_ids[0] if normalized_subject_ids else None
    row.planning_scope = normalized_scope
    row.title = title.strip() if title else None
    row.module_title = module_title.strip() if module_title else None
    row.start_date = start_date
    row.end_date = end_date
    row.estimated_weeks = estimated_weeks
    row.instructional_days_count = instructional_days_count
    row.notes = notes.strip() if notes else None
    row.updated_at = datetime.now(UTC)
    db.flush()

    _sync_planning_draft_links(
        db,
        draft=row,
        subject_ids=normalized_subject_ids,
        pacing_item_ids=normalized_pacing_item_ids,
        standard_ids=normalized_standard_ids,
    )
    db.flush()

    if normalized_status == "ready":
        readiness = _collect_planning_draft_context(db, draft=row).readiness
        if not readiness.is_ready:
            raise ValueError("Planning draft is not ready: " + "; ".join(readiness.missing_items))
    row.status = normalized_status
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row


def create_planning_draft(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    planning_scope: str,
    school_year_id: uuid.UUID | None,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
    subject_ids: list[uuid.UUID] | None,
    pacing_item_ids: list[uuid.UUID] | None,
    standard_ids: list[uuid.UUID] | None,
    pacing_guide_period_id: uuid.UUID | None = None,
    title: str | None,
    module_title: str | None,
    start_date: date | None,
    end_date: date | None,
    estimated_weeks: int | None,
    instructional_days_count: int | None,
    notes: str | None,
    status: str,
) -> TeacherAssistPlanningInputDraft:
    now = datetime.now(UTC)
    row = TeacherAssistPlanningInputDraft(
        tenant_id=tenant_id,
        user_id=user.id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        planning_scope=validate_planning_scope(planning_scope),
        title=title.strip() if title else None,
        module_title=module_title.strip() if module_title else None,
        start_date=start_date,
        end_date=end_date,
        estimated_weeks=estimated_weeks,
        instructional_days_count=instructional_days_count,
        notes=notes.strip() if notes else None,
        status=validate_planning_draft_status(status),
        pacing_guide_period_id=pacing_guide_period_id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return _apply_planning_draft_changes(
        db,
        row=row,
        planning_scope=planning_scope,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        subject_ids=subject_ids,
        pacing_item_ids=pacing_item_ids,
        standard_ids=standard_ids,
        title=title,
        module_title=module_title,
        start_date=start_date,
        end_date=end_date,
        estimated_weeks=estimated_weeks,
        instructional_days_count=instructional_days_count,
        notes=notes,
        status=status,
    )


def update_planning_draft(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
    planning_scope: str,
    school_year_id: uuid.UUID | None,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
    subject_ids: list[uuid.UUID] | None,
    pacing_item_ids: list[uuid.UUID] | None,
    standard_ids: list[uuid.UUID] | None,
    title: str | None,
    module_title: str | None,
    start_date: date | None,
    end_date: date | None,
    estimated_weeks: int | None,
    instructional_days_count: int | None,
    notes: str | None,
    status: str,
) -> TeacherAssistPlanningInputDraft:
    row = get_planning_draft_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    return _apply_planning_draft_changes(
        db,
        row=row,
        planning_scope=planning_scope,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        subject_ids=subject_ids,
        pacing_item_ids=pacing_item_ids,
        standard_ids=standard_ids,
        title=title,
        module_title=module_title,
        start_date=start_date,
        end_date=end_date,
        estimated_weeks=estimated_weeks,
        instructional_days_count=instructional_days_count,
        notes=notes,
        status=status,
    )


def list_planning_draft_resources(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> list[TeacherAssistPlanningInputDraftResource]:
    return db.scalars(
        select(TeacherAssistPlanningInputDraftResource)
        .join(
            TeacherAssistPlanningInputDraft,
            TeacherAssistPlanningInputDraft.id
            == TeacherAssistPlanningInputDraftResource.planning_input_draft_id,
        )
        .where(
            TeacherAssistPlanningInputDraft.tenant_id == tenant_id,
            TeacherAssistPlanningInputDraft.user_id == user_id,
        )
        .order_by(TeacherAssistPlanningInputDraftResource.created_at.asc())
    ).all()


def attach_planning_draft_resource(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
    resource_library_item_id: uuid.UUID,
) -> TeacherAssistPlanningInputDraftResource:
    draft = get_planning_draft_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    resource = get_resource_or_404(db, tenant_id=tenant_id, resource_id=resource_library_item_id)
    existing = db.scalars(
        select(TeacherAssistPlanningInputDraftResource).where(
            TeacherAssistPlanningInputDraftResource.planning_input_draft_id == draft.id,
            TeacherAssistPlanningInputDraftResource.resource_library_item_id == resource.id,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    row = TeacherAssistPlanningInputDraftResource(
        planning_input_draft_id=draft.id,
        resource_library_item_id=resource.id,
        created_at=datetime.now(UTC),
    )
    db.add(row)
    db.flush()
    return row


def update_planning_draft_status(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
    status: str,
) -> TeacherAssistPlanningInputDraft:
    row = get_planning_draft_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    normalized_status = validate_planning_draft_status(status)
    if normalized_status == "ready":
        readiness = _collect_planning_draft_context(db, draft=row).readiness
        if not readiness.is_ready:
            raise ValueError("Planning draft is not ready: " + "; ".join(readiness.missing_items))
    row.status = normalized_status
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row

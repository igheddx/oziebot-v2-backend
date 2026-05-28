from __future__ import annotations

from datetime import UTC, date, datetime
import uuid
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_resource import TeacherAssistAssignmentResource
from oziebot_api.models.teacher_assist_assignment_standard import TeacherAssistAssignmentStandard
from oziebot_api.models.teacher_assist_class_subject import TeacherAssistClassSubject
from oziebot_api.models.teacher_assist_resource_library_item import TeacherAssistResourceLibraryItem
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.constants import (
    validate_assignment_status,
    validate_assignment_type,
)
from oziebot_api.services.teacher_assist.instructional_plan_validator import contains_pii_like_content
from oziebot_api.services.teacher_assist.planning import get_resource_or_404, get_standard_or_404
from oziebot_api.services.teacher_assist.setup import (
    get_class_or_404,
    get_grading_period_or_404,
    get_school_year_or_404,
    get_subject_or_404,
)
from oziebot_api.services.teacher_assist.workflow_service import get_visible_weekly_plan_or_404

ASSIGNMENT_STATUS_TRANSITIONS: dict[str, set[str]] = {
    "draft": {"draft", "ready", "archived"},
    "ready": {"draft", "ready", "assigned", "archived"},
    "assigned": {"assigned", "collected", "archived"},
    "collected": {"collected", "review_in_progress", "archived"},
    "review_in_progress": {"review_in_progress", "reviewed", "archived"},
    "reviewed": {"reviewed", "archived"},
    "archived": {"archived"},
}


def _trim_required_text(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _trim_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _normalize_uuid_list(values: list[uuid.UUID] | None) -> list[uuid.UUID]:
    normalized: list[uuid.UUID] = []
    seen: set[uuid.UUID] = set()
    for value in values or []:
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _validate_assignment_status_transition(*, current_status: str | None, next_status: str) -> str:
    normalized_next = validate_assignment_status(next_status)
    if current_status is None:
        return normalized_next
    normalized_current = validate_assignment_status(current_status)
    allowed = ASSIGNMENT_STATUS_TRANSITIONS[normalized_current]
    if normalized_next not in allowed:
        raise ValueError(
            f"Assignment status cannot transition from {normalized_current} to {normalized_next}"
        )
    return normalized_next


def _validate_assignment_content(
    *,
    title: str,
    description: str | None,
    instructions: str | None,
    rubric_json: dict[str, Any] | None,
    source_context_json: dict[str, Any] | None,
) -> tuple[str, str | None, str | None, dict[str, Any] | None, dict[str, Any] | None]:
    normalized_title = _trim_required_text(title, field_name="Title")
    normalized_description = _trim_optional_text(description)
    normalized_instructions = _trim_optional_text(instructions)
    if contains_pii_like_content(
        {
            "title": normalized_title,
            "description": normalized_description,
            "instructions": normalized_instructions,
            "rubric_json": rubric_json,
            "source_context_json": source_context_json,
        }
    ):
        raise ValueError("Assignments cannot include student-identifying or PII-like content")
    return (
        normalized_title,
        normalized_description,
        normalized_instructions,
        rubric_json,
        source_context_json,
    )


def _validate_assignment_references(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    due_date: date | None,
    standard_ids: list[uuid.UUID],
    resource_ids: list[uuid.UUID],
) -> tuple[list[TeacherAssistStandard], list[TeacherAssistResourceLibraryItem]]:
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    teacher_class = get_class_or_404(db, tenant_id=tenant_id, class_id=class_id)
    subject = get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)

    if teacher_class.school_year_id != school_year.id:
        raise ValueError("Class must belong to the selected school year")

    if grading_period_id is not None:
        grading_period = get_grading_period_or_404(
            db,
            tenant_id=tenant_id,
            grading_period_id=grading_period_id,
        )
        if grading_period.school_year_id != school_year.id:
            raise ValueError("Grading period must belong to the selected school year")

    class_subject_ids = {
        row.subject_id
        for row in db.scalars(
            select(TeacherAssistClassSubject).where(TeacherAssistClassSubject.class_id == teacher_class.id)
        ).all()
    }
    if class_subject_ids and subject.id not in class_subject_ids:
        raise ValueError("Selected subject must be attached to the selected class")

    if due_date is not None and (due_date < school_year.start_date or due_date > school_year.end_date):
        raise ValueError("Due date must fall within the selected school year")

    standards = [get_standard_or_404(db, tenant_id=tenant_id, standard_id=standard_id) for standard_id in standard_ids]
    for standard in standards:
        if standard.subject_id is not None and standard.subject_id != subject.id:
            raise ValueError("Standards must belong to the selected subject")
        if standard.school_year_id is not None and standard.school_year_id != school_year.id:
            raise ValueError("Standards must belong to the selected school year")

    resources = [
        get_resource_or_404(db, tenant_id=tenant_id, resource_id=resource_id) for resource_id in resource_ids
    ]
    return standards, resources


def get_assignment_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
) -> TeacherAssistAssignment:
    row = db.scalars(
        select(TeacherAssistAssignment).where(
            TeacherAssistAssignment.id == assignment_id,
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Assignment not found")
    return row


def list_assignments(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    status: str | None = None,
    assignment_type: str | None = None,
    q: str | None = None,
) -> list[TeacherAssistAssignment]:
    stmt = select(TeacherAssistAssignment).where(
        TeacherAssistAssignment.tenant_id == tenant_id,
        TeacherAssistAssignment.teacher_user_id == user_id,
    )
    if school_year_id is not None:
        stmt = stmt.where(TeacherAssistAssignment.school_year_id == school_year_id)
    if grading_period_id is not None:
        stmt = stmt.where(TeacherAssistAssignment.grading_period_id == grading_period_id)
    if class_id is not None:
        stmt = stmt.where(TeacherAssistAssignment.class_id == class_id)
    if subject_id is not None:
        stmt = stmt.where(TeacherAssistAssignment.subject_id == subject_id)
    if status is not None:
        stmt = stmt.where(TeacherAssistAssignment.status == validate_assignment_status(status))
    if assignment_type is not None:
        stmt = stmt.where(
            TeacherAssistAssignment.assignment_type == validate_assignment_type(assignment_type)
        )
    normalized_q = (q or "").strip().lower()
    if normalized_q:
        pattern = f"%{normalized_q}%"
        stmt = stmt.where(
            or_(
                func.lower(TeacherAssistAssignment.title).like(pattern),
                func.lower(func.coalesce(TeacherAssistAssignment.description, "")).like(pattern),
                func.lower(func.coalesce(TeacherAssistAssignment.instructions, "")).like(pattern),
            )
        )
    return db.scalars(
        stmt.order_by(TeacherAssistAssignment.updated_at.desc(), TeacherAssistAssignment.created_at.desc())
    ).all()


def list_assignment_standards(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_ids: list[uuid.UUID] | None = None,
) -> list[TeacherAssistAssignmentStandard]:
    stmt = (
        select(TeacherAssistAssignmentStandard)
        .join(TeacherAssistAssignment, TeacherAssistAssignment.id == TeacherAssistAssignmentStandard.assignment_id)
        .where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
        )
        .order_by(TeacherAssistAssignmentStandard.created_at.asc())
    )
    if assignment_ids:
        stmt = stmt.where(TeacherAssistAssignmentStandard.assignment_id.in_(assignment_ids))
    return db.scalars(stmt).all()


def list_assignment_resources(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_ids: list[uuid.UUID] | None = None,
) -> list[TeacherAssistAssignmentResource]:
    stmt = (
        select(TeacherAssistAssignmentResource)
        .join(TeacherAssistAssignment, TeacherAssistAssignment.id == TeacherAssistAssignmentResource.assignment_id)
        .where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
        )
        .order_by(TeacherAssistAssignmentResource.created_at.asc())
    )
    if assignment_ids:
        stmt = stmt.where(TeacherAssistAssignmentResource.assignment_id.in_(assignment_ids))
    return db.scalars(stmt).all()


def _sync_assignment_standards(
    db: Session,
    *,
    assignment: TeacherAssistAssignment,
    standard_ids: list[uuid.UUID],
) -> None:
    existing = {row.standard_id: row for row in assignment.standard_links}
    target_ids = set(standard_ids)
    for standard_id, row in existing.items():
        if standard_id not in target_ids:
            db.delete(row)
    now = datetime.now(UTC)
    for standard_id in standard_ids:
        if standard_id in existing:
            continue
        db.add(
            TeacherAssistAssignmentStandard(
                assignment_id=assignment.id,
                standard_id=standard_id,
                created_at=now,
            )
        )


def _sync_assignment_resources(
    db: Session,
    *,
    assignment: TeacherAssistAssignment,
    resource_ids: list[uuid.UUID],
) -> None:
    existing = {row.resource_library_item_id: row for row in assignment.resource_links}
    target_ids = set(resource_ids)
    for resource_id, row in existing.items():
        if resource_id not in target_ids:
            db.delete(row)
    now = datetime.now(UTC)
    for resource_id in resource_ids:
        if resource_id in existing:
            continue
        db.add(
            TeacherAssistAssignmentResource(
                assignment_id=assignment.id,
                resource_library_item_id=resource_id,
                created_at=now,
            )
        )


def create_assignment(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    title: str,
    description: str | None,
    assignment_type: str | None,
    due_date: date | None,
    status: str | None,
    instructions: str | None,
    rubric_json: dict[str, Any] | None,
    source_plan_id: uuid.UUID | None,
    source_context_json: dict[str, Any] | None,
    standard_ids: list[uuid.UUID] | None = None,
    resource_ids: list[uuid.UUID] | None = None,
) -> TeacherAssistAssignment:
    normalized_standard_ids = _normalize_uuid_list(standard_ids)
    normalized_resource_ids = _normalize_uuid_list(resource_ids)
    normalized_title, normalized_description, normalized_instructions, normalized_rubric_json, normalized_source_context_json = _validate_assignment_content(
        title=title,
        description=description,
        instructions=instructions,
        rubric_json=rubric_json,
        source_context_json=source_context_json,
    )
    normalized_assignment_type = validate_assignment_type(assignment_type)
    normalized_status = _validate_assignment_status_transition(
        current_status=None,
        next_status=status or "draft",
    )
    _validate_assignment_references(
        db,
        tenant_id=tenant_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        due_date=due_date,
        standard_ids=normalized_standard_ids,
        resource_ids=normalized_resource_ids,
    )
    now = datetime.now(UTC)
    row = TeacherAssistAssignment(
        tenant_id=tenant_id,
        teacher_user_id=user_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        title=normalized_title,
        description=normalized_description,
        assignment_type=normalized_assignment_type,
        due_date=due_date,
        status=normalized_status,
        instructions=normalized_instructions,
        rubric_json=normalized_rubric_json,
        source_plan_id=source_plan_id,
        source_context_json=normalized_source_context_json,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    _sync_assignment_standards(db, assignment=row, standard_ids=normalized_standard_ids)
    _sync_assignment_resources(db, assignment=row, resource_ids=normalized_resource_ids)
    record_activity_event(
        db,
        tenant_id=row.tenant_id,
        user_id=row.teacher_user_id,
        event_type="assignment_created",
        event_category="assignment",
        entity_type="assignment",
        entity_id=row.id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        summary_text=f"Created assignment '{row.title}'.",
        details_json={
            "assignment_type": row.assignment_type,
            "status": row.status,
            "source_plan_id": str(row.source_plan_id) if row.source_plan_id else None,
        },
    )
    db.flush()
    db.refresh(row)
    return row


def update_assignment(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    title: str,
    description: str | None,
    assignment_type: str | None,
    due_date: date | None,
    status: str | None,
    instructions: str | None,
    rubric_json: dict[str, Any] | None,
    source_plan_id: uuid.UUID | None,
    source_context_json: dict[str, Any] | None,
    standard_ids: list[uuid.UUID] | None = None,
    resource_ids: list[uuid.UUID] | None = None,
) -> TeacherAssistAssignment:
    row = get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    normalized_standard_ids = _normalize_uuid_list(standard_ids)
    normalized_resource_ids = _normalize_uuid_list(resource_ids)
    normalized_title, normalized_description, normalized_instructions, normalized_rubric_json, normalized_source_context_json = _validate_assignment_content(
        title=title,
        description=description,
        instructions=instructions,
        rubric_json=rubric_json,
        source_context_json=source_context_json,
    )
    normalized_assignment_type = validate_assignment_type(assignment_type)
    prior_status = row.status
    normalized_status = _validate_assignment_status_transition(
        current_status=row.status,
        next_status=status or row.status,
    )
    _validate_assignment_references(
        db,
        tenant_id=tenant_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        due_date=due_date,
        standard_ids=normalized_standard_ids,
        resource_ids=normalized_resource_ids,
    )
    row.school_year_id = school_year_id
    row.grading_period_id = grading_period_id
    row.class_id = class_id
    row.subject_id = subject_id
    row.title = normalized_title
    row.description = normalized_description
    row.assignment_type = normalized_assignment_type
    row.due_date = due_date
    row.status = normalized_status
    row.instructions = normalized_instructions
    row.rubric_json = normalized_rubric_json
    row.source_plan_id = source_plan_id
    row.source_context_json = normalized_source_context_json
    row.updated_at = datetime.now(UTC)
    _sync_assignment_standards(db, assignment=row, standard_ids=normalized_standard_ids)
    _sync_assignment_resources(db, assignment=row, resource_ids=normalized_resource_ids)
    record_activity_event(
        db,
        tenant_id=row.tenant_id,
        user_id=row.teacher_user_id,
        event_type="assignment_updated",
        event_category="assignment",
        entity_type="assignment",
        entity_id=row.id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        summary_text=f"Updated assignment '{row.title}'.",
        details_json={
            "assignment_type": row.assignment_type,
            "status": row.status,
            "previous_status": prior_status,
        },
    )
    db.flush()
    db.refresh(row)
    return row


def update_assignment_status(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    status: str,
) -> TeacherAssistAssignment:
    row = get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    previous_status = row.status
    row.status = _validate_assignment_status_transition(current_status=row.status, next_status=status)
    row.updated_at = datetime.now(UTC)
    record_activity_event(
        db,
        tenant_id=row.tenant_id,
        user_id=row.teacher_user_id,
        event_type="assignment_status_changed",
        event_category="assignment",
        entity_type="assignment",
        entity_id=row.id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        summary_text=f"Changed assignment '{row.title}' from {previous_status} to {row.status}.",
        details_json={"previous_status": previous_status, "status": row.status},
    )
    db.flush()
    return row


def attach_assignment_standard(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    standard_id: uuid.UUID,
) -> TeacherAssistAssignment:
    row = get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    _validate_assignment_references(
        db,
        tenant_id=tenant_id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        due_date=row.due_date,
        standard_ids=_normalize_uuid_list([link.standard_id for link in row.standard_links] + [standard_id]),
        resource_ids=[link.resource_library_item_id for link in row.resource_links],
    )
    if all(link.standard_id != standard_id for link in row.standard_links):
        db.add(
            TeacherAssistAssignmentStandard(
                assignment_id=row.id,
                standard_id=standard_id,
                created_at=datetime.now(UTC),
            )
        )
    row.updated_at = datetime.now(UTC)
    db.flush()
    db.refresh(row)
    return row


def attach_assignment_resource(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    resource_library_item_id: uuid.UUID,
) -> TeacherAssistAssignment:
    row = get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    _validate_assignment_references(
        db,
        tenant_id=tenant_id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        due_date=row.due_date,
        standard_ids=[link.standard_id for link in row.standard_links],
        resource_ids=_normalize_uuid_list(
            [link.resource_library_item_id for link in row.resource_links] + [resource_library_item_id]
        ),
    )
    if all(link.resource_library_item_id != resource_library_item_id for link in row.resource_links):
        db.add(
            TeacherAssistAssignmentResource(
                assignment_id=row.id,
                resource_library_item_id=resource_library_item_id,
                created_at=datetime.now(UTC),
            )
        )
    row.updated_at = datetime.now(UTC)
    db.flush()
    db.refresh(row)
    return row


def _assignment_seed_metadata(plan: TeacherAssistWeeklyPlan) -> dict[str, Any]:
    source_context = dict(plan.source_context_json or {})
    draft = dict(source_context.get("draft") or {})
    school_year = dict(source_context.get("school_year") or {})
    grading_period = dict(source_context.get("grading_period") or {})
    class_context = dict(source_context.get("class") or {})
    subjects = list(source_context.get("subjects") or [])
    standards = list(source_context.get("standards") or [])
    resources = list(source_context.get("resources") or [])
    content_resources = list(plan.content_json.get("resources_used") or [])
    subject_ids = [
        str(subject.get("id"))
        for subject in subjects
        if isinstance(subject, dict) and subject.get("id")
    ]
    resource_ids = [
        str(resource.get("id"))
        for resource in resources
        if isinstance(resource, dict) and resource.get("id")
    ]
    if not resource_ids:
        resource_ids = [
            str(resource.get("id"))
            for resource in content_resources
            if isinstance(resource, dict) and resource.get("id")
        ]
    return {
        "school_year_id": school_year.get("id") or draft.get("school_year_id"),
        "grading_period_id": grading_period.get("id") or draft.get("grading_period_id"),
        "class_id": class_context.get("id") or draft.get("class_id"),
        "subject_id": draft.get("subject_id") or (subject_ids[0] if subject_ids else None),
        "subject_ids": subject_ids,
        "standard_ids": [
            str(standard.get("id"))
            for standard in standards
            if isinstance(standard, dict) and standard.get("id")
        ],
        "resource_ids": resource_ids,
    }


def create_assignment_from_weekly_plan(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan_id: uuid.UUID,
    title: str | None,
    description: str | None,
    assignment_type: str | None,
    due_date: date | None,
    instructions: str | None,
    rubric_json: dict[str, Any] | None,
) -> TeacherAssistAssignment:
    weekly_plan = get_visible_weekly_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        weekly_plan_id=weekly_plan_id,
    )
    metadata = _assignment_seed_metadata(weekly_plan)
    if not metadata.get("school_year_id"):
        raise ValueError("Weekly plan is missing school year context for assignment creation")
    if not metadata.get("class_id"):
        raise ValueError("Weekly plan is missing class context for assignment creation")
    if not metadata.get("subject_id"):
        raise ValueError("Weekly plan is missing subject context for assignment creation")

    overview = _trim_optional_text(str(weekly_plan.content_json.get("overview") or ""))
    review_notes = _trim_optional_text(str(weekly_plan.content_json.get("review_notes") or ""))
    starter_title = title or str(weekly_plan.content_json.get("plan_title") or weekly_plan.title)
    starter_description = description if description is not None else overview
    starter_instructions = instructions if instructions is not None else review_notes

    return create_assignment(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=uuid.UUID(str(metadata["school_year_id"])),
        grading_period_id=uuid.UUID(str(metadata["grading_period_id"]))
        if metadata.get("grading_period_id")
        else None,
        class_id=uuid.UUID(str(metadata["class_id"])),
        subject_id=uuid.UUID(str(metadata["subject_id"])),
        title=starter_title,
        description=starter_description,
        assignment_type=assignment_type or "other",
        due_date=due_date,
        status="draft",
        instructions=starter_instructions,
        rubric_json=rubric_json,
        source_plan_id=weekly_plan.id,
        source_context_json={
            "starter": "weekly_plan_assignment",
            "weekly_plan_id": str(weekly_plan.id),
            "weekly_plan_title": weekly_plan.title,
            "planning_scope": weekly_plan.planning_scope,
        },
        standard_ids=[uuid.UUID(value) for value in metadata.get("standard_ids", [])],
        resource_ids=[uuid.UUID(value) for value in metadata.get("resource_ids", [])],
    )

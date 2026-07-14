from __future__ import annotations

from datetime import UTC, date, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_assignment_standard import TeacherAssistAssignmentStandard
from oziebot_api.models.teacher_assist_newsletter import TeacherAssistNewsletter
from oziebot_api.models.teacher_assist_newsletter_version import TeacherAssistNewsletterVersion
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.teacher_assist_planning_input_draft import TeacherAssistPlanningInputDraft
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.assignments import list_assignments
from oziebot_api.services.teacher_assist.constants import (
    validate_newsletter_status,
    validate_newsletter_version_source,
)
from oziebot_api.services.teacher_assist.setup import (
    get_class_or_404,
    get_grading_period_or_404,
    get_school_year_or_404,
    get_subject_or_404,
)


INSTRUCTIONAL_ASSIGNMENT_STATUSES = (
    "assigned",
    "collected",
    "review_in_progress",
    "reviewed",
)


def _normalize_title(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("Newsletter title is required")
    return normalized


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    return value


def get_newsletter_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter_id: uuid.UUID,
    load_versions: bool = False,
) -> TeacherAssistNewsletter:
    query = select(TeacherAssistNewsletter).where(
        TeacherAssistNewsletter.id == newsletter_id,
        TeacherAssistNewsletter.tenant_id == tenant_id,
        TeacherAssistNewsletter.owner_user_id == user_id,
    )
    if load_versions:
        query = query.options(
            selectinload(TeacherAssistNewsletter.versions),
            selectinload(TeacherAssistNewsletter.current_version),
            selectinload(TeacherAssistNewsletter.subject),
            selectinload(TeacherAssistNewsletter.teacher_class),
        )
    else:
        query = query.options(
            selectinload(TeacherAssistNewsletter.subject),
            selectinload(TeacherAssistNewsletter.teacher_class),
        )
    row = db.scalars(query).one_or_none()
    if row is None:
        raise LookupError("Newsletter not found")
    return row


def list_newsletters(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list[TeacherAssistNewsletter]:
    query = select(TeacherAssistNewsletter).where(
        TeacherAssistNewsletter.tenant_id == tenant_id,
        TeacherAssistNewsletter.owner_user_id == user_id,
    )
    if school_year_id is not None:
        query = query.where(TeacherAssistNewsletter.school_year_id == school_year_id)
    if grading_period_id is not None:
        query = query.where(TeacherAssistNewsletter.grading_period_id == grading_period_id)
    if class_id is not None:
        query = query.where(TeacherAssistNewsletter.class_id == class_id)
    if subject_id is not None:
        query = query.where(TeacherAssistNewsletter.subject_id == subject_id)
    if status is not None:
        query = query.where(TeacherAssistNewsletter.status == validate_newsletter_status(status))
    return db.scalars(
        query.order_by(
            TeacherAssistNewsletter.updated_at.desc(),
            TeacherAssistNewsletter.created_at.desc(),
        )
    ).all()


def _validate_newsletter_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
) -> None:
    get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    if grading_period_id is not None:
        period = get_grading_period_or_404(
            db, tenant_id=tenant_id, grading_period_id=grading_period_id
        )
        if period.school_year_id != school_year_id:
            raise ValueError("Grading period does not belong to the selected school year")
    teacher_class = get_class_or_404(db, tenant_id=tenant_id, class_id=class_id)
    if teacher_class.school_year_id != school_year_id:
        raise ValueError("Class does not belong to the selected school year")
    get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)


def create_newsletter(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    title: str | None = None,
    teacher_notes: str | None = None,
    week_start_date: date | None = None,
    week_end_date: date | None = None,
    pacing_guide_id: uuid.UUID | None = None,
    pacing_guide_period_id: uuid.UUID | None = None,
) -> TeacherAssistNewsletter:
    _validate_newsletter_context(
        db,
        tenant_id=tenant_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
    )
    subject = get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)
    normalized_title = _normalize_title(title or f"Weekly Newsletter — {subject.name}")
    now = datetime.now(UTC)
    newsletter = TeacherAssistNewsletter(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        title=normalized_title,
        status=validate_newsletter_status("draft"),
        week_start_date=week_start_date,
        week_end_date=week_end_date,
        teacher_notes=(teacher_notes or "").strip() or None,
        pacing_guide_id=pacing_guide_id,
        pacing_guide_period_id=pacing_guide_period_id,
        created_at=now,
        updated_at=now,
    )
    db.add(newsletter)
    db.flush()
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="newsletter_created",
        event_category="communication",
        entity_type="newsletter",
        entity_id=newsletter.id,
        school_year_id=newsletter.school_year_id,
        grading_period_id=newsletter.grading_period_id,
        class_id=newsletter.class_id,
        subject_id=newsletter.subject_id,
        summary_text=f"Created newsletter draft '{newsletter.title}'.",
        details_json={
            "newsletter_id": str(newsletter.id),
            "status": newsletter.status,
        },
    )
    db.flush()
    db.refresh(newsletter)
    return newsletter


def _next_version_number(db: Session, *, newsletter_id: uuid.UUID) -> int:
    latest = db.scalar(
        select(TeacherAssistNewsletterVersion.version_number)
        .where(TeacherAssistNewsletterVersion.newsletter_id == newsletter_id)
        .order_by(TeacherAssistNewsletterVersion.version_number.desc())
        .limit(1)
    )
    return int(latest or 0) + 1


def create_newsletter_version(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter: TeacherAssistNewsletter,
    content_json: dict[str, Any],
    version_source: str,
    prompt_context_json: dict[str, Any] | None = None,
    provider_name: str | None = None,
    provider_model: str | None = None,
    prompt_version: str | None = None,
    ai_usage_event_id: uuid.UUID | None = None,
    change_reason: str | None = None,
) -> TeacherAssistNewsletterVersion:
    now = datetime.now(UTC)
    version = TeacherAssistNewsletterVersion(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        newsletter_id=newsletter.id,
        version_number=_next_version_number(db, newsletter_id=newsletter.id),
        version_source=validate_newsletter_version_source(version_source),
        content_json=content_json,
        prompt_context_json=prompt_context_json,
        provider_name=provider_name,
        provider_model=provider_model,
        prompt_version=prompt_version,
        ai_usage_event_id=ai_usage_event_id,
        created_by_user_id=user_id,
        change_reason=change_reason,
        created_at=now,
    )
    db.add(version)
    db.flush()
    newsletter.current_version_id = version.id
    newsletter.updated_at = now
    normalized_source = validate_newsletter_version_source(version_source)
    if normalized_source in {"ai_draft", "ai_section_regen"}:
        newsletter.status = validate_newsletter_status("review")
        newsletter.latest_ai_usage_event_id = ai_usage_event_id
    elif normalized_source == "teacher_edit":
        if newsletter.status == validate_newsletter_status("draft"):
            newsletter.status = validate_newsletter_status("review")
    db.flush()
    db.refresh(version)
    return version


def update_newsletter(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter_id: uuid.UUID,
    title: str | None = None,
    status: str | None = None,
    teacher_notes: str | None = None,
    week_start_date: date | None = None,
    week_end_date: date | None = None,
) -> TeacherAssistNewsletter:
    newsletter = get_newsletter_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter_id=newsletter_id,
    )
    if title is not None:
        newsletter.title = _normalize_title(title)
    if teacher_notes is not None:
        newsletter.teacher_notes = teacher_notes.strip() or None
    if week_start_date is not None:
        newsletter.week_start_date = week_start_date
    if week_end_date is not None:
        newsletter.week_end_date = week_end_date
    if status is not None:
        newsletter.status = validate_newsletter_status(status)
    newsletter.updated_at = datetime.now(UTC)
    db.flush()
    db.refresh(newsletter)
    return newsletter


def create_teacher_newsletter_version(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter_id: uuid.UUID,
    content_json: dict[str, Any],
    change_reason: str | None = None,
) -> TeacherAssistNewsletterVersion:
    newsletter = get_newsletter_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter_id=newsletter_id,
    )
    if newsletter.status == "archived":
        raise ValueError("Archived newsletters cannot be edited")
    version = create_newsletter_version(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter=newsletter,
        content_json=content_json,
        version_source="teacher_edit",
        change_reason=change_reason,
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="newsletter_version_created",
        event_category="communication",
        entity_type="newsletter",
        entity_id=newsletter.id,
        school_year_id=newsletter.school_year_id,
        grading_period_id=newsletter.grading_period_id,
        class_id=newsletter.class_id,
        subject_id=newsletter.subject_id,
        summary_text=f"Teacher saved newsletter version {version.version_number}.",
        details_json={
            "newsletter_id": str(newsletter.id),
            "version_id": str(version.id),
            "version_source": version.version_source,
        },
    )
    db.flush()
    return version


def list_newsletter_versions(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter_id: uuid.UUID,
) -> list[TeacherAssistNewsletterVersion]:
    get_newsletter_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter_id=newsletter_id,
    )
    return db.scalars(
        select(TeacherAssistNewsletterVersion)
        .where(
            TeacherAssistNewsletterVersion.tenant_id == tenant_id,
            TeacherAssistNewsletterVersion.owner_user_id == user_id,
            TeacherAssistNewsletterVersion.newsletter_id == newsletter_id,
        )
        .order_by(TeacherAssistNewsletterVersion.version_number.asc())
    ).all()


def build_newsletter_prompt_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter: TeacherAssistNewsletter,
) -> dict[str, Any]:
    weekly_plans = db.scalars(
        select(TeacherAssistWeeklyPlan)
        .join(
            TeacherAssistPlanningInputDraft,
            TeacherAssistPlanningInputDraft.id == TeacherAssistWeeklyPlan.planning_input_draft_id,
        )
        .where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
            TeacherAssistWeeklyPlan.status == "completed",
            TeacherAssistPlanningInputDraft.class_id == newsletter.class_id,
            TeacherAssistPlanningInputDraft.subject_id == newsletter.subject_id,
        )
        .order_by(TeacherAssistWeeklyPlan.updated_at.desc())
        .limit(8)
    ).all()
    weekly_plan_summaries = []
    for plan in weekly_plans:
        if newsletter.grading_period_id:
            draft = db.get(TeacherAssistPlanningInputDraft, plan.planning_input_draft_id)
            if draft and draft.grading_period_id != newsletter.grading_period_id:
                continue
        content = plan.content_json or {}
        weekly_plan_summaries.append(
            {
                "plan_id": str(plan.id),
                "title": plan.title,
                "planning_scope": plan.planning_scope,
                "module_title": plan.module_title,
                "objectives": (
                    content.get("learning_objectives") or content.get("objectives") or []
                )[:5],
                "topics": (content.get("topics") or content.get("weekly_topics") or [])[:5],
            }
        )

    assignments = list_assignments(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=newsletter.school_year_id,
        grading_period_id=newsletter.grading_period_id,
        class_id=newsletter.class_id,
        subject_id=newsletter.subject_id,
    )
    instructional_assignments = []
    assignment_ids: list[uuid.UUID] = []
    for row in assignments:
        if row.status not in INSTRUCTIONAL_ASSIGNMENT_STATUSES:
            continue
        assignment_ids.append(row.id)
        instructional_assignments.append(
            {
                "assignment_id": str(row.id),
                "title": row.title,
                "assignment_type": row.assignment_type,
                "status": row.status,
                "due_date": row.due_date.isoformat() if row.due_date else None,
            }
        )

    standard_codes: list[str] = []
    if assignment_ids:
        assignment_standards = db.scalars(
            select(TeacherAssistAssignmentStandard)
            .where(TeacherAssistAssignmentStandard.assignment_id.in_(assignment_ids))
            .limit(40)
        ).all()
        standard_ids = {row.standard_id for row in assignment_standards}
        if standard_ids:
            standards = db.scalars(
                select(TeacherAssistStandard).where(TeacherAssistStandard.id.in_(standard_ids))
            ).all()
            standard_codes = sorted({row.code for row in standards if row.code})

    grading_period_title = None
    if newsletter.grading_period_id:
        period = get_grading_period_or_404(
            db,
            tenant_id=tenant_id,
            grading_period_id=newsletter.grading_period_id,
        )
        grading_period_title = period.title

    return _json_safe_value(
        {
            "newsletter_id": str(newsletter.id),
            "school_year_id": str(newsletter.school_year_id),
            "grading_period_id": str(newsletter.grading_period_id)
            if newsletter.grading_period_id
            else None,
            "grading_period_title": grading_period_title,
            "class_id": str(newsletter.class_id),
            "subject_id": str(newsletter.subject_id),
            "week_start_date": newsletter.week_start_date.isoformat()
            if newsletter.week_start_date
            else None,
            "week_end_date": newsletter.week_end_date.isoformat()
            if newsletter.week_end_date
            else None,
            "teacher_notes": newsletter.teacher_notes,
            "weekly_plan_summaries": weekly_plan_summaries[:5],
            "instructional_assignments": instructional_assignments[:10],
            "standards_covered_hints": standard_codes[:12],
            "pii_policy": "NO_STUDENT_NAMES_GRADES_BEHAVIOR",
            "anonymous_only": True,
        }
    )


def serialize_newsletter(newsletter: TeacherAssistNewsletter) -> dict[str, Any]:
    return {
        "id": newsletter.id,
        "tenant_id": newsletter.tenant_id,
        "owner_user_id": newsletter.owner_user_id,
        "school_year_id": newsletter.school_year_id,
        "grading_period_id": newsletter.grading_period_id,
        "class_id": newsletter.class_id,
        "subject_id": newsletter.subject_id,
        "title": newsletter.title,
        "status": newsletter.status,
        "week_start_date": newsletter.week_start_date,
        "week_end_date": newsletter.week_end_date,
        "teacher_notes": newsletter.teacher_notes,
        "current_version_id": newsletter.current_version_id,
        "latest_ai_usage_event_id": newsletter.latest_ai_usage_event_id,
        "created_at": newsletter.created_at,
        "updated_at": newsletter.updated_at,
        "subject_name": newsletter.subject.name if newsletter.subject else None,
        "class_name": newsletter.teacher_class.name if newsletter.teacher_class else None,
    }


def serialize_newsletter_version(version: TeacherAssistNewsletterVersion) -> dict[str, Any]:
    return {
        "id": version.id,
        "newsletter_id": version.newsletter_id,
        "version_number": version.version_number,
        "version_source": version.version_source,
        "content_json": version.content_json,
        "prompt_context_json": version.prompt_context_json,
        "provider_name": version.provider_name,
        "provider_model": version.provider_model,
        "prompt_version": version.prompt_version,
        "ai_usage_event_id": version.ai_usage_event_id,
        "created_by_user_id": version.created_by_user_id,
        "change_reason": version.change_reason,
        "created_at": version.created_at,
    }

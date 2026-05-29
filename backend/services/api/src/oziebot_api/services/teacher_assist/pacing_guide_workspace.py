from __future__ import annotations

from datetime import UTC, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.education_catalog import EducationCurriculumResource, EducationObjective, EducationSubject
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_objective import TeacherAssistPacingGuideObjective
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_period_note import TeacherAssistPacingGuidePeriodNote
from oziebot_api.models.teacher_assist_resource_library_item import TeacherAssistResourceLibraryItem
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.current_week_resolver import (
    CurrentWeekResolver,
    _serialize_period,
    build_current_week_payload,
    build_objective_coverage,
    build_pacing_guide_timeline,
)
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    get_catalog_pacing_guide_detail,
    list_catalog_pacing_guides,
)
from oziebot_api.services.teacher_assist.instructional_weeks import find_instructional_week_for_period
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create


def _instructional_week_navigation(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    period_id: uuid.UUID | None,
) -> dict:
    if period_id is None:
        return {}
    week = find_instructional_week_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        pacing_guide_period_id=period_id,
    )
    if week is not None:
        return {
            "instructional_week_id": str(week.id),
            "instructional_week_href": f"/teacher-assist/week/{week.id}",
            "instructional_week_status": week.status,
        }
    return {
        "create_instructional_week_href": (
            f"/teacher-assist/planning/weeks?period_id={period_id}&action=create_instructional_week"
        ),
    }


def _now() -> datetime:
    return datetime.now(UTC)


def upsert_pacing_period_note(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    period_id: uuid.UUID,
    notes: str | None,
) -> TeacherAssistPacingGuidePeriodNote:
    period = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide period not found")
    row = db.scalars(
        select(TeacherAssistPacingGuidePeriodNote).where(
            TeacherAssistPacingGuidePeriodNote.tenant_id == tenant_id,
            TeacherAssistPacingGuidePeriodNote.user_id == user_id,
            TeacherAssistPacingGuidePeriodNote.period_id == period_id,
        )
    ).one_or_none()
    now = _now()
    if row is None:
        row = TeacherAssistPacingGuidePeriodNote(
            tenant_id=tenant_id,
            user_id=user_id,
            period_id=period_id,
            notes=notes.strip() if notes else None,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.notes = notes.strip() if notes else None
        row.updated_at = now
    db.flush()
    return row


def build_pacing_guide_workspace(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    guide_id: uuid.UUID | None = None,
    period_id: uuid.UUID | None = None,
) -> dict:
    current = build_current_week_payload(db, tenant_id=tenant_id, user_id=user_id, guide_id=guide_id)
    active_guide_id = current.get("active_pacing_guide_id")
    guides = list_catalog_pacing_guides(db, tenant_id=tenant_id, active_only=True)
    if active_guide_id is None:
        return {
            "current_week_context": current,
            "timeline": [],
            "objective_coverage": None,
            "selected_period": None,
            "available_guides": [
                {"id": row.id, "title": row.title, "guide_type": row.guide_type, "period_count": len(row.periods)}
                for row in guides
            ],
        }
    detail = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=active_guide_id)
    selected_period_id = period_id or (current.get("current_week") or {}).get("id")
    timeline = build_pacing_guide_timeline(detail.periods, current_period_id=selected_period_id)
    coverage = build_objective_coverage(db, tenant_id=tenant_id, user_id=user_id, guide_id=active_guide_id)
    selected_period = next((row for row in detail.periods if row.id == selected_period_id), None)
    upcoming_period_id = (current.get("upcoming_week") or {}).get("id")
    return {
        "current_week_context": current,
        "timeline": timeline,
        "objective_coverage": coverage,
        "selected_period": _serialize_period(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            period=selected_period,
        ),
        "instructional_week": _instructional_week_navigation(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            period_id=uuid.UUID(str(selected_period_id)) if selected_period_id else None,
        ),
        "upcoming_instructional_week": _instructional_week_navigation(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            period_id=uuid.UUID(str(upcoming_period_id)) if upcoming_period_id else None,
        ),
        "available_guides": [
            {"id": row.id, "title": row.title, "guide_type": row.guide_type, "period_count": len(row.periods)}
            for row in guides
        ],
    }


def build_period_launch_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    period_id: uuid.UUID,
) -> dict:
    period = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
        .options(
            selectinload(TeacherAssistPacingGuidePeriod.objectives).selectinload(
                TeacherAssistPacingGuideObjective.objective
            ),
            selectinload(TeacherAssistPacingGuidePeriod.resources),
        )
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide period not found")
    guide = db.get(TeacherAssistPacingGuide, period.pacing_guide_id)
    if guide is None:
        raise LookupError("Pacing guide not found")

    context = CurrentWeekResolver.resolve(db, tenant_id=tenant_id, user_id=user.id, guide_id=guide.id)
    grading_period_id = context.grading_period.id if context and context.grading_period else None
    subject_id = None
    if guide.catalog_subject_id is not None:
        catalog_subject = db.get(EducationSubject, guide.catalog_subject_id)
        if catalog_subject is not None:
            subject = db.scalars(
                select(TeacherAssistSubject).where(
                    TeacherAssistSubject.tenant_id == tenant_id,
                    TeacherAssistSubject.name.ilike(f"%{catalog_subject.display_name}%"),
                )
            ).first()
            subject_id = subject.id if subject else None
    if subject_id is None:
        prefs = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user.id)
        subject_id = prefs.last_subject_id

    objective_codes: list[str] = []
    standard_ids: list[uuid.UUID] = []
    for mapping in period.objectives:
        objective = mapping.objective or db.get(EducationObjective, mapping.objective_id)
        if objective is None:
            continue
        objective_codes.append(objective.objective_id)
        standard = db.scalars(
            select(TeacherAssistStandard).where(
                TeacherAssistStandard.tenant_id == tenant_id,
                TeacherAssistStandard.code == objective.objective_id,
            )
        ).first()
        if standard is not None:
            standard_ids.append(standard.id)

    resource_ids: list[uuid.UUID] = []
    resource_summaries: list[dict] = []
    for mapping in period.resources:
        title = None
        if mapping.catalog_resource_id is not None:
            resource = db.get(EducationCurriculumResource, mapping.catalog_resource_id)
            title = resource.title if resource else None
        if mapping.resource_library_item_id is not None:
            resource_ids.append(mapping.resource_library_item_id)
        elif title:
            library_item = db.scalars(
                select(TeacherAssistResourceLibraryItem).where(
                    TeacherAssistResourceLibraryItem.tenant_id == tenant_id,
                    TeacherAssistResourceLibraryItem.title == title,
                )
            ).first()
            if library_item is not None:
                resource_ids.append(library_item.id)
        resource_summaries.append(
            {
                "catalog_resource_id": str(mapping.catalog_resource_id) if mapping.catalog_resource_id else None,
                "resource_library_item_id": str(mapping.resource_library_item_id)
                if mapping.resource_library_item_id
                else None,
                "title": title,
                "is_primary": mapping.is_primary,
                "notes": mapping.notes,
            }
        )

    note = db.scalars(
        select(TeacherAssistPacingGuidePeriodNote).where(
            TeacherAssistPacingGuidePeriodNote.tenant_id == tenant_id,
            TeacherAssistPacingGuidePeriodNote.user_id == user.id,
            TeacherAssistPacingGuidePeriodNote.period_id == period_id,
        )
    ).one_or_none()

    notes_parts = [part for part in [period.description, note.notes if note else None] if part]
    combined_notes = "\n\n".join(notes_parts) if notes_parts else None
    return {
        "pacing_guide_id": str(guide.id),
        "pacing_guide_period_id": str(period.id),
        "pacing_guide_title": guide.title,
        "period_title": period.title,
        "school_year_id": str(guide.school_year_id),
        "grading_period_id": str(grading_period_id) if grading_period_id else None,
        "subject_id": str(subject_id) if subject_id else None,
        "title": period.title,
        "module_title": period.title,
        "start_date": period.start_date.isoformat() if period.start_date else None,
        "end_date": period.end_date.isoformat() if period.end_date else None,
        "notes": combined_notes,
        "objective_codes": objective_codes,
        "standard_ids": [str(value) for value in standard_ids],
        "resource_ids": [str(value) for value in resource_ids],
        "resources": resource_summaries,
        "planning_draft": {
            "planning_scope": "weekly",
            "school_year_id": str(guide.school_year_id),
            "grading_period_id": str(grading_period_id) if grading_period_id else None,
            "subject_id": str(subject_id) if subject_id else None,
            "pacing_guide_period_id": str(period.id),
            "title": period.title,
            "module_title": period.title,
            "start_date": period.start_date.isoformat() if period.start_date else None,
            "end_date": period.end_date.isoformat() if period.end_date else None,
            "standard_ids": [str(value) for value in standard_ids],
            "notes": combined_notes,
        },
        "assignment": {
            "school_year_id": str(guide.school_year_id),
            "grading_period_id": str(grading_period_id) if grading_period_id else None,
            "subject_id": str(subject_id) if subject_id else None,
            "title": f"{period.title} Assignment",
            "description": period.description,
            "standard_ids": [str(value) for value in standard_ids],
            "resource_ids": [str(value) for value in resource_ids],
        },
        "newsletter": {
            "school_year_id": str(guide.school_year_id),
            "grading_period_id": str(grading_period_id) if grading_period_id else None,
            "subject_id": str(subject_id) if subject_id else None,
            "title": f"Weekly Newsletter — {period.title}",
            "week_start_date": period.start_date.isoformat() if period.start_date else None,
            "week_end_date": period.end_date.isoformat() if period.end_date else None,
            "notes": "\n\n".join(
                [
                    f"Objectives: {', '.join(objective_codes)}" if objective_codes else "",
                    period.description or "",
                ]
            ).strip(),
        },
    }

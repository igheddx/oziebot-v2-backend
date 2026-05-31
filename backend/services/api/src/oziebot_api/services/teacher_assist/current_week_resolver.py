from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.education_catalog import EducationSubject
from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_objective import TeacherAssistPacingGuideObjective
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_period_note import TeacherAssistPacingGuidePeriodNote
from oziebot_api.models.teacher_assist_pacing_guide_resource import TeacherAssistPacingGuideResource
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_user_preference import TeacherAssistUserPreference
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.services.teacher_assist.pacing_guide_foundation import get_catalog_pacing_guide_detail
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create


@dataclass
class ResolvedWeekContext:
    guide: TeacherAssistPacingGuide
    school_year: TeacherAssistSchoolYear | None
    grading_period: TeacherAssistGradingPeriod | None
    guide_grading_period: TeacherAssistPacingGuidePeriod | None
    guide_unit: TeacherAssistPacingGuidePeriod | None
    current_week: TeacherAssistPacingGuidePeriod | None
    upcoming_week: TeacherAssistPacingGuidePeriod | None
    manual_override: bool


def _periods_by_type(periods: list[TeacherAssistPacingGuidePeriod]) -> dict[str, list[TeacherAssistPacingGuidePeriod]]:
    grouped: dict[str, list[TeacherAssistPacingGuidePeriod]] = {}
    for period in sorted(periods, key=lambda row: row.sequence_number):
        grouped.setdefault(period.period_type, []).append(period)
    return grouped


def _contains_date(period: TeacherAssistPacingGuidePeriod, target: date) -> bool:
    if period.start_date and period.end_date:
        return period.start_date <= target <= period.end_date
    if period.start_date:
        return period.start_date <= target
    return False


def _resolve_week_period(
    weeks: list[TeacherAssistPacingGuidePeriod],
    *,
    target_date: date,
    manual_period_id: uuid.UUID | None,
) -> tuple[TeacherAssistPacingGuidePeriod | None, bool]:
    if manual_period_id is not None:
        manual = next((row for row in weeks if row.id == manual_period_id), None)
        if manual is not None:
            return manual, True
    for week in weeks:
        if _contains_date(week, target_date):
            return week, False
    upcoming = next((week for week in weeks if week.start_date and week.start_date > target_date), None)
    if upcoming is not None:
        return upcoming, False
    return (weeks[0] if weeks else None), False


def _resolve_parent_period(
    periods: list[TeacherAssistPacingGuidePeriod],
    *,
    child: TeacherAssistPacingGuidePeriod | None,
    target_date: date,
) -> TeacherAssistPacingGuidePeriod | None:
    if child is None or not periods:
        return None
    child_start = child.start_date or target_date
    child_end = child.end_date or child_start
    for period in sorted(periods, key=lambda row: row.sequence_number):
        if period.start_date and period.end_date:
            if period.start_date <= child_start and period.end_date >= child_end:
                return period
        elif period.sequence_number <= child.sequence_number:
            candidate = period
    return candidate if periods else None


def _resolve_tenant_grading_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID | None,
    target_date: date,
) -> TeacherAssistGradingPeriod | None:
    if school_year_id is None:
        return None
    rows = db.scalars(
        select(TeacherAssistGradingPeriod).where(
            TeacherAssistGradingPeriod.school_year_id == school_year_id,
        )
    ).all()
    for row in rows:
        if row.start_date and row.end_date and row.start_date <= target_date <= row.end_date:
            return row
    return rows[0] if rows else None


def resolve_active_pacing_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    guide_id: uuid.UUID | None = None,
    allow_auto_fallback: bool = True,
) -> TeacherAssistPacingGuide | None:
    preferences = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    selected_id = guide_id or preferences.active_pacing_guide_id
    if selected_id is not None:
        guide = db.scalars(
            select(TeacherAssistPacingGuide).where(
                TeacherAssistPacingGuide.id == selected_id,
                TeacherAssistPacingGuide.tenant_id == tenant_id,
                TeacherAssistPacingGuide.is_active.is_(True),
            )
        ).one_or_none()
        if guide is not None:
            return guide
    if not allow_auto_fallback:
        return None
    for guide_type in ("TEACHER", "GRADE_LEVEL", "DISTRICT"):
        guide = db.scalars(
            select(TeacherAssistPacingGuide)
            .where(
                TeacherAssistPacingGuide.tenant_id == tenant_id,
                TeacherAssistPacingGuide.is_active.is_(True),
                TeacherAssistPacingGuide.guide_type == guide_type,
            )
            .order_by(TeacherAssistPacingGuide.updated_at.desc())
        ).first()
        if guide is not None:
            return guide
    return None


class CurrentWeekResolver:
    @staticmethod
    def resolve(
        db: Session,
        *,
        tenant_id: uuid.UUID,
        user_id: uuid.UUID,
        guide_id: uuid.UUID | None = None,
        as_of_date: date | None = None,
        require_explicit_guide_selection: bool = False,
    ) -> ResolvedWeekContext | None:
        preferences = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
        guide = resolve_active_pacing_guide(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            guide_id=guide_id,
            allow_auto_fallback=not require_explicit_guide_selection,
        )
        if guide is None:
            return None
        detail = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide.id)
        target_date = as_of_date or date.today()
        grouped = _periods_by_type(detail.periods)
        weeks = grouped.get("WEEK", [])
        current_week, manual_override = _resolve_week_period(
            weeks,
            target_date=target_date,
            manual_period_id=preferences.manual_pacing_period_id,
        )
        upcoming_week = None
        if current_week is not None:
            later_weeks = [row for row in weeks if row.sequence_number > current_week.sequence_number]
            upcoming_week = later_weeks[0] if later_weeks else None
        school_year = db.get(TeacherAssistSchoolYear, guide.school_year_id)
        grading_period = _resolve_tenant_grading_period(
            db,
            tenant_id=tenant_id,
            school_year_id=guide.school_year_id,
            target_date=target_date,
        )
        guide_grading_period = _resolve_parent_period(
            grouped.get("GRADING_PERIOD", []),
            child=current_week,
            target_date=target_date,
        )
        guide_unit = _resolve_parent_period(
            grouped.get("UNIT", []),
            child=current_week,
            target_date=target_date,
        )
        return ResolvedWeekContext(
            guide=detail,
            school_year=school_year,
            grading_period=grading_period,
            guide_grading_period=guide_grading_period,
            guide_unit=guide_unit,
            current_week=current_week,
            upcoming_week=upcoming_week,
            manual_override=manual_override,
        )


def _serialize_objective(row: TeacherAssistPacingGuideObjective) -> dict[str, Any]:
    objective = getattr(row, "objective", None)
    return {
        "id": row.id,
        "objective_id": row.objective_id,
        "is_required": row.is_required,
        "notes": row.notes,
        "objective_code": getattr(objective, "objective_id", None),
        "objective_description": getattr(objective, "description", None),
    }


def _serialize_resource(row: TeacherAssistPacingGuideResource) -> dict[str, Any]:
    catalog_resource = getattr(row, "catalog_resource", None)
    return {
        "id": row.id,
        "catalog_resource_id": row.catalog_resource_id,
        "resource_library_item_id": row.resource_library_item_id,
        "is_primary": row.is_primary,
        "notes": row.notes,
        "resource_title": getattr(catalog_resource, "title", None),
        "resource_type": getattr(catalog_resource, "resource_type", None),
        "storage_key": getattr(catalog_resource, "storage_key", None),
        "external_url": None,
    }


def _serialize_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    period: TeacherAssistPacingGuidePeriod | None,
) -> dict[str, Any] | None:
    if period is None:
        return None
    note = db.scalars(
        select(TeacherAssistPacingGuidePeriodNote).where(
            TeacherAssistPacingGuidePeriodNote.tenant_id == tenant_id,
            TeacherAssistPacingGuidePeriodNote.user_id == user_id,
            TeacherAssistPacingGuidePeriodNote.period_id == period.id,
        )
    ).one_or_none()
    linked_plans = db.scalars(
        select(TeacherAssistWeeklyPlan).where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
            TeacherAssistWeeklyPlan.pacing_guide_period_id == period.id,
        )
    ).all()
    return {
        "id": period.id,
        "period_type": period.period_type,
        "title": period.title,
        "description": period.description,
        "sequence_number": period.sequence_number,
        "start_date": period.start_date.isoformat() if period.start_date else None,
        "end_date": period.end_date.isoformat() if period.end_date else None,
        "objectives": [_serialize_objective(row) for row in period.objectives],
        "resources": [_serialize_resource(row) for row in period.resources],
        "teacher_notes": note.notes if note is not None else None,
        "linked_plan_count": len(linked_plans),
        "linked_plans": [
            {
                "id": row.id,
                "title": row.title,
                "status": row.status,
                "navigation_href": f"/teacher-assist/weekly-planning/plans?id={row.id}",
            }
            for row in linked_plans[:5]
        ],
    }


def build_teaching_progress(periods: list[TeacherAssistPacingGuidePeriod], *, as_of_date: date) -> dict[str, Any]:
    weeks = [row for row in periods if row.period_type == "WEEK"]
    completed_weeks = [
        row for row in weeks if row.end_date is not None and row.end_date < as_of_date
    ]
    all_objective_ids: set[uuid.UUID] = set()
    covered_objective_ids: set[uuid.UUID] = set()
    for week in weeks:
        for objective in week.objectives:
            all_objective_ids.add(objective.objective_id)
            if week.end_date is not None and week.end_date < as_of_date:
                covered_objective_ids.add(objective.objective_id)
    remaining_objectives = len(all_objective_ids - covered_objective_ids)
    return {
        "weeks_total": len(weeks),
        "weeks_completed": len(completed_weeks),
        "weeks_remaining": max(len(weeks) - len(completed_weeks), 0),
        "objectives_total": len(all_objective_ids),
        "objectives_covered": len(covered_objective_ids),
        "objectives_remaining": remaining_objectives,
    }


def build_current_week_payload(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    guide_id: uuid.UUID | None = None,
    as_of_date: date | None = None,
    require_explicit_guide_selection: bool = False,
) -> dict[str, Any]:
    target_date = as_of_date or date.today()
    context = CurrentWeekResolver.resolve(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        guide_id=guide_id,
        as_of_date=target_date,
        require_explicit_guide_selection=require_explicit_guide_selection,
    )
    preferences = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    if context is None:
        return {
            "has_active_guide": False,
            "active_pacing_guide_id": preferences.active_pacing_guide_id,
            "manual_pacing_period_id": preferences.manual_pacing_period_id,
            "as_of_date": target_date.isoformat(),
        }
    guide = context.guide
    catalog_subject = (
        db.get(EducationSubject, guide.catalog_subject_id) if guide.catalog_subject_id is not None else None
    )
    detail = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide.id)
    progress = build_teaching_progress(detail.periods, as_of_date=target_date)
    return {
        "has_active_guide": True,
        "as_of_date": target_date.isoformat(),
        "manual_override": context.manual_override,
        "active_pacing_guide_id": guide.id,
        "manual_pacing_period_id": preferences.manual_pacing_period_id,
        "school_year": {
            "id": context.school_year.id,
            "title": context.school_year.title,
            "start_date": context.school_year.start_date.isoformat(),
            "end_date": context.school_year.end_date.isoformat(),
        }
        if context.school_year is not None
        else None,
        "pacing_guide": {
            "id": guide.id,
            "title": guide.title,
            "guide_type": guide.guide_type,
            "school_year_label": guide.school_year_label,
            "catalog_subject_code": catalog_subject.subject_code if catalog_subject else None,
            "catalog_subject_name": catalog_subject.display_name if catalog_subject else None,
        },
        "grading_period": {
            "id": context.grading_period.id,
            "title": context.grading_period.title,
            "start_date": context.grading_period.start_date.isoformat() if context.grading_period.start_date else None,
            "end_date": context.grading_period.end_date.isoformat() if context.grading_period.end_date else None,
        }
        if context.grading_period is not None
        else None,
        "guide_grading_period": _serialize_period(
            db, tenant_id=tenant_id, user_id=user_id, period=context.guide_grading_period
        ),
        "guide_unit": _serialize_period(db, tenant_id=tenant_id, user_id=user_id, period=context.guide_unit),
        "current_week": _serialize_period(db, tenant_id=tenant_id, user_id=user_id, period=context.current_week),
        "upcoming_week": _serialize_period(db, tenant_id=tenant_id, user_id=user_id, period=context.upcoming_week),
        "teaching_progress": progress,
    }


def build_pacing_guide_timeline(periods: list[TeacherAssistPacingGuidePeriod], *, current_period_id: uuid.UUID | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for period in sorted(periods, key=lambda row: (row.period_type, row.sequence_number)):
        rows.append(
            {
                "id": period.id,
                "period_type": period.period_type,
                "title": period.title,
                "sequence_number": period.sequence_number,
                "start_date": period.start_date.isoformat() if period.start_date else None,
                "end_date": period.end_date.isoformat() if period.end_date else None,
                "is_current": period.id == current_period_id,
            }
        )
    return rows


def build_objective_coverage(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    guide_id: uuid.UUID,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    target_date = as_of_date or date.today()
    detail = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide_id)
    planned_objective_ids: set[uuid.UUID] = set()
    for plan in db.scalars(
        select(TeacherAssistWeeklyPlan).where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
            TeacherAssistWeeklyPlan.pacing_guide_period_id.is_not(None),
        )
    ).all():
        if plan.pacing_guide_period_id is None:
            continue
        period = next((row for row in detail.periods if row.id == plan.pacing_guide_period_id), None)
        if period is None:
            continue
        for objective in period.objectives:
            planned_objective_ids.add(objective.objective_id)

    rows: list[dict[str, Any]] = []
    seen: set[uuid.UUID] = set()
    for period in sorted(detail.periods, key=lambda row: row.sequence_number):
        for mapping in period.objectives:
            if mapping.objective_id in seen:
                continue
            seen.add(mapping.objective_id)
            objective = getattr(mapping, "objective", None)
            status = "not_yet_scheduled"
            if period.end_date is not None and period.end_date < target_date:
                status = "covered"
            if mapping.objective_id in planned_objective_ids:
                status = "planned"
            rows.append(
                {
                    "objective_id": mapping.objective_id,
                    "objective_code": getattr(objective, "objective_id", None),
                    "objective_description": getattr(objective, "description", None),
                    "period_id": period.id,
                    "period_title": period.title,
                    "coverage_status": status,
                }
            )
    return {
        "pacing_guide_id": guide_id,
        "objectives": rows,
        "summary": {
            "total": len(rows),
            "covered": sum(1 for row in rows if row["coverage_status"] == "covered"),
            "planned": sum(1 for row in rows if row["coverage_status"] == "planned"),
            "not_yet_scheduled": sum(1 for row in rows if row["coverage_status"] == "not_yet_scheduled"),
        },
    }

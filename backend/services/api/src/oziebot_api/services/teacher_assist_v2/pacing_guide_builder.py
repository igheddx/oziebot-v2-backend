"""Root-admin pacing guide builder create/update."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.education_catalog import get_objective_or_404
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    add_pacing_guide_objective,
    create_catalog_pacing_guide,
    create_pacing_guide_period,
    get_catalog_pacing_guide_detail,
    update_catalog_pacing_guide,
)
from oziebot_api.services.teacher_assist_v2.pacing_guide_period_days import (
    daily_plans_metadata_snapshot,
    replace_period_days,
)
from oziebot_api.services.teacher_assist_v2.school_years import get_platform_school_year_or_404


def _now() -> datetime:
    return datetime.now(UTC)


def _field_errors(**errors: str) -> ValueError:
    return ValueError({key: value for key, value in errors.items() if value})


def _validate_objectives_for_guide(
    db: Session,
    *,
    catalog_grade_id: uuid.UUID,
    catalog_subject_id: uuid.UUID,
    objective_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not objective_rows:
        raise _field_errors(objectives="Select at least one learning objective.")
    validated: list[dict[str, Any]] = []
    seen: set[uuid.UUID] = set()
    for row in objective_rows:
        objective_id = uuid.UUID(str(row["objective_id"]))
        if objective_id in seen:
            continue
        seen.add(objective_id)
        objective = get_objective_or_404(db, objective_id)
        if objective.grade_id and objective.grade_id != catalog_grade_id:
            raise _field_errors(objectives="Selected objective does not match the guide grade.")
        if objective.subject_id and objective.subject_id != catalog_subject_id:
            raise _field_errors(objectives="Selected objective does not match the guide subject.")
        validated.append(
            {
                "objective_id": objective_id,
                "is_required": bool(row.get("is_required", True)),
                "notes": (row.get("notes") or None),
            }
        )
    return validated


def _validate_weeks(weeks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not weeks:
        raise _field_errors(weeks="Add at least one week with a daily teaching plan.")
    validated: list[dict[str, Any]] = []
    for index, week in enumerate(weeks, start=1):
        daily_plans = week.get("daily_plans") or []
        if not daily_plans:
            raise _field_errors(weeks=f"Week {index} needs at least one day in the daily plan.")
        normalized_days = []
        for day in daily_plans:
            day_label = str(day.get("day_label") or "").strip()
            daily_topic = str(day.get("daily_topic") or "").strip()
            if not day_label:
                continue
            if not daily_topic:
                raise _field_errors(weeks=f"Week {index} {day_label} needs a daily topic.")
            normalized_days.append(
                {
                    "day_label": day_label,
                    "daily_topic": daily_topic,
                    "objective_focus": day.get("objective_focus"),
                    "teacher_notes": day.get("teacher_notes"),
                    "materials_needed": day.get("materials_needed"),
                    "assessment_check": day.get("assessment_check"),
                }
            )
        if not normalized_days:
            raise _field_errors(weeks=f"Week {index} needs labeled days in the daily plan.")
        validated.append(
            {
                "title": str(week.get("title") or f"Week {index}").strip(),
                "description": (week.get("description") or None),
                "sequence_number": int(week.get("sequence_number") or index),
                "unit_title": (week.get("unit_title") or None),
                "daily_plans": normalized_days,
                "objective_ids": [uuid.UUID(str(value)) for value in week.get("objective_ids") or []],
            }
        )
    return validated


def _apply_scope_metadata(guide: TeacherAssistPacingGuide, *, body: dict[str, Any]) -> None:
    ownership_scope = str(body.get("ownership_scope") or "district").strip().lower()
    if ownership_scope == "school" and not body.get("catalog_school_id"):
        raise _field_errors(catalog_school_id="School is required for school-scoped pacing guides.")
    guide.ownership_type = "DISTRICT" if ownership_scope == "district" else "SCHOOL"
    guide.visibility_scope = "DISTRICT" if ownership_scope == "district" else "SCHOOL"
    guide.metadata_json = {
        "ownership_scope": ownership_scope,
        "unit_title": body.get("unit_title"),
        "estimated_duration_weeks": body.get("estimated_duration_weeks"),
        "start_week": body.get("start_week"),
        "end_week": body.get("end_week"),
        "platform_school_year_id": str(body["platform_school_year_id"]),
    }


def _replace_guide_structure(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    guide: TeacherAssistPacingGuide,
    objectives: list[dict[str, Any]],
    weeks: list[dict[str, Any]],
) -> None:
    db.execute(
        delete(TeacherAssistPacingGuidePeriod).where(TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id)
    )
    db.flush()

    default_objectives = objectives
    for week in weeks:
        period = create_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=guide.id,
            period_type="WEEK",
            title=week["title"],
            description=week.get("description"),
            sequence_number=week["sequence_number"],
            start_date=None,
            end_date=None,
        )
        day_rows = replace_period_days(db, period=period, daily_plans=week["daily_plans"])
        period.metadata_json = {
            "unit_title": week.get("unit_title"),
            "daily_plans": daily_plans_metadata_snapshot(day_rows),
        }
        period.updated_at = _now()
        week_objective_ids = week.get("objective_ids") or [row["objective_id"] for row in default_objectives]
        if not week_objective_ids:
            week_objective_ids = [row["objective_id"] for row in default_objectives]
        for objective_id in week_objective_ids:
            matching = next((row for row in default_objectives if row["objective_id"] == objective_id), None)
            add_pacing_guide_objective(
                db,
                tenant_id=tenant_id,
                period_id=period.id,
                objective_id=objective_id,
                is_required=matching["is_required"] if matching else True,
                notes=matching["notes"] if matching else None,
            )
    guide.updated_at = _now()
    db.flush()


def create_v2_pacing_guide_from_builder(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    body: dict[str, Any],
) -> TeacherAssistPacingGuide:
    if not body.get("catalog_district_id"):
        raise _field_errors(catalog_district_id="District is required.")
    if not body.get("platform_school_year_id"):
        raise _field_errors(platform_school_year_id="School year is required.")
    if not body.get("catalog_grade_id"):
        raise _field_errors(catalog_grade_id="Grade is required.")
    if not body.get("catalog_subject_id"):
        raise _field_errors(catalog_subject_id="Subject is required.")

    platform_year = get_platform_school_year_or_404(
        db, school_year_id=uuid.UUID(str(body["platform_school_year_id"]))
    )
    from oziebot_api.services.teacher_assist_v2.pacing_guides import ensure_tenant_school_year

    tenant_year = ensure_tenant_school_year(db, tenant_id=tenant_id, platform_year=platform_year)
    ownership_scope = str(body.get("ownership_scope") or "district").strip().lower()
    catalog_school_id = uuid.UUID(str(body["catalog_school_id"])) if body.get("catalog_school_id") else None
    if ownership_scope == "school" and catalog_school_id is None:
        raise _field_errors(catalog_school_id="School is required for school-scoped pacing guides.")

    objectives = _validate_objectives_for_guide(
        db,
        catalog_grade_id=uuid.UUID(str(body["catalog_grade_id"])),
        catalog_subject_id=uuid.UUID(str(body["catalog_subject_id"])),
        objective_rows=body.get("objectives") or [],
    )
    weeks = _validate_weeks(body.get("weeks") or [])

    guide = create_catalog_pacing_guide(
        db,
        tenant_id=tenant_id,
        actor=actor,
        school_year_id=tenant_year.id,
        guide_type="DISTRICT",
        title=str(body["title"]).strip(),
        description=(body.get("description") or None),
        catalog_state_id=uuid.UUID(str(body["catalog_state_id"])) if body.get("catalog_state_id") else None,
        catalog_district_id=uuid.UUID(str(body["catalog_district_id"])),
        catalog_school_id=catalog_school_id if ownership_scope == "school" else None,
        catalog_grade_id=uuid.UUID(str(body["catalog_grade_id"])),
        catalog_subject_id=uuid.UUID(str(body["catalog_subject_id"])),
        is_template=False,
        is_shared=True,
    )
    _apply_scope_metadata(guide, body=body)
    _replace_guide_structure(db, tenant_id=tenant_id, guide=guide, objectives=objectives, weeks=weeks)
    return get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide.id)


def update_v2_pacing_guide_from_builder(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    pacing_guide_id: uuid.UUID,
    body: dict[str, Any],
) -> TeacherAssistPacingGuide:
    existing = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    platform_year = get_platform_school_year_or_404(
        db, school_year_id=uuid.UUID(str(body["platform_school_year_id"]))
    )
    from oziebot_api.services.teacher_assist_v2.pacing_guides import ensure_tenant_school_year

    tenant_year = ensure_tenant_school_year(db, tenant_id=tenant_id, platform_year=platform_year)
    ownership_scope = str(body.get("ownership_scope") or "district").strip().lower()
    catalog_school_id = uuid.UUID(str(body["catalog_school_id"])) if body.get("catalog_school_id") else None
    if ownership_scope == "school" and catalog_school_id is None:
        raise _field_errors(catalog_school_id="School is required for school-scoped pacing guides.")

    guide = update_catalog_pacing_guide(
        db,
        tenant_id=tenant_id,
        pacing_guide_id=pacing_guide_id,
        actor=actor,
        school_year_id=tenant_year.id,
        guide_type="DISTRICT",
        title=str(body["title"]).strip(),
        description=(body.get("description") or None),
        catalog_state_id=uuid.UUID(str(body["catalog_state_id"])) if body.get("catalog_state_id") else existing.catalog_state_id,
        catalog_district_id=uuid.UUID(str(body["catalog_district_id"])),
        catalog_school_id=catalog_school_id if ownership_scope == "school" else None,
        catalog_grade_id=uuid.UUID(str(body["catalog_grade_id"])),
        catalog_subject_id=uuid.UUID(str(body["catalog_subject_id"])),
        is_template=existing.is_template,
        is_active=existing.is_active,
        is_shared=True,
    )
    objectives = _validate_objectives_for_guide(
        db,
        catalog_grade_id=uuid.UUID(str(body["catalog_grade_id"])),
        catalog_subject_id=uuid.UUID(str(body["catalog_subject_id"])),
        objective_rows=body.get("objectives") or [],
    )
    weeks = _validate_weeks(body.get("weeks") or [])
    _apply_scope_metadata(guide, body=body)
    _replace_guide_structure(db, tenant_id=tenant_id, guide=guide, objectives=objectives, weeks=weeks)
    return get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide.id)


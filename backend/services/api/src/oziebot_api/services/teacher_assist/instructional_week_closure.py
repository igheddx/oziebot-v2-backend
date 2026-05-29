"""Instructional week closure and summary generation."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_instructional_week_closure import (
    TeacherAssistInstructionalWeekClosure,
    TeacherAssistInstructionalWeekSummary,
)
from oziebot_api.models.teacher_assist_instructional_week import TeacherAssistInstructionalWeek
from oziebot_api.services.teacher_assist.assignment_coverage import build_assignment_coverage_view
from oziebot_api.services.teacher_assist.constants import WEEK_CLOSURE_CHECKLIST_KEYS, validate_instructional_week_closure_status
from oziebot_api.services.teacher_assist.instructional_reflections import list_instructional_reflections
from oziebot_api.services.teacher_assist.instructional_weeks import get_instructional_week
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist.reteach_workspace import build_reteach_workspace
from oziebot_api.services.teacher_assist.student_support_groups import list_support_groups, serialize_support_group


def _now() -> datetime:
    return datetime.now(UTC)


def default_closure_checklist() -> dict[str, bool]:
    return {key: False for key in WEEK_CLOSURE_CHECKLIST_KEYS}


def get_or_create_week_closure(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID,
) -> TeacherAssistInstructionalWeekClosure:
    get_instructional_week(
        db, tenant_id=tenant_id, user_id=user_id, instructional_week_id=instructional_week_id
    )
    row = db.scalars(
        select(TeacherAssistInstructionalWeekClosure).where(
            TeacherAssistInstructionalWeekClosure.tenant_id == tenant_id,
            TeacherAssistInstructionalWeekClosure.owner_user_id == user_id,
            TeacherAssistInstructionalWeekClosure.instructional_week_id == instructional_week_id,
        )
    ).one_or_none()
    if row is not None:
        return row
    now = _now()
    row = TeacherAssistInstructionalWeekClosure(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        instructional_week_id=instructional_week_id,
        status=validate_instructional_week_closure_status("in_progress"),
        checklist_json=default_closure_checklist(),
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_week_closure_checklist(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID,
    checklist: dict[str, bool],
) -> TeacherAssistInstructionalWeekClosure:
    row = get_or_create_week_closure(
        db, tenant_id=tenant_id, user_id=user_id, instructional_week_id=instructional_week_id
    )
    merged = default_closure_checklist()
    merged.update({key: bool(value) for key, value in checklist.items() if key in merged})
    row.checklist_json = merged
    row.updated_at = _now()
    if all(merged.values()):
        row.status = validate_instructional_week_closure_status("completed")
        row.closed_at = _now()
    db.flush()
    return row


def generate_instructional_week_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID,
) -> TeacherAssistInstructionalWeekSummary:
    week = get_instructional_week(
        db, tenant_id=tenant_id, user_id=user_id, instructional_week_id=instructional_week_id
    )
    performance = ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=instructional_week_id,
    )
    coverage = build_assignment_coverage_view(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=instructional_week_id,
    )
    reteach = build_reteach_workspace(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=instructional_week_id,
    )
    reflections = list_instructional_reflections(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=instructional_week_id,
    )
    groups = list_support_groups(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        instructional_week_id=instructional_week_id,
    )
    closure = get_or_create_week_closure(
        db, tenant_id=tenant_id, user_id=user_id, instructional_week_id=instructional_week_id
    )

    summary_json: dict[str, Any] = {
        "week_title": week.title,
        "week_number": week.week_number,
        "objectives_covered": len(performance.get("objectives") or []),
        "assignments_completed": len(coverage.get("assignments") or []),
        "assessments_completed": sum(
            1 for row in coverage.get("assignments") or [] if row.get("students_assessed", 0) > 0
        ),
        "mastery_results": performance.get("objectives") or [],
        "reteach_candidates": reteach.get("objectives_requiring_reteach") or [],
        "support_groups": [serialize_support_group(row) for row in groups],
        "teacher_reflection": reflections[0].what_worked if reflections else None,
        "closure_checklist": closure.checklist_json,
        "reusable_next_year": True,
    }

    now = _now()
    row = TeacherAssistInstructionalWeekSummary(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        instructional_week_id=instructional_week_id,
        summary_json=summary_json,
        reusable_next_year=True,
        generated_at=now,
        created_at=now,
    )
    db.add(row)
    db.flush()
    return row


def serialize_week_closure(row: TeacherAssistInstructionalWeekClosure) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "instructional_week_id": str(row.instructional_week_id),
        "status": row.status,
        "checklist": row.checklist_json,
        "closed_at": row.closed_at.isoformat() if row.closed_at else None,
        "updated_at": row.updated_at.isoformat(),
    }


def serialize_week_summary(row: TeacherAssistInstructionalWeekSummary) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "instructional_week_id": str(row.instructional_week_id),
        "summary": row.summary_json,
        "reusable_next_year": row.reusable_next_year,
        "generated_at": row.generated_at.isoformat(),
    }

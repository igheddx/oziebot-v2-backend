"""Instructional Recovery Queue — CRUD and state machine for teacher-directed recovery items."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
)
from oziebot_api.models.teacher_assist_v2_recovery_queue import TeacherAssistV2RecoveryQueue
from oziebot_api.models.user import User

# ── Constants ──────────────────────────────────────────────────────────────────

RECOVERY_RESPONSE_TYPES = {
    "embedded_review",
    "bell_ringer",
    "small_group",
    "whole_class_recovery",
    "spiral_review",
    "conference",
    "homework_reinforcement",
    "defer",
    "dismiss",
}

RECOVERY_RECOMMENDATION_TYPES = {"whole_class", "small_group", "individual_follow_up", "extension"}

PRIORITY_LEVELS = ("LOW", "MEDIUM", "HIGH", "CRITICAL")

_SUGGESTED_PRIORITY: dict[str, str] = {
    "whole_class": "HIGH",
    "small_group": "MEDIUM",
    "individual_follow_up": "LOW",
    "extension": "LOW",
}

# Valid status transitions: current → allowed next statuses
_STATUS_TRANSITIONS: dict[str, set[str]] = {
    "pending": {"scheduled", "deferred", "dismissed"},
    "scheduled": {"completed", "deferred", "dismissed"},
    "deferred": {"scheduled", "dismissed"},
    "completed": set(),
    "dismissed": set(),
}

ACTIVE_STATUSES = ("pending", "scheduled", "deferred")


# ── Helpers ────────────────────────────────────────────────────────────────────


def _infer_best_before(
    db: Session,
    *,
    assignment_id: uuid.UUID | None,
    instructional_package_id: uuid.UUID | None,
) -> tuple[date | None, str | None]:
    """Return (best_before_date, reason) inferred from the assignment's week position.

    Phase 7: sets best_before to 3 calendar days before the next week starts.
    Phase 8 will replace this with a KDG edge scan.
    """
    assignment: TeacherAssistV2Assignment | None = None
    package: TeacherAssistV2InstructionalPackage | None = None

    if assignment_id is not None:
        assignment = db.get(TeacherAssistV2Assignment, assignment_id)
    if assignment is not None and assignment.instructional_package_id is not None:
        package = db.get(TeacherAssistV2InstructionalPackage, assignment.instructional_package_id)
    elif instructional_package_id is not None:
        package = db.get(TeacherAssistV2InstructionalPackage, instructional_package_id)

    if package is None or assignment is None:
        return None, None

    week_number: int = assignment.week_number
    plan_start: date = package.plan_start_date
    # Next week starts (week_number * 7) calendar days after plan start
    next_week_start = plan_start + timedelta(weeks=week_number)
    best_before = next_week_start - timedelta(days=3)

    if best_before <= date.today():
        return None, None

    return (
        best_before,
        f"Recovery before Week {week_number + 1} (starting {next_week_start.isoformat()}) "
        "maintains instructional continuity for dependent objectives.",
    )


def _serialize_item(item: TeacherAssistV2RecoveryQueue) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "assignment_id": str(item.assignment_id) if item.assignment_id else None,
        "instructional_package_id": str(item.instructional_package_id)
        if item.instructional_package_id
        else None,
        "education_objective_id": str(item.education_objective_id)
        if item.education_objective_id
        else None,
        "objective_code": item.objective_code,
        "recommendation_type": item.recommendation_type,
        "students_affected": item.students_affected_json or [],
        "students_affected_count": len(item.students_affected_json or []),
        "misconception_text": item.misconception_text,
        "reason": item.reason,
        "evidence_snapshot": item.evidence_snapshot_json,
        "mastery_snapshot": item.mastery_snapshot_json,
        "strategy_metadata": item.strategy_metadata_json,
        "priority": item.priority,
        "suggested_priority": item.suggested_priority,
        "best_before": item.best_before.isoformat() if item.best_before else None,
        "best_before_reason": item.best_before_reason,
        "best_before_at_risk": (
            item.best_before is not None and item.best_before <= date.today() + timedelta(days=5)
        ),
        "status": item.status,
        "teacher_response": item.teacher_response,
        "teacher_notes": item.teacher_notes,
        "scheduled_for": item.scheduled_for.isoformat() if item.scheduled_for else None,
        "completed_at": item.completed_at.isoformat() if item.completed_at else None,
        # Phase 8 stubs — null in Phase 7
        "success_criteria": item.success_criteria_json,
        "timeline_phase": item.timeline_phase,
        "post_recovery_mastery_snapshot": item.post_recovery_mastery_snapshot_json,
        "created_at": item.created_at.isoformat(),
        "updated_at": item.updated_at.isoformat(),
    }


# ── Public API ─────────────────────────────────────────────────────────────────


def create_queue_item(
    db: Session,
    *,
    user: User,
    recommendation_type: str,
    reason: str,
    students_affected: list[int],
    assignment_id: uuid.UUID | None = None,
    instructional_package_id: uuid.UUID | None = None,
    education_objective_id: uuid.UUID | None = None,
    misconception_text: str | None = None,
    evidence_snapshot: dict[str, Any] | None = None,
    mastery_snapshot: dict[str, Any] | None = None,
    priority: str | None = None,
) -> dict[str, Any]:
    if recommendation_type not in RECOVERY_RECOMMENDATION_TYPES:
        raise ValueError(f"Invalid recommendation_type: {recommendation_type!r}")
    if priority and priority not in PRIORITY_LEVELS:
        raise ValueError(f"Invalid priority: {priority!r}")

    # Resolve objective label
    objective_code: str | None = None
    if education_objective_id is not None:
        obj = db.get(EducationObjective, education_objective_id)
        if obj is not None:
            objective_code = obj.objective_id

    suggested_priority = _SUGGESTED_PRIORITY.get(recommendation_type, "MEDIUM")
    effective_priority = priority or suggested_priority

    best_before, best_before_reason = _infer_best_before(
        db,
        assignment_id=assignment_id,
        instructional_package_id=instructional_package_id,
    )

    strategy_metadata: dict[str, Any] = {
        "strategy_version": "deterministic_v1",
        "recommended_strategy": recommendation_type,
        "strategy_basis": reason,
        "confidence_score": None,
        "confidence_label": None,
        "confidence_basis": None,
    }

    # Phase 8: Pre-populate success_criteria_json from mastery snapshot context
    success_criteria: dict[str, Any] = {
        "target_mastery_percent": 80.0,
        "assessment_type": "exit_ticket",
        "evaluation_window_days": 5,
        "specific_gap_to_close": misconception_text or "Objective gap",
        "observable_evidence": "Student demonstrates mastery of the objective",
        "current_mastery_percent": (mastery_snapshot or {}).get("mastery_percentage"),
    }
    if mastery_snapshot and mastery_snapshot.get("mastery_percentage") is not None:
        gain_needed = max(0.0, 80.0 - float(mastery_snapshot["mastery_percentage"]))
        success_criteria["mastery_gain_needed"] = gain_needed

    now = datetime.now(UTC)
    item = TeacherAssistV2RecoveryQueue(
        id=uuid.uuid4(),
        tenant_id=user.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment_id,
        instructional_package_id=instructional_package_id,
        education_objective_id=education_objective_id,
        objective_code=objective_code,
        recommendation_type=recommendation_type,
        students_affected_json=students_affected,
        misconception_text=misconception_text,
        reason=reason,
        evidence_snapshot_json=evidence_snapshot,
        mastery_snapshot_json=mastery_snapshot,
        strategy_metadata_json=strategy_metadata,
        priority=effective_priority,
        suggested_priority=suggested_priority,
        best_before=best_before,
        best_before_reason=best_before_reason,
        status="pending",
        # Phase 8: 4-stage recovery lifecycle
        timeline_phase="recovery_goal",
        success_criteria_json=success_criteria,
        created_at=now,
        updated_at=now,
    )
    db.add(item)
    db.flush()
    return _serialize_item(item)


def list_queue_items(
    db: Session,
    *,
    user: User,
    statuses: list[str] | None = None,
    assignment_id: uuid.UUID | None = None,
    instructional_package_id: uuid.UUID | None = None,
) -> list[dict[str, Any]]:
    if statuses is None:
        statuses = list(ACTIVE_STATUSES)

    stmt = select(TeacherAssistV2RecoveryQueue).where(
        TeacherAssistV2RecoveryQueue.teacher_user_id == user.id,
        TeacherAssistV2RecoveryQueue.status.in_(statuses),
    )
    if assignment_id is not None:
        stmt = stmt.where(TeacherAssistV2RecoveryQueue.assignment_id == assignment_id)
    if instructional_package_id is not None:
        stmt = stmt.where(
            TeacherAssistV2RecoveryQueue.instructional_package_id == instructional_package_id
        )

    # Sort: CRITICAL first, then by best_before ASC (nulls last), then created_at
    priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    rows = db.scalars(stmt.order_by(TeacherAssistV2RecoveryQueue.created_at.asc())).all()
    rows = sorted(
        rows,
        key=lambda r: (
            priority_order.get(r.priority, 99),
            r.best_before or date(9999, 12, 31),
            r.created_at,
        ),
    )
    return [_serialize_item(r) for r in rows]


def update_queue_item(
    db: Session,
    *,
    user: User,
    item_id: uuid.UUID,
    teacher_response: str | None = None,
    teacher_notes: str | None = None,
    scheduled_for: date | None = None,
    priority: str | None = None,
    status: str | None = None,
    post_recovery_mastery_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = db.scalars(
        select(TeacherAssistV2RecoveryQueue).where(
            TeacherAssistV2RecoveryQueue.id == item_id,
            TeacherAssistV2RecoveryQueue.teacher_user_id == user.id,
        )
    ).first()
    if item is None:
        raise LookupError("Recovery queue item not found")

    now = datetime.now(UTC)

    # Resolve target status from teacher_response if status not explicitly given
    derived_status: str | None = None
    if teacher_response is not None:
        if teacher_response not in RECOVERY_RESPONSE_TYPES:
            raise ValueError(f"Invalid teacher_response: {teacher_response!r}")
        if teacher_response == "dismiss":
            derived_status = "dismissed"
        elif teacher_response == "defer":
            derived_status = "deferred"
        else:
            derived_status = "scheduled"
        item.teacher_response = teacher_response

    target_status = status or derived_status
    if target_status is not None and target_status != item.status:
        allowed = _STATUS_TRANSITIONS.get(item.status, set())
        if target_status not in allowed:
            raise ValueError(
                f"Cannot transition status from {item.status!r} to {target_status!r}. "
                f"Allowed: {sorted(allowed) or 'none (terminal state)'}"
            )
        item.status = target_status
        if target_status == "completed":
            item.completed_at = now
            # Phase 8: Advance to verification stage when activity is delivered
            if item.timeline_phase in (None, "recovery_goal", "recovery_activity"):
                item.timeline_phase = "recovery_verification"

    if teacher_notes is not None:
        item.teacher_notes = teacher_notes
    if scheduled_for is not None:
        item.scheduled_for = scheduled_for
    if priority is not None:
        if priority not in PRIORITY_LEVELS:
            raise ValueError(f"Invalid priority: {priority!r}")
        item.priority = priority
    if post_recovery_mastery_snapshot is not None:
        item.post_recovery_mastery_snapshot_json = post_recovery_mastery_snapshot
        # Phase 8: Recording outcome closes the 4-stage lifecycle
        item.timeline_phase = "recovery_outcome"

    item.updated_at = now
    db.flush()
    return _serialize_item(item)

"""Deterministic Instructional Recovery Budget — no AI calls, no writes."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
)
from oziebot_api.models.teacher_assist_v2_recovery_queue import TeacherAssistV2RecoveryQueue
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.recovery_queue import ACTIVE_STATUSES

# ── Constants ──────────────────────────────────────────────────────────────────

MINUTES_PER_CLASS_PERIOD = 50

# Estimated class-time cost per teacher_response type (minutes)
# conference: base per student, multiplied by student count
RECOVERY_TIME_MINUTES: dict[str, int] = {
    "embedded_review": 10,
    "bell_ringer": 8,
    "small_group": 20,
    "whole_class_recovery": 45,
    "spiral_review": 30,
    "conference": 10,
    "homework_reinforcement": 0,
    "defer": 0,
    "dismiss": 0,
}

# Lesson components in displacement priority order (most interruptible first)
# Assumption: if no explicit time allocation in plan, use these defaults
_DEFAULT_COMPONENT_MINUTES: dict[str, int] = {
    "independent_practice": 10,
    "guided_practice": 15,
    "direct_instruction": 15,
    "hook": 5,
    "closure": 5,
}
_DISPLACEABLE_COMPONENTS = ("independent_practice", "guided_practice", "direct_instruction")


# ── Helpers ────────────────────────────────────────────────────────────────────


def _item_estimated_minutes(item: TeacherAssistV2RecoveryQueue) -> int:
    response = item.teacher_response or item.recommendation_type
    # Map recommendation_type to closest response time estimate
    _type_map = {
        "whole_class": "whole_class_recovery",
        "small_group": "small_group",
        "individual_follow_up": "conference",
        "extension": "embedded_review",
    }
    key = _type_map.get(response, response)
    base = RECOVERY_TIME_MINUTES.get(key, 20)
    if key == "conference":
        count = len(item.students_affected_json or [])
        return base * max(1, count)
    return base


def _pacing_impact_label(percent: float) -> str:
    if percent < 10:
        return "Low — recovery fits within existing instructional margins."
    elif percent < 25:
        return "Moderate — recovery is feasible with minor scheduling adjustments."
    elif percent < 50:
        return "High — recovery will require deliberate pacing trade-offs."
    else:
        return "Critical — recovery volume exceeds available time; prioritization is required."


def _displaced_components(recovery_minutes: int) -> tuple[str, int, str]:
    """Return (component_name, displaced_minutes, impact_note) for the given recovery time."""
    remaining = recovery_minutes
    displaced: list[str] = []
    total_displaced = 0

    for component in _DISPLACEABLE_COMPONENTS:
        if remaining <= 0:
            break
        component_budget = _DEFAULT_COMPONENT_MINUTES[component]
        taken = min(remaining, component_budget)
        if taken > 0:
            displaced.append(component)
            total_displaced += taken
            remaining -= taken

    if not displaced:
        return ("none", 0, "Recovery fits within bell-ringer time; no primary component displaced.")

    label = " + ".join(c.replace("_", " ") for c in displaced)
    if "independent_practice" in displaced and "guided_practice" not in displaced:
        note = "Students may not complete independent practice for the scheduled objective on this day."
    elif "guided_practice" in displaced:
        note = "Independent practice and guided practice displaced. Consider scheduling on a review day."
    else:
        note = "Significant instructional time displaced. Schedule on a review or flex day if possible."

    return (label, total_displaced, note)


def _remaining_instructional_days(package: TeacherAssistV2InstructionalPackage) -> int:
    today = date.today()
    if today >= package.plan_end_date:
        return 0
    elapsed_weeks = max(0, (today - package.plan_start_date).days // 7)
    total_weeks = package.week_end - package.week_start + 1
    remaining_weeks = max(0, total_weeks - elapsed_weeks)
    # 5 instructional days per week
    return remaining_weeks * 5


# ── Public API ─────────────────────────────────────────────────────────────────


def compute_recovery_budget(
    db: Session,
    *,
    user: User,
    package_id: uuid.UUID,
) -> dict[str, Any]:
    package = db.scalars(
        select(TeacherAssistV2InstructionalPackage).where(
            TeacherAssistV2InstructionalPackage.id == package_id,
            TeacherAssistV2InstructionalPackage.teacher_user_id == user.id,
        )
    ).first()
    if package is None:
        raise LookupError("Instructional package not found")

    if not package.plan_start_date or not package.plan_end_date:
        return {
            "package_id": str(package_id),
            "budget_available": False,
            "reason": "Package start or end date is not set.",
            "generated_at": datetime.now(UTC).isoformat(),
        }

    # Pull active queue items for this package AND for assignments within it
    items = db.scalars(
        select(TeacherAssistV2RecoveryQueue).where(
            TeacherAssistV2RecoveryQueue.teacher_user_id == user.id,
            TeacherAssistV2RecoveryQueue.status.in_(list(ACTIVE_STATUSES)),
            TeacherAssistV2RecoveryQueue.instructional_package_id == package_id,
        )
    ).all()

    remaining_days = _remaining_instructional_days(package)
    remaining_minutes = remaining_days * MINUTES_PER_CLASS_PERIOD

    pending_count = sum(1 for i in items if i.status == "pending")
    scheduled_count = sum(1 for i in items if i.status == "scheduled")
    deferred_count = sum(1 for i in items if i.status == "deferred")

    total_recovery_minutes = sum(_item_estimated_minutes(i) for i in items)
    pacing_impact_percent = (
        round((total_recovery_minutes / remaining_minutes) * 100, 1)
        if remaining_minutes > 0
        else 100.0
    )

    # Trade-off analysis per item
    trade_off_items: list[dict[str, Any]] = []
    for item in items:
        est_minutes = _item_estimated_minutes(item)
        displaced_label, displaced_minutes, impact_note = _displaced_components(est_minutes)
        at_risk = item.best_before is not None and item.best_before <= date.today() + __import__(
            "datetime"
        ).timedelta(days=5)
        trade_off_items.append(
            {
                "queue_item_id": str(item.id),
                "objective_code": item.objective_code,
                "recommendation_type": item.recommendation_type,
                "teacher_response": item.teacher_response,
                "status": item.status,
                "scheduled_for": item.scheduled_for.isoformat() if item.scheduled_for else None,
                "students_affected_count": len(item.students_affected_json or []),
                "estimated_minutes": est_minutes,
                "displaced_component": displaced_label,
                "displaced_minutes": displaced_minutes,
                "impact_note": impact_note,
                "at_risk": at_risk,
            }
        )

    total_displaced = sum(t["displaced_minutes"] for t in trade_off_items)
    trade_off_available = bool(package.instructional_design_plan_json)

    return {
        "package_id": str(package_id),
        "budget_available": True,
        "remaining_instructional_days": remaining_days,
        "remaining_instructional_minutes": remaining_minutes,
        "estimated_recovery_minutes": total_recovery_minutes,
        "pacing_impact_percent": pacing_impact_percent,
        "pacing_impact_label": _pacing_impact_label(pacing_impact_percent),
        "pending_items_count": pending_count,
        "scheduled_items_count": scheduled_count,
        "deferred_items_count": deferred_count,
        "recovery_breakdown": trade_off_items,
        "trade_off_analysis": {
            "available": trade_off_available,
            "total_displaced_minutes": total_displaced,
            "displacement_note": (
                "Estimates assume recovery occurs on the next available lesson day. "
                "Scheduling on a review or flex day reduces displacement of primary instruction."
            )
            if trade_off_available
            else None,
            "reason": None
            if trade_off_available
            else "Lesson plan does not contain a structured daily progression.",
        },
        "budget_note": (
            "Estimates only. Recovery decisions do not modify the district pacing guide or your lesson plan."
        ),
        "generated_at": datetime.now(UTC).isoformat(),
    }

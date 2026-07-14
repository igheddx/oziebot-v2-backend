"""Teacher Workspace — Today aggregation service.

Zero AI calls. Zero generation. Pure read from existing tables.
Single entry point: build_today_classroom(). All queries are sequential
(SQLAlchemy Session is not thread-safe); total DB target < 150ms.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationSubject
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.models.teacher_assist_v2_recovery_queue import TeacherAssistV2RecoveryQueue
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.instructional_delivery_profile import (
    get_strand_slots_for_day,
    profile_is_constrained,
)
from oziebot_api.services.teacher_assist_v2.instructional_design_plan import get_plan_for_week_subject
from oziebot_api.services.teacher_assist_v2.planning_constants import WEEKDAY_LABELS

# Artifact types surfaced in the Today workspace
_TODAY_ARTIFACT_TYPES = frozenset({
    "daily_lesson_plan",
    "subject_slide_deck",
    "student_lesson_deck",
    "bell_ringer",
    "exit_ticket",
})

_RECOVERY_DUE_WINDOW_DAYS = 5  # show recovery items best_before within N days


# ── Package / week helpers ─────────────────────────────────────────────────────

def _today_week_number(pkg: TeacherAssistV2InstructionalPackage, today: date) -> int | None:
    """Return the active week number if today falls within this package's range."""
    offset = (today - pkg.plan_start_date).days // 7
    wn = pkg.week_start + offset
    return wn if pkg.week_start <= wn <= pkg.week_end else None


def _subject_pairs(db: Session, pkg: TeacherAssistV2InstructionalPackage) -> list[tuple[str, uuid.UUID]]:
    """Return (display_name, id) for each subject in teaching order."""
    ids = [uuid.UUID(str(v)) for v in pkg.subject_ids_json]
    if not ids:
        return []
    rows = db.scalars(select(EducationSubject).where(EducationSubject.id.in_(ids))).all()
    by_id = {r.id: (r.display_name, r.id) for r in rows}
    return [by_id[sid] for sid in ids if sid in by_id]


# ── Plan JSON extraction helpers ───────────────────────────────────────────────

def _day_entry(plan: dict, week_number: int, subject_name: str, day_of_week: str) -> dict | None:
    ws = get_plan_for_week_subject(plan, week_number, subject_name)
    design = (ws or {}).get("instructional_design") or {}
    for day in design.get("daily_progression") or []:
        if (day.get("day") or "").lower() == day_of_week.lower():
            return day
    return None


def _tomorrow_entry(
    plan: dict, week_number: int, subject_name: str, today_dow: str
) -> dict | None:
    if today_dow not in WEEKDAY_LABELS:
        return None
    idx = WEEKDAY_LABELS.index(today_dow)
    if idx < len(WEEKDAY_LABELS) - 1:
        return _day_entry(plan, week_number, subject_name, WEEKDAY_LABELS[idx + 1])
    # Friday → Monday next week
    return _day_entry(plan, week_number + 1, subject_name, "Monday")


def _teaching_focus(day_entry: dict | None, week_subj: dict | None) -> dict | None:
    """Assemble 1-2 coaching sentences from plan data. No AI."""
    if not day_entry and not week_subj:
        return None

    coaching: list[str] = []

    reteach = (day_entry or {}).get("reteach_if_needed")
    if reteach:
        coaching.append(f"Watch for: {reteach}")

    design = (week_subj or {}).get("instructional_design") or {}
    kdg = design.get("knowledge_dependency_graph") or {}
    activation: str | None = None
    misconception: str | None = None

    nodes = kdg.values() if isinstance(kdg, dict) else (kdg if isinstance(kdg, list) else [])
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("activation_strategy") and not activation:
            activation = str(node["activation_strategy"])
        if node.get("gap_consequence") and not misconception:
            misconception = str(node["gap_consequence"])

    if activation and len(coaching) < 2:
        coaching.append(f"Before you begin: {activation}")

    return {
        "coaching": coaching[:2],
        "watch_for": reteach,
        "activation_strategy": activation,
        "anticipated_misconception": misconception,
        "success_evidence": (day_entry or {}).get("observable_mastery_evidence"),
    }


def _timeline(subject_name: str) -> list[dict]:
    """Build a standard instructional timeline. Durations are planning defaults."""
    slots = [
        ("Bell Ringer", 8, "bell_ringer"),
        ("Lesson", 20, None),
        ("Guided Practice", 12, None),
        ("Exit Ticket", 5, "exit_ticket"),
    ]
    result = []
    minute = 0
    for label, duration, artifact_type in slots:
        entry: dict[str, Any] = {"label": label, "start_minute": minute, "duration_minutes": duration}
        if artifact_type:
            entry["artifact_type"] = artifact_type
        result.append(entry)
        minute += duration
    return result


# ── CIP summary ───────────────────────────────────────────────────────────────

def _build_cip_summary(
    pkg: TeacherAssistV2InstructionalPackage,
    day_of_week: str,
) -> dict[str, Any] | None:
    """Return the Classroom Instruction Profile summary for the given day.

    Returns None when no constrained delivery profile is set.
    The returned dict is included in SubjectToday for the today screen display.
    """
    profile = pkg.instructional_delivery_profile
    if not profile_is_constrained(profile):
        return None
    slots = get_strand_slots_for_day(profile, day_of_week)
    if not slots:
        return None
    return {
        "mode": profile.get("mode"),
        "strand_slots_today": slots,
    }


# ── Subject context ────────────────────────────────────────────────────────────

def _build_subject_context(
    *,
    db: Session,
    pkg: TeacherAssistV2InstructionalPackage,
    week_number: int,
    subject_name: str,
    subject_id: uuid.UUID,
    day_of_week: str,
    recovery_today: list[dict],
) -> dict:
    plan = pkg.instructional_design_plan_json or {}
    week_subj = get_plan_for_week_subject(plan, week_number, subject_name)
    today_entry = _day_entry(plan, week_number, subject_name, day_of_week)
    tmrw_entry = _tomorrow_entry(plan, week_number, subject_name, day_of_week)

    # Artifacts for today: subject-scoped + day_label match
    art_rows = db.scalars(
        select(TeacherAssistV2InstructionalPackageArtifact).where(
            TeacherAssistV2InstructionalPackageArtifact.package_id == pkg.id,
            TeacherAssistV2InstructionalPackageArtifact.subject_id == subject_id,
            TeacherAssistV2InstructionalPackageArtifact.day_label == day_of_week,
            TeacherAssistV2InstructionalPackageArtifact.artifact_type.in_(_TODAY_ARTIFACT_TYPES),
            TeacherAssistV2InstructionalPackageArtifact.status == "ready",
        )
    ).all()

    # daily_lesson_plan has no subject_id (covers all subjects for the day)
    lp_rows = db.scalars(
        select(TeacherAssistV2InstructionalPackageArtifact).where(
            TeacherAssistV2InstructionalPackageArtifact.package_id == pkg.id,
            TeacherAssistV2InstructionalPackageArtifact.artifact_type == "daily_lesson_plan",
            TeacherAssistV2InstructionalPackageArtifact.day_label == day_of_week,
            TeacherAssistV2InstructionalPackageArtifact.status == "ready",
        )
    ).all()

    artifacts: dict[str, dict] = {}
    for row in [*art_rows, *lp_rows]:
        if row.artifact_type not in artifacts:
            artifacts[row.artifact_type] = {
                "artifact_id": str(row.id),
                "title": row.title,
                "status": row.status,
                "artifact_type": row.artifact_type,
            }

    # Instructional contract — first available for this week
    design = (week_subj or {}).get("instructional_design") or {}
    contracts = design.get("instructional_contracts") or {}
    exit_ticket_stem: str | None = None
    if isinstance(contracts, dict):
        for v in contracts.values():
            if isinstance(v, dict) and v.get("exit_ticket_stem"):
                exit_ticket_stem = str(v["exit_ticket_stem"])
                break

    # Recovery items for this package
    pkg_recovery = [r for r in recovery_today if r.get("package_id") == str(pkg.id)]

    # Tomorrow context
    tomorrow_context: dict | None = None
    if tmrw_entry:
        idx = WEEKDAY_LABELS.index(day_of_week) if day_of_week in WEEKDAY_LABELS else -1
        tmrw_label = WEEKDAY_LABELS[(idx + 1) % len(WEEKDAY_LABELS)] if idx >= 0 else None
        tomorrow_context = {
            "day_label": tmrw_label,
            "student_goal": tmrw_entry.get("student_goal"),
            "teacher_goal": tmrw_entry.get("teacher_goal"),
            "builds_on_today": (today_entry or {}).get("prepares_for_tomorrow"),
        }

    return {
        "package_id": str(pkg.id),
        "package_title": pkg.title,
        "subject_name": subject_name,
        "subject_id": str(subject_id),
        "week_number": week_number,
        "week_label": f"Week {week_number}",
        "day_label": day_of_week,

        # Plan context
        "student_goal": (today_entry or {}).get("student_goal"),
        "teacher_goal": (today_entry or {}).get("teacher_goal"),
        "builds_from_yesterday": (today_entry or {}).get("builds_from_yesterday"),
        "prepares_for_tomorrow": (today_entry or {}).get("prepares_for_tomorrow"),
        "reteach_if_needed": (today_entry or {}).get("reteach_if_needed"),
        "observable_mastery_evidence": (today_entry or {}).get("observable_mastery_evidence"),
        "exit_ticket_stem": exit_ticket_stem,

        # Teaching focus coaching (1-2 sentences for Before Class prep)
        "teaching_focus": _teaching_focus(today_entry, week_subj),

        # Instructional timeline
        "timeline": _timeline(subject_name),

        # Artifact presence flags + IDs
        "artifacts": artifacts,
        "has_lesson_plan": "daily_lesson_plan" in artifacts,
        "has_slide_deck": ("subject_slide_deck" in artifacts or "student_lesson_deck" in artifacts),
        "has_bell_ringer": "bell_ringer" in artifacts,
        "has_exit_ticket": "exit_ticket" in artifacts,

        # Recovery items for this package
        "recovery_items": pkg_recovery,

        # Tomorrow preview
        "tomorrow": tomorrow_context,

        # Classroom Instruction Profile — strand-level delivery config for today
        "classroom_instruction_profile": _build_cip_summary(pkg, day_of_week),
    }


# ── Aggregation helpers ────────────────────────────────────────────────────────

def _recovery_today_items(db: Session, *, user: User, today: date) -> list[dict]:
    window_end = today + timedelta(days=_RECOVERY_DUE_WINDOW_DAYS)
    rows = db.scalars(
        select(TeacherAssistV2RecoveryQueue).where(
            TeacherAssistV2RecoveryQueue.teacher_user_id == user.id,
            TeacherAssistV2RecoveryQueue.status.in_(["pending", "scheduled"]),
            (
                (TeacherAssistV2RecoveryQueue.scheduled_for == today)
                | (
                    (TeacherAssistV2RecoveryQueue.best_before != None)  # noqa: E711
                    & (TeacherAssistV2RecoveryQueue.best_before <= window_end)
                )
            ),
        )
    ).all()

    return [
        {
            "queue_item_id": str(r.id),
            "package_id": str(r.instructional_package_id) if r.instructional_package_id else None,
            "assignment_id": str(r.assignment_id) if r.assignment_id else None,
            "objective_code": r.objective_code,
            "priority": r.priority,
            "recommendation_type": r.recommendation_type,
            "teacher_response": r.teacher_response,
            "status": r.status,
            "scheduled_for": r.scheduled_for.isoformat() if r.scheduled_for else None,
            "best_before": r.best_before.isoformat() if r.best_before else None,
            "misconception_text": r.misconception_text,
            "timeline_phase": r.timeline_phase,
            "students_affected_count": len(r.students_affected_json or []),
            "is_today": r.scheduled_for == today,
        }
        for r in rows
    ]


def _verification_due_items(db: Session, *, user: User, today: date) -> list[dict]:
    rows = db.scalars(
        select(TeacherAssistV2RecoveryQueue).where(
            TeacherAssistV2RecoveryQueue.teacher_user_id == user.id,
            TeacherAssistV2RecoveryQueue.timeline_phase == "recovery_verification",
        )
    ).all()

    result = []
    for r in rows:
        if not r.completed_at:
            continue
        criteria = r.success_criteria_json or {}
        window = int(criteria.get("evaluation_window_days") or 5)
        days_since = (today - r.completed_at.date()).days
        if days_since >= window:
            result.append({
                "queue_item_id": str(r.id),
                "objective_code": r.objective_code,
                "misconception_text": r.misconception_text,
                "priority": r.priority,
                "completed_at": r.completed_at.isoformat(),
                "days_since_completion": days_since,
                "evaluation_window_days": window,
            })
    return result


def _grading_queue_items(db: Session, *, user: User) -> list[dict]:
    """Assignments with unreviewed DRAFT grades awaiting teacher confirmation."""
    agg = db.execute(
        select(
            TeacherAssistV2AssignmentGrade.assignment_id,
            func.count().label("draft_count"),
        )
        .where(
            TeacherAssistV2AssignmentGrade.teacher_user_id == user.id,
            TeacherAssistV2AssignmentGrade.status == "DRAFT",
        )
        .group_by(TeacherAssistV2AssignmentGrade.assignment_id)
    ).all()

    result = []
    for row in agg:
        assignment = db.get(TeacherAssistV2Assignment, row.assignment_id)
        if assignment is None:
            continue
        result.append({
            "assignment_id": str(assignment.id),
            "title": assignment.title,
            "package_id": str(assignment.instructional_package_id),
            "pending_grade_count": row.draft_count,
            "week_number": assignment.week_number,
        })
    return result


def _morning_brief(
    *,
    subjects_today: list[dict],
    grading_queue: list[dict],
    recovery_today: list[dict],
    today: date,
) -> dict:
    pending_grades = sum(s["pending_grade_count"] for s in grading_queue)
    high_priority = [r for r in recovery_today if r["priority"] in ("HIGH", "CRITICAL")]
    focus_items = [
        {"subject": s["subject_name"], "focus": s["student_goal"] or s.get("teacher_goal")}
        for s in subjects_today
        if s.get("student_goal") or s.get("teacher_goal")
    ]

    if not subjects_today:
        readiness = "No lessons scheduled for today."
    elif not any(s.get("student_goal") for s in subjects_today):
        readiness = "Package context loading — check back shortly."
    elif high_priority:
        n = len(high_priority)
        readiness = (
            f"You're ready to teach. "
            f"{n} high-priority recovery item{'s' if n != 1 else ''} scheduled today."
        )
    elif pending_grades > 0:
        readiness = (
            f"You're ready to teach. "
            f"{pending_grades} submission{'s' if pending_grades != 1 else ''} awaiting grading."
        )
    else:
        readiness = "You're ready to teach."

    return {
        "date_label": today.strftime("%A, %B %-d"),
        "subject_names": [s["subject_name"] for s in subjects_today],
        "focus_items": focus_items,
        "pending_grade_count": pending_grades,
        "recovery_today_count": len(recovery_today),
        "high_priority_recovery_count": len(high_priority),
        "readiness_statement": readiness,
    }


def _before_class(
    *,
    subjects_today: list[dict],
    grading_queue: list[dict],
    recovery_today: list[dict],
) -> list[dict]:
    items: list[dict] = []

    for s in subjects_today:
        subject = s["subject_name"]
        pkg_id = s["package_id"]

        # Lesson plan
        lp = s["artifacts"].get("daily_lesson_plan")
        items.append({
            "label": f"Lesson Plan — {subject}",
            "status": "ready" if lp else "pending",
            "action": "view" if lp else None,
            "artifact_id": (lp or {}).get("artifact_id"),
            "package_id": pkg_id,
            "subject_name": subject,
            "icon": "check" if lp else "pending",
        })

        # Presentation
        deck = s["artifacts"].get("student_lesson_deck") or s["artifacts"].get("subject_slide_deck")
        if s["has_slide_deck"]:
            items.append({
                "label": f"Student Presentation — {subject}",
                "status": "ready" if deck else "pending",
                "action": "present" if deck else None,
                "artifact_id": (deck or {}).get("artifact_id"),
                "package_id": pkg_id,
                "subject_name": subject,
                "icon": "check" if deck else "pending",
            })

        # Exit ticket
        et = s["artifacts"].get("exit_ticket")
        if et:
            items.append({
                "label": f"Exit Ticket — {subject}",
                "status": "ready",
                "action": "print",
                "artifact_id": et["artifact_id"],
                "package_id": pkg_id,
                "subject_name": subject,
                "icon": "check",
            })

        # Teaching focus coaching (one item per subject)
        tf = s.get("teaching_focus") or {}
        watch_for = tf.get("watch_for")
        if watch_for:
            items.append({
                "label": f"Teaching Focus — {subject}",
                "status": "coaching",
                "action": None,
                "subject_name": subject,
                "note": watch_for,
                "icon": "coaching",
            })

    # Recovery items scheduled for today
    for r in recovery_today:
        if r.get("is_today"):
            label = r.get("objective_code") or r.get("recommendation_type", "Recovery")
            items.append({
                "label": f"Recovery — {label}",
                "status": "recovery",
                "action": "view_recovery",
                "queue_item_id": r["queue_item_id"],
                "priority": r["priority"],
                "icon": "recovery",
            })

    # Grading backlog
    if grading_queue:
        total = sum(s["pending_grade_count"] for s in grading_queue)
        items.append({
            "label": f"{total} submission{'s' if total != 1 else ''} awaiting grading",
            "status": "alert",
            "action": "grade",
            "icon": "alert",
        })

    return items


def _end_of_day(
    *,
    db: Session,
    user: User,
    today: date,
    subjects_today: list[dict],
    grading_queue: list[dict],
    verification_due: list[dict],
) -> dict:
    today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=UTC)
    today_end = datetime.combine(today, datetime.max.time()).replace(tzinfo=UTC)

    completed_today_count = db.scalar(
        select(func.count(TeacherAssistV2RecoveryQueue.id)).where(
            TeacherAssistV2RecoveryQueue.teacher_user_id == user.id,
            TeacherAssistV2RecoveryQueue.status == "completed",
            TeacherAssistV2RecoveryQueue.completed_at >= today_start,
            TeacherAssistV2RecoveryQueue.completed_at <= today_end,
        )
    ) or 0

    tomorrow_focuses = [
        {
            "subject": s["subject_name"],
            "focus": (s.get("tomorrow") or {}).get("student_goal") or (s.get("tomorrow") or {}).get("teacher_goal"),
            "builds_on_today": (s.get("tomorrow") or {}).get("builds_on_today"),
        }
        for s in subjects_today
        if s.get("tomorrow") and (
            (s["tomorrow"].get("student_goal") or s["tomorrow"].get("teacher_goal"))
        )
    ]

    return {
        "recovery_completed_today": completed_today_count,
        "remaining_grading": sum(s["pending_grade_count"] for s in grading_queue),
        "verification_pending": len(verification_due),
        "tomorrow_focuses": tomorrow_focuses,
        "reflection_prompt": (
            "Take a moment: Which students showed mastery today? Who needs additional support tomorrow?"
            if subjects_today else None
        ),
    }


def _alerts(*, recovery_today: list[dict], verification_due: list[dict]) -> list[dict]:
    result: list[dict] = []
    for r in recovery_today:
        if len(result) >= 3:
            break
        if r["priority"] in ("HIGH", "CRITICAL"):
            result.append({
                "type": "recovery_due",
                "priority": r["priority"],
                "message": f"{r['priority']} recovery — {r.get('objective_code') or r['recommendation_type']}",
                "queue_item_id": r["queue_item_id"],
            })
    for v in verification_due:
        if len(result) >= 3:
            break
        result.append({
            "type": "verification_due",
            "priority": "MEDIUM",
            "message": f"Recovery verification due — {v.get('objective_code') or 'check student progress'}",
            "queue_item_id": v["queue_item_id"],
        })
    return result


# ── Public API ─────────────────────────────────────────────────────────────────

def build_today_classroom(
    db: Session,
    *,
    user: User,
    today: date | None = None,
) -> dict[str, Any]:
    """Aggregate today's classroom view. Zero AI calls. Zero generation."""
    today = today or date.today()
    day_of_week = today.strftime("%A")

    # Active packages for today
    all_pkgs = db.scalars(
        select(TeacherAssistV2InstructionalPackage).where(
            TeacherAssistV2InstructionalPackage.teacher_user_id == user.id,
            TeacherAssistV2InstructionalPackage.status.in_(["generated", "open"]),
        )
    ).all()

    active: list[tuple[TeacherAssistV2InstructionalPackage, int]] = [
        (pkg, wn)
        for pkg in all_pkgs
        if (wn := _today_week_number(pkg, today)) is not None
    ]

    recovery_today = _recovery_today_items(db, user=user, today=today)
    verification_due = _verification_due_items(db, user=user, today=today)
    grading_queue = _grading_queue_items(db, user=user)

    subjects_today: list[dict] = []
    for pkg, week_number in active:
        for subject_name, subject_id in _subject_pairs(db, pkg):
            subjects_today.append(
                _build_subject_context(
                    db=db,
                    pkg=pkg,
                    week_number=week_number,
                    subject_name=subject_name,
                    subject_id=subject_id,
                    day_of_week=day_of_week,
                    recovery_today=recovery_today,
                )
            )

    return {
        "date": today.isoformat(),
        "day_of_week": day_of_week,
        "morning_brief": _morning_brief(
            subjects_today=subjects_today,
            grading_queue=grading_queue,
            recovery_today=recovery_today,
            today=today,
        ),
        "subjects_today": subjects_today,
        "before_class": _before_class(
            subjects_today=subjects_today,
            grading_queue=grading_queue,
            recovery_today=recovery_today,
        ),
        "grading_queue": grading_queue,
        "recovery_today": recovery_today,
        "verification_due": verification_due,
        "end_of_day": _end_of_day(
            db=db,
            user=user,
            today=today,
            subjects_today=subjects_today,
            grading_queue=grading_queue,
            verification_due=verification_due,
        ),
        "alerts": _alerts(recovery_today=recovery_today, verification_due=verification_due),
    }

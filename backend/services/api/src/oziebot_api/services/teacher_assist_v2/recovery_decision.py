"""Learning Recovery Decision Engine — deterministic strategy recommendation from mastery evidence.

Priority order (matches the Learning Recovery Planner philosophy):
1. Read existing plan data (reteach_if_needed, KDG, instructional contract)
2. Apply deterministic decision logic
3. AI is invoked only during artifact generation, never here
"""

from __future__ import annotations

import uuid
from collections import Counter
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
)
from oziebot_api.models.teacher_assist_v2_mastery_evidence import TeacherAssistV2MasteryEvidence
from oziebot_api.models.user import User

# ── Recovery intent → preferred artifact types (internal only) ─────────────────

_INTENT_ARTIFACT_PREFERENCES: dict[str, tuple[str, ...]] = {
    "understanding": (
        "recovery_mini_lesson",
        "recovery_small_group_packet",
        "recovery_conference_guide",
    ),
    "skill": ("recovery_guided_practice", "recovery_assignment", "recovery_small_group_packet"),
    "vocabulary": ("recovery_bell_ringer", "recovery_homework", "recovery_assignment"),
    "fluency": ("recovery_bell_ringer", "recovery_assignment", "recovery_exit_ticket"),
    "confidence": ("recovery_conference_guide", "recovery_homework", "recovery_mini_lesson"),
}

# ── Strategy metadata ──────────────────────────────────────────────────────────

_STRATEGY_TIME_MINUTES: dict[str, int] = {
    "continue": 0,
    "spiral_review": 0,
    "bell_ringer": 8,
    "embedded_recovery": 12,
    "guided_practice_replacement": 15,
    "small_group": 20,
    "individual_conference": 10,
    "homework_reinforcement": 0,
    "whole_class_recovery": 45,
}

_STRATEGY_PACING_IMPACT: dict[str, str] = {
    "continue": "none",
    "spiral_review": "none",
    "bell_ringer": "low",
    "embedded_recovery": "low",
    "guided_practice_replacement": "low",
    "small_group": "moderate",
    "individual_conference": "low",
    "homework_reinforcement": "none",
    "whole_class_recovery": "high",
}

_STRATEGY_PRIORITY: dict[str, str] = {
    "continue": "LOW",
    "spiral_review": "LOW",
    "bell_ringer": "MEDIUM",
    "embedded_recovery": "MEDIUM",
    "guided_practice_replacement": "MEDIUM",
    "small_group": "HIGH",
    "individual_conference": "MEDIUM",
    "homework_reinforcement": "LOW",
    "whole_class_recovery": "HIGH",
}


# ── Recovery intent inference (internal — never shown to teachers) ─────────────


def _infer_recovery_intent(
    *,
    suspected_misconception: str | None,
    mastery_percentage: float,
    average_percentage: float,
    below_mastery_percent: float,
    students_below_count: int,
    total_students: int,
) -> str:
    """Infer recovery intent from evidence signals.

    Returns one of: understanding, skill, vocabulary, fluency, confidence.
    This guides artifact type selection — it is not shown to teachers.
    """
    # Vocabulary: misconception references terminology or word meaning
    if suspected_misconception:
        lower = suspected_misconception.lower()
        vocab_signals = {"vocabular", "term", "definit", "word meaning", "concept", "meaning of"}
        if any(s in lower for s in vocab_signals):
            return "vocabulary"

    # Confidence: very few students below mastery (isolated, not widespread)
    if below_mastery_percent < 0.20 and students_below_count <= max(2, int(total_students * 0.15)):
        return "confidence"

    # Fluency: mastery percentage is moderate but average lags — inconsistent application
    if mastery_percentage >= 40 and average_percentage >= 60:
        return "fluency"

    # Skill: misconception references applying or procedural steps
    if suspected_misconception:
        lower = suspected_misconception.lower()
        skill_signals = {
            "apply",
            "using",
            "step",
            "procedur",
            "how to",
            "process",
            "method",
            "technique",
        }
        if any(s in lower for s in skill_signals):
            return "skill"

    return "understanding"


# ── Strategy selection (10-level hierarchy) ────────────────────────────────────


def _select_strategy(
    *,
    mastery_percentage: float,
    below_mastery_percent: float,
    has_downstream_risk: bool,
    plan_reteach_hint: str | None,
    shared_misconception: str | None,
    next_lesson_reinforces: bool,
    students_below_count: int,
    remaining_weeks: int,
) -> tuple[str, int, str]:
    """Return (strategy, level, why) — least disruptive effective intervention."""

    if mastery_percentage >= 80.0:
        return (
            "continue",
            1,
            f"{mastery_percentage:.0f}% of students achieved mastery. No recovery needed — "
            "monitor with next assessment.",
        )

    if (
        70 <= mastery_percentage < 80
        and next_lesson_reinforces
        and not has_downstream_risk
        and remaining_weeks > 1
    ):
        return (
            "spiral_review",
            2,
            f"{mastery_percentage:.0f}% mastery — developing. Next lesson revisits this concept. "
            "A spaced spiral review across 2–3 days closes the gap without displacing instruction.",
        )

    if 60 <= mastery_percentage < 80 and plan_reteach_hint and not has_downstream_risk:
        return (
            "bell_ringer",
            3,
            f"{mastery_percentage:.0f}% mastery — developing. The instructional plan already includes "
            "a reteach strategy. A targeted bell-ringer applies it with minimal class time impact.",
        )

    if 55 <= mastery_percentage < 75 and shared_misconception and next_lesson_reinforces:
        return (
            "embedded_recovery",
            4,
            f"{mastery_percentage:.0f}% mastery with a shared learning gap: '{shared_misconception}'. "
            "An embedded recovery block before guided practice addresses the gap without a separate lesson.",
        )

    if 45 <= mastery_percentage < 65 and not shared_misconception:
        return (
            "guided_practice_replacement",
            5,
            f"{mastery_percentage:.0f}% mastery — significant gap, no single shared misconception. "
            "Replacing guided practice with a targeted recovery activity addresses varied needs.",
        )

    if below_mastery_percent >= 0.60:
        pacing_alert = (
            " Note: This gap is significant. If whole-class recovery does not close it, "
            "district curriculum support may be needed."
            if below_mastery_percent >= 0.80
            else ""
        )
        return (
            "whole_class_recovery",
            9,
            f"{below_mastery_percent * 100:.0f}% of students are below mastery. "
            f"A dedicated recovery lesson is needed before the next objective builds on this one.{pacing_alert}",
        )

    if students_below_count > 0 and below_mastery_percent < 0.20:
        return (
            "individual_conference",
            7,
            f"{students_below_count} student{'s' if students_below_count != 1 else ''} below mastery "
            f"({below_mastery_percent * 100:.0f}% of class). Individual conferences are the least "
            "disruptive option and preserve instruction for the rest of the class.",
        )

    return (
        "small_group",
        6,
        f"{below_mastery_percent * 100:.0f}% of students are below mastery. "
        "A small-group pull during independent practice targets the gap without disrupting the full class.",
    )


# ── Plan context extraction ────────────────────────────────────────────────────


def _extract_plan_context(
    package: TeacherAssistV2InstructionalPackage,
    *,
    week_number: int,
) -> dict[str, Any]:
    """Extract reteach_if_needed, KDG, instructional_contract, and daily_progression from the plan."""
    plan = package.instructional_design_plan_json or {}
    weeks = plan.get("weeks") or []

    # Find the target week by week_number or sequence_number
    target_week: dict[str, Any] = {}
    for w in weeks:
        if w.get("week_number") == week_number or w.get("sequence_number") == week_number:
            target_week = w
            break
    if not target_week and weeks:
        target_week = weeks[0]

    # Find first subject that has instructional_design data
    subjects_in_week = target_week.get("subjects") or []
    target_subject: dict[str, Any] = {}
    for s in subjects_in_week:
        if s.get("instructional_design"):
            target_subject = s
            break

    instructional_design = target_subject.get("instructional_design") or {}
    daily_progression = instructional_design.get("daily_progression") or []
    instructional_contracts = instructional_design.get("instructional_contracts") or {}

    # PRIMARY: reteach_if_needed — read from all days, take most specific non-empty
    reteach_hints = [
        day.get("reteach_if_needed", "")
        for day in daily_progression
        if day.get("reteach_if_needed")
    ]
    plan_reteach_hint = reteach_hints[0] if reteach_hints else None

    # Continuity fields
    builds_from_yesterday: str | None = None
    prepares_for_tomorrow: str | None = None
    for day in daily_progression:
        if day.get("builds_from_yesterday") and not builds_from_yesterday:
            builds_from_yesterday = day["builds_from_yesterday"]
        if day.get("prepares_for_tomorrow") and not prepares_for_tomorrow:
            prepares_for_tomorrow = day["prepares_for_tomorrow"]

    # Observable mastery evidence from first day that has it
    observable_mastery_evidence: str | None = None
    scaffold: str | None = None
    for day in daily_progression:
        if day.get("observable_mastery_evidence") and not observable_mastery_evidence:
            observable_mastery_evidence = day["observable_mastery_evidence"]
        diff = day.get("differentiation") or {}
        if diff.get("scaffold") and not scaffold:
            scaffold = diff["scaffold"]

    kdg = plan.get("knowledge_dependency_graph") or []

    return {
        "plan_reteach_hint": plan_reteach_hint,
        "builds_from_yesterday": builds_from_yesterday,
        "prepares_for_tomorrow": prepares_for_tomorrow,
        "instructional_contract": instructional_contracts,
        "knowledge_dependency_graph": kdg,
        "observable_mastery_evidence": observable_mastery_evidence,
        "scaffold": scaffold,
        "week_data": target_week,
        "subject_data": target_subject,
    }


def _extract_kdg_context(kdg: list[dict], objective_code: str | None) -> dict[str, Any]:
    if not objective_code or not kdg:
        return {"has_downstream_risk": False, "gap_consequence": None, "activation_strategy": None}

    for entry in kdg:
        if entry.get("objective_code") == objective_code:
            deps = entry.get("dependencies") or []
            gap_consequences = [d["gap_consequence"] for d in deps if d.get("gap_consequence")]
            activation_strategies = [
                d["activation_strategy"] for d in deps if d.get("activation_strategy")
            ]
            may_need_activation = [d for d in deps if d.get("status") == "may_need_activation"]
            return {
                "has_downstream_risk": bool(may_need_activation),
                "gap_consequence": gap_consequences[0] if gap_consequences else None,
                "activation_strategy": activation_strategies[0] if activation_strategies else None,
            }

    return {"has_downstream_risk": False, "gap_consequence": None, "activation_strategy": None}


# ── 4-Stage plan and success criteria ─────────────────────────────────────────


def _build_stage_plan(
    *,
    strategy: str,
    mastery_percentage: float,
    misconception: str | None,
    observable_mastery_evidence: str | None,
    instructional_contract: dict[str, Any],
    recovery_intent: str,
    evaluation_window_days: int,
) -> dict[str, Any]:
    exit_ticket_stem = instructional_contract.get("exit_ticket_stem", "")
    success_target = (
        observable_mastery_evidence or exit_ticket_stem or "demonstrates mastery of the objective"
    )
    goal_description = (
        f"Address: {misconception}" if misconception else f"Close the gap in {recovery_intent}"
    )
    activity_label = strategy.replace("_", " ").title()

    return {
        "stage_1_recovery_goal": {
            "label": "Recovery Goal",
            "description": goal_description,
            "success_target": success_target,
            "target_mastery_percent": 80.0,
            "assessment_type": "exit_ticket" if exit_ticket_stem else "observation",
            "evaluation_window_days": evaluation_window_days,
        },
        "stage_2_recovery_activity": {
            "label": "Recovery Activity",
            "description": f"Deliver a {activity_label} focused on {goal_description.lower()}",
            "recommended_strategy": strategy,
            "estimated_minutes": _STRATEGY_TIME_MINUTES.get(strategy, 0),
            "pacing_impact": _STRATEGY_PACING_IMPACT.get(strategy, "low"),
        },
        "stage_3_recovery_verification": {
            "label": "Recovery Verification",
            "description": (
                f"Within {evaluation_window_days} days, confirm students can: {success_target}"
            ),
            "verification_method": "exit_ticket" if exit_ticket_stem else "teacher_observation",
            "expected_mastery_gain_percent": max(10, 80 - mastery_percentage),
        },
        "stage_4_recovery_outcome": {
            "label": "Recovery Outcome",
            "description": (
                "Record whether mastery improved. Compare before/after snapshots to "
                "measure recovery effectiveness."
            ),
            "tracking": "post_recovery_mastery_snapshot",
        },
    }


def _build_success_criteria_template(
    *,
    mastery_percentage: float,
    misconception: str | None,
    instructional_contract: dict[str, Any],
    observable_mastery_evidence: str | None,
    evaluation_window_days: int,
) -> dict[str, Any]:
    return {
        "target_mastery_percent": 80.0,
        "assessment_type": (
            "exit_ticket" if instructional_contract.get("exit_ticket_stem") else "observation"
        ),
        "evaluation_window_days": evaluation_window_days,
        "specific_gap_to_close": misconception or "Objective gap",
        "observable_evidence": (
            observable_mastery_evidence
            or instructional_contract.get("exit_ticket_stem")
            or "Student demonstrates mastery of the objective"
        ),
        "current_mastery_percent": mastery_percentage,
        "mastery_gain_needed": max(0.0, 80.0 - mastery_percentage),
    }


# ── Main public function ───────────────────────────────────────────────────────


def build_recovery_decision(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> dict[str, Any]:
    """Build a deterministic recovery strategy recommendation from confirmed mastery evidence.

    Returns a RecoveryDecision with 4-stage plan and success criteria template.
    Teacher always decides whether to act — this is advisory only.
    """
    # ── Load assignment ────────────────────────────────────────────────────────
    assignment = db.scalars(
        select(TeacherAssistV2Assignment).where(
            TeacherAssistV2Assignment.id == assignment_id,
            TeacherAssistV2Assignment.teacher_user_id == user.id,
        )
    ).first()
    if assignment is None:
        raise LookupError("Assignment not found.")

    package: TeacherAssistV2InstructionalPackage | None = None
    if assignment.instructional_package_id:
        package = db.get(TeacherAssistV2InstructionalPackage, assignment.instructional_package_id)

    # ── Gather confirmed mastery evidence ──────────────────────────────────────
    evidence_rows = db.scalars(
        select(TeacherAssistV2MasteryEvidence).where(
            TeacherAssistV2MasteryEvidence.assignment_id == assignment_id,
            TeacherAssistV2MasteryEvidence.teacher_user_id == user.id,
            TeacherAssistV2MasteryEvidence.is_current.is_(True),
            TeacherAssistV2MasteryEvidence.teacher_confirmed.is_(True),
        )
    ).all()

    if not evidence_rows:
        return {
            "available": False,
            "reason": "No confirmed mastery evidence found for this assignment.",
            "assignment_id": str(assignment_id),
        }

    total_students = len(evidence_rows)
    mastery_count = sum(1 for e in evidence_rows if e.mastery_level == "mastery")
    developing_count = sum(1 for e in evidence_rows if e.mastery_level == "developing")
    beginning_count = sum(1 for e in evidence_rows if e.mastery_level == "beginning")
    below_mastery_count = developing_count + beginning_count

    mastery_percentage = mastery_count / total_students * 100
    below_mastery_percent = below_mastery_count / total_students
    avg_percentage = sum(e.percentage for e in evidence_rows) / total_students

    # ── Get misconceptions from joined grades ──────────────────────────────────
    grade_ids = [e.assignment_grade_id for e in evidence_rows]
    grade_rows = db.scalars(
        select(TeacherAssistV2AssignmentGrade).where(
            TeacherAssistV2AssignmentGrade.id.in_(grade_ids)
        )
    ).all()
    misconceptions = [
        g.rubric_json.get("suspected_misconception")
        for g in grade_rows
        if g.rubric_json and g.rubric_json.get("suspected_misconception")
    ]
    shared_misconception: str | None = None
    if misconceptions:
        most_common = Counter(misconceptions).most_common(1)
        shared_misconception = most_common[0][0] if most_common else None

    # ── Extract plan context ────────────────────────────────────────────────────
    plan_context: dict[str, Any] = {}
    kdg_context: dict[str, Any] = {
        "has_downstream_risk": False,
        "gap_consequence": None,
        "activation_strategy": None,
    }
    objective_code: str | None = None

    if package is not None:
        # Resolve objective code from assignment
        obj_ids = assignment.education_objective_ids_json or []
        if obj_ids:
            first_obj = db.get(EducationObjective, uuid.UUID(str(obj_ids[0])))
            if first_obj:
                objective_code = first_obj.objective_id

        plan_context = _extract_plan_context(package, week_number=assignment.week_number)
        kdg_context = _extract_kdg_context(
            plan_context.get("knowledge_dependency_graph") or [],
            objective_code,
        )

    plan_reteach_hint = plan_context.get("plan_reteach_hint")
    next_lesson_reinforces = bool(plan_context.get("prepares_for_tomorrow"))
    has_downstream_risk = kdg_context.get("has_downstream_risk", False)
    instructional_contract = plan_context.get("instructional_contract") or {}
    observable_mastery_evidence = plan_context.get("observable_mastery_evidence")

    # ── Infer recovery intent (internal — guides artifact selection) ───────────
    recovery_intent = _infer_recovery_intent(
        suspected_misconception=shared_misconception,
        mastery_percentage=mastery_percentage,
        average_percentage=avg_percentage,
        below_mastery_percent=below_mastery_percent,
        students_below_count=below_mastery_count,
        total_students=total_students,
    )

    # ── Select strategy from hierarchy ─────────────────────────────────────────
    remaining_weeks = max(1, (package.week_end - assignment.week_number)) if package else 1

    strategy, level, why = _select_strategy(
        mastery_percentage=mastery_percentage,
        below_mastery_percent=below_mastery_percent,
        has_downstream_risk=has_downstream_risk,
        plan_reteach_hint=plan_reteach_hint,
        shared_misconception=shared_misconception,
        next_lesson_reinforces=next_lesson_reinforces,
        students_below_count=below_mastery_count,
        remaining_weeks=remaining_weeks,
    )

    # ── Select recommended artifact type from intent ────────────────────────────
    preferred_artifacts = _INTENT_ARTIFACT_PREFERENCES.get(
        recovery_intent, ("recovery_mini_lesson",)
    )
    recommended_artifact_type = preferred_artifacts[0]

    # Strategy-level override takes precedence over intent
    _strategy_artifact_override: dict[str, str] = {
        "whole_class_recovery": "recovery_presentation",
        "small_group": "recovery_small_group_packet",
        "bell_ringer": "recovery_bell_ringer",
        "spiral_review": "recovery_spiral_review",
        "individual_conference": "recovery_conference_guide",
        "guided_practice_replacement": "recovery_guided_practice",
        "continue": "",
    }
    override = _strategy_artifact_override.get(strategy, "")
    if override:
        recommended_artifact_type = override

    # ── Evaluation window ──────────────────────────────────────────────────────
    _eval_window: dict[str, int] = {
        "spiral_review": 5,
        "bell_ringer": 2,
        "embedded_recovery": 2,
        "whole_class_recovery": 7,
    }
    evaluation_window_days = _eval_window.get(strategy, 5)

    # ── Build 4-stage plan and success criteria ────────────────────────────────
    stage_plan = _build_stage_plan(
        strategy=strategy,
        mastery_percentage=mastery_percentage,
        misconception=shared_misconception,
        observable_mastery_evidence=observable_mastery_evidence,
        instructional_contract=instructional_contract,
        recovery_intent=recovery_intent,
        evaluation_window_days=evaluation_window_days,
    )

    success_criteria_template = _build_success_criteria_template(
        mastery_percentage=mastery_percentage,
        misconception=shared_misconception,
        instructional_contract=instructional_contract,
        observable_mastery_evidence=observable_mastery_evidence,
        evaluation_window_days=evaluation_window_days,
    )

    students_below = [
        e.student_number for e in evidence_rows if e.mastery_level in ("developing", "beginning")
    ]

    return {
        "available": True,
        "assignment_id": str(assignment_id),
        "instructional_package_id": (
            str(assignment.instructional_package_id)
            if assignment.instructional_package_id
            else None
        ),
        "objective_code": objective_code,
        "strategy": strategy,
        "level": level,
        "why": why,
        "recovery_intent": recovery_intent,
        "recommended_artifact_type": recommended_artifact_type,
        "suggested_priority": _STRATEGY_PRIORITY.get(strategy, "MEDIUM"),
        "estimated_minutes": _STRATEGY_TIME_MINUTES.get(strategy, 0),
        "pacing_impact": _STRATEGY_PACING_IMPACT.get(strategy, "low"),
        "plan_reteach_hint": plan_reteach_hint,
        "has_downstream_risk": has_downstream_risk,
        "shared_misconception": shared_misconception,
        "students_below": students_below,
        "students_below_count": below_mastery_count,
        "mastery_percentage": round(mastery_percentage, 1),
        "below_mastery_percent": round(below_mastery_percent * 100, 1),
        "average_percentage": round(avg_percentage, 1),
        "total_students_assessed": total_students,
        "success_criteria_template": success_criteria_template,
        "stage_plan": stage_plan,
    }

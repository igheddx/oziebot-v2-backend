"""Today's Teaching Brief — deterministic assembly from existing generation data.

Consumes: instructional_design_plan_json, instructional_validation_report_json,
          instructional_alignment_report_json, and package.artifacts.
Zero AI calls. Runs at the end of _populate_instructional_package().
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_v2_instructional_package import (
        TeacherAssistV2InstructionalPackage,
    )

# ── Timing template (50-min default period) ─────────────────────────────────
# Segments ordered as they appear in a lesson. "flexible" segments can be trimmed;
# "skippable" segments can be dropped entirely if the class runs far behind.
_TIMING_TEMPLATE: list[dict[str, Any]] = [
    {"name": "Hook", "minutes": 5, "flexible": True, "trim_to": 3, "skippable": False},
    {
        "name": "Learning Target",
        "minutes": 2,
        "flexible": False,
        "trim_to": None,
        "skippable": False,
    },
    {"name": "Teacher Modeling", "minutes": 12, "flexible": True, "trim_to": 8, "skippable": False},
    {"name": "Guided Practice", "minutes": 15, "flexible": True, "trim_to": 10, "skippable": False},
    {
        "name": "Independent Practice",
        "minutes": 10,
        "flexible": True,
        "trim_to": 7,
        "skippable": True,
    },
    {"name": "Discussion", "minutes": 5, "flexible": True, "trim_to": 3, "skippable": True},
    {"name": "Exit Ticket", "minutes": 3, "flexible": False, "trim_to": None, "skippable": False},
]


# ── Helpers ──────────────────────────────────────────────────────────────────


def _weekday(day_label: str | None) -> str:
    """'W1-Monday' → 'Monday'; 'Monday' → 'Monday'."""
    if not day_label:
        return ""
    return day_label.split("-", 1)[1] if "-" in day_label else day_label


def _week_num(day_label: str | None) -> int:
    """'W1-Monday' → 1; 'Monday' → 0."""
    if not day_label:
        return 0
    if day_label.startswith("W") and "-" in day_label:
        try:
            return int(day_label[1 : day_label.index("-")])
        except ValueError:
            pass
    return 0


def _gap_consequences(knowledge_graph: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for obj in knowledge_graph:
        for dep in obj.get("dependencies") or []:
            gap = (dep.get("gap_consequence") or "").strip()
            if gap:
                out.append(gap)
    return out


def _activation_strategies(knowledge_graph: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for obj in knowledge_graph:
        for dep in obj.get("dependencies") or []:
            strat = (dep.get("activation_strategy") or "").strip()
            if strat:
                out.append(strat)
    return out


def _compute_confidence(
    instructional_confidence: str | None,
    alignment_confidence: str | None,
    artifact_confidences: list[str],
) -> dict[str, str]:
    if alignment_confidence == "Needs Review":
        return {
            "level": "Needs Review",
            "explanation": "Cross-material alignment issues were detected. A quick review of your lesson materials before class is recommended.",
        }
    if instructional_confidence == "Needs Review":
        return {
            "level": "Ready with Notes",
            "explanation": "Your curriculum plan has items worth a quick look before you teach. The materials are ready to use.",
        }
    if "Needs Review" in artifact_confidences:
        return {
            "level": "Ready with Notes",
            "explanation": "One or more generated materials have flagged items. Worth a quick review before class.",
        }
    return {
        "level": "Ready",
        "explanation": "All lesson materials are aligned and validated. You're ready to teach.",
    }


def _build_timing(total_minutes: int) -> dict[str, Any]:
    default_total = sum(s["minutes"] for s in _TIMING_TEMPLATE)
    scale = total_minutes / default_total if default_total > 0 else 1.0

    segments: list[dict[str, Any]] = []
    for seg in _TIMING_TEMPLATE:
        scaled = max(1, round(seg["minutes"] * scale))
        trim = max(1, round(seg["trim_to"] * scale)) if seg["trim_to"] else None
        segments.append(
            {
                "name": seg["name"],
                "minutes": scaled,
                "flexible": seg["flexible"],
                "trim_to_minutes": trim,
                "skippable": seg["skippable"],
            }
        )

    trim_phrases = [
        f"{s['name']} ({s['minutes']}→{s['trim_to_minutes']}m)"
        for s in segments
        if s["flexible"] and not s["skippable"] and s["trim_to_minutes"]
    ]
    skip_names = [s["name"] for s in segments if s["skippable"]]

    if_behind_parts: list[str] = []
    if trim_phrases:
        if_behind_parts.append(f"Trim: {', '.join(trim_phrases)}.")
    if skip_names:
        if_behind_parts.append(f"Skip if needed: {', '.join(skip_names)}.")
    if_behind_parts.append("The Exit Ticket is non-negotiable.")

    return {
        "total_minutes": total_minutes,
        "segments": segments,
        "if_running_behind": " ".join(if_behind_parts),
        "non_negotiable": "Exit Ticket",
    }


def _build_critical_moments(
    day_entry: dict[str, Any],
    gap_consequences: list[str],
) -> list[dict[str, str]]:
    moments: list[dict[str, str]] = []

    # 1. The formative assessment checkpoint
    formative = (day_entry.get("formative_assessment") or "").strip()
    if formative:
        moments.append(
            {
                "moment": "During guided and independent practice",
                "why_it_matters": formative,
                "suggested_move": (
                    "Circulate and listen before addressing the whole class. "
                    "Note which students are struggling before deciding whether to pause and reteach."
                ),
            }
        )

    # 2. The transition from modeling to independent work
    if day_entry.get("teacher_modeling"):
        moments.append(
            {
                "moment": "When students begin working independently for the first time",
                "why_it_matters": (
                    "This is where confusion from teacher modeling surfaces. "
                    "Students who nodded along may not yet be able to do it alone."
                ),
                "suggested_move": (
                    "Give a 30-second 'Try it right now' before fully releasing. "
                    "Scan the room immediately — don't wait until the end of independent time."
                ),
            }
        )

    # 3. The most impactful knowledge gap
    if gap_consequences:
        moments.append(
            {
                "moment": "When students get stuck or give unexpected answers",
                "why_it_matters": gap_consequences[0],
                "suggested_move": (
                    "Name the confusion out loud: 'Some of us are thinking X — "
                    "let's look at why that's a different question from what we're solving.'"
                ),
            }
        )

    return moments[:3]


def _build_before_class(
    day_entry: dict[str, Any],
    daily_plan_content: dict[str, Any] | None,
    subject_name: str | None,
    has_student_deck: bool,
) -> dict[str, Any]:
    raw_materials: list[str] = []

    if daily_plan_content:
        subjects = daily_plan_content.get("subjects") or []
        # Try to match by subject name; fall back to first subject
        matched: list[Any] = []
        for subj in subjects:
            if (subj.get("subject_name") or "").lower() == (subject_name or "").lower():
                matched = [str(m) for m in (subj.get("materials") or []) if m]
                break
        raw_materials = matched or (
            [str(m) for m in ((subjects[0].get("materials") or []) if subjects else []) if m]
        )

    tasks: list[str] = []

    student_goal = (day_entry.get("student_goal") or "").strip()
    if student_goal:
        tasks.append(f'Review today\'s learning target before class: "{student_goal}"')

    for material in raw_materials:
        m = material.strip()
        m_lower = m.lower()
        if "anchor chart" in m_lower:
            tasks.append(f"Post anchor chart: {m}")
        elif any(
            w in m_lower for w in ("ticket", "worksheet", "handout", "slip", "printout", "print")
        ):
            tasks.append(f"Print and distribute: {m}")
        elif any(w in m_lower for w in ("text", "book", "passage", "article", "page", "p.")):
            tasks.append(f"Bookmark and have ready: {m}")
        else:
            tasks.append(f"Have ready: {m}")

    if has_student_deck:
        tasks.append(
            "Open the student lesson deck on your device and confirm the slides load before class."
        )

    return {"preparation_tasks": tasks}


def _build_classroom_support(
    day_entry: dict[str, Any],
    gap_consequences: list[str],
    activation_strategies: list[str],
) -> dict[str, Any]:
    in_the_moment: list[str] = []

    formative = (day_entry.get("formative_assessment") or "").strip()
    if formative:
        in_the_moment.append(formative)

    for strat in activation_strategies[:2]:
        s = strat.strip()
        if s:
            in_the_moment.append(s)

    observable = (day_entry.get("observable_mastery_evidence") or "").strip()

    return {
        "common_misconceptions": gap_consequences[:3],
        "in_the_moment": in_the_moment,
        "mastery_looks_like": observable,
    }


# ── Per-day-subject assembly ─────────────────────────────────────────────────


def _assemble_day_subject(
    day_entry: dict[str, Any],
    subject_name: str,
    week_design: dict[str, Any],
    knowledge_graph: list[dict[str, Any]],
    instructional_confidence: str | None,
    alignment_confidence: str | None,
    artifact_confidences: list[str],
    daily_plan_content: dict[str, Any] | None,
    has_student_deck: bool,
    period_minutes: int,
) -> dict[str, Any]:
    gaps = _gap_consequences(knowledge_graph)
    activations = _activation_strategies(knowledge_graph)

    student_goal = (day_entry.get("student_goal") or "").strip()
    instructional_purpose = (day_entry.get("instructional_purpose") or "").strip()
    builds = (day_entry.get("builds_from_yesterday") or "").strip()
    prepares = (day_entry.get("prepares_for_tomorrow") or "").strip()
    end_of_week = (week_design.get("end_of_week_mastery") or "").strip()

    exit_ticket = (
        (day_entry.get("exit_ticket") or "")
        or (week_design.get("instructional_contracts") or {}).get("exit_ticket_stem")
        or ""
    ).strip()

    reteach = (day_entry.get("reteach_if_needed") or "").strip()
    scaffold = (day_entry.get("differentiation") or {}).get("scaffold") or ""
    extension = (day_entry.get("differentiation") or {}).get("extension") or ""
    discussion = (day_entry.get("discussion_prompt") or "").strip()

    why_parts: list[str] = []
    if builds:
        why_parts.append(f"Builds on: {builds}.")
    if prepares:
        why_parts.append(f"Prepares students for: {prepares}.")
    if end_of_week:
        why_parts.append(f"By the end of the week: {end_of_week}.")

    reflection_prompts = [
        "Which student misconception appeared most often during practice?",
        (
            f"Did students meet today's learning target: '{student_goal}'?"
            if student_goal
            else "Did students meet today's learning target?"
        ),
        "What would you adjust in tomorrow's lesson based on today's exit ticket results?",
        "Which students need a follow-up conversation before the next lesson?",
    ]

    return {
        "subject_name": subject_name,
        "lesson_snapshot": {
            "learning_target": student_goal,
            "lesson_time_minutes": period_minutes,
            "assessment": exit_ticket,
            "key_misconception": gaps[0] if gaps else None,
            "confidence": _compute_confidence(
                instructional_confidence, alignment_confidence, artifact_confidences
            ),
        },
        "before_class": _build_before_class(
            day_entry, daily_plan_content, subject_name, has_student_deck
        ),
        "daily_brief": {
            "big_idea": instructional_purpose,
            "learning_target": student_goal,
            "why_it_matters": " ".join(why_parts),
        },
        "estimated_timing": _build_timing(period_minutes),
        "critical_moments": _build_critical_moments(day_entry, gaps),
        "classroom_support": _build_classroom_support(day_entry, gaps, activations),
        "student_support": {
            "if_struggling": {
                "reteach_strategy": reteach,
                "scaffold_recommendation": scaffold.strip() if isinstance(scaffold, str) else "",
            },
            "if_mastering_quickly": {
                "extension_activity": extension.strip() if isinstance(extension, str) else "",
                "enrichment_discussion": discussion,
            },
        },
        "after_lesson": {
            "reflection_prompts": reflection_prompts,
        },
    }


# ── Main entry point ─────────────────────────────────────────────────────────


def assemble_teaching_brief(
    package: "TeacherAssistV2InstructionalPackage",
) -> dict[str, Any]:
    """Assemble Today's Teaching Brief from existing generation data. Zero AI calls."""
    plan: dict[str, Any] = package.instructional_design_plan_json or {}
    validation_report: dict[str, Any] = package.instructional_validation_report_json or {}
    alignment_report: dict[str, Any] = package.instructional_alignment_report_json or {}

    knowledge_graph: list[dict[str, Any]] = plan.get("knowledge_dependency_graph") or []
    weeks_plan: list[dict[str, Any]] = plan.get("weeks") or []

    instructional_confidence: str | None = validation_report.get("confidence_label") or None
    alignment_weeks: list[dict[str, Any]] = alignment_report.get("weeks") or []
    alignment_confidence: str | None = None
    if alignment_weeks:
        alignment_confidence = (
            "Needs Review"
            if any(w.get("week_alignment_confidence") == "Needs Review" for w in alignment_weeks)
            else "Ready"
        )

    artifact_confidences: list[str] = []
    daily_plan_by_label: dict[str, dict[str, Any]] = {}
    student_deck_day_labels: set[str] = set()

    for artifact in package.artifacts or []:
        meta: dict[str, Any] = artifact.metadata_json or {}
        conf = meta.get("artifact_confidence") or meta.get("confidence") or None
        if conf:
            artifact_confidences.append(str(conf))

        day = artifact.day_label
        if not day:
            continue
        if artifact.artifact_type == "daily_lesson_plan":
            content = artifact.content_json
            if isinstance(content, dict):
                daily_plan_by_label[day] = content
        elif artifact.artifact_type == "student_lesson_deck":
            student_deck_day_labels.add(day)

    days_output: list[dict[str, Any]] = []

    for week_entry in weeks_plan:
        week_num: int = int(week_entry.get("week") or 0)
        for subject_entry in week_entry.get("subjects") or []:
            subject_name: str = (subject_entry.get("subject") or "").strip()
            design: dict[str, Any] = subject_entry.get("instructional_design") or {}
            daily_progression: list[dict[str, Any]] = design.get("daily_progression") or []

            for day_entry in daily_progression:
                weekday: str = (day_entry.get("day") or "").strip()
                if not weekday:
                    continue

                day_label = f"W{week_num}-{weekday}" if week_num else weekday

                container: dict[str, Any] | None = next(
                    (d for d in days_output if d["day_label"] == day_label), None
                )
                if container is None:
                    container = {"day_label": day_label, "week_num": week_num, "subjects": []}
                    days_output.append(container)

                subject_brief = _assemble_day_subject(
                    day_entry=day_entry,
                    subject_name=subject_name,
                    week_design=design,
                    knowledge_graph=knowledge_graph,
                    instructional_confidence=instructional_confidence,
                    alignment_confidence=alignment_confidence,
                    artifact_confidences=artifact_confidences,
                    daily_plan_content=daily_plan_by_label.get(day_label),
                    has_student_deck=(day_label in student_deck_day_labels),
                    period_minutes=50,
                )
                container["subjects"].append(subject_brief)

    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "days": days_output,
    }

"""Copilot intent handlers — explainable analysis from context packets (no auto-actions)."""

from __future__ import annotations

import re
from typing import Any


SUGGESTED_QUESTIONS = [
    "What objectives need reteaching?",
    "Which students need support?",
    "What should I focus on next week?",
    "Summarize this week.",
    "What objectives have low mastery?",
    "Which assignments had the best outcomes?",
    "Create small groups.",
    "Identify gaps in assessment coverage.",
    "Generate intervention ideas.",
    "Prepare grading period summary.",
    "What resources should I use?",
    "What objectives have weak coverage?",
    "What should I reteach?",
    "What patterns are emerging in my reflections?",
]


def _match_intent(question: str) -> str:
    text = question.strip().lower()
    rules: list[tuple[str, str]] = [
        (r"reteach|re-teach", "reteach_assistant"),
        (r"small group|build group|group students", "small_group_builder"),
        (r"summarize this week|summarise this week|week summary", "week_summarizer"),
        (r"grading period|quarter summary|term summary", "grading_period_summarizer"),
        (r"student.*support|need intervention|need support", "student_support"),
        (r"low mastery|struggling objective|objectives.*struggling", "objective_analysis"),
        (r"next week|focus on next", "focus_next_week"),
        (r"assignment.*outcome|best assignment|best outcomes", "assignment_outcomes"),
        (r"assessment.*gap|coverage gap|no assessment", "lesson_gap_analysis"),
        (r"intervention|reteach idea", "intervention_ideas"),
        (r"resource|what should i use", "resource_recommender"),
        (r"weak coverage|lesson gap|coverage", "lesson_gap_analysis"),
        (r"pattern|reflection", "reflection_assistant"),
        (r"objective.*reteach|need reteaching", "objective_analysis"),
    ]
    for pattern, intent in rules:
        if re.search(pattern, text):
            return intent
    return "general_assistant"


def _response(
    *,
    intent: str,
    answer: str,
    why: str,
    evidence: list[dict[str, Any]],
    source_data: list[str],
    packets_used: list[str],
    recommendations: list[dict[str, Any]] | None = None,
    confidence: str = "high",
    draft_groups: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "intent": intent,
        "answer": answer,
        "why": why,
        "evidence": evidence,
        "source_data": source_data,
        "context_packets_used": packets_used,
        "recommendations": recommendations or [],
        "draft_groups": draft_groups or [],
        "confidence": confidence,
        "requires_teacher_review": True,
        "no_automatic_actions": True,
    }


def analyze_copilot_question(*, question: str, context: dict[str, Any]) -> dict[str, Any]:
    intent = _match_intent(question)
    packets = context.get("context_packets") or {}
    objectives = (packets.get("objectives") or {}).get("objectives") or []
    weakest = (packets.get("objectives") or {}).get("weakest") or []
    performance = (packets.get("mastery") or {}).get("performance") or {}
    reteach_ws = (packets.get("reteach") or {}).get("workspace") or {}
    coverage = (packets.get("assessments") or {}).get("assignment_coverage") or {}
    assignments = coverage.get("assignments") or []
    reflections = (packets.get("reflections") or {}).get("items") or []
    resources = (packets.get("resources") or {}).get("recommended_reuse") or []
    current_week = (packets.get("current_week") or {}).get("current_week") or {}
    grading_period = (packets.get("pacing_guide") or {}).get("grading_period") or {}

    if intent == "objective_analysis" or intent == "reteach_assistant":
        targets = reteach_ws.get("objectives_requiring_reteach") or weakest[:5]
        if not targets:
            return _response(
                intent=intent,
                answer="No objectives are currently flagged below the mastery review threshold.",
                why="Objective performance and reteach workspace show no objectives with mastery below 50% and assessed students.",
                evidence=[],
                source_data=["objectives packet", "reteach workspace"],
                packets_used=["objectives", "mastery", "reteach"],
                confidence="medium",
            )
        lines = [f"- {row.get('objective_code') or 'Objective'}: {row.get('mastery_pct', 0)}% mastery" for row in targets]
        return _response(
            intent=intent,
            answer="These objectives may need reteaching:\n" + "\n".join(lines),
            why="Objectives with mastery below 50% and at least one assessed student are flagged for review.",
            evidence=[{"type": "objective", "payload": row} for row in targets],
            source_data=["teacher_assist_instructional_evidence", "objective_performance"],
            packets_used=["objectives", "mastery", "reteach"],
            recommendations=[
                {
                    "action_key": "create_reteach_plan",
                    "label": "Review reteach workspace",
                    "navigation_href": "/teacher-assist/reteach",
                }
            ],
        )

    if intent == "student_support":
        students = performance.get("students_needing_support") or []
        near = performance.get("students_near_mastery") or []
        combined = students + near
        if not combined:
            return _response(
                intent=intent,
                answer="No students are currently flagged for intervention based on confirmed evidence.",
                why="Student support signals require teacher-confirmed evidence at beginning/developing mastery levels.",
                evidence=[],
                source_data=["instructional evidence", "mastery evaluations"],
                packets_used=["mastery"],
                confidence="medium",
            )
        return _response(
            intent=intent,
            answer=f"{len(combined)} student(s) show support signals. Review evidence before grouping.",
            why="Students with developing mastery or repeated support signals appear in objective performance.",
            evidence=[{"type": "student", "payload": row} for row in combined[:10]],
            source_data=["instructional_evidence"],
            packets_used=["mastery", "objectives"],
            recommendations=[
                {"action_key": "review_support", "label": "Open reteach workspace", "navigation_href": "/teacher-assist/reteach"}
            ],
        )

    if intent == "small_group_builder":
        suggested = reteach_ws.get("suggested_groups") or []
        draft_groups = [
            {
                "title": row.get("title"),
                "suggested_activities": row.get("suggested_activities") or [],
                "status": "draft",
                "requires_teacher_confirmation": True,
            }
            for row in suggested[:4]
        ]
        return _response(
            intent=intent,
            answer="Draft small groups prepared from mastery and objective performance. Confirm before saving.",
            why="Groups are suggested from objectives requiring reteach and student support signals.",
            evidence=[{"type": "suggested_group", "payload": row} for row in suggested],
            source_data=["objective_performance", "reteach workspace"],
            packets_used=["reteach", "mastery", "objectives"],
            draft_groups=draft_groups,
            recommendations=[
                {"action_key": "save_support_group", "label": "Review support groups", "navigation_href": "/teacher-assist/reteach"}
            ],
        )

    if intent == "week_summarizer":
        obj_count = len(objectives)
        assign_count = len(assignments)
        assessed = sum(1 for row in assignments if (row.get("students_assessed") or 0) > 0)
        reteach_count = len(reteach_ws.get("objectives_requiring_reteach") or [])
        return _response(
            intent=intent,
            answer=(
                f"Week '{current_week.get('title', 'Current week')}': {obj_count} objectives tracked, "
                f"{assign_count} assignments, {assessed} with student evidence, {reteach_count} reteach candidates."
            ),
            why="Summary aggregates instructional week objectives, assignment coverage, mastery, and reflections.",
            evidence=[
                {"type": "week", "payload": current_week},
                {"type": "coverage_summary", "payload": coverage.get("summary")},
            ],
            source_data=["instructional week", "assignment coverage", "reflections"],
            packets_used=["current_week", "objectives", "assessments", "reteach", "reflections"],
        )

    if intent == "grading_period_summarizer":
        return _response(
            intent=intent,
            answer=(
                f"Grading period '{grading_period.get('title', 'Current period')}': "
                f"{len(objectives)} objectives in scope, "
                f"{len([row for row in objectives if (row.get('mastery_pct') or 0) >= 70])} strong, "
                f"{len(weakest)} need attention."
            ),
            why="Period summary uses objective performance across the active instructional context.",
            evidence=[{"type": "objective", "payload": row} for row in objectives[:8]],
            source_data=["objective_performance", "grading period"],
            packets_used=["pacing_guide", "objectives", "mastery"],
        )

    if intent == "assignment_outcomes":
        ranked = sorted(assignments, key=lambda row: row.get("mastery_pct") or 0, reverse=True)
        top = ranked[:3]
        if not top:
            return _response(
                intent=intent,
                answer="No assignment coverage data yet for this week.",
                why="Assignment coverage requires linked assignments with grade or evidence records.",
                evidence=[],
                source_data=["assignment_coverage"],
                packets_used=["assessments"],
                confidence="low",
            )
        lines = [f"- {row.get('title')}: {row.get('mastery_pct')}% mastery, {row.get('students_assessed')} students" for row in top]
        return _response(
            intent=intent,
            answer="Assignments with strongest mastery signals:\n" + "\n".join(lines),
            why="Ranked by average objective mastery % linked to each assignment.",
            evidence=[{"type": "assignment", "payload": row} for row in top],
            source_data=["assignment_coverage"],
            packets_used=["assessments", "objectives"],
        )

    if intent == "lesson_gap_analysis":
        gaps = [row for row in objectives if (row.get("students_assessed") or 0) == 0]
        low = weakest[:5]
        return _response(
            intent=intent,
            answer=f"Found {len(gaps)} objectives without assessment evidence and {len(low)} low-mastery objectives.",
            why="Gap analysis combines unassessed objectives, low mastery, and assignment coverage.",
            evidence=[{"type": "gap_objective", "payload": row} for row in (gaps + low)[:8]],
            source_data=["objective_performance", "assignment_coverage"],
            packets_used=["objectives", "assessments", "mastery"],
        )

    if intent == "resource_recommender":
        if not resources:
            return _response(
                intent=intent,
                answer="No reuse or catalog resource recommendations found for this week yet.",
                why="Resource recommender checks reuse engine and week context resources.",
                evidence=[],
                source_data=["reuse engine", "week resources"],
                packets_used=["resources"],
                confidence="low",
            )
        lines = [f"- {row.get('title', 'Resource')} (score {row.get('reuse_score', {}).get('score', '—')})" for row in resources[:5]]
        return _response(
            intent=intent,
            answer="Recommended resources for this week:\n" + "\n".join(lines),
            why="Recommendations come from the reuse engine and catalog-aligned week context.",
            evidence=[{"type": "resource", "payload": row} for row in resources[:5]],
            source_data=["reuse engine", "catalog"],
            packets_used=["resources"],
            recommendations=[
                {"action_key": "open_resource", "label": row.get("title"), "navigation_href": row.get("navigation_href")}
                for row in resources[:3]
                if row.get("navigation_href")
            ],
        )

    if intent == "reflection_assistant":
        themes: list[str] = []
        for row in reflections:
            for key in ("what_worked", "what_didnt_work", "student_challenges", "adjustments_needed"):
                value = row.get(key)
                if value:
                    themes.append(str(value)[:120])
        if not themes:
            return _response(
                intent=intent,
                answer="No instructional reflections recorded yet for this week.",
                why="Reflection assistant analyzes stored instructional reflection entries.",
                evidence=[],
                source_data=["instructional_reflections"],
                packets_used=["reflections"],
                confidence="low",
            )
        return _response(
            intent=intent,
            answer="Recurring reflection themes:\n" + "\n".join(f"- {theme}" for theme in themes[:5]),
            why="Patterns extracted from teacher-authored reflection fields (not AI-generated).",
            evidence=[{"type": "reflection", "payload": row} for row in reflections[:5]],
            source_data=["instructional_reflections"],
            packets_used=["reflections", "mastery"],
        )

    if intent == "focus_next_week":
        upcoming = (packets.get("current_week") or {}).get("upcoming_week") or {}
        return _response(
            intent=intent,
            answer=f"Focus next week on '{upcoming.get('title', 'upcoming week')}' objectives and close any reteach gaps from this week.",
            why="Uses upcoming pacing week plus current reteach and mastery signals.",
            evidence=[{"type": "upcoming_week", "payload": upcoming}],
            source_data=["pacing guide", "reteach workspace"],
            packets_used=["current_week", "reteach", "objectives"],
            recommendations=(packets.get("recommendations") or {}).get("loop_recommendations") or [],
        )

    if intent == "intervention_ideas":
        ideas = []
        for row in (reteach_ws.get("suggested_groups") or [])[:3]:
            ideas.extend(row.get("suggested_activities") or [])
        if not ideas:
            ideas = ["Small-group reteach", "Guided practice checkpoint", "Short reassessment"]
        return _response(
            intent=intent,
            answer="Intervention ideas (review before using):\n" + "\n".join(f"- {idea}" for idea in ideas[:6]),
            why="Suggestions derived from reteach workspace and objective performance — not auto-applied.",
            evidence=[{"type": "reteach_objective", "payload": row} for row in (reteach_ws.get("objectives_requiring_reteach") or [])[:5]],
            source_data=["reteach workspace"],
            packets_used=["reteach", "mastery"],
        )

    return _response(
        intent="general_assistant",
        answer=(
            "I can help analyze objectives, student support, week summaries, resources, and reteach needs "
            "using your current instructional context. Try one of the suggested questions."
        ),
        why="General fallback when no specific intent matched.",
        evidence=[],
        source_data=["context engine"],
        packets_used=list(packets.keys()),
        confidence="low",
    )


def analyze_admin_copilot_question(*, question: str, context: dict[str, Any]) -> dict[str, Any]:
    text = question.strip().lower()
    packets = context.get("context_packets") or {}
    resources = (packets.get("resources") or {}).get("recommended_reuse") or []
    objectives = (packets.get("objectives") or {}).get("objectives") or []

    if "resource" in text or "catalog" in text:
        return _response(
            intent="admin_catalog_gaps",
            answer="Catalog gap review: verify objective-resource mappings in Education Catalog admin for objectives lacking linked resources.",
            why="Admin copilot surfaces catalog alignment gaps; full district scan requires catalog admin APIs.",
            evidence=[{"type": "resource_sample", "payload": row} for row in resources[:3]],
            source_data=["education catalog", "reuse engine"],
            packets_used=["resources", "objectives"],
            confidence="medium",
        )
    if "pacing" in text:
        guide = (packets.get("pacing_guide") or {}).get("pacing_guide") or {}
        return _response(
            intent="admin_pacing_gaps",
            answer=f"Review pacing guide '{guide.get('title', 'active guide')}' for incomplete week periods or missing objectives.",
            why="Admin pacing analysis uses active guide context from teacher preferences.",
            evidence=[{"type": "pacing_guide", "payload": guide}],
            source_data=["pacing guides"],
            packets_used=["pacing_guide"],
        )
    return _response(
        intent="admin_general",
        answer="Admin copilot can help identify catalog gaps, pacing guide completeness, and curriculum mapping issues.",
        why="Use specific questions about resources, pacing guides, or curriculum mappings.",
        evidence=[{"type": "objective_count", "payload": {"count": len(objectives)}}],
        source_data=["context engine"],
        packets_used=list(packets.keys()),
        confidence="low",
    )

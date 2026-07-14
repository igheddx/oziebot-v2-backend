"""Curriculum Validation & Instructional Quality Engine — TeacherAssist v2.

Pipeline position: Phase 0d — after plan generation (Phase 0b+0c), before plan lock.

Core question: 'Can an average student realistically master every district objective
by the end of the allotted duration following this instructional journey?'

Two-tier validation:
  Tier 1 — Deterministic rules (R1-R19): structural completeness, district fidelity,
            assessment alignment, cognitive load heuristics. Pure Python. < 1s.
  Tier 2 — AI pedagogical review: learning progression, backward design integrity,
            cognitive load, mastery realism, ELA cohesion, student engagement,
            instructional continuity. One AI call per subject. Sequential.

Revision strategy:
  Issues with severity critical/severe/moderate trigger a targeted revision call.
  Revision scope is minimal: day → week → subject (in that order of preference).
  Maximum 2 revision rounds per subject. After round 2, proceed with the best plan.

Improvement Source taxonomy (on every issue):
  Prompt        — tuning the AI generation prompt prevents recurrence
  Rule          — a deterministic rule can be added or tightened to detect this
  Architecture  — the plan schema or pipeline design must change
  Teacher Input — missing/incomplete curriculum data; only the teacher can resolve

Issue kind taxonomy separates:
  educational  — pedagogically unsound content (wrong sequencing, broken arc, etc.)
  generation   — AI generation artifacts (empty fields, placeholder text, repetition)

Never blocks artifact generation. All exceptions swallowed; caller receives partial
report on any failure and generation proceeds.
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_usage import record_teacher_assist_ai_usage
from oziebot_api.services.teacher_assist.openai_json_client import execute_openai_json_completion
from oziebot_api.services.teacher_assist.prompt_contracts import (
    V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
)
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_provider_model
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.instructional_design_plan import (
    get_plan_for_day,
    get_plan_for_week_subject,
)
from oziebot_api.services.teacher_assist_v2.planning_constants import WEEKDAY_LABELS

logger = logging.getLogger(__name__)

_REPORT_SCHEMA_VERSION = "1.0"
_VALIDATION_PROMPT_VERSION = "validation-v1.0"
_MAX_REVISION_ROUNDS = 2

_CONFIDENCE_THRESHOLDS = {
    "Excellent": 9.0,
    "Very Good": 7.5,
}

_CATEGORY_WEIGHTS: dict[str, float] = {
    "district_fidelity": 0.25,
    "learning_progression": 0.20,
    "backward_design": 0.15,
    "assessment_alignment": 0.10,
    "mastery_progression": 0.10,
    "cognitive_load": 0.07,
    "instructional_continuity": 0.05,
    "student_engagement": 0.05,
    "differentiation": 0.02,
    "reading_writing_cohesion": 0.01,
}

_COLLABORATIVE_MARKERS = frozenset(
    ["partner", "pair", "group", "team", "together", "discuss", "share", "collaborative"]
)
_READING_MARKERS = frozenset(["read", "text", "passage", "excerpt"])
_WRITING_MARKERS = frozenset(["write", "draft", "compose", "paragraph", "response"])

_REQUIRED_DAY_FIELDS = [
    "student_goal", "teacher_goal", "instructional_purpose",
    "teacher_modeling", "guided_practice", "independent_practice",
    "exit_ticket", "observable_mastery_evidence", "reteach_if_needed",
]


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _provider_api_params(settings: Settings) -> tuple[str, str | None, str | None]:
    provider = (settings.teacher_assist_ai_provider or "mock").strip().lower()
    if provider == "gemini":
        return (
            "gemini",
            (settings.teacher_assist_gemini_api_key or "").strip() or None,
            (settings.teacher_assist_gemini_base_url or "").strip()
            or "https://generativelanguage.googleapis.com/v1beta/openai",
        )
    return ("openai", None, None)


def _issue(
    *,
    severity: str,
    category: str,
    issue_kind: str,
    improvement_source: str,
    description: str,
    recommendation: str,
    week: int | None = None,
    day: str | None = None,
    affected_scope: str,
    affected_key: str,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "category": category,
        "issue_kind": issue_kind,
        "improvement_source": improvement_source,
        "week": week,
        "day": day,
        "description": description,
        "recommendation": recommendation,
        "affected_scope": affected_scope,
        "affected_key": affected_key,
        "auto_revised": False,
        "revision_outcome": None,
    }


# ── Tier 1: Deterministic validation rules ────────────────────────────────────────────────

def _validate_district_fidelity(
    week_num: int,
    subject_name: str,
    district_anchors: dict[str, Any],
    instructional_design: dict[str, Any],
    issues: list[dict[str, Any]],
) -> None:
    """R1-R6: District fidelity checks."""
    primary_objectives = district_anchors.get("primary_objectives") or []
    supporting_objectives = district_anchors.get("supporting_objectives") or []
    primary_codes = {obj.get("teks_code") for obj in primary_objectives if obj.get("teks_code")}
    all_allowed_codes = primary_codes | {
        obj.get("teks_code") for obj in supporting_objectives if obj.get("teks_code")
    }

    contracts = instructional_design.get("instructional_contracts") or {}
    quiz_objectives = set(contracts.get("quiz_objectives") or [])

    # R1: Required TEKS must appear in quiz_objectives
    missing_primary = primary_codes - quiz_objectives
    if missing_primary:
        issues.append(_issue(
            severity="severe",
            category="district_fidelity",
            issue_kind="educational",
            improvement_source="Architecture",
            description=f"Week {week_num} {subject_name}: Required TEKS {sorted(missing_primary)} are absent "
                        "from instructional_contracts.quiz_objectives. These objectives will not be assessed.",
            recommendation="The instructional design plan must include all primary_objectives TEKS codes in "
                           "quiz_objectives. Review the backward design: does the week's instruction cover "
                           "every required objective before Friday?",
            week=week_num, day=None,
            affected_scope="week", affected_key=f"W{week_num}-{subject_name}",
        ))

    # R2: No phantom objectives (AI-invented TEKS not in district pacing)
    if all_allowed_codes:
        phantom = quiz_objectives - all_allowed_codes
        if phantom:
            issues.append(_issue(
                severity="critical",
                category="district_fidelity",
                issue_kind="educational",
                improvement_source="Architecture",
                description=f"Week {week_num} {subject_name}: quiz_objectives contains TEKS codes {sorted(phantom)} "
                            "that are not in the district pacing guide for this week. AI introduced unsupported objectives.",
                recommendation="Remove phantom TEKS codes from quiz_objectives. The district defines the objectives; "
                               "TeacherAssist must not add new ones.",
                week=week_num, day=None,
                affected_scope="week", affected_key=f"W{week_num}-{subject_name}",
            ))

    # R3: All district daily topics must have a corresponding day entry
    daily_topics = district_anchors.get("daily_topics") or {}
    progression = instructional_design.get("daily_progression") or []
    plan_days_lower = {(d.get("day") or "").lower() for d in progression}
    missing_days = {day for day in daily_topics if day.lower() not in plan_days_lower}
    if missing_days:
        issues.append(_issue(
            severity="critical",
            category="district_fidelity",
            issue_kind="generation",
            improvement_source="Prompt",
            description=f"Week {week_num} {subject_name}: District-defined day(s) {sorted(missing_days)} are absent "
                        "from daily_progression. Instruction for those days is missing entirely.",
            recommendation="The instructional design plan must have exactly one entry per weekday matching "
                           "district_anchors.daily_topics keys. Ensure Monday–Friday are all present.",
            week=week_num, day=None,
            affected_scope="subject", affected_key=f"W{week_num}-{subject_name}",
        ))

    # R4: Assessment check days must have non-empty formative_assessment
    assessment_checks = district_anchors.get("assessment_checks") or {}
    day_map = {(d.get("day") or "").lower(): d for d in progression}
    for check_day in assessment_checks:
        day_entry = day_map.get(check_day.lower())
        if day_entry and not (day_entry.get("formative_assessment") or "").strip():
            issues.append(_issue(
                severity="moderate",
                category="district_fidelity",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name}: District pacing marks {check_day} as an assessment day, "
                            "but formative_assessment is empty in the instructional design plan.",
                recommendation="Fill formative_assessment with a specific teacher-facing check: what the teacher "
                               "observes, listens for, or collects from students during this assessment moment.",
                week=week_num, day=check_day,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{check_day}",
            ))


def _validate_structural_completeness(
    week_num: int,
    subject_name: str,
    instructional_design: dict[str, Any],
    total_weeks: int,
    issues: list[dict[str, Any]],
) -> None:
    """R5-R12: Structural completeness checks."""
    progression = instructional_design.get("daily_progression") or []
    contracts = instructional_design.get("instructional_contracts") or {}

    # R7: All 5 weekdays must be present
    plan_days_lower = [(d.get("day") or "").lower() for d in progression]
    expected = [day.lower() for day in WEEKDAY_LABELS]
    missing = [day for day in expected if day not in plan_days_lower]
    if missing:
        issues.append(_issue(
            severity="critical",
            category="district_fidelity",
            issue_kind="generation",
            improvement_source="Prompt",
            description=f"Week {week_num} {subject_name}: daily_progression is missing day(s): "
                        f"{[d.title() for d in missing]}. Artifact generation will have no instruction for those days.",
            recommendation="The plan must contain exactly 5 day entries (Monday through Friday). "
                           "Revision should regenerate the full subject for this week.",
            week=week_num, day=None,
            affected_scope="subject", affected_key=f"W{week_num}-{subject_name}",
        ))

    # R8: Required fields non-empty
    for day_entry in progression:
        day_label = (day_entry.get("day") or "").title()
        empty_fields = [
            f for f in _REQUIRED_DAY_FIELDS
            if not (day_entry.get(f) or "").strip()
        ]
        diff_scaffold = not ((day_entry.get("differentiation") or {}).get("scaffold") or "").strip()
        diff_ext = not ((day_entry.get("differentiation") or {}).get("extension") or "").strip()
        if diff_scaffold:
            empty_fields.append("differentiation.scaffold")
        if diff_ext:
            empty_fields.append("differentiation.extension")

        if len(empty_fields) >= 3:
            issues.append(_issue(
                severity="moderate",
                category="district_fidelity",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} {day_label}: {len(empty_fields)} required fields are "
                            f"empty: {empty_fields[:5]}{'...' if len(empty_fields) > 5 else ''}.",
                recommendation="The AI generation prompt should enforce non-empty required fields. "
                               "Revision should fill missing fields with specific, substantive content.",
                week=week_num, day=day_label,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
            ))
        elif len(empty_fields) >= 1:
            issues.append(_issue(
                severity="warning",
                category="district_fidelity",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} {day_label}: {len(empty_fields)} field(s) are "
                            f"empty: {empty_fields}.",
                recommendation="Fill empty fields with substantive content specific to the lesson.",
                week=week_num, day=day_label,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
            ))

    # R9: Temporal continuity null/non-null rules
    day_map = {(d.get("day") or "").lower(): d for d in progression}
    for i, day_label in enumerate(WEEKDAY_LABELS):
        entry = day_map.get(day_label.lower())
        if not entry:
            continue
        if i == 0 and (entry.get("builds_from_yesterday") or "").strip():
            issues.append(_issue(
                severity="moderate",
                category="instructional_continuity",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} Monday: builds_from_yesterday should be null on "
                            "Monday but contains content. Monday begins the week fresh.",
                recommendation="Set builds_from_yesterday to null on Monday.",
                week=week_num, day="Monday",
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-Monday",
            ))
        if i > 0 and not (entry.get("builds_from_yesterday") or "").strip():
            issues.append(_issue(
                severity="moderate",
                category="instructional_continuity",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} {day_label}: builds_from_yesterday is empty. "
                            "Every day Tue–Fri must explicitly connect to the previous day's lesson.",
                recommendation="Fill builds_from_yesterday with a specific reference to what students "
                               "practiced yesterday.",
                week=week_num, day=day_label,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
            ))
        last_day = WEEKDAY_LABELS[-1]
        if day_label.lower() == last_day.lower() and (entry.get("prepares_for_tomorrow") or "").strip():
            issues.append(_issue(
                severity="moderate",
                category="instructional_continuity",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} Friday: prepares_for_tomorrow should be null on "
                            "Friday but contains content.",
                recommendation="Set prepares_for_tomorrow to null on Friday.",
                week=week_num, day="Friday",
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-Friday",
            ))
        if day_label.lower() != last_day.lower() and not (entry.get("prepares_for_tomorrow") or "").strip():
            issues.append(_issue(
                severity="moderate",
                category="instructional_continuity",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} {day_label}: prepares_for_tomorrow is empty. "
                            "Every day Mon–Thu must preview tomorrow's lesson.",
                recommendation="Fill prepares_for_tomorrow with a specific preview of tomorrow's content.",
                week=week_num, day=day_label,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
            ))

    # R10: Instructional contracts non-empty
    contract_fields = ["exit_ticket_stem", "rubric_primary_criterion", "core_activity_name"]
    empty_contracts = [f for f in contract_fields if not (contracts.get(f) or "").strip()]
    if not contracts.get("quiz_objectives"):
        empty_contracts.append("quiz_objectives")
    if empty_contracts:
        issues.append(_issue(
            severity="severe",
            category="assessment_alignment",
            issue_kind="generation",
            improvement_source="Prompt",
            description=f"Week {week_num} {subject_name}: instructional_contracts is incomplete. "
                        f"Empty fields: {empty_contracts}. Cross-artifact alignment is broken.",
            recommendation="instructional_contracts must be fully populated. Every field in the contracts "
                           "is used by at least one artifact generator — empty contracts produce misaligned artifacts.",
            week=week_num, day=None,
            affected_scope="week", affected_key=f"W{week_num}-{subject_name}",
        ))

    # R11: Unit mastery arc completeness is checked at package level (caller handles)

    # R12: Week mastery target non-empty
    if not (instructional_design.get("end_of_week_mastery") or "").strip():
        issues.append(_issue(
            severity="severe",
            category="backward_design",
            issue_kind="generation",
            improvement_source="Prompt",
            description=f"Week {week_num} {subject_name}: end_of_week_mastery is empty. "
                        "The backward design spine is broken — there is no target for the week's instruction.",
            recommendation="end_of_week_mastery must be a specific, observable mastery statement. "
                           "All daily instruction builds toward this target.",
            week=week_num, day=None,
            affected_scope="week", affected_key=f"W{week_num}-{subject_name}",
        ))
    elif len((instructional_design.get("learning_journey_rationale") or "").strip()) < 50:
        issues.append(_issue(
            severity="moderate",
            category="backward_design",
            issue_kind="generation",
            improvement_source="Prompt",
            description=f"Week {week_num} {subject_name}: learning_journey_rationale is too brief "
                        f"({len((instructional_design.get('learning_journey_rationale') or '').strip())} chars). "
                        "It should explain why this 5-day sequence leads to the week's mastery target.",
            recommendation="Expand learning_journey_rationale to explain the pedagogical reasoning behind "
                           "the weekly sequence — not just what students do, but why in this order.",
            week=week_num, day=None,
            affected_scope="week", affected_key=f"W{week_num}-{subject_name}",
        ))


def _validate_assessment_alignment(
    week_num: int,
    subject_name: str,
    district_anchors: dict[str, Any],
    instructional_design: dict[str, Any],
    issues: list[dict[str, Any]],
) -> None:
    """R13-R14: Assessment alignment checks."""
    primary_objectives = district_anchors.get("primary_objectives") or []
    primary_codes = {obj.get("teks_code") for obj in primary_objectives if obj.get("teks_code")}
    contracts = instructional_design.get("instructional_contracts") or {}
    quiz_objectives = set(contracts.get("quiz_objectives") or [])

    # R13: Quiz objectives must be primary (not just supporting)
    if primary_codes and quiz_objectives:
        non_primary_in_quiz = quiz_objectives - primary_codes
        if non_primary_in_quiz:
            issues.append(_issue(
                severity="moderate",
                category="assessment_alignment",
                issue_kind="educational",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name}: quiz_objectives includes {sorted(non_primary_in_quiz)} "
                            "which are supporting (not primary) TEKS. Quizzes should assess primary objectives.",
                recommendation="Restrict quiz_objectives to primary_objectives TEKS codes only. "
                               "Supporting TEKS may be reinforced but should not be the primary assessment target.",
                week=week_num, day=None,
                affected_scope="week", affected_key=f"W{week_num}-{subject_name}",
            ))

    # R14: observable_mastery_evidence must be substantive
    for day_entry in instructional_design.get("daily_progression") or []:
        day_label = (day_entry.get("day") or "").title()
        evidence = (day_entry.get("observable_mastery_evidence") or "").strip()
        if evidence and len(evidence) < 20:
            issues.append(_issue(
                severity="warning",
                category="assessment_alignment",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} {day_label}: observable_mastery_evidence is too brief "
                            f"('{evidence}'). Placeholder text does not give teachers actionable observation criteria.",
                recommendation="Write a specific, observable description of what mastery looks like: "
                               "what the teacher hears, sees, or reads from students.",
                week=week_num, day=day_label,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
            ))


def _validate_cognitive_load(
    week_num: int,
    subject_name: str,
    instructional_design: dict[str, Any],
    issues: list[dict[str, Any]],
) -> None:
    """R15-R17: Cognitive load heuristic checks."""
    progression = instructional_design.get("daily_progression") or []

    # R15: Three consecutive teacher-centered days (no collaborative markers)
    consecutive_teacher_centered = 0
    first_non_collab_day: str | None = None
    for day_entry in progression:
        gp = (day_entry.get("guided_practice") or "").lower()
        has_collab = any(marker in gp for marker in _COLLABORATIVE_MARKERS)
        if not has_collab:
            consecutive_teacher_centered += 1
            if first_non_collab_day is None:
                first_non_collab_day = (day_entry.get("day") or "").title()
        else:
            consecutive_teacher_centered = 0
            first_non_collab_day = None

    if consecutive_teacher_centered >= 3:
        issues.append(_issue(
            severity="warning",
            category="student_engagement",
            issue_kind="educational",
            improvement_source="Prompt",
            description=f"Week {week_num} {subject_name}: {consecutive_teacher_centered} consecutive days "
                        f"(starting {first_non_collab_day}) have no collaborative activity in guided_practice. "
                        "Students have no structured peer learning opportunity.",
            recommendation="Convert at least one guided_practice to a collaborative structure: "
                           "partner work, turn-and-talk, small group, or discussion protocol.",
            week=week_num, day=first_non_collab_day,
            affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{first_non_collab_day}",
        ))

    # R16: Same primary activity verb 3+ consecutive days
    def _primary_verb(text: str) -> str | None:
        text = text.strip()
        match = re.search(r"\bstudents\s+(\w+)", text, re.IGNORECASE)
        return match.group(1).lower() if match else None

    verbs = [_primary_verb(d.get("guided_practice") or "") for d in progression]
    for i in range(len(verbs) - 2):
        if verbs[i] and verbs[i] == verbs[i + 1] == verbs[i + 2]:
            day_label = (progression[i].get("day") or "").title()
            issues.append(_issue(
                severity="warning",
                category="student_engagement",
                issue_kind="educational",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name}: The same guided_practice activity verb "
                            f"('{verbs[i]}') appears in 3 consecutive days starting {day_label}. "
                            "Activity repetition signals low engagement variation.",
                recommendation="Vary the instructional mode for guided_practice: if two days use written "
                               "response, the third should use discussion, sorting, annotation, or another modality.",
                week=week_num, day=day_label,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
            ))
            break

    # R17: Dual-demand day (heavy reading + heavy writing) without scaffold
    for day_entry in progression:
        day_label = (day_entry.get("day") or "").title()
        ip = (day_entry.get("independent_practice") or "").lower()
        has_reading = any(m in ip for m in _READING_MARKERS)
        has_writing = any(m in ip for m in _WRITING_MARKERS)
        scaffold = (day_entry.get("differentiation") or {}).get("scaffold") or ""
        scaffold_lower = scaffold.lower()
        scaffold_reduces = any(m in scaffold_lower for m in _READING_MARKERS | _WRITING_MARKERS)
        if has_reading and has_writing and not scaffold_reduces:
            issues.append(_issue(
                severity="warning",
                category="cognitive_load",
                issue_kind="educational",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} {day_label}: independent_practice demands both "
                            "heavy reading and writing simultaneously, but differentiation.scaffold does not "
                            "reduce either demand for struggling students.",
                recommendation="Add a scaffold that reduces one demand: provide the text pre-read "
                               "(reduce reading demand) OR offer sentence frames (reduce writing demand).",
                week=week_num, day=day_label,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
            ))


def _validate_differentiation(
    week_num: int,
    subject_name: str,
    instructional_design: dict[str, Any],
    issues: list[dict[str, Any]],
) -> None:
    """R18-R19: Differentiation quality checks."""
    for day_entry in instructional_design.get("daily_progression") or []:
        day_label = (day_entry.get("day") or "").title()
        diff = day_entry.get("differentiation") or {}
        scaffold = (diff.get("scaffold") or "").strip()
        extension = (diff.get("extension") or "").strip()
        ip = (day_entry.get("independent_practice") or "").strip()

        # R18: Scaffold must substantively reduce demand (not just repeat the task with help)
        if scaffold and ip and len(scaffold) >= 10:
            scaffold_words = set(scaffold.lower().split())
            ip_words = set(ip.lower().split())
            overlap_ratio = len(scaffold_words & ip_words) / max(len(ip_words), 1)
            if overlap_ratio > 0.7 and len(scaffold) < len(ip) * 0.35:
                issues.append(_issue(
                    severity="warning",
                    category="differentiation",
                    issue_kind="educational",
                    improvement_source="Prompt",
                    description=f"Week {week_num} {subject_name} {day_label}: differentiation.scaffold appears "
                                "to restate independent_practice with minimal modification. A real scaffold "
                                "reduces cognitive demand, not just adds 'with teacher help'.",
                    recommendation="Rewrite scaffold to explicitly reduce one cognitive demand: "
                                   "provide vocabulary pre-teaching, reduce writing length, pre-read the text, "
                                   "or offer a partially completed graphic organizer.",
                    week=week_num, day=day_label,
                    affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
                ))

        # R19: Extension must be substantive
        if extension and len(extension) < 25:
            issues.append(_issue(
                severity="info",
                category="differentiation",
                issue_kind="generation",
                improvement_source="Prompt",
                description=f"Week {week_num} {subject_name} {day_label}: differentiation.extension is too "
                            f"brief ('{extension}'). Students who meet mastery early need real challenge work.",
                recommendation="Write a specific extension task that adds analytical complexity, "
                               "a synthesis challenge, or a creative application beyond the day's objective.",
                week=week_num, day=day_label,
                affected_scope="day", affected_key=f"W{week_num}-{subject_name}-{day_label}",
            ))


def _validate_unit_mastery_arc(
    plan: dict[str, Any],
    total_weeks: int,
    issues: list[dict[str, Any]],
) -> None:
    """R11: Unit mastery arc structural completeness (package level)."""
    arc = plan.get("unit_mastery_arc") or {}
    if not (arc.get("terminal_mastery") or "").strip():
        issues.append(_issue(
            severity="severe",
            category="backward_design",
            issue_kind="generation",
            improvement_source="Architecture",
            description="unit_mastery_arc.terminal_mastery is empty. The backward design spine has no "
                        "terminal target — every week's mastery gate and every artifact is unanchored.",
            recommendation="Regenerate the full instructional design plan. terminal_mastery must define "
                           "what mastery looks like at the end of the package.",
            week=None, day=None,
            affected_scope="package", affected_key="package",
        ))

    gates = arc.get("mastery_gates") or []
    if len(gates) < max(total_weeks - 1, 1):
        issues.append(_issue(
            severity="severe",
            category="backward_design",
            issue_kind="generation",
            improvement_source="Architecture",
            description=f"unit_mastery_arc has only {len(gates)} mastery gate(s) for a {total_weeks}-week package. "
                        "Every week transition must have a gate.",
            recommendation="The plan needs at least one mastery gate per week transition. "
                           "Each gate defines what students must demonstrate to stay on the arc.",
            week=None, day=None,
            affected_scope="package", affected_key="package",
        ))


def _deterministic_validation_for_subject(
    plan: dict[str, Any],
    week_num: int,
    subject_name: str,
    total_weeks: int,
) -> list[dict[str, Any]]:
    """Run all deterministic rules for one week × subject. Returns list of issues."""
    week_subj = get_plan_for_week_subject(plan, week_num, subject_name)
    if not week_subj:
        return [_issue(
            severity="critical",
            category="district_fidelity",
            issue_kind="generation",
            improvement_source="Architecture",
            description=f"Week {week_num} {subject_name}: No entry found in the instructional design plan. "
                        "Artifact generation for this week+subject has no instructional basis.",
            recommendation="Regenerate the instructional design plan. The plan must have an entry for "
                           "every week × subject combination.",
            week=week_num, day=None,
            affected_scope="subject", affected_key=f"W{week_num}-{subject_name}",
        )]

    district_anchors = week_subj.get("district_anchors") or {}
    instructional_design = week_subj.get("instructional_design") or {}
    issues: list[dict[str, Any]] = []

    _validate_district_fidelity(week_num, subject_name, district_anchors, instructional_design, issues)
    _validate_structural_completeness(week_num, subject_name, instructional_design, total_weeks, issues)
    _validate_assessment_alignment(week_num, subject_name, district_anchors, instructional_design, issues)
    _validate_cognitive_load(week_num, subject_name, instructional_design, issues)
    _validate_differentiation(week_num, subject_name, instructional_design, issues)

    return issues


def _deterministic_validation(
    plan: dict[str, Any],
    generation_context: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Run deterministic validation for all weeks × subjects.

    Returns dict keyed by subject_name → list of issues.
    Also runs package-level arc checks.
    """
    subjects = [s.get("subject_name") or "" for s in (generation_context.get("subjects") or []) if s.get("subject_name")]
    total_weeks = len(generation_context.get("weeks") or [])
    weeks = generation_context.get("weeks") or []

    result: dict[str, list[dict[str, Any]]] = {s: [] for s in subjects}

    # Package-level arc check — attach issues to the first subject (or a special key)
    arc_issues: list[dict[str, Any]] = []
    _validate_unit_mastery_arc(plan, total_weeks, arc_issues)
    if arc_issues and subjects:
        result[subjects[0]].extend(arc_issues)

    for week in weeks:
        week_num = int(week.get("sequence_number") or 0)
        for subject_name in subjects:
            subj_issues = _deterministic_validation_for_subject(plan, week_num, subject_name, total_weeks)
            result[subject_name].extend(subj_issues)

    return result


# ── Tier 2: AI pedagogical review ────────────────────────────────────────────────────────

_AI_REVIEW_SCHEMA: dict[str, Any] = {
    "subject": "string",
    "overall_subject_score": 8.5,
    "categories": {
        "learning_progression": {
            "score": 8.5,
            "status": "pass",
            "rationale": "string",
            "score_evidence": "string",
            "recommendation": "string or null",
        },
        "backward_design": {
            "score": 9.0,
            "status": "pass",
            "rationale": "string",
            "score_evidence": "string",
            "recommendation": "string or null",
        },
        "cognitive_load": {
            "score": 8.0,
            "status": "pass",
            "rationale": "string",
            "score_evidence": "string",
            "recommendation": "string or null",
        },
        "mastery_progression": {
            "score": 8.5,
            "status": "pass",
            "rationale": "string",
            "score_evidence": "string",
            "recommendation": "string or null",
        },
        "reading_writing_cohesion": {
            "score": 9.0,
            "status": "pass",
            "rationale": "string",
            "score_evidence": "string",
            "recommendation": "string or null",
        },
        "student_engagement": {
            "score": 8.0,
            "status": "pass",
            "rationale": "string",
            "score_evidence": "string",
            "recommendation": "string or null",
        },
        "instructional_continuity": {
            "score": 9.0,
            "status": "pass",
            "rationale": "string",
            "score_evidence": "string",
            "recommendation": "string or null",
        },
    },
    "educational_issues": [
        {
            "severity": "string",
            "category": "string",
            "improvement_source": "string",
            "week": 1,
            "day": "string or null",
            "description": "string",
            "recommendation": "string",
            "affected_scope": "string",
            "affected_key": "string",
        }
    ],
    "generation_issues": [
        {
            "severity": "string",
            "category": "string",
            "improvement_source": "string",
            "week": 1,
            "day": "string or null",
            "description": "string",
            "recommendation": "string",
            "affected_scope": "string",
            "affected_key": "string",
        }
    ],
    "revision_needed": False,
    "suggested_revision_scope": "none",
}

_AI_REVIEW_INSTRUCTION = (
    "You are an expert instructional coach reviewing — not generating — an instructional design plan. "
    "Your task is to evaluate whether the plan can realistically lead an average student to master "
    "every district objective by the end of the allotted duration.\n\n"

    "YOU ARE A REVIEWER, NOT A GENERATOR. Do not rewrite lesson content. Evaluate what exists.\n\n"

    "REVIEW CONTEXT:\n"
    "  district_anchors: district-defined objectives, daily topics, and assessment moments (read-only facts)\n"
    "  unit_mastery_arc: terminal mastery target + per-week mastery gates\n"
    "  knowledge_dependency_graph: per-objective dependency analysis\n"
    "  instructional_design_for_subject: the full week-by-week plan for this subject\n"
    "  already_identified_issues: issues already caught by deterministic rules — do NOT re-report these\n\n"

    "EVALUATE THESE 7 CATEGORIES:\n\n"

    "1. learning_progression (weight 20%)\n"
    "   Do prerequisite concepts precede dependent concepts? Are the KDG 'assumed_mastered' dependencies\n"
    "   activated early in the week? Is complexity increasing day-by-day or is the hardest content on Monday?\n"
    "   Does the instruction for each objective appear before students are assessed on it?\n\n"

    "2. backward_design (weight 15%)\n"
    "   Does each day's observable_mastery_evidence genuinely contribute to the week's end_of_week_mastery?\n"
    "   Does the week's mastery gate connect to unit_mastery_arc.terminal_mastery?\n"
    "   Is there a broken link anywhere in: daily evidence → week mastery → unit mastery?\n\n"

    "3. cognitive_load (weight 7%)\n"
    "   Are any days overloaded with unrelated concepts, vocabulary spikes, or simultaneous heavy demands?\n"
    "   The rule-based tier already flagged obvious patterns — evaluate subtle overload here.\n"
    "   Is the cognitive complexity appropriate for the grade level?\n\n"

    "4. mastery_progression (weight 10%)\n"
    "   Can an average student realistically demonstrate today's observable_mastery_evidence given the\n"
    "   instruction provided (teacher_modeling + guided_practice)? Is the instruction sufficient preparation\n"
    "   for the independent_practice task and exit ticket?\n\n"

    "5. reading_writing_cohesion (weight 1% — ELA only, pro-rate for other subjects)\n"
    "   Does writing reinforce reading? Do mentor texts support writing instruction? Are writing prompts\n"
    "   disconnected from reading instruction? Does the reading-writing connection build across the week?\n\n"

    "6. student_engagement (weight 5%)\n"
    "   Is there appropriate variation in instructional modes across the full week?\n"
    "   Are there collaborative structures on most days? Is the discussion_prompt varied day-to-day?\n"
    "   Is the overall weekly arc engaging for students at this grade level?\n\n"

    "7. instructional_continuity (weight 5%)\n"
    "   Does each day's builds_from_yesterday accurately reference the previous day's actual content?\n"
    "   Is any content repeated unnecessarily? Is review excessive relative to new instruction?\n"
    "   Does the week feel like one coherent journey or five disconnected lessons?\n\n"

    "SCORING:\n"
    "  10 = exemplary (no issues), 9 = excellent (minor observations), 8 = good (1 warning-level issue),\n"
    "  7 = adequate (1 moderate issue), 5-6 = needs work (multiple moderate issues or 1 severe),\n"
    "  3-4 = significant problems, 1-2 = fundamental failure\n\n"

    "STATUS:\n"
    "  pass = score ≥ 8, warning = score 6-7, fail = score ≤ 5\n\n"

    "FOR EVERY CATEGORY — required fields:\n"
    "  score_evidence: Quote or paraphrase specific content from the plan that justifies your score.\n"
    "    Do not write 'the plan covers prerequisites' — name the specific day, field, and content.\n"
    "  recommendation: null if score ≥ 9. Otherwise: one specific, actionable improvement.\n\n"

    "FOR EACH ISSUE:\n"
    "  issue_kind: 'educational' (pedagogically unsound) or 'generation' (AI artifact: empty, placeholder, repeated)\n"
    "  improvement_source: 'Prompt' (AI prompt fix), 'Rule' (deterministic rule fix),\n"
    "    'Architecture' (plan schema/pipeline change), or 'Teacher Input' (only teacher can resolve)\n"
    "  severity: 'critical' (blocks learning), 'severe' (breaks arc), 'moderate' (degrades quality), 'warning'\n"
    "  affected_scope: 'package', 'subject', 'week', or 'day'\n"
    "  affected_key: use format 'W{week}-{Subject}' for week, 'W{week}-{Subject}-{Day}' for day\n\n"

    "revision_needed: true if any educational_issue or generation_issue has severity critical/severe/moderate\n"
    "suggested_revision_scope: 'none' | 'day' | 'week' | 'subject' | 'package' — minimum scope to fix all issues\n\n"

    "IMPORTANT: Do not report issues already in already_identified_issues. Focus on what rules cannot catch."
)


def _ai_review_for_subject(
    db: Session,
    *,
    effective_settings: Settings,
    model_name: str,
    provider_name: str,
    api_key: str | None,
    base_url: str | None,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    user: User,
    plan: dict[str, Any],
    subject_name: str,
    generation_context: dict[str, Any],
    deterministic_issues: list[dict[str, Any]],
) -> dict[str, Any]:
    """One AI call to review the plan for a single subject."""
    weeks_data = []
    for week in (plan.get("weeks") or []):
        week_num = int(week.get("week") or 0)
        ws = get_plan_for_week_subject(plan, week_num, subject_name)
        if ws:
            weeks_data.append({
                "week": week_num,
                "district_anchors": ws.get("district_anchors"),
                "instructional_design": ws.get("instructional_design"),
            })

    prompt_payload = {
        "subject_name": subject_name,
        "grade_code": generation_context.get("grade_code"),
        "grade_display_name": generation_context.get("grade_display_name"),
        "total_weeks": len(weeks_data),
        "unit_mastery_arc": plan.get("unit_mastery_arc"),
        "knowledge_dependency_graph": [
            entry for entry in (plan.get("knowledge_dependency_graph") or [])
        ],
        "instructional_design_for_subject": weeks_data,
        "already_identified_issues": [
            {"severity": i["severity"], "category": i["category"], "description": i["description"]}
            for i in deterministic_issues
        ],
    }

    result = execute_openai_json_completion(
        effective_settings,
        model_name=model_name,
        instruction=_AI_REVIEW_INSTRUCTION,
        prompt_payload=prompt_payload,
        required_output_schema=_AI_REVIEW_SCHEMA,
        _api_key=api_key,
        _base_url=base_url,
        _provider=provider_name,
    )
    record_teacher_assist_ai_usage(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        feature=V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
        provider=result.provider,
        model=result.model,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        estimated_cost_cents=result.estimated_cost_cents,
        metadata={
            "operation_type": "instructional_validation",
            "subject_name": subject_name,
            "package_id": str(package_id),
            "related_entity_type": "instructional_package",
            "related_entity_id": str(package_id),
        },
    )
    return result.content_json


# ── Revision ─────────────────────────────────────────────────────────────────────────────

_SEVERITY_RANK = {"critical": 4, "severe": 3, "moderate": 2, "warning": 1, "info": 0}


def _needs_revision(issues: list[dict[str, Any]]) -> bool:
    return any(
        _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["moderate"]
        for i in issues
    )


def _classify_revision_scope(issues: list[dict[str, Any]]) -> tuple[str, list[int], list[tuple[int, str]]]:
    """Return (scope, affected_weeks, affected_day_keys).

    scope: "none" | "day" | "week" | "subject"
    """
    actionable = [
        i for i in issues
        if _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["moderate"]
    ]
    if not actionable:
        return "none", [], []

    has_package_scope = any(i.get("affected_scope") == "package" for i in actionable)
    has_subject_scope = any(i.get("affected_scope") == "subject" for i in actionable)
    critical_or_severe = [
        i for i in actionable
        if _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["severe"]
    ]

    if has_package_scope or has_subject_scope or critical_or_severe:
        affected_weeks = sorted({i["week"] for i in actionable if i.get("week")})
        return "subject", affected_weeks, []

    affected_weeks = sorted({i["week"] for i in actionable if i.get("week")})
    affected_day_pairs = [(i["week"], i["day"]) for i in actionable if i.get("week") and i.get("day")]

    if len(affected_weeks) > 2:
        return "subject", affected_weeks, []
    if len(affected_weeks) == 1 and len(set(affected_day_pairs)) <= 1 and affected_day_pairs:
        return "day", affected_weeks, affected_day_pairs
    return "week", affected_weeks, affected_day_pairs


_REVISION_DAY_SCHEMA: dict[str, Any] = {
    "day": "string",
    "instructional_purpose": "string",
    "student_goal": "string",
    "teacher_goal": "string",
    "builds_from_yesterday": "string or null",
    "prepares_for_tomorrow": "string or null",
    "teacher_modeling": "string",
    "guided_practice": "string",
    "independent_practice": "string",
    "discussion_prompt": "string",
    "formative_assessment": "string",
    "exit_ticket": "string",
    "observable_mastery_evidence": "string",
    "differentiation": {"scaffold": "string", "extension": "string"},
    "reteach_if_needed": "string",
}

_REVISION_WEEK_SCHEMA: dict[str, Any] = {
    "end_of_week_mastery": "string",
    "learning_journey_rationale": "string",
    "instructional_contracts": {
        "exit_ticket_stem": "string",
        "quiz_objectives": ["string"],
        "rubric_primary_criterion": "string",
        "core_activity_name": "string",
    },
    "daily_progression": [_REVISION_DAY_SCHEMA],
}


def _build_revision_instruction(
    scope: str,
    issues: list[dict[str, Any]],
    subject_name: str,
    week_num: int | None,
    day_label: str | None,
) -> str:
    issue_bullets = "\n".join(
        f"  - [{i.get('severity', '?').upper()}] {i.get('description', '')}\n"
        f"    Fix: {i.get('recommendation', '')}"
        for i in issues
        if _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["moderate"]
    )
    scope_desc = {
        "day": f"the {day_label} lesson entry for {subject_name} Week {week_num}",
        "week": f"the full instructional_design for {subject_name} Week {week_num}",
        "subject": f"all week entries for {subject_name}",
    }.get(scope, scope)

    return (
        f"You are revising {scope_desc} of an instructional design plan to fix specific validation issues.\n\n"
        "RULES:\n"
        "1. Address ONLY the issues listed below. Do not change content not mentioned.\n"
        "2. district_anchors are READ ONLY. Never modify objective codes, daily topics, or assessment checks.\n"
        "3. Maintain backward design: every day must build toward end_of_week_mastery.\n"
        "4. Preserve instructional_contracts — they are used verbatim by artifact generators.\n"
        "5. Do not change instructional_purpose, teacher_goal, or formative_assessment to student-facing content.\n\n"
        "ISSUES TO FIX:\n"
        f"{issue_bullets}\n\n"
        "Return only the revised content in the required JSON schema."
    )


def _revise_subject_plan(
    db: Session,
    *,
    effective_settings: Settings,
    model_name: str,
    provider_name: str,
    api_key: str | None,
    base_url: str | None,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    user: User,
    plan: dict[str, Any],
    subject_name: str,
    scope: str,
    affected_weeks: list[int],
    affected_day_keys: list[tuple[int, str]],
    issues: list[dict[str, Any]],
    generation_context: dict[str, Any],
) -> dict[str, Any]:
    """Call the AI to revise a scoped slice of the plan. Returns a dict of replacements.

    Return structure:
      {"weeks": {week_num: {"day": {day_label: day_entry}} | {"week": instructional_design}}}
    """
    actionable = [
        i for i in issues
        if _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["moderate"]
    ]
    if not actionable:
        return {}

    if scope == "day" and affected_day_keys:
        week_num, day_label = affected_day_keys[0]
        ws = get_plan_for_week_subject(plan, week_num, subject_name)
        if not ws:
            return {}

        instruction = _build_revision_instruction("day", actionable, subject_name, week_num, day_label)
        existing_day = get_plan_for_day(plan, week_num, subject_name, day_label) or {}
        prompt_payload = {
            "subject_name": subject_name,
            "week_num": week_num,
            "day_label": day_label,
            "district_anchors": ws.get("district_anchors"),
            "instructional_contracts": (ws.get("instructional_design") or {}).get("instructional_contracts"),
            "end_of_week_mastery": (ws.get("instructional_design") or {}).get("end_of_week_mastery"),
            "unit_mastery_arc": plan.get("unit_mastery_arc"),
            "existing_day_entry": existing_day,
        }
        result = execute_openai_json_completion(
            effective_settings,
            model_name=model_name,
            instruction=instruction,
            prompt_payload=prompt_payload,
            required_output_schema=_REVISION_DAY_SCHEMA,
            _api_key=api_key,
            _base_url=base_url,
            _provider=provider_name,
        )
        record_teacher_assist_ai_usage(
            db, tenant_id=tenant_id, user_id=user.id,
            feature=V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
            provider=result.provider, model=result.model,
            input_tokens=result.input_tokens, output_tokens=result.output_tokens,
            estimated_cost_cents=result.estimated_cost_cents,
            metadata={"operation_type": "instructional_validation_revision", "scope": "day",
                      "package_id": str(package_id)},
        )
        return {"scope": "day", "week_num": week_num, "day_label": day_label, "content": result.content_json}

    elif scope in ("week", "subject"):
        target_weeks = affected_weeks if scope == "week" else [
            int(w.get("week") or 0) for w in (plan.get("weeks") or [])
        ]
        revisions: list[dict[str, Any]] = []
        for week_num in target_weeks:
            ws = get_plan_for_week_subject(plan, week_num, subject_name)
            if not ws:
                continue
            week_issues = [i for i in actionable if i.get("week") == week_num or i.get("week") is None]
            if not week_issues:
                continue
            instruction = _build_revision_instruction("week", week_issues, subject_name, week_num, None)
            prompt_payload = {
                "subject_name": subject_name,
                "week_num": week_num,
                "district_anchors": ws.get("district_anchors"),
                "existing_instructional_design": ws.get("instructional_design"),
                "unit_mastery_arc": plan.get("unit_mastery_arc"),
                "knowledge_dependency_graph": plan.get("knowledge_dependency_graph"),
            }
            result = execute_openai_json_completion(
                effective_settings,
                model_name=model_name,
                instruction=instruction,
                prompt_payload=prompt_payload,
                required_output_schema=_REVISION_WEEK_SCHEMA,
                _api_key=api_key,
                _base_url=base_url,
                _provider=provider_name,
            )
            record_teacher_assist_ai_usage(
                db, tenant_id=tenant_id, user_id=user.id,
                feature=V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
                provider=result.provider, model=result.model,
                input_tokens=result.input_tokens, output_tokens=result.output_tokens,
                estimated_cost_cents=result.estimated_cost_cents,
                metadata={"operation_type": "instructional_validation_revision", "scope": "week",
                          "package_id": str(package_id), "week_num": week_num},
            )
            revisions.append({"week_num": week_num, "content": result.content_json})
        return {"scope": "week_or_subject", "revisions": revisions}

    return {}


def _apply_plan_revision(
    plan: dict[str, Any],
    subject_name: str,
    revision: dict[str, Any],
    issues: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Merge a revision result into the plan. Returns (updated_plan, updated_issues)."""
    if not revision:
        return plan, issues

    scope = revision.get("scope")
    if scope == "day":
        week_num = revision["week_num"]
        day_label = revision["day_label"]
        new_day = revision["content"]
        for week_entry in plan.get("weeks") or []:
            if int(week_entry.get("week") or 0) != week_num:
                continue
            for subj_entry in week_entry.get("subjects") or []:
                if (subj_entry.get("subject") or "").lower() != subject_name.lower():
                    continue
                progression = (subj_entry.get("instructional_design") or {}).get("daily_progression") or []
                for i, day in enumerate(progression):
                    if (day.get("day") or "").lower() == day_label.lower():
                        progression[i] = new_day
                        break
        revised_issues = [
            {**i, "auto_revised": True, "revision_outcome": "revised"}
            if i.get("day") and i["day"].lower() == day_label.lower() and i.get("week") == week_num
            else i
            for i in issues
        ]
        return plan, revised_issues

    elif scope == "week_or_subject":
        for rev in (revision.get("revisions") or []):
            week_num = rev["week_num"]
            new_design = rev["content"]
            for week_entry in plan.get("weeks") or []:
                if int(week_entry.get("week") or 0) != week_num:
                    continue
                for subj_entry in week_entry.get("subjects") or []:
                    if (subj_entry.get("subject") or "").lower() != subject_name.lower():
                        continue
                    # Preserve district_anchors — never touched by revision
                    subj_entry["instructional_design"] = new_design
        revised_issues = [
            {**i, "auto_revised": True, "revision_outcome": "revised"}
            for i in issues
        ]
        return plan, revised_issues

    return plan, issues


# ── Score computation and report assembly ─────────────────────────────────────────────────

def _compute_subject_score(
    deterministic_issues: list[dict[str, Any]],
    ai_review: dict[str, Any] | None,
) -> float:
    """Compute a weighted category score for one subject."""
    if ai_review:
        base_score = float(ai_review.get("overall_subject_score") or 8.0)

        # Apply deterministic penalties on top of the AI score
        severity_penalties = {"critical": 3.0, "severe": 1.5, "moderate": 0.5}
        penalty = sum(
            severity_penalties.get(i.get("severity") or "info", 0.0)
            for i in deterministic_issues
        )
        return max(0.0, min(10.0, base_score - penalty))

    # No AI review — score from deterministic issues alone
    severity_deductions = {"critical": 4.0, "severe": 2.5, "moderate": 1.0, "warning": 0.3}
    score = 10.0
    for issue in deterministic_issues:
        score -= severity_deductions.get(issue.get("severity") or "info", 0.0)
    return max(0.0, min(10.0, score))


def _compute_confidence_label(
    overall_score: float,
    all_issues: list[dict[str, Any]],
) -> tuple[str, str]:
    """Return (confidence_label, confidence_description)."""
    has_critical = any(
        _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["critical"]
        for i in all_issues
    )
    has_severe = any(
        _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["severe"]
        for i in all_issues
    )
    district_fidelity_fail = any(
        i.get("category") == "district_fidelity"
        and _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["severe"]
        for i in all_issues
    )

    if overall_score >= _CONFIDENCE_THRESHOLDS["Excellent"] and not has_severe and not district_fidelity_fail:
        return (
            "Excellent",
            "The instructional plan demonstrates strong backward design with coherent daily progression "
            "toward unit mastery. All district objectives are covered and the learning sequence is "
            "appropriate for the grade level.",
        )
    if overall_score >= _CONFIDENCE_THRESHOLDS["Very Good"] and not has_critical and not district_fidelity_fail:
        return (
            "Very Good",
            "The instructional plan is well-structured with minor areas for improvement. District "
            "objectives are covered and the learning sequence is sound.",
        )
    return (
        "Needs Review",
        "The instructional plan has issues that may affect student learning. TeacherAssist has "
        "attempted automatic revision where possible. Review the plan before using it for instruction.",
    )


def _build_category_entry(
    category_name: str,
    weight: float,
    ai_data: dict[str, Any] | None,
    issues_for_category: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a full category entry with score, evidence, and recommendation."""
    if ai_data:
        score = float(ai_data.get("score") or 8.0)
        status = ai_data.get("status") or ("pass" if score >= 8.0 else "warning" if score >= 6.0 else "fail")
        score_evidence = ai_data.get("score_evidence") or ai_data.get("rationale") or ""
        recommendation = ai_data.get("recommendation")
    else:
        severity_deductions = {"critical": 4.0, "severe": 2.5, "moderate": 1.0, "warning": 0.3}
        score = 10.0 - sum(severity_deductions.get(i.get("severity") or "info", 0.0) for i in issues_for_category)
        score = max(0.0, min(10.0, score))
        status = "pass" if score >= 8.0 else "warning" if score >= 6.0 else "fail"
        score_evidence = f"Deterministic check: {len(issues_for_category)} issue(s) found." if issues_for_category else "No issues detected by rule-based checks."
        recommendation = issues_for_category[0].get("recommendation") if issues_for_category else None

    return {
        "score": round(score, 1),
        "status": status,
        "weight": weight,
        "score_evidence": score_evidence,
        "recommendation": recommendation,
        "issues": [i for i in issues_for_category],
    }


def _build_subject_entry(
    subject_name: str,
    deterministic_issues: list[dict[str, Any]],
    ai_review: dict[str, Any] | None,
    revision_history: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build the per-subject block of the validation report."""
    all_issues = list(deterministic_issues)
    educational_issues: list[dict[str, Any]] = []
    generation_issues: list[dict[str, Any]] = []

    if ai_review:
        for ai_issue in (ai_review.get("educational_issues") or []):
            normalized = {
                "severity": ai_issue.get("severity", "warning"),
                "category": ai_issue.get("category", "learning_progression"),
                "issue_kind": "educational",
                "improvement_source": ai_issue.get("improvement_source", "Prompt"),
                "week": ai_issue.get("week"),
                "day": ai_issue.get("day"),
                "description": ai_issue.get("description", ""),
                "recommendation": ai_issue.get("recommendation", ""),
                "affected_scope": ai_issue.get("affected_scope", "week"),
                "affected_key": ai_issue.get("affected_key", ""),
                "auto_revised": False,
                "revision_outcome": None,
            }
            all_issues.append(normalized)
        for ai_issue in (ai_review.get("generation_issues") or []):
            normalized = {
                "severity": ai_issue.get("severity", "warning"),
                "category": ai_issue.get("category", "district_fidelity"),
                "issue_kind": "generation",
                "improvement_source": ai_issue.get("improvement_source", "Prompt"),
                "week": ai_issue.get("week"),
                "day": ai_issue.get("day"),
                "description": ai_issue.get("description", ""),
                "recommendation": ai_issue.get("recommendation", ""),
                "affected_scope": ai_issue.get("affected_scope", "week"),
                "affected_key": ai_issue.get("affected_key", ""),
                "auto_revised": False,
                "revision_outcome": None,
            }
            all_issues.append(normalized)

    for issue in all_issues:
        kind = issue.get("issue_kind") or "educational"
        if kind == "generation":
            generation_issues.append(issue)
        else:
            educational_issues.append(issue)

    subject_score = _compute_subject_score(deterministic_issues, ai_review)

    ai_cats = (ai_review.get("categories") or {}) if ai_review else {}
    categories: dict[str, Any] = {}
    for cat_name, weight in _CATEGORY_WEIGHTS.items():
        cat_issues = [i for i in all_issues if i.get("category") == cat_name]
        ai_cat_data = ai_cats.get(cat_name)
        categories[cat_name] = _build_category_entry(cat_name, weight, ai_cat_data, cat_issues)

    validation_method = "deterministic+ai" if ai_review else "deterministic"

    return {
        "subject": subject_name,
        "subject_score": round(subject_score, 1),
        "validation_method": validation_method,
        "categories": categories,
        "educational_issues": educational_issues,
        "generation_issues": generation_issues,
        "issues": all_issues,
        "revision_history": revision_history,
    }


def _build_validation_report(
    subject_entries: list[dict[str, Any]],
    plan_meta: dict[str, Any],
    model: str,
    revision_rounds_total: int,
    validation_method: str,
) -> dict[str, Any]:
    """Assemble the final validation report."""
    all_issues = [i for entry in subject_entries for i in (entry.get("issues") or [])]
    if subject_entries:
        overall_score = round(
            sum(e["subject_score"] for e in subject_entries) / len(subject_entries), 1
        )
    else:
        overall_score = 10.0

    confidence_label, confidence_description = _compute_confidence_label(overall_score, all_issues)

    teacher_summary_map = {
        "Excellent": "Your instructional plan is ready. TeacherAssist has reviewed it for educational quality and district alignment.",
        "Very Good": "Your instructional plan is ready with minor notes. TeacherAssist has reviewed it for educational quality.",
        "Needs Review": "TeacherAssist reviewed your instructional plan and found areas that may benefit from revision before use.",
    }

    return {
        "report_meta": {
            "schema_version": _REPORT_SCHEMA_VERSION,
            "generated_at": _now().isoformat(),
            "model": model,
            "prompt_version": _VALIDATION_PROMPT_VERSION,
            "total_subjects_validated": len(subject_entries),
            "validation_method": validation_method,
            "revision_rounds_total": revision_rounds_total,
        },
        "overall_score": overall_score,
        "confidence_label": confidence_label,
        "confidence_description": confidence_description,
        "teacher_summary": teacher_summary_map.get(confidence_label, teacher_summary_map["Needs Review"]),
        "subjects": subject_entries,
    }


# ── Main entry point ──────────────────────────────────────────────────────────────────────

def validate_and_revise_instructional_plan(
    db: Session,
    *,
    settings: Settings,
    user: User,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    plan: dict[str, Any],
    generation_context: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate the instructional design plan and revise where possible.

    Validation is the explicit owner of the plan from this call until it returns.
    No other process touches the plan during validation.

    Returns (final_plan, validation_report).

    The plan returned may differ from the plan received if revision improved it.
    On any exception, returns (original_plan, partial_report) — never raises.
    Never blocks artifact generation.
    """
    if not plan:
        empty_report = _build_validation_report(
            [], plan.get("plan_meta") or {}, "n/a", 0, "skipped"
        )
        return plan, empty_report

    try:
        from oziebot_api.services.teacher_assist.ai_mode import is_teacher_assist_real_ai_active
        run_ai_review = is_teacher_assist_real_ai_active(db, settings)
    except Exception:
        run_ai_review = False

    try:
        effective_settings = resolve_teacher_assist_settings(db, settings)
        provider_name, api_key, base_url = _provider_api_params(effective_settings)
        model_name = get_teacher_assist_provider_model(effective_settings, provider_name=provider_name)
    except Exception:
        logger.exception("package=%s: validation cannot resolve provider settings — skipping AI review", package_id)
        run_ai_review = False
        model_name = "unknown"

    subjects = [
        s.get("subject_name") or ""
        for s in (generation_context.get("subjects") or [])
        if s.get("subject_name")
    ]
    plan_meta = plan.get("plan_meta") or {}
    current_plan = plan
    subject_entries: list[dict[str, Any]] = []
    total_revision_rounds = 0
    validation_method = "deterministic"

    # ── Tier 1: Deterministic validation ──────────────────────────────────────────────
    try:
        det_issues_by_subject = _deterministic_validation(current_plan, generation_context)
    except Exception:
        logger.exception("package=%s: deterministic validation failed — proceeding", package_id)
        det_issues_by_subject = {s: [] for s in subjects}

    for subject_name in subjects:
        det_issues = det_issues_by_subject.get(subject_name) or []

        # Short-circuit: critical district fidelity issues → go straight to revision
        has_critical = any(
            _SEVERITY_RANK.get(i.get("severity") or "info", 0) >= _SEVERITY_RANK["critical"]
            for i in det_issues
        )

        ai_review: dict[str, Any] | None = None
        revision_history: list[dict[str, Any]] = []

        # ── Tier 2: AI pedagogical review (unless critical — proceed to revision first) ──
        if run_ai_review and not has_critical:
            try:
                ai_review = _ai_review_for_subject(
                    db,
                    effective_settings=effective_settings,
                    model_name=model_name,
                    provider_name=provider_name,
                    api_key=api_key,
                    base_url=base_url,
                    tenant_id=tenant_id,
                    package_id=package_id,
                    user=user,
                    plan=current_plan,
                    subject_name=subject_name,
                    generation_context=generation_context,
                    deterministic_issues=det_issues,
                )
                validation_method = "deterministic+ai"
                logger.info(
                    "package=%s subject=%s: AI validation score=%.1f, revision_needed=%s",
                    package_id, subject_name,
                    float(ai_review.get("overall_subject_score") or 0),
                    ai_review.get("revision_needed"),
                )
            except Exception:
                logger.exception(
                    "package=%s subject=%s: AI validation failed — proceeding with deterministic only",
                    package_id, subject_name,
                )

        # ── Revision loop ──────────────────────────────────────────────────────────────
        all_current_issues = list(det_issues)
        if ai_review:
            for ai_issue in (ai_review.get("educational_issues") or []) + (ai_review.get("generation_issues") or []):
                all_current_issues.append({
                    "severity": ai_issue.get("severity", "warning"),
                    "category": ai_issue.get("category", "learning_progression"),
                    "issue_kind": "educational" if ai_issue in (ai_review.get("educational_issues") or []) else "generation",
                    "improvement_source": ai_issue.get("improvement_source", "Prompt"),
                    "week": ai_issue.get("week"),
                    "day": ai_issue.get("day"),
                    "description": ai_issue.get("description", ""),
                    "recommendation": ai_issue.get("recommendation", ""),
                    "affected_scope": ai_issue.get("affected_scope", "week"),
                    "affected_key": ai_issue.get("affected_key", ""),
                    "auto_revised": False,
                    "revision_outcome": None,
                })

        for revision_round in range(1, _MAX_REVISION_ROUNDS + 1):
            if not run_ai_review:
                break
            if not _needs_revision(all_current_issues):
                break

            scope, affected_weeks, affected_day_keys = _classify_revision_scope(all_current_issues)
            if scope == "none":
                break

            try:
                revision = _revise_subject_plan(
                    db,
                    effective_settings=effective_settings,
                    model_name=model_name,
                    provider_name=provider_name,
                    api_key=api_key,
                    base_url=base_url,
                    tenant_id=tenant_id,
                    package_id=package_id,
                    user=user,
                    plan=current_plan,
                    subject_name=subject_name,
                    scope=scope,
                    affected_weeks=affected_weeks,
                    affected_day_keys=affected_day_keys,
                    issues=all_current_issues,
                    generation_context=generation_context,
                )
                current_plan, all_current_issues = _apply_plan_revision(
                    current_plan, subject_name, revision, all_current_issues
                )
                total_revision_rounds += 1
                revision_history.append({
                    "round": revision_round,
                    "scope": scope,
                    "affected_weeks": affected_weeks,
                    "issues_addressed": len([i for i in all_current_issues if i.get("auto_revised")]),
                })
                logger.info(
                    "package=%s subject=%s: revision round %d complete (scope=%s)",
                    package_id, subject_name, revision_round, scope,
                )

                # Re-run deterministic validation on the revised plan
                try:
                    new_det = _deterministic_validation(current_plan, generation_context)
                    all_current_issues = [
                        i for i in all_current_issues if i.get("auto_revised")
                    ] + (new_det.get(subject_name) or [])
                except Exception:
                    logger.exception(
                        "package=%s subject=%s: post-revision deterministic check failed",
                        package_id, subject_name,
                    )
            except Exception:
                logger.exception(
                    "package=%s subject=%s: revision round %d failed — proceeding with current plan",
                    package_id, subject_name, revision_round,
                )
                break

        # Rebuild det_issues from all_current_issues (may have been revised)
        final_det_issues = [i for i in all_current_issues if i.get("issue_kind") != "educational" or not i.get("auto_revised")]
        subject_entry = _build_subject_entry(
            subject_name,
            deterministic_issues=final_det_issues,
            ai_review=ai_review,
            revision_history=revision_history,
        )
        subject_entries.append(subject_entry)

    report = _build_validation_report(
        subject_entries=subject_entries,
        plan_meta=plan_meta,
        model=model_name,
        revision_rounds_total=total_revision_rounds,
        validation_method=validation_method,
    )

    logger.info(
        "package=%s: validation complete — score=%.1f confidence=%s revision_rounds=%d",
        package_id,
        report["overall_score"],
        report["confidence_label"],
        total_revision_rounds,
    )

    return current_plan, report

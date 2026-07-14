"""Instructional Design Plan — backward-designed instructional sequence for TeacherAssist v2.

TeacherAssist operates by one principle:
  The district defines WHAT students learn.
  TeacherAssist determines the most effective instructional HOW
  while remaining faithful to district intent.

This module generates and provides lookup access to the Instructional Design Plan —
a backward-designed instructional sequence stored once per package that guides
every artifact generator. It is distinct from the curriculum sequence plan:

  curriculum_sequence_plan  → WHAT to teach each week and in what order (week-level themes,
                               TEKS distribution, mentor texts)
  instructional_design_plan → HOW to teach it — day-by-day, subject-by-subject, with a
                               coherent arc from Monday to Friday built backward from
                               end-of-week mastery targets

Architecture:
  Package → Week → Subject → Learning Journey → Daily Instruction

Each week+subject entry contains two structurally separated layers:
  district_anchors     — district-defined data extracted programmatically from the pacing
                         guide. Never written by the AI. Represents the WHAT.
  instructional_design — AI-generated pedagogical design. Represents the HOW. The AI writes
                         only within the boundaries set by district_anchors.

Package-level structures:
  plan_meta              — provenance metadata (model, prompt version, generated_at)
  unit_mastery_arc       — terminal mastery target + per-week mastery gates + cross-subject
                           connections. This is the backward design starting point.
  knowledge_dependency_graph — per-objective dependency analysis. Identifies what students
                               must already know before instruction can be effective, with
                               gap consequences and activation strategies.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select as _sa_select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_document_extraction import (
    TeacherAssistV2DocumentExtraction,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_mode import is_teacher_assist_real_ai_active
from oziebot_api.services.teacher_assist.ai_usage import (
    record_teacher_assist_ai_usage,
)
from oziebot_api.services.teacher_assist.openai_json_client import execute_openai_json_completion
from oziebot_api.services.teacher_assist.prompt_contracts import (
    V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
)
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_provider_model
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.instructional_delivery_profile import (
    build_planning_constraint,
    resolve_profile,
)
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import (
    material_labels_from_pacing,
    resolve_pacing_day_plan,
)
from oziebot_api.services.teacher_assist_v2.planning_constants import WEEKDAY_LABELS

logger = logging.getLogger(__name__)

_PLAN_SCHEMA_VERSION = "1.1"
_PLAN_PROMPT_VERSION = "instructional-design-v1.1"
_FULL_DOC_CHAR_LIMIT = 60_000

# Multi-subject, multi-week threshold above which we split into per-subject AI calls
# to keep output token count per call within quality bounds.
_SINGLE_CALL_MAX_SUBJECT_WEEK_PAIRS = 6


# ── Provider resolution (mirrors instructional_package_ai._provider_api_params) ──────────


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


# ── District anchors — programmatic extraction from pacing guide ──────────────────────────


def _build_district_anchors_for_week_subject(
    week_subject: dict[str, Any],
    *,
    excluded_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Extract district-defined data for one week × subject from the pacing guide.

    This data is never written by the AI. It represents the district's WHAT:
    objectives, pacing topics, assessment moments, and approved materials.
    """
    pacing_context = (week_subject or {}).get("pacing_context") or {}
    objectives = (week_subject or {}).get("objectives") or []

    primary_objectives = []
    supporting_objectives = []
    for obj in objectives:
        code = obj.get("objective_code") or ""
        description = obj.get("description") or ""
        if not code and not description:
            continue
        entry = {
            "teks_code": code,
            "description": description,
            "education_objective_id": str(obj["education_objective_id"])
            if obj.get("education_objective_id")
            else None,
        }
        if obj.get("is_required") or obj.get("is_primary"):
            primary_objectives.append(entry)
        else:
            supporting_objectives.append(entry)

    daily_topics: dict[str, str] = {}
    assessment_checks: dict[str, str] = {}
    for day_label in WEEKDAY_LABELS:
        day_plan = resolve_pacing_day_plan(week_subject, day_label)
        if not day_plan:
            continue
        topic = day_plan.get("daily_topic")
        check = day_plan.get("assessment_check")
        if topic:
            daily_topics[day_label] = str(topic)
        if check:
            assessment_checks[day_label] = str(check)

    pacing_materials = material_labels_from_pacing(
        pacing_context,
        include_week_level=True,
        excluded_material_ids=excluded_ids,
    )

    return {
        "primary_objectives": primary_objectives,
        "supporting_objectives": supporting_objectives,
        "daily_topics": daily_topics,
        "assessment_checks": assessment_checks,
        "pacing_materials": pacing_materials,
        "week_description": pacing_context.get("week_description") or "",
    }


def _build_all_district_anchors(
    generation_context: dict[str, Any],
    *,
    excluded_ids: set[str] | None = None,
) -> dict[tuple[int, str], dict[str, Any]]:
    """Build district_anchors for every week × subject in the package.

    Returns a dict keyed by (week_num, subject_name_lower) → anchors dict.
    """
    anchors: dict[tuple[int, str], dict[str, Any]] = {}
    for week in generation_context.get("weeks") or []:
        week_num = int(week.get("sequence_number") or 0)
        week_subjects_list = week.get("subjects") or []
        subject_lookup = {
            row["subject_id"]: row for row in generation_context.get("subjects") or []
        }
        for ws in week_subjects_list:
            subj_id = ws.get("subject_id") or ""
            subj_meta = subject_lookup.get(subj_id) or {}
            subj_name = (ws.get("subject_name") or subj_meta.get("subject_name") or "").strip()
            if not subj_name:
                continue
            key = (week_num, subj_name.lower())
            anchors[key] = _build_district_anchors_for_week_subject(ws, excluded_ids=excluded_ids)
    return anchors


# ── Full curriculum document loading ──────────────────────────────────────────────────────


def _load_full_curriculum_docs(
    db: Session,
    generation_context: dict[str, Any],
) -> list[dict[str, Any]]:
    """Load full extracted text of every curriculum file for this package.

    Bypasses the excerpt limits used for artifact prompts so the instructional
    design AI can read the complete Essential Unit of Study and derive the full
    instructional arc.
    """
    pacing_mats = generation_context.get("pacing_materials") or []
    file_mat_ids = [
        uuid.UUID(str(m["id"]))
        for m in pacing_mats
        if m.get("material_kind") == "file" and m.get("id")
    ]
    if not file_mat_ids:
        return []

    ext_rows = db.scalars(
        _sa_select(TeacherAssistV2DocumentExtraction).where(
            TeacherAssistV2DocumentExtraction.supporting_material_id.in_(file_mat_ids)
        )
    ).all()
    ext_by_mat = {str(row.supporting_material_id): row for row in ext_rows}

    docs = []
    for mat in pacing_mats:
        if mat.get("material_kind") != "file":
            continue
        mid = str(mat.get("id", ""))
        row = ext_by_mat.get(mid)
        if row is None:
            continue
        text = (row.teacher_edited_text or row.extracted_text or "").strip()
        if text:
            docs.append(
                {
                    "title": mat.get("title"),
                    "filename": mat.get("original_filename"),
                    "resource_type": mat.get("resource_type"),
                    "text": text[:_FULL_DOC_CHAR_LIMIT],
                    "truncated": len(text) > _FULL_DOC_CHAR_LIMIT,
                }
            )
    return docs


# ── Plan schema ───────────────────────────────────────────────────────────────────────────

_PLAN_OUTPUT_SCHEMA: dict[str, Any] = {
    "unit_mastery_arc": {
        "terminal_mastery": "string",
        "mastery_gates": [
            {
                "after_week": 1,
                "gate": "string",
                "observable_evidence": "string",
            }
        ],
        "cross_subject_connections": [
            {
                "primary_subject": "string",
                "connected_subject": "string",
                "connection": "string",
                "shared_days": ["string"],
            }
        ],
    },
    "knowledge_dependency_graph": [
        {
            "objective_code": "string",
            "objective_description": "string",
            "dependencies": [
                {
                    "knowledge_node": "string",
                    "description": "string",
                    "teks_anchor": "string or null",
                    "status": "string",
                    "gap_consequence": "string",
                    "activation_strategy": "string",
                }
            ],
        }
    ],
    "weeks": [
        {
            "week": 1,
            "subjects": [
                {
                    "subject": "string",
                    "instructional_design": {
                        "end_of_week_mastery": "string",
                        "learning_journey_rationale": "string",
                        "instructional_contracts": {
                            "exit_ticket_stem": "string",
                            "quiz_objectives": ["string"],
                            "rubric_primary_criterion": "string",
                            "core_activity_name": "string",
                        },
                        "daily_progression": [
                            {
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
                                "differentiation": {
                                    "scaffold": "string",
                                    "extension": "string",
                                },
                                "reteach_if_needed": "string",
                            }
                        ],
                    },
                }
            ],
        }
    ],
}


# ── AI instruction ─────────────────────────────────────────────────────────────────────────


def _build_instruction(
    total_weeks: int,
    subjects: list[dict[str, Any]],
    grade_code: str | None,
    grade_display_name: str | None,
    subject_filter: str | None = None,
    delivery_profile: dict[str, Any] | None = None,
) -> str:
    """Build the generation instruction for the instructional design plan.

    subject_filter: if set, only generate weeks entries for this subject name
    (used when splitting multi-subject packages into per-subject calls).
    """
    grade_clause = (
        f" This curriculum is for Grade {grade_code} ({grade_display_name})." if grade_code else ""
    )
    subject_names = [s.get("subject_name", "") for s in subjects if s.get("subject_name")]
    subject_list = ", ".join(subject_names) if subject_names else "the assigned subject"
    target_subject_clause = (
        f"\n\nSUBJECT SCOPE: Generate instructional_design entries ONLY for subject '{subject_filter}'. "
        "Include unit_mastery_arc and knowledge_dependency_graph for all objectives across all subjects."
        if subject_filter
        else ""
    )
    week_plural = f"{total_weeks} week" if total_weeks == 1 else f"{total_weeks} weeks"

    return (
        "You are a district instructional design specialist. Your responsibility is to design the most "
        f"effective instructional sequence for a {week_plural} package covering {subject_list}.{grade_clause} "
        "You operate by one principle: the district defines WHAT students learn — you determine the most "
        "effective HOW while remaining completely faithful to district intent.\n\n"
        "WHAT YOU ARE GIVEN (district-defined — READ ONLY):\n"
        "  district_anchors: per week × subject — primary objectives, supporting objectives, daily topics,\n"
        "    assessment checks, and pacing materials. These are extracted directly from the district pacing\n"
        "    guide. Do not modify, reorder, or substitute any of this data.\n"
        "  curriculum_sequence_plan: week themes, TEKS distribution, and mentor texts.\n"
        "  full_curriculum_documents: complete text of all district curriculum files.\n\n"
        "WHAT YOU GENERATE (instructional HOW — your design):\n"
        "  unit_mastery_arc: the terminal mastery target for the full package and per-week mastery gates.\n"
        "    Design this FIRST using backward design — start from what students must be able to do by the\n"
        "    end, then define what each week's mastery gate must achieve to reach that terminal target.\n"
        "  knowledge_dependency_graph: for every objective, identify knowledge that must already exist in\n"
        "    students before instruction can produce learning. Not skills to teach — pre-instructional\n"
        "    dependencies. Include gap consequences and activation strategies.\n"
        "  instructional_design per week × subject: the pedagogical HOW for each day within each week.\n"
        "    Design BACKWARD from end_of_week_mastery → learning_journey_rationale → daily_progression.\n"
        "    Monday introduces. Tuesday deepens. Wednesday applies. Thursday extends. Friday assesses.\n"
        "    Each day adds exactly one cognitive layer and removes one scaffold from the previous day.\n\n"
        "BACKWARD DESIGN SEQUENCE (follow this order):\n"
        "  1. Write unit_mastery_arc.terminal_mastery — what does mastery look like at the end of Week "
        f"{total_weeks}?\n"
        "  2. Write mastery_gates — what must students demonstrate after each week to stay on this arc?\n"
        "  3. For each week + subject, write end_of_week_mastery consistent with the gate for that week.\n"
        "  4. Write learning_journey_rationale — why does this 5-day sequence lead to that mastery?\n"
        "  5. Write daily_progression BACKWARD from Friday, ensuring each prior day is a prerequisite\n"
        "     for the next. Friday's observable_mastery_evidence defines the week's mastery standard.\n\n"
        "KNOWLEDGE DEPENDENCY GRAPH:\n"
        "  For each objective_code in district_anchors.primary_objectives (across all weeks):\n"
        "    - List 2-4 dependency nodes that must already exist for instruction to be effective\n"
        "    - status must be one of:\n"
        "        'assumed_mastered'    → should exist from prior instruction; activate, do not reteach\n"
        "        'develop_this_week'   → will be built as part of this week's progression\n"
        "        'may_need_activation' → likely present but may need a brief activation moment\n"
        "    - gap_consequence: what breaks instructionally if this dependency is absent. Be specific.\n"
        "    - activation_strategy: the exact classroom move to activate or check this dependency.\n\n"
        "INSTRUCTIONAL_PURPOSE — required for every day:\n"
        "  Answer: Why is this specific lesson being taught TODAY — not Monday or Thursday, but in this\n"
        "  position in the week? How does mastering today's lesson advance students toward the unit's\n"
        "  terminal mastery? What is the pedagogical cost if this lesson is skipped or shortened?\n"
        "  This field is teacher-facing only. Never expose to students. Do not reference TEKS codes.\n\n"
        "INSTRUCTIONAL_CONTRACTS — non-negotiable cross-artifact alignment:\n"
        "  exit_ticket_stem: the exact sentence frame for the weekly exit ticket — other artifacts will\n"
        "    use this verbatim. Must be completable in 3-5 minutes and produce written evidence.\n"
        "  quiz_objectives: TEKS codes from primary_objectives that the quiz MUST assess. Only include\n"
        "    objectives that instruction has actually covered by Friday of this week.\n"
        "  rubric_primary_criterion: the primary criterion string — rubric artifact uses this verbatim.\n"
        "  core_activity_name: the central instructional activity name for the week — all artifacts use\n"
        "    this exact name when referring to this activity.\n\n"
        "DIFFERENTIATION — required for every day:\n"
        "  scaffold: what a teacher does for students who need support — specific, actionable, not\n"
        "    'provide extra help'. Reduces one cognitive demand while preserving the core skill.\n"
        "  extension: what a student does when they've met today's mastery target — adds complexity\n"
        "    without moving to next week's content.\n\n"
        "DISTRICT FIDELITY RULES:\n"
        "  - daily_topics in district_anchors are district-defined. Center each day's teacher_modeling,\n"
        "    guided_practice, and independent_practice on that day's district topic.\n"
        "  - assessment_checks in district_anchors define WHEN assessments occur. Do not move them.\n"
        "  - pacing_materials are district-approved. Reference only materials from this list for\n"
        "    teacher_modeling and guided_practice examples. Do not substitute or invent materials.\n"
        "  - student_goal must use 'I can' language, 10 words max, no TEKS codes.\n"
        "  - teacher_goal explains what the teacher accomplishes instructionally — not what students do.\n"
        "  - builds_from_yesterday is null on Monday; references specific yesterday content on Tue-Fri.\n"
        "  - prepares_for_tomorrow is null on Friday; previews specific tomorrow content on Mon-Thu.\n\n"
        + (
            build_planning_constraint(delivery_profile) + "\n\n"
            if build_planning_constraint(delivery_profile)
            else ""
        )
        + f"Return exactly {total_weeks} week entries, one per district week in order.{target_subject_clause}"
    )


# ── AI call(s) ───────────────────────────────────────────────────────────────────────────


def _call_ai_for_plan(
    effective_settings: Settings,
    *,
    model_name: str,
    provider_name: str,
    api_key: str | None,
    base_url: str | None,
    db: Session,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    user: User,
    generation_context: dict[str, Any],
    curriculum_sequence_plan: dict[int, dict[str, Any]],
    full_curriculum_docs: list[dict[str, Any]],
    district_anchors: dict[tuple[int, str], dict[str, Any]],
    subject_filter: str | None = None,
    delivery_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Single AI call for the full plan (or a single subject subset)."""
    subjects = generation_context.get("subjects") or []
    total_weeks = len(generation_context.get("weeks") or [])
    grade_code = generation_context.get("grade_code")
    grade_display_name = generation_context.get("grade_display_name")

    instruction = _build_instruction(
        total_weeks=total_weeks,
        subjects=subjects,
        grade_code=grade_code,
        grade_display_name=grade_display_name,
        subject_filter=subject_filter,
        delivery_profile=delivery_profile,
    )

    # Build a serializable view of district_anchors keyed by "W{week}-{Subject}"
    # so the AI can easily cross-reference which anchors belong to which week+subject.
    anchors_for_prompt: list[dict[str, Any]] = []
    for week in generation_context.get("weeks") or []:
        week_num = int(week.get("sequence_number") or 0)
        for ws in week.get("subjects") or []:
            subj_name = (ws.get("subject_name") or "").strip()
            if subject_filter and subj_name.lower() != subject_filter.lower():
                continue
            key = (week_num, subj_name.lower())
            anchors = district_anchors.get(key) or {}
            anchors_for_prompt.append(
                {
                    "week": week_num,
                    "subject": subj_name,
                    "district_anchors": anchors,
                }
            )

    prompt_payload: dict[str, Any] = {
        "package_weeks": total_weeks,
        "grade_code": grade_code,
        "grade_display_name": grade_display_name,
        "week_day_labels": list(WEEKDAY_LABELS),
        "subjects": [
            {"subject_name": s.get("subject_name"), "subject_id": s.get("subject_id")}
            for s in subjects
        ],
        "curriculum_sequence_plan": curriculum_sequence_plan,
        "week_district_anchors": anchors_for_prompt,
        "full_curriculum_documents": full_curriculum_docs,
        "district_document_context": generation_context.get("district_document_context"),
        "teacher_supplemental_notes": generation_context.get("teacher_supplemental_notes"),
        "teacher_document_context": generation_context.get("teacher_document_context"),
        "pacing_materials": generation_context.get("pacing_materials"),
        # Delivery profile included so the AI can cross-reference the constraint text
        # in the instruction with the structured profile data.
        "instructional_delivery_profile": delivery_profile,
    }

    result = execute_openai_json_completion(
        effective_settings,
        model_name=model_name,
        instruction=instruction,
        prompt_payload=prompt_payload,
        required_output_schema=_PLAN_OUTPUT_SCHEMA,
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
            "operation_type": "instructional_design_plan",
            "package_id": str(package_id),
            "related_entity_type": "instructional_package",
            "related_entity_id": str(package_id),
            "subject_filter": subject_filter or "all",
        },
    )
    return result.content_json


# ── Plan assembly ─────────────────────────────────────────────────────────────────────────


def _merge_district_anchors_into_plan(
    plan: dict[str, Any],
    district_anchors: dict[tuple[int, str], dict[str, Any]],
) -> dict[str, Any]:
    """Inject district_anchors into the AI-generated plan structure.

    district_anchors are district-defined data that the AI read as context but
    did not write. They are merged alongside the AI's instructional_design for
    each week + subject so artifact generators have both layers in one place.
    """
    for week_entry in plan.get("weeks") or []:
        week_num = int(week_entry.get("week") or 0)
        for subj_entry in week_entry.get("subjects") or []:
            subj_name = (subj_entry.get("subject") or "").lower()
            key = (week_num, subj_name)
            if key in district_anchors:
                subj_entry["district_anchors"] = district_anchors[key]
    return plan


def _merge_per_subject_plans(per_subject_plans: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge per-subject AI outputs into a single plan structure.

    The first call's unit_mastery_arc and knowledge_dependency_graph are used.
    Subsequent calls' KDG entries are appended (deduplicated by objective_code).
    Week entries are merged by week number, collecting all subjects.
    """
    if not per_subject_plans:
        return {}

    merged = {
        "unit_mastery_arc": per_subject_plans[0].get("unit_mastery_arc") or {},
        "knowledge_dependency_graph": list(
            per_subject_plans[0].get("knowledge_dependency_graph") or []
        ),
        "weeks": {},
    }

    # Deduplicate KDG by objective_code across subsequent subject calls
    seen_objective_codes = {
        entry.get("objective_code")
        for entry in merged["knowledge_dependency_graph"]
        if entry.get("objective_code")
    }
    for plan in per_subject_plans[1:]:
        for kdg_entry in plan.get("knowledge_dependency_graph") or []:
            code = kdg_entry.get("objective_code")
            if code and code not in seen_objective_codes:
                merged["knowledge_dependency_graph"].append(kdg_entry)
                seen_objective_codes.add(code)

    # Merge weeks by week number, accumulating subjects from each per-subject plan
    week_map: dict[int, dict[str, Any]] = {}
    for plan in per_subject_plans:
        for week_entry in plan.get("weeks") or []:
            week_num = int(week_entry.get("week") or 0)
            if week_num not in week_map:
                week_map[week_num] = {"week": week_num, "subjects": []}
            week_map[week_num]["subjects"].extend(week_entry.get("subjects") or [])

    merged["weeks"] = sorted(week_map.values(), key=lambda w: w["week"])
    return merged


# ── Lookup helpers (used by artifact generators) ──────────────────────────────────────────


def get_plan_for_week_subject(
    plan: dict[str, Any] | None,
    week_num: int,
    subject_name: str,
) -> dict[str, Any] | None:
    """Return the full week+subject entry (district_anchors + instructional_design).

    Used by week-scoped artifact generators: quiz, assignment, rubric,
    subject_slide_deck, writing_response, vocabulary_list, parent_newsletter_summary.
    """
    for week in (plan or {}).get("weeks") or []:
        if int(week.get("week") or 0) == week_num:
            for subj in week.get("subjects") or []:
                if (subj.get("subject") or "").lower() == subject_name.lower():
                    return subj
    return None


def get_plan_for_day(
    plan: dict[str, Any] | None,
    week_num: int,
    subject_name: str,
    day_label: str,
) -> dict[str, Any] | None:
    """Return a single day entry from daily_progression.

    Used by day-scoped artifact generators: daily_lesson_plan, student_lesson_deck
    (daily variant), exit_ticket, bell_ringer.
    """
    week_subj = get_plan_for_week_subject(plan, week_num, subject_name)
    if not week_subj:
        return None
    design = week_subj.get("instructional_design") or {}
    for day in design.get("daily_progression") or []:
        if (day.get("day") or "").lower() == day_label.lower():
            return day
    return None


def get_plan_district_anchors(
    plan: dict[str, Any] | None,
    week_num: int,
    subject_name: str,
) -> dict[str, Any] | None:
    """Return district_anchors for a week+subject entry."""
    week_subj = get_plan_for_week_subject(plan, week_num, subject_name)
    return (week_subj or {}).get("district_anchors")


def get_plan_instructional_design_week(
    plan: dict[str, Any] | None,
    week_num: int,
    subject_name: str,
) -> dict[str, Any] | None:
    """Return only the instructional_design portion of a week+subject entry."""
    week_subj = get_plan_for_week_subject(plan, week_num, subject_name)
    return (week_subj or {}).get("instructional_design")


def get_plan_for_all_subjects_on_day(
    plan: dict[str, Any] | None,
    week_num: int,
    subject_names: list[str],
    day_label: str,
) -> dict[str, dict[str, Any]]:
    """Return a map of subject_name → day entry for all subjects on a given day.

    Used by daily_lesson_plan which covers all subjects for one day.
    """
    result: dict[str, dict[str, Any]] = {}
    for subj_name in subject_names:
        day_entry = get_plan_for_day(plan, week_num, subj_name, day_label)
        if day_entry:
            result[subj_name] = day_entry
    return result


_PREV_DAY_MAP: dict[str, str | None] = {
    "Monday": None,
    "Tuesday": "Monday",
    "Wednesday": "Tuesday",
    "Thursday": "Wednesday",
    "Friday": "Thursday",
}


def resolve_verified_previous_learning(
    plan: dict[str, Any] | None,
    week_num: int,
    subject_names: list[str],
    current_day_label: str,
) -> dict[str, Any] | None:
    """Return deterministic prior-day learning context from the IDP.

    Returns None when current_day_label is Monday (no prior day within the week).
    Returns None when the plan is empty.

    Shape: {
      "day": "W<N>-<PreviousDay>",
      "per_subject": {
        "<SubjectName>": {
          "student_goal": str | None,
          "teacher_modeling": str | None,
          "exit_ticket": str | None,
          "observable_mastery_evidence": str | None,
          "topic": str | None,
          "objective_descriptions": [str],
        }
      }
    }

    This is injected into prompt_payload as "verified_previous_learning".
    The AI must only reference prior learning that appears here — never invent it.
    """
    if not plan or not subject_names or not current_day_label:
        return None

    prev_day = _PREV_DAY_MAP.get(current_day_label.strip().capitalize())
    if not prev_day:
        return None  # Monday — no prior day this week

    per_subject: dict[str, dict[str, Any]] = {}

    for subj_name in subject_names:
        if not subj_name:
            continue

        day_entry = get_plan_for_day(plan, week_num, subj_name, prev_day) or {}
        anchors = get_plan_district_anchors(plan, week_num, subj_name) or {}

        # Extract objective descriptions only (no TEKS codes — those are teacher-only).
        obj_descriptions = [
            obj["description"]
            for obj in (anchors.get("primary_objectives") or [])
            if obj.get("description")
        ]

        topic = (anchors.get("daily_topics") or {}).get(prev_day)

        per_subject[subj_name] = {
            "student_goal": day_entry.get("student_goal"),
            "teacher_modeling": day_entry.get("teacher_modeling"),
            "exit_ticket": day_entry.get("exit_ticket"),
            "observable_mastery_evidence": day_entry.get("observable_mastery_evidence"),
            "topic": topic,
            "objective_descriptions": obj_descriptions,
        }

    if not per_subject:
        return None

    return {
        "day": f"W{week_num}-{prev_day}",
        "per_subject": per_subject,
    }


# ── Main entry point ──────────────────────────────────────────────────────────────────────


def generate_instructional_design_plan(
    db: Session,
    *,
    settings: Settings,
    user: User,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    generation_context: dict[str, Any],
    curriculum_sequence_plan: dict[int, dict[str, Any]],
    excluded_ids: set[str] | None = None,
    delivery_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate the Instructional Design Plan for a package.

    Produces one backward-designed instructional plan covering all weeks × subjects.
    The plan is generated once before any artifact generation begins. Every artifact
    generator consumes a slice of this plan rather than reasoning independently from
    raw curriculum data.

    Returns the full plan dict (ready to store as instructional_design_plan_json).
    Returns {} on any failure — callers handle gracefully; artifacts fall back to
    current behavior when the plan is absent.

    Generation strategy:
      Single AI call  — if total subject × week pairs ≤ _SINGLE_CALL_MAX_SUBJECT_WEEK_PAIRS
      Per-subject calls — otherwise, to keep output token count within quality bounds.
      Each per-subject call produces the full unit_mastery_arc and KDG; results are
      merged with the first call's arc and deduplicated KDG entries.
    """
    if not is_teacher_assist_real_ai_active(db, settings):
        return {}

    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name, api_key, base_url = _provider_api_params(effective_settings)
    model_name = get_teacher_assist_provider_model(effective_settings, provider_name=provider_name)

    subjects = generation_context.get("subjects") or []
    total_weeks = len(generation_context.get("weeks") or [])
    total_pairs = len(subjects) * total_weeks

    # Phase 0b: build district_anchors for all weeks × subjects (pure Python, no AI)
    district_anchors = _build_all_district_anchors(generation_context, excluded_ids=excluded_ids)

    # Phase 0c-prep: load full curriculum documents once for all AI calls
    full_curriculum_docs = _load_full_curriculum_docs(db, generation_context)

    resolved_profile = resolve_profile(delivery_profile)
    common_call_kwargs = dict(
        effective_settings=effective_settings,
        model_name=model_name,
        provider_name=provider_name,
        api_key=api_key,
        base_url=base_url,
        db=db,
        tenant_id=tenant_id,
        package_id=package_id,
        user=user,
        generation_context=generation_context,
        curriculum_sequence_plan=curriculum_sequence_plan,
        full_curriculum_docs=full_curriculum_docs,
        district_anchors=district_anchors,
        delivery_profile=resolved_profile,
    )

    if total_pairs <= _SINGLE_CALL_MAX_SUBJECT_WEEK_PAIRS or len(subjects) <= 1:
        # Single call covers all weeks × subjects
        ai_plan = _call_ai_for_plan(**common_call_kwargs)
    else:
        # Per-subject calls to maintain quality at high week × subject counts
        per_subject_plans: list[dict[str, Any]] = []
        for subj in subjects:
            subj_name = subj.get("subject_name") or ""
            if not subj_name:
                continue
            subj_plan = _call_ai_for_plan(**common_call_kwargs, subject_filter=subj_name)
            per_subject_plans.append(subj_plan)
        ai_plan = _merge_per_subject_plans(per_subject_plans)

    if not ai_plan:
        return {}

    # Merge district_anchors into the AI output structure
    final_plan = _merge_district_anchors_into_plan(ai_plan, district_anchors)

    # Add plan_meta — Python-generated, not AI. Enables provenance tracking.
    final_plan["plan_meta"] = {
        "schema_version": _PLAN_SCHEMA_VERSION,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "model": model_name,
        "prompt_version": _PLAN_PROMPT_VERSION,
        "total_weeks": total_weeks,
        "total_subjects": len(subjects),
    }

    logger.info(
        "package=%s: instructional design plan generated — %d weeks, %d subjects, model=%s",
        package_id,
        total_weeks,
        len(subjects),
        model_name,
    )
    return final_plan

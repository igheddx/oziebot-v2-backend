"""Strand Learning Journeys — cross-week, per-strand instructional arc for TeacherAssist v2.

Phase 0f: runs after the instructional design plan is validated (Phase 0d) and the
vocabulary registry is built (Phase 0e). Reads the already-generated design plan and
stitches the per-week, per-subject entries into continuous cross-week learning arcs —
one arc (journey) per instructional strand.

Key principles from the v3 spec:
  - Every instructional block owns its own learning journey.
  - Resources bind to OBJECTIVES, not calendar days.
  - Cross-strand connections are intentional — never forced.
  - Curriculum duration is derived from the curriculum, not hardcoded.
"""

from __future__ import annotations

import logging
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
from oziebot_api.services.teacher_assist_v2.planning_constants import WEEKDAY_LABELS

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = "1"
_PROMPT_VERSION = "strand-journeys-v1"


# ── Provider resolution (mirrors instructional_design_plan._provider_api_params) ─────────


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


# ── Output schema ─────────────────────────────────────────────────────────────────────────

_JOURNEY_OUTPUT_SCHEMA: dict[str, Any] = {
    "curriculum_duration_weeks": 1,
    "curriculum_duration_derived": True,
    "strand_journeys": {
        "Reading": {
            "strand_name": "string",
            "total_instructional_days": 1,
            "objectives": [
                {
                    "sequence": 1,
                    "week": 1,
                    "teks_codes": ["string"],
                    "objective": "string",
                    "curriculum_resource": "string",
                    "estimated_days": 1,
                    "requires_preteach": True,
                    "prerequisite_knowledge": ["string"],
                    "mastery_evidence": "string",
                    "instructional_focus": "string",
                }
            ],
            "resource_bindings": [
                {
                    "resource_title": "string",
                    "bound_to_objective_sequences": [1],
                    "binding_reason": "string",
                }
            ],
            "cross_strand_connections": [
                {
                    "connected_strand": "string",
                    "at_objective_sequence": 1,
                    "connection_type": "string",
                    "connection": "string",
                }
            ],
        }
    },
}


# ── AI instruction ─────────────────────────────────────────────────────────────────────────

_JOURNEY_SYSTEM_INSTRUCTION = """You are a curriculum mapping specialist for TeacherAssist v2.

You have been given:
  - An already-validated instructional design plan covering multiple weeks × subjects.
  - A curriculum sequence plan with week-level themes and mentor texts.
  - Full generation context including subjects, grade, and delivery profile.

Your task: synthesize this into STRAND LEARNING JOURNEYS — one continuous cross-week arc per instructional strand.

LEARNING JOURNEY RULES:
1. Each strand (e.g. Reading, Writing, Vocabulary) gets its own journey.
2. Each journey is a sequenced list of OBJECTIVES — one objective entry per week the strand is active.
3. Objectives are ordered by prerequisite dependency, not by calendar day.
4. Resources (books, texts, mentor texts) bind to OBJECTIVES, not days.
   - If "Desmond and the Very Mean World" supports character motivation AND theme analysis,
     list it under BOTH objectives, not under "Week 2" and "Week 3."
5. Cross-strand connections are ONLY recorded when they are intentional and curriculum-supported.
   - DO NOT force Reading → Writing connections.
   - If the curriculum says "use this week's read-aloud as a writing mentor text," that is intentional.
   - If there is no clear connection, leave cross_strand_connections empty.
6. curriculum_duration_weeks = the number of weeks in the package (derived from the design plan).
7. curriculum_duration_derived = true always (you derived it from the plan, not a UI input).
8. requires_preteach = true when this objective introduces a NEW concept or vocabulary that students
   have not encountered before in this unit.
9. instructional_focus must be one of: "introduce", "deepen", "apply", "extend", "assess"

RESOURCE BINDING RULES:
- resource_title must exactly match a book/text title from the curriculum sequence plan or design plan.
- Never invent resource titles.
- bound_to_objective_sequences = list of sequence numbers this resource supports.
- If a resource spans the entire unit, list all objective sequences.

OUTPUT: One strand_journeys entry per subject in the package. Use the subject_name exactly as given.
The objectives list for each strand should cover all weeks where that strand appears."""


def _build_prompt_payload(
    generation_context: dict[str, Any],
    instructional_design_plan: dict[str, Any],
    curriculum_sequence_plan: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    subjects = generation_context.get("subjects") or []
    weeks = generation_context.get("weeks") or []
    total_weeks = len(weeks)

    # Build a compact view of the design plan: unit arc + per-week/subject learning journey rationale
    design_summary: list[dict[str, Any]] = []
    for week_entry in instructional_design_plan.get("weeks") or []:
        week_num = week_entry.get("week")
        for subj_entry in week_entry.get("subjects") or []:
            subj = subj_entry.get("subject") or ""
            idesign = subj_entry.get("instructional_design") or {}
            anchors = subj_entry.get("district_anchors") or {}
            design_summary.append(
                {
                    "week": week_num,
                    "subject": subj,
                    "end_of_week_mastery": idesign.get("end_of_week_mastery"),
                    "learning_journey_rationale": idesign.get("learning_journey_rationale"),
                    "primary_objectives": anchors.get("primary_objectives") or [],
                    "supporting_objectives": anchors.get("supporting_objectives") or [],
                    "pacing_materials": anchors.get("pacing_materials") or [],
                }
            )

    return {
        "total_weeks": total_weeks,
        "week_day_labels": list(WEEKDAY_LABELS),
        "subjects": [
            {"subject_name": s.get("subject_name"), "subject_id": s.get("subject_id")}
            for s in subjects
        ],
        "grade_code": generation_context.get("grade_code"),
        "grade_display_name": generation_context.get("grade_display_name"),
        "curriculum_sequence_plan": curriculum_sequence_plan,
        "design_plan_summary": design_summary,
        "unit_mastery_arc": instructional_design_plan.get("unit_mastery_arc") or {},
        "knowledge_dependency_graph": instructional_design_plan.get("knowledge_dependency_graph")
        or [],
        "instructional_delivery_profile": generation_context.get("instructional_delivery_profile"),
        "district_document_context": generation_context.get("district_document_context"),
    }


# ── Public API ─────────────────────────────────────────────────────────────────────────────


def generate_strand_learning_journeys(
    db: Session,
    *,
    settings: Settings,
    user: User,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    generation_context: dict[str, Any],
    instructional_design_plan: dict[str, Any],
    curriculum_sequence_plan: dict[int, dict[str, Any]],
    total_planned_days: int | None = None,
) -> dict[str, Any]:
    """Generate cross-week strand learning journeys from the instructional design plan.

    Phase 0f: runs once per package after the design plan is validated. Stitches the
    week-by-week instructional design into continuous per-strand arcs.

    Returns a dict with strand_journeys keyed by strand name, or {} on failure.
    """
    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name, api_key, base_url = _provider_api_params(effective_settings)
    model_name = get_teacher_assist_provider_model(effective_settings, provider_name=provider_name)

    prompt_payload = _build_prompt_payload(
        generation_context,
        instructional_design_plan,
        curriculum_sequence_plan,
    )
    if total_planned_days is not None:
        prompt_payload["total_planned_days"] = total_planned_days
        total_curriculum_days = generation_context.get("total_curriculum_days")
        if total_curriculum_days and total_planned_days < total_curriculum_days:
            lost = total_curriculum_days - total_planned_days
            prompt_payload["lost_instructional_days"] = lost
            prompt_payload["compression_required"] = True
            prompt_payload["compression_instruction"] = (
                f"{lost} instructional day(s) have been lost. "
                f"Journeys must be compressed to fit {total_planned_days} total planned days. "
                "Combine compatible objectives into single days where needed. "
                "Do NOT remove required objectives or TEKS standards — compress, never delete."
            )

    result = execute_openai_json_completion(
        effective_settings,
        model_name=model_name,
        instruction=_JOURNEY_SYSTEM_INSTRUCTION,
        prompt_payload=prompt_payload,
        required_output_schema=_JOURNEY_OUTPUT_SCHEMA,
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
            "operation_type": "strand_learning_journeys",
            "package_id": str(package_id),
            "related_entity_type": "instructional_package",
            "related_entity_id": str(package_id),
        },
    )

    content = result.content_json or {}
    if not content:
        return {}

    content["schema_version"] = _SCHEMA_VERSION
    content["generated_at"] = datetime.now(tz=timezone.utc).isoformat()
    content["prompt_version"] = _PROMPT_VERSION
    return content


def get_journey_position(
    strand_learning_journeys: dict[str, Any],
    strand_name: str,
    week: int,
    day_label: str,
) -> dict[str, Any] | None:
    """Return the current objective context for a strand on a given week+day.

    Returns dict with: objective, teks_codes, curriculum_resource, sequence,
    total_objectives, is_new_concept, requires_preteach
    Or None if journey data is unavailable.
    """
    strand_journeys = (strand_learning_journeys or {}).get("strand_journeys") or {}
    if not isinstance(strand_journeys, dict):
        return None

    # Try exact match first, then case-insensitive
    journey = strand_journeys.get(strand_name)
    if journey is None:
        lower = strand_name.lower()
        journey = next(
            (v for k, v in strand_journeys.items() if k.lower() == lower),
            None,
        )
    if not journey:
        return None

    objectives = journey.get("objectives") or []
    if not objectives:
        return None

    # Find the objective for this week
    current_obj = next((o for o in objectives if o.get("week") == week), None)
    if not current_obj:
        return None

    # Determine if this is the first day of this objective (new concept)
    prev_obj = next((o for o in objectives if o.get("week") == week - 1), None)
    is_new_concept = prev_obj is None or prev_obj.get("objective") != current_obj.get("objective")

    return {
        "objective": current_obj.get("objective"),
        "teks_codes": current_obj.get("teks_codes") or [],
        "curriculum_resource": current_obj.get("curriculum_resource"),
        "sequence": current_obj.get("sequence"),
        "total_objectives": len(objectives),
        "is_new_concept": is_new_concept,
        "requires_preteach": bool(current_obj.get("requires_preteach")) and is_new_concept,
        "mastery_evidence": current_obj.get("mastery_evidence"),
        "instructional_focus": current_obj.get("instructional_focus"),
    }


def build_journey_position_context(
    strand_learning_journeys: dict[str, Any] | None,
    week: int | None,
    day_label: str | None,
    subject_name: str | None = None,
) -> str | None:
    """Build a brief learning journey position note for injection into AI prompts.

    Returns a multi-line string or None if no journey data is available.
    """
    if not strand_learning_journeys or not week:
        return None

    strand_journeys = strand_learning_journeys.get("strand_journeys") or {}
    if not strand_journeys or not isinstance(strand_journeys, dict):
        return None

    lines: list[str] = ["LEARNING JOURNEY CONTEXT (v3):"]
    found_any = False

    for strand_name, journey in strand_journeys.items():
        # If subject_name is given, only include the matching strand
        if (
            subject_name
            and subject_name.lower() not in strand_name.lower()
            and strand_name.lower() not in subject_name.lower()
        ):
            # For daily_lesson_plan (multi-strand), include all; for single-subject, filter
            pass  # We'll include all strands for multi-subject context

        objectives = journey.get("objectives") or []
        current_obj = next((o for o in objectives if o.get("week") == week), None)
        if not current_obj:
            continue

        found_any = True
        seq = current_obj.get("sequence", "?")
        total = len(objectives)
        resource = current_obj.get("curriculum_resource") or ""
        obj_text = current_obj.get("objective") or ""
        focus = current_obj.get("instructional_focus") or ""
        preteach_flag = (
            " [NEW CONCEPT — PRETEACH REQUIRED]" if current_obj.get("requires_preteach") else ""
        )

        resource_part = f" | Resource: {resource}" if resource else ""
        focus_part = f" | Focus: {focus}" if focus else ""
        lines.append(
            f"  {strand_name} Journey: Objective {seq}/{total} — {obj_text}"
            f"{resource_part}{focus_part}{preteach_flag}"
        )

        # Cross-strand connections for this objective
        for conn in journey.get("cross_strand_connections") or []:
            if conn.get("at_objective_sequence") == current_obj.get("sequence"):
                conn_type = conn.get("connection_type") or "intentional"
                lines.append(
                    f"  [CROSS-STRAND {conn_type.upper()}] "
                    f"{strand_name} ↔ {conn.get('connected_strand')}: {conn.get('connection')}"
                )

    if not found_any:
        return None

    # Append resource binding reminder
    lines.append(
        "  NOTE: Curriculum resources above are bound to OBJECTIVES, not days. "
        "Continue using the listed resource until the objective changes."
    )
    return "\n".join(lines)

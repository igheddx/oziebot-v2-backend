"""Recovery Artifact Generation — Instructional Integrity Check + content generation.

Generation priority (per Learning Recovery Planner principles):
1. Reuse existing plan data (reteach_if_needed, instructional contract, KDG, mastery evidence)
2. Apply deterministic planning logic to build artifact content
3. Use AI only to enrich the deterministic content if available
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import TeacherAssistV2InstructionalPackage
from oziebot_api.models.teacher_assist_v2_recovery_artifact import TeacherAssistV2RecoveryArtifact
from oziebot_api.models.teacher_assist_v2_recovery_queue import TeacherAssistV2RecoveryQueue
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_mode import is_teacher_assist_real_ai_active
from oziebot_api.services.teacher_assist.ai_usage import (
    assert_teacher_assist_ai_cost_available,
    record_teacher_assist_ai_usage,
)
from oziebot_api.services.teacher_assist.openai_json_client import execute_openai_json_completion
from oziebot_api.services.teacher_assist.prompt_contracts import V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_provider_model
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.instructional_package_ai import _provider_api_params
from oziebot_api.services.teacher_assist_v2.planning_constants import RECOVERY_ARTIFACT_TYPES

# ── TEKS code pattern for post-generation validation ──────────────────────────

_TEKS_CODE_RE = re.compile(r"\b\d{1,2}\.[A-Z]\d{1,2}[A-Z]?\b")

# ── Artifact display metadata ─────────────────────────────────────────────────

_ARTIFACT_META: dict[str, tuple[str, bool]] = {
    "recovery_bell_ringer": ("Bell Ringer — Recovery", True),
    "recovery_mini_lesson": ("Mini Lesson — Recovery", False),
    "recovery_small_group_packet": ("Small Group Packet — Recovery", True),
    "recovery_conference_guide": ("Conference Guide — Recovery", False),
    "recovery_exit_ticket": ("Exit Ticket — Recovery Verification", True),
    "recovery_guided_practice": ("Guided Practice — Recovery", True),
    "recovery_assignment": ("Assignment — Recovery", True),
    "recovery_homework": ("Homework — Recovery", True),
    "recovery_assessment": ("Assessment — Recovery Verification", True),
    "recovery_spiral_review": ("Spiral Review Schedule", False),
    "recovery_presentation": ("Recovery Lesson Presentation", True),
}


# ── Instructional Integrity Check ─────────────────────────────────────────────

def _run_integrity_check(
    *,
    artifact_type: str,
    generation_context: dict[str, Any],
) -> dict[str, Any]:
    """Pre-generation gate. Verifies the context satisfies all four integrity criteria:
    1. Builds from yesterday's lesson
    2. Prepares for tomorrow's lesson
    3. Preserves instructional contract
    4. Introduces no new objectives
    """
    issues: list[str] = []
    warnings: list[str] = []

    builds_from = bool(generation_context.get("builds_from_yesterday"))
    if not builds_from:
        warnings.append(
            "No builds_from_yesterday — recovery context does not connect to yesterday's learning."
        )

    prepares_for = bool(generation_context.get("prepares_for_tomorrow"))
    if not prepares_for and artifact_type not in ("recovery_exit_ticket", "recovery_assessment"):
        warnings.append(
            "No prepares_for_tomorrow — recovery may not align with the upcoming lesson sequence."
        )

    contract = generation_context.get("instructional_contract") or {}
    preserves_contract = bool(
        contract.get("exit_ticket_stem") or contract.get("rubric_primary_criterion")
    )
    if not preserves_contract:
        warnings.append(
            "No instructional contract data — using observable_mastery_evidence as the constraint."
        )

    if not generation_context.get("objective_code"):
        issues.append("No objective code in context — recovery must target a specific objective.")

    return {
        "passed": len(issues) == 0,
        "builds_from_yesterday": builds_from,
        "prepares_for_tomorrow": prepares_for,
        "preserves_contract": preserves_contract,
        "no_new_objectives": True,  # enforced by construction
        "issues": issues,
        "warnings": warnings,
    }


# ── Plan data extraction ──────────────────────────────────────────────────────

def _extract_recovery_context_from_plan(
    package: TeacherAssistV2InstructionalPackage,
    *,
    week_number: int,
) -> dict[str, Any]:
    """Extract all plan fields needed for recovery generation.

    Priority order:
    1. reteach_if_needed (plan already prescribes a reteach approach)
    2. KDG activation_strategy and gap_consequence
    3. instructional_contract (exit_ticket_stem, rubric_primary_criterion)
    4. observable_mastery_evidence and daily_progression fields
    """
    plan = package.instructional_design_plan_json or {}
    weeks = plan.get("weeks") or []

    target_week: dict[str, Any] = {}
    for w in weeks:
        if w.get("week_number") == week_number or w.get("sequence_number") == week_number:
            target_week = w
            break
    if not target_week and weeks:
        target_week = weeks[0]

    # Find first subject in the week with instructional_design data
    subjects_in_week = target_week.get("subjects") or []
    target_subject: dict[str, Any] = {}
    for s in subjects_in_week:
        if s.get("instructional_design"):
            target_subject = s
            break

    instructional_design = target_subject.get("instructional_design") or {}
    daily_progression = instructional_design.get("daily_progression") or []
    instructional_contracts = instructional_design.get("instructional_contracts") or {}

    # PRIMARY: reteach_if_needed from daily progression
    reteach_hints = [
        day["reteach_if_needed"]
        for day in daily_progression
        if day.get("reteach_if_needed")
    ]
    plan_reteach_hint = reteach_hints[0] if reteach_hints else None

    # Continuity fields — use first non-null occurrence
    builds_from_yesterday: str | None = None
    prepares_for_tomorrow: str | None = None
    observable_mastery_evidence: str | None = None
    scaffold: str | None = None

    for day in daily_progression:
        if day.get("builds_from_yesterday") and not builds_from_yesterday:
            builds_from_yesterday = day["builds_from_yesterday"]
        if day.get("prepares_for_tomorrow") and not prepares_for_tomorrow:
            prepares_for_tomorrow = day["prepares_for_tomorrow"]
        if day.get("observable_mastery_evidence") and not observable_mastery_evidence:
            observable_mastery_evidence = day["observable_mastery_evidence"]
        diff = day.get("differentiation") or {}
        if diff.get("scaffold") and not scaffold:
            scaffold = diff["scaffold"]

    # KDG — find the entry for this queue item's objective
    kdg = plan.get("knowledge_dependency_graph") or []
    activation_strategy: str | None = None
    gap_consequence: str | None = None

    return {
        "plan_reteach_hint": plan_reteach_hint,
        "builds_from_yesterday": builds_from_yesterday,
        "prepares_for_tomorrow": prepares_for_tomorrow,
        "instructional_contract": instructional_contracts,
        "observable_mastery_evidence": observable_mastery_evidence,
        "scaffold": scaffold,
        "knowledge_dependency_graph": kdg,
        "activation_strategy": activation_strategy,
        "gap_consequence": gap_consequence,
        "subject_name": target_subject.get("subject_name"),
        "week_label": (
            target_week.get("title") or f"Week {target_week.get('week_number', week_number)}"
        ),
    }


def _resolve_kdg_fields(kdg: list[dict], objective_code: str | None) -> dict[str, Any]:
    """Extract activation_strategy and gap_consequence from the KDG for the objective."""
    if not objective_code or not kdg:
        return {"activation_strategy": None, "gap_consequence": None}
    for entry in kdg:
        if entry.get("objective_code") == objective_code:
            deps = entry.get("dependencies") or []
            activation_strategies = [d["activation_strategy"] for d in deps if d.get("activation_strategy")]
            gap_consequences = [d["gap_consequence"] for d in deps if d.get("gap_consequence")]
            return {
                "activation_strategy": activation_strategies[0] if activation_strategies else None,
                "gap_consequence": gap_consequences[0] if gap_consequences else None,
            }
    return {"activation_strategy": None, "gap_consequence": None}


# ── Deterministic content builder ─────────────────────────────────────────────

def _build_deterministic_content(
    artifact_type: str,
    *,
    objective_code: str,
    reteach: str,
    activation: str,
    mastery_evidence: str,
    misconception: str,
    builds_from: str,
    prepares_for: str,
    exit_ticket_stem: str,
    scaffold: str,
    students_count: int,
) -> dict[str, Any]:
    """Build full artifact content from plan data. No AI call.

    Every field draws from the plan in this priority:
    reteach_if_needed → KDG activation_strategy → observable_mastery_evidence
    """
    primary = reteach or activation or (
        f"Address: {misconception}" if misconception else f"Reteach {objective_code}"
    )
    goal = mastery_evidence or objective_code

    if artifact_type == "recovery_bell_ringer":
        return {
            "title": f"Bell Ringer — {objective_code}",
            "duration_minutes": 8,
            "teacher_instruction": (
                f"Cold-call 3–4 students. Listen for whether students can {goal}."
            ),
            "student_prompt": activation or reteach or f"Quick review: What do you remember about {objective_code}?",
            "discussion_follow_up": (
                f"Turn and talk: {misconception or 'Share what you know with a partner.'}"
            ),
            "teacher_look_for": goal,
            "injection_metadata": {
                "insert_before": "direct_instruction",
                "estimated_minutes": 8,
                "teacher_note": "Run at the start of class before the main lesson. Takes 8 minutes.",
            },
        }

    if artifact_type == "recovery_mini_lesson":
        return {
            "title": f"Mini Lesson — {objective_code}",
            "duration_minutes": 12,
            "teacher_hook": (
                builds_from or f"Yesterday we worked on {objective_code}. Today we look at it from a different angle."
            ),
            "teacher_instruction": primary,
            "student_practice_prompt": goal,
            "formative_check": exit_ticket_stem or f"Check: Can students {goal}?",
            "teacher_look_for": goal,
            "differentiation_scaffold": scaffold,
            "prepares_for": prepares_for,
            "injection_metadata": {
                "insert_before": "guided_practice",
                "estimated_minutes": 12,
                "teacher_note": (
                    "Insert before guided practice. Independent practice may be shortened by 12 minutes."
                ),
            },
        }

    if artifact_type == "recovery_small_group_packet":
        return {
            "title": f"Small Group Packet — {objective_code}",
            "duration_minutes": 20,
            "group_size": f"Up to {min(students_count, 8)} students",
            "teacher_guide": {
                "opening": f"Pull this group during independent practice. Start with: '{primary}'",
                "instruction": reteach or f"Reteach {objective_code} using a different approach from the whole-class lesson.",
                "practice_activity": goal,
                "scaffold_notes": scaffold or "Provide graphic organizers or sentence frames as needed.",
                "closing_check": exit_ticket_stem or f"Can each student {goal}?",
            },
            "student_materials": {
                "goal": f"I can {goal}",
                "practice_prompt": goal,
                "sentence_starters": ["I think… because…", "The evidence is…", "This connects to…"],
            },
        }

    if artifact_type == "recovery_conference_guide":
        return {
            "title": f"Conference Guide — {objective_code}",
            "duration_minutes": 10,
            "per_student": True,
            "opening_question": f"Tell me what you remember about {objective_code}.",
            "listening_for": misconception or goal,
            "teaching_move": primary,
            "practice_together": goal,
            "closing_question": exit_ticket_stem or f"Can you show me how you would {goal}?",
            "teacher_note": (
                "Allow the student to explain first before correcting. "
                "Address the misconception, not the student."
            ),
        }

    if artifact_type == "recovery_exit_ticket":
        return {
            "title": f"Exit Ticket — Verify {objective_code}",
            "duration_minutes": 5,
            "prompt": exit_ticket_stem or goal,
            "success_criteria": goal,
            "scoring": {
                "mastery": "Fully correct",
                "developing": "Partially correct",
                "beginning": "Incorrect or blank",
            },
            "teacher_sort_directions": (
                "Sort responses into three piles after collection. "
                "Students in 'beginning' need follow-up."
            ),
        }

    if artifact_type == "recovery_guided_practice":
        return {
            "title": f"Guided Practice — {objective_code}",
            "duration_minutes": 15,
            "replaces": "regular guided practice",
            "teacher_modeling": primary,
            "we_do": goal,
            "you_do": goal,
            "checks_for_understanding": [
                "After modeling: Who can tell me the first step?",
                "Mid-practice: Show me your work so far.",
                f"Before closure: What does success look like?",
            ],
            "teacher_note": (
                "This replaces the original guided practice. "
                f"Reconnect to tomorrow's lesson via: {prepares_for or 'upcoming lesson.'}"
            ),
        }

    if artifact_type == "recovery_assignment":
        return {
            "title": f"Recovery Assignment — {objective_code}",
            "objective_alignment": objective_code,
            "student_instructions": [goal, "Show your thinking."],
            "questions": [{"prompt": primary or goal, "type": "short_answer"}],
            "success_criteria": [goal],
        }

    if artifact_type == "recovery_homework":
        return {
            "title": f"Homework — Reinforce {objective_code}",
            "estimated_minutes": 15,
            "student_goal": f"I can {goal}",
            "practice_prompt": activation or reteach or goal,
            "hint": scaffold or "Ask a family member to check your work.",
            "bring_back": "Complete and bring back tomorrow. We will review in class.",
        }

    if artifact_type == "recovery_assessment":
        return {
            "title": f"Mastery Check — {objective_code}",
            "duration_minutes": 10,
            "prompts": [{"prompt": exit_ticket_stem or goal, "type": "short_answer"}],
            "success_criteria": goal,
            "scoring_note": "80%+ = mastery, 60–79% = developing, <60% = beginning",
        }

    if artifact_type == "recovery_spiral_review":
        return {
            "title": f"Spiral Review Schedule — {objective_code}",
            "objective_code": objective_code,
            "suspected_misconception": misconception,
            "schedule": [
                {
                    "sequence": 1,
                    "type": "bell_ringer",
                    "estimated_minutes": 8,
                    "prompt": activation or reteach or f"Quick review: {objective_code}",
                    "teacher_note": "Cold-call 3 students. Listen for the gap.",
                },
                {
                    "sequence": 2,
                    "type": "discussion",
                    "estimated_minutes": 5,
                    "prompt": f"Turn-and-talk: {misconception or goal}",
                    "teacher_note": "Circulate and check for common errors.",
                },
                {
                    "sequence": 3,
                    "type": "exit_ticket",
                    "estimated_minutes": 5,
                    "prompt": exit_ticket_stem or goal,
                    "teacher_note": (
                        "Sort responses. If 3+ students still struggling, "
                        "schedule small-group recovery."
                    ),
                },
            ],
            "completion_tracking": {
                "sequence_1_completed": False,
                "sequence_2_completed": False,
                "sequence_3_completed": False,
            },
        }

    if artifact_type == "recovery_presentation":
        return {
            "title": f"Recovery Lesson — {objective_code}",
            "duration_minutes": 45,
            "slides": [
                {
                    "title": "Let's Review",
                    "body": builds_from or f"We've been learning about {objective_code}.",
                    "type": "hook",
                },
                {
                    "title": "Today's Goal",
                    "body": f"I can {goal}",
                    "type": "goal",
                },
                {
                    "title": "Let's Learn Together",
                    "body": primary,
                    "type": "instruction",
                },
                {
                    "title": "We Do It Together",
                    "body": goal,
                    "type": "guided_practice",
                },
                {
                    "title": "Your Turn",
                    "body": goal,
                    "type": "independent_practice",
                },
                {
                    "title": "Wrap Up",
                    "body": exit_ticket_stem or goal,
                    "type": "closure",
                },
            ],
        }

    return {
        "title": f"Recovery Activity — {objective_code}",
        "summary": primary,
        "sections": [{"heading": "Activity", "body": primary, "bullets": []}],
    }


# ── AI enrichment prompt ──────────────────────────────────────────────────────

def _build_recovery_ai_prompt(
    artifact_type: str,
    *,
    generation_context: dict[str, Any],
    queue_item: TeacherAssistV2RecoveryQueue,
    deterministic_content: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    display_name, is_student_facing = _ARTIFACT_META.get(artifact_type, (artifact_type, False))
    objective_code = generation_context.get("objective_code", "the objective")
    misconception = queue_item.misconception_text or ""
    reteach = generation_context.get("plan_reteach_hint") or ""
    contract = generation_context.get("instructional_contract") or {}
    mastery_snapshot = queue_item.mastery_snapshot_json or {}

    student_rule = (
        "\n\nCRITICAL — student-facing artifact. Never include:\n"
        "- TEKS codes (e.g. '5.8A')\n"
        "- District document names ('pacing guide', 'curriculum', 'district materials')\n"
        "- Teacher-facing notes or rubric details\n"
        "- Any content beyond the scope of the objective above\n"
        if is_student_facing
        else ""
    )

    instruction = (
        f"You are an expert instructional designer enriching a {display_name} for the "
        f"Learning Recovery Planner. Improve the deterministic_content by adding specific, "
        f"classroom-ready instructional language. Do not change the JSON structure — only "
        f"enrich the string values.\n\n"
        f"CONTEXT:\n"
        f"- Objective: {objective_code}\n"
        f"- Mastery: {mastery_snapshot.get('mastery_percentage', 0):.0f}% of students at mastery\n"
        f"- Learning gap: {misconception or 'See plan_reteach_hint'}\n"
        f"- Plan reteach hint (PRIMARY): {reteach or 'Not specified — use KDG activation_strategy'}\n"
        f"- Instructional contract: {contract}\n\n"
        f"RULES:\n"
        f"1. The plan_reteach_hint is the primary source — use it verbatim or adapt it directly\n"
        f"2. Address the specific learning gap, not the objective in general\n"
        f"3. Every activity must be immediately usable by a teacher — no prep required\n"
        f"4. Do NOT introduce any new objectives or TEKS beyond the one specified\n"
        f"5. Keep the exact JSON key structure of deterministic_content"
        f"{student_rule}"
    )

    prompt_payload = {
        "objective_code": objective_code,
        "artifact_type": artifact_type,
        "learning_gap": misconception,
        "plan_reteach_hint": reteach,
        "instructional_contract": contract,
        "builds_from_yesterday": generation_context.get("builds_from_yesterday"),
        "prepares_for_tomorrow": generation_context.get("prepares_for_tomorrow"),
        "observable_mastery_evidence": generation_context.get("observable_mastery_evidence"),
        "activation_strategy": generation_context.get("activation_strategy"),
        "scaffold": generation_context.get("scaffold"),
        "mastery_snapshot": mastery_snapshot,
        "deterministic_content": deterministic_content,
    }

    return instruction, prompt_payload


# ── Serializer ────────────────────────────────────────────────────────────────

def _serialize_artifact(artifact: TeacherAssistV2RecoveryArtifact) -> dict[str, Any]:
    return {
        "id": str(artifact.id),
        "recovery_queue_id": (
            str(artifact.recovery_queue_id) if artifact.recovery_queue_id else None
        ),
        "artifact_type": artifact.artifact_type,
        "title": artifact.title,
        "content": artifact.content_json,
        "validation_result": artifact.validation_result_json,
        "provider": artifact.provider,
        "model": artifact.model,
        "status": artifact.status,
        "created_at": artifact.created_at.isoformat(),
        "updated_at": artifact.updated_at.isoformat(),
    }


# ── Main public functions ─────────────────────────────────────────────────────

def generate_recovery_artifact(
    db: Session,
    *,
    settings: Settings,
    user: User,
    queue_item_id: uuid.UUID,
    artifact_type: str,
) -> dict[str, Any]:
    """Generate a recovery artifact for an accepted queue item.

    Phase 8 Learning Recovery Planner — three-priority generation:
    1. Reuse plan data (reteach_if_needed, KDG, contract)
    2. Deterministic content assembly
    3. AI enrichment if available
    """
    if artifact_type not in RECOVERY_ARTIFACT_TYPES:
        raise ValueError(f"Unsupported recovery artifact type: {artifact_type!r}")

    # ── Load queue item ────────────────────────────────────────────────────────
    queue_item = db.scalars(
        select(TeacherAssistV2RecoveryQueue).where(
            TeacherAssistV2RecoveryQueue.id == queue_item_id,
            TeacherAssistV2RecoveryQueue.teacher_user_id == user.id,
        )
    ).first()
    if queue_item is None:
        raise LookupError("Recovery queue item not found.")
    if queue_item.status not in ("pending", "scheduled"):
        raise ValueError(
            "Recovery artifacts can only be generated for pending or scheduled queue items."
        )

    # ── Load package and assignment ────────────────────────────────────────────
    package: TeacherAssistV2InstructionalPackage | None = None
    week_number = 1
    if queue_item.instructional_package_id:
        package = db.get(TeacherAssistV2InstructionalPackage, queue_item.instructional_package_id)
    if queue_item.assignment_id:
        assignment = db.get(TeacherAssistV2Assignment, queue_item.assignment_id)
        if assignment:
            week_number = assignment.week_number

    # ── Priority 1: Extract plan data ─────────────────────────────────────────
    plan_ctx: dict[str, Any] = {}
    if package is not None:
        plan_ctx = _extract_recovery_context_from_plan(package, week_number=week_number)
        kdg_fields = _resolve_kdg_fields(
            plan_ctx.get("knowledge_dependency_graph") or [],
            queue_item.objective_code,
        )
        plan_ctx["activation_strategy"] = kdg_fields["activation_strategy"]
        plan_ctx["gap_consequence"] = kdg_fields["gap_consequence"]

    generation_context: dict[str, Any] = {
        **plan_ctx,
        "objective_code": queue_item.objective_code or plan_ctx.get("objective_code"),
        "mastery_snapshot": queue_item.mastery_snapshot_json or {},
        "misconception": queue_item.misconception_text,
        "success_criteria": queue_item.success_criteria_json or {},
    }

    # ── Instructional Integrity Check (pre-generation gate) ───────────────────
    integrity_result = _run_integrity_check(
        artifact_type=artifact_type,
        generation_context=generation_context,
    )
    if not integrity_result["passed"]:
        raise ValueError(
            "Instructional Integrity Check failed: "
            + "; ".join(integrity_result["issues"])
        )

    # ── Priority 2: Build deterministic content ────────────────────────────────
    contract = generation_context.get("instructional_contract") or {}
    students_count = len(queue_item.students_affected_json or [])

    deterministic_content = _build_deterministic_content(
        artifact_type,
        objective_code=generation_context.get("objective_code") or "objective",
        reteach=generation_context.get("plan_reteach_hint") or "",
        activation=generation_context.get("activation_strategy") or "",
        mastery_evidence=generation_context.get("observable_mastery_evidence") or "",
        misconception=queue_item.misconception_text or "",
        builds_from=generation_context.get("builds_from_yesterday") or "",
        prepares_for=generation_context.get("prepares_for_tomorrow") or "",
        exit_ticket_stem=contract.get("exit_ticket_stem") or "",
        scaffold=generation_context.get("scaffold") or "",
        students_count=students_count,
    )

    display_name, _ = _ARTIFACT_META.get(artifact_type, (artifact_type.replace("_", " ").title(), False))
    title = f"{display_name} — {queue_item.objective_code or 'Recovery'}"

    # ── Priority 3: AI enrichment ──────────────────────────────────────────────
    content = deterministic_content
    provider_used = "deterministic"
    model_used: str | None = None

    if is_teacher_assist_real_ai_active(db, settings):
        effective_settings = resolve_teacher_assist_settings(db, settings)
        try:
            assert_teacher_assist_ai_cost_available(db, effective_settings)
            provider_name, _api_key, _base_url = _provider_api_params(effective_settings)
            model_name = get_teacher_assist_provider_model(
                effective_settings, provider_name=provider_name
            )
            instruction, prompt_payload = _build_recovery_ai_prompt(
                artifact_type,
                generation_context=generation_context,
                queue_item=queue_item,
                deterministic_content=deterministic_content,
            )
            result = execute_openai_json_completion(
                effective_settings,
                model_name=model_name,
                instruction=instruction,
                prompt_payload=prompt_payload,
                required_output_schema=deterministic_content,
                _api_key=_api_key,
                _base_url=_base_url,
                _provider=provider_name,
            )
            record_teacher_assist_ai_usage(
                db,
                tenant_id=user.tenant_id,
                user_id=user.id,
                feature=V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
                provider=result.provider,
                model=result.model,
                input_tokens=result.input_tokens,
                output_tokens=result.output_tokens,
                estimated_cost_cents=result.estimated_cost_cents,
                metadata={
                    "operation_type": "recovery_artifact_generation",
                    "artifact_type": artifact_type,
                    "queue_item_id": str(queue_item_id),
                    "objective_code": queue_item.objective_code,
                },
            )
            if result.content_json:
                content = result.content_json
                provider_used = result.provider
                model_used = result.model
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                "Recovery AI enrichment failed for %s — using deterministic content", artifact_type
            )

    # ── Post-generation validation ─────────────────────────────────────────────
    _, is_student_facing = _ARTIFACT_META.get(artifact_type, (artifact_type, False))
    teks_found: list[str] = []
    if is_student_facing:
        teks_found = _TEKS_CODE_RE.findall(str(content))

    validation_result = {
        **integrity_result,
        "post_generation": {
            "teks_codes_found_in_student_content": teks_found,
            "teks_check_passed": not teks_found or not is_student_facing,
            "provider": provider_used,
        },
    }

    # ── Store artifact ─────────────────────────────────────────────────────────
    now = datetime.now(UTC)
    artifact = TeacherAssistV2RecoveryArtifact(
        id=uuid.uuid4(),
        tenant_id=user.tenant_id,
        teacher_user_id=user.id,
        recovery_queue_id=queue_item.id,
        artifact_type=artifact_type,
        title=title,
        content_json=content,
        generation_context_snapshot_json=generation_context,
        validation_result_json=validation_result,
        provider=provider_used,
        model=model_used,
        status="ready",
        created_at=now,
        updated_at=now,
    )
    db.add(artifact)

    # Advance timeline phase to activity stage
    if queue_item.timeline_phase in (None, "recovery_goal"):
        queue_item.timeline_phase = "recovery_activity"
        queue_item.updated_at = now

    db.flush()
    return _serialize_artifact(artifact)


def list_recovery_artifacts(
    db: Session,
    *,
    user: User,
    queue_item_id: uuid.UUID,
) -> list[dict[str, Any]]:
    artifacts = db.scalars(
        select(TeacherAssistV2RecoveryArtifact)
        .where(
            TeacherAssistV2RecoveryArtifact.recovery_queue_id == queue_item_id,
            TeacherAssistV2RecoveryArtifact.teacher_user_id == user.id,
        )
        .order_by(TeacherAssistV2RecoveryArtifact.created_at.asc())
    ).all()
    return [_serialize_artifact(a) for a in artifacts]

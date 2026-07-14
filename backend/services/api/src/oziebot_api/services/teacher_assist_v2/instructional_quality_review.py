"""AI-powered Instructional Quality Review for TeacherAssist v2.

Runs as Phase 3b after each artifact is generated and persisted. Acts as an
instructional coach — evaluates quality across 8 dimensions, flags issues,
and applies targeted auto-revisions to safe-to-change fields.

Dimensions reviewed:
  1. Curriculum Fidelity — objectives, TEKS, resource alignment
  2. Instructional Continuity — yesterday/tomorrow references
  3. Classroom Authenticity — teacher language, realistic activities
  4. Instructional Rhythm — all 9 steps present and complete
  5. Lesson Variety — hooks, strategies, closures
  6. Images — instructional, age-appropriate, non-corporate, non-repetitive
  7. Cognitive Load — pacing, concept density, vocabulary load
  8. Teacher Experience — "would I teach this tomorrow?"

What the reviewer MAY auto-revise:
  - closure text (if generic)
  - transition text (if missing or abrupt)
  - preteach engagement_question (if generic)
  - checks_for_understanding (if surface-level)

What the reviewer MUST NOT change:
  - learning_target, teks_alignment, curriculum_resource selection
  - objectives, TEKS codes, pacing guide alignment
  - the core instructional sequence or subject order
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
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

logger = logging.getLogger(__name__)

# ── Review version ───────────────────────────────────────────────────────────
# Bump this when the reviewer instruction or scoring logic changes.
# Frontend uses this to detect stale persisted reviews.
QUALITY_REVIEW_VERSION = "2026-07-11"

# ── Status values ─────────────────────────────────────────────────────────────

REVIEW_STATUS_READY = "ready"
REVIEW_STATUS_READY_WITH_NOTES = "ready_with_notes"
REVIEW_STATUS_NEEDS_REVIEW = "needs_review"
REVIEW_STATUS_MISSING_RESOURCE = "missing_curriculum_resource"
REVIEW_STATUS_IMAGE_REVIEW = "image_review_needed"
REVIEW_STATUS_CONTINUITY = "continuity_review_needed"

# Artifact types that receive the full 8-dimension quality review.
REVIEWABLE_TYPES = frozenset({"daily_lesson_plan", "student_lesson_deck", "subject_slide_deck"})

# ── Review output schema ──────────────────────────────────────────────────────

QUALITY_REVIEW_SCHEMA: dict[str, Any] = {
    "overall_score": 0,
    "review_status": "string",
    "strengths": ["string"],
    "suggestions": ["string"],
    "corrections_applied": ["string"],
    "teacher_attention_needed": ["string"],
    "flags": {
        "missing_curriculum_resource": False,
        "image_review_needed": False,
        "continuity_issue": False,
        "placeholder_language": False,
        "incomplete_rhythm": False,
        "weak_teacher_experience": False,
        "learning_goal_discipline": False,
    },
    "categories": {
        "curriculum_fidelity": {"score": 0, "findings": ["string"]},
        "instructional_continuity": {"score": 0, "findings": ["string"]},
        "classroom_authenticity": {"score": 0, "findings": ["string"]},
        "instructional_rhythm": {"score": 0, "findings": ["string"]},
        "lesson_variety": {"score": 0, "findings": ["string"]},
        "images": {"score": 0, "findings": ["string"]},
        "cognitive_load": {"score": 0, "findings": ["string"]},
        "teacher_experience": {"score": 0, "finding": "string"},
    },
    "revisions": {
        "subjects": {
            "<subject_name>": {
                "closure": "string | null",
                "transition": "string | null",
                "engagement_question": "string | null",
                "checks_for_understanding": ["string"],
            }
        }
    },
    "review_scope": {
        "artifact_type": "string",
        "days_analyzed": 0,
        "strands_analyzed": 0,
        "scope_note": "string",
    },
}

# ── Reviewer instruction ──────────────────────────────────────────────────────

_REVIEWER_SYSTEM_INSTRUCTION = """\
You are an expert instructional coach reviewing AI-generated elementary classroom materials.

YOUR ROLE IS NOT TO REWRITE THE CURRICULUM.
Your role is to identify instructional quality issues and apply targeted improvements
to specific fields where you have authority to revise.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REVIEW DIMENSIONS — score each 0–100
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. CURRICULUM FIDELITY
   - Objectives align with curriculum and TEKS
   - No generic placeholder language ("choose a story", "any text", "selected book")
   - Curriculum resource matches today's lesson objective
   - TEKS codes are real and correctly described

2. INSTRUCTIONAL CONTINUITY
   - "Yesterday we..." references are TRUE based on verified_previous_learning
   - Lesson builds logically from prior day
   - No invented continuity — if yesterday's data is absent, no prior-day claims
   - Transition to next day is logical

3. CLASSROOM AUTHENTICITY
   - Teacher language reads as what a real teacher would say — not bureaucratic jargon
   - Student activities are achievable in the stated time block
   - Instructional delivery profile is honored (curriculum_text_access, closure_required)
   - No impossible classroom assumptions ("students will have access to 30 laptops")

4. INSTRUCTIONAL RHYTHM
   Verify all 9 steps are present AND substantive (not filler text):
   Activation → Preteach → Learning Target → Teacher Modeling → Guided Practice →
   Independent Practice → CFU (3 questions) → Closure → Transition
   A step with content like "students will practice" is NOT substantive.

   LEARNING GOAL DISCIPLINE — check for each subject block:
   a) MAX 2 learning goals per block. If learning_goals has 3+ entries, or learning_target
      contains 3+ "I can" statements — flag as a critical instructional rhythm defect.
      Score this dimension no higher than 60 when any block has >2 goals.
   b) READING goals must be about comprehension/analysis. Writing goals must be about
      composing/crafting. If a Reading block lists writing goals, or Writing lists reading
      goals, that is a goal-discipline violation — flag it.
   c) CLOSURE must align to the block's learning goals:
      - Reading closure assesses the Reading goal (not writing)
      - Writing closure assesses the Writing goal (not reading comprehension)
      - A combined "Reading AND Writing" exit ticket is only appropriate if the curriculum
        explicitly requires a cross-strand culminating task
   d) WRITING must have its own learning goals — not inherit or mirror Reading goals.
      If writing blocks share identical goals with reading blocks, flag it.

5. LESSON VARIETY
   - Hook / activation is specific to curriculum, not generic
   - Discussion prompts probe thinking, not just recall
   - Closure is specific to today's learning target, not generic
   - Engagement strategies vary appropriately across recent lessons

   STRATEGY CATEGORIES — apply different standards to each:

   CORE PRACTICES (Teacher Modeling, Guided Practice, Independent Practice, Formative Check,
   Closure) — these are EXPECTED every lesson. Never flag frequency of core practices.
   Teacher Modeling ×5 days is NOT an issue — it is the teaching model.

   INSTRUCTIONAL SUPPORTS (Graphic Organizer, Anchor Chart, Worked Example) — flag ONLY when:
   - The same support is used with substantially identical instructions on adjacent days, AND
   - There is no intentional progression in complexity.
   Using a graphic organizer 3 days is fine if each day builds on the prior one.

   ENGAGEMENT STRATEGIES (Turn and Talk, Think-Pair-Share, Quick Write, etc.) — flag ONLY when:
   - The same strategy appears in >40% of lesson blocks analyzed, AND
   - The directions or prompts are substantially identical (copy-paste repetition), AND
   - The repetition does not serve an explicit pedagogical purpose.

   NEVER write findings like "Graphic Organizer ×3 — overused" or "Teacher Modeling ×4 — overused".
   Findings must be specific: name which days and describe what was repeated.
   Example: "Turn and Talk was used with nearly identical partner prompts on Days 1, 3, and 4 in Reading."
   If the organizers or strategies differ meaningfully, DO NOT flag them.

6. IMAGES (slide decks only)
   - Image search terms describe children in educational activities, not adults/business
   - Each slide has a distinct instructional_activity
   - No repeated image concepts within the same day
   - Image visually reinforces the slide's learning objective

7. COGNITIVE LOAD
   - Lesson introduces an achievable number of new concepts (max 2 per block)
   - Vocabulary preview covers the words students need — no more, no less
   - Teacher talk time is balanced with student activity
   - Pacing is realistic for the grade level

8. TEACHER EXPERIENCE
   Ask: "If I were an experienced classroom teacher, would I confidently teach this lesson
   tomorrow WITHOUT editing?"
   Score 100 if yes. Score lower for each item a teacher would need to fix.
   Immediate deductions:
   - Any block with 3+ learning goals: -20 points (unusable in classroom — teacher cannot manage)
   - Reading and Writing goals merged into one block: -15 points
   - Closure/exit ticket misaligned to block goal: -10 points per block

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHAT YOU MAY REVISE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

For daily_lesson_plan, you may revise per subject block:
  - closure: if generic ("share one thing"), rewrite with today's learning target;
    if closure assesses the wrong block's skill (e.g. Reading closure about writing),
    rewrite to align with the block's learning_goals
  - transition: if missing or abrupt, write a specific 1–2 sentence handoff
  - engagement_question: if generic, rewrite as curriculum-specific curiosity question
  - checks_for_understanding: if questions are recall-level, elevate to thinking-level

DO NOT REVISE:
  - learning_target (TEKS authority)
  - learning_goals (TEKS authority — if too many goals exist, flag but do not silently drop entries)
  - teks_alignment (TEKS authority)
  - curriculum_resource (resource bank authority)
  - resource_rationale (resource bank authority)
  - teacher_says in teacher_modeling_script (curriculum delivery authority)
  - objectives, TEKS codes, pacing guide content

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCORING GUIDE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

90–100: Excellent — teacher can use as-is
75–89:  Good — minor suggestions; ready with notes
60–74:  Adequate — teacher should review suggestions
< 60:   Needs revision before use

review_status values:
  "ready" — overall_score >= 90 and no flags
  "ready_with_notes" — overall_score >= 75
  "needs_review" — overall_score < 75 or flags raised
  "missing_curriculum_resource" — flag.missing_curriculum_resource is true
  "image_review_needed" — flag.image_review_needed is true (combine with primary status)
  "continuity_review_needed" — flag.continuity_issue is true

Use the most severe status when multiple flags apply.

BE DECISIVE. Score accurately. Do not be overly generous.
A lesson with generic closures and no preteach does NOT score 95.
"""


def _build_review_payload(
    content: dict[str, Any],
    artifact_type: str,
    generation_context: dict[str, Any],
    prior_artifacts_summary: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the prompt_payload for the quality review AI call.

    Includes the artifact content + only the context fields needed for
    quality evaluation (not the full generation context).
    """
    return {
        "artifact_type": artifact_type,
        "artifact_content": content,
        "grade_code": generation_context.get("grade_code"),
        "grade_display_name": generation_context.get("grade_display_name"),
        "instructional_delivery_profile": generation_context.get("instructional_delivery_profile"),
        "curriculum_resource_alignment": generation_context.get("resource_alignment_map"),
        "instructional_variety": generation_context.get("variety_context"),
        "verified_previous_learning": generation_context.get("verified_previous_learning_summary"),
        "prior_artifacts_summary": prior_artifacts_summary or [],
        "review_instructions": (
            "Review this artifact across all 8 quality dimensions. "
            "Score each dimension 0–100 and calculate an overall_score as the weighted average. "
            "Provide: 2–3 strengths, 2–4 actionable suggestions, corrections you are applying, "
            "and items the teacher must address manually. "
            "In the revisions block, provide ONLY improved text for fields you are authorized to change. "
            "Set a field to null if no improvement is needed. "
            "For daily_lesson_plan: assess each subject block separately; "
            "revisions are keyed by subject_name."
        ),
    }


def _determine_status_from_report(report: dict[str, Any]) -> str:
    """Map quality report to artifact status string."""
    flags = report.get("flags") or {}
    score = int(report.get("overall_score") or 0)

    if flags.get("missing_curriculum_resource"):
        return REVIEW_STATUS_MISSING_RESOURCE
    if flags.get("continuity_issue"):
        return REVIEW_STATUS_CONTINUITY
    if score >= 90 and not any(flags.values()):
        return REVIEW_STATUS_READY
    if score >= 75:
        status = REVIEW_STATUS_READY_WITH_NOTES
        if flags.get("image_review_needed"):
            return REVIEW_STATUS_IMAGE_REVIEW
        return status
    return REVIEW_STATUS_NEEDS_REVIEW


def _apply_revisions(
    content: dict[str, Any],
    artifact_type: str,
    revisions: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    """Apply targeted revisions from the quality report to the artifact content.

    Returns (updated_content, list_of_applied_corrections).
    Only modifies fields the reviewer is authorized to change.
    Mutations are in-place on a copy of content.
    """
    import copy

    content = copy.deepcopy(content)
    applied: list[str] = []

    if artifact_type == "daily_lesson_plan":
        subject_revisions = revisions.get("subjects") or {}
        for block in content.get("subjects") or []:
            subject_name = block.get("subject_name") or ""
            block_revisions = subject_revisions.get(subject_name) or {}
            if not block_revisions:
                continue

            if block_revisions.get("closure"):
                old = block.get("closure") or ""
                block["closure"] = block_revisions["closure"]
                if old != block["closure"]:
                    applied.append(f"{subject_name}: closure revised")

            if block_revisions.get("transition"):
                old = block.get("transition") or ""
                block["transition"] = block_revisions["transition"]
                if old != block["transition"]:
                    applied.append(f"{subject_name}: transition revised")

            if block_revisions.get("engagement_question"):
                preteach = block.get("preteach")
                if isinstance(preteach, dict):
                    old = preteach.get("engagement_question") or ""
                    preteach["engagement_question"] = block_revisions["engagement_question"]
                    if old != preteach["engagement_question"]:
                        applied.append(f"{subject_name}: engagement question revised")

            if block_revisions.get("checks_for_understanding"):
                new_cfu = block_revisions["checks_for_understanding"]
                if isinstance(new_cfu, list) and len(new_cfu) >= 3:
                    block["checks_for_understanding"] = new_cfu
                    applied.append(f"{subject_name}: checks for understanding revised")

    return content, applied


def review_artifact(
    db: Session,
    *,
    settings: Settings,
    user: User,
    tenant_id,
    package_id,
    artifact_type: str,
    content: dict[str, Any],
    generation_context: dict[str, Any],
    prior_artifacts_summary: list[dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None, list[str]]:
    """Run AI quality review on a single generated artifact.

    Returns:
        (quality_report, revised_content_or_None, corrections_applied)

    quality_report: structured review dict (stored in metadata_json)
    revised_content: updated artifact content with auto-revisions applied (or None)
    corrections_applied: list of correction descriptions for the report
    """
    if artifact_type not in REVIEWABLE_TYPES:
        return {}, None, []

    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name, _api_key, _base_url = _get_provider_params(settings)
    if provider_name not in {"openai", "gemini"}:
        # Skip review in mock/demo mode — return a placeholder report.
        return _mock_report(), None, []

    model_name = get_teacher_assist_provider_model(settings, provider_name=provider_name)
    prompt_payload = _build_review_payload(
        content, artifact_type, generation_context, prior_artifacts_summary
    )

    try:
        result = execute_openai_json_completion(
            effective_settings,
            model_name=model_name,
            instruction=_REVIEWER_SYSTEM_INSTRUCTION,
            prompt_payload=prompt_payload,
            required_output_schema=QUALITY_REVIEW_SCHEMA,
            _api_key=_api_key,
            _base_url=_base_url,
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
                "operation_type": "quality_review",
                "artifact_type": artifact_type,
                "package_id": str(package_id),
                "related_entity_type": "instructional_package",
                "related_entity_id": str(package_id),
            },
        )
        report = result.content_json or {}
    except Exception:
        logger.exception(
            "package=%s: quality review AI call failed for artifact_type=%s — skipping review",
            package_id,
            artifact_type,
        )
        return {}, None, []

    # Apply auto-revisions if the report includes them
    revisions = report.get("revisions") or {}
    revised_content: dict[str, Any] | None = None
    corrections_applied: list[str] = list(report.get("corrections_applied") or [])

    if revisions and artifact_type == "daily_lesson_plan":
        updated_content, auto_corrections = _apply_revisions(content, artifact_type, revisions)
        if auto_corrections:
            revised_content = updated_content
            corrections_applied = list(set(corrections_applied + auto_corrections))
            report["corrections_applied"] = corrections_applied
            # Clear relevant flags so the post-correction status reflects the fixed state.
            correction_text = " ".join(auto_corrections).lower()
            flags = report.setdefault("flags", {})
            if any(f in correction_text for f in ("closure", "engagement question")):
                flags["placeholder_language"] = False
            if "transition" in correction_text:
                flags["incomplete_rhythm"] = False

    # Strip the revisions block from the stored report (corrections already applied).
    report.pop("revisions", None)

    # Group corrections by field type for compact display in the UI.
    if corrections_applied:
        _type_map = {
            "closure": "closure",
            "transition": "transition",
            "engagement question": "engagement_question",
            "checks for understanding": "checks_for_understanding",
        }
        _groups: dict[str, int] = {}
        for c in corrections_applied:
            cl = c.lower()
            for keyword, group in _type_map.items():
                if keyword in cl:
                    _groups[group] = _groups.get(group, 0) + 1
                    break
            else:
                _groups["other"] = _groups.get("other", 0) + 1
        report["correction_groups"] = _groups

    # Annotate review scope so the UI can explain what was analyzed.
    _scope_notes = {
        "daily_lesson_plan": "Single lesson plan (1 day · all strands)",
        "student_lesson_deck": "Single slide deck (1 lesson)",
        "subject_slide_deck": "Single slide deck (1 subject · 1 week)",
    }
    report.setdefault("review_scope", {})
    report["review_scope"]["artifact_type"] = artifact_type
    report["review_scope"]["scope_note"] = _scope_notes.get(
        artifact_type, f"Single {artifact_type.replace('_', ' ')}"
    )

    # Compute final status after any flag corrections from auto-revisions.
    report["review_status"] = _determine_status_from_report(report)
    report["reviewed_at"] = datetime.now(UTC).isoformat()
    report["review_version"] = QUALITY_REVIEW_VERSION

    logger.info(
        "package=%s: quality review complete for artifact_type=%s score=%s status=%s corrections=%d",
        package_id,
        artifact_type,
        report.get("overall_score"),
        report.get("review_status"),
        len(corrections_applied),
    )
    return report, revised_content, corrections_applied


def _get_provider_params(settings: Settings) -> tuple[str, str | None, str | None]:
    provider = (settings.teacher_assist_ai_provider or "mock").strip().lower()
    if provider == "openai":
        return provider, settings.teacher_assist_openai_api_key, None
    if provider == "gemini":
        base_url = (
            settings.teacher_assist_gemini_base_url or ""
        ).strip() or "https://generativelanguage.googleapis.com/v1beta/openai"
        return provider, settings.teacher_assist_gemini_api_key, base_url
    return provider, None, None


def _mock_report() -> dict[str, Any]:
    return {
        "overall_score": 85,
        "review_status": REVIEW_STATUS_READY_WITH_NOTES,
        "strengths": ["Lesson structure follows the 9-step rhythm."],
        "suggestions": ["Review in a live AI-enabled environment for full quality analysis."],
        "corrections_applied": [],
        "teacher_attention_needed": [],
        "flags": {
            "missing_curriculum_resource": False,
            "image_review_needed": False,
            "continuity_issue": False,
            "placeholder_language": False,
            "incomplete_rhythm": False,
            "weak_teacher_experience": False,
        },
        "categories": {
            "curriculum_fidelity": {"score": 85, "findings": []},
            "instructional_continuity": {"score": 85, "findings": []},
            "classroom_authenticity": {"score": 85, "findings": []},
            "instructional_rhythm": {"score": 85, "findings": []},
            "lesson_variety": {"score": 85, "findings": []},
            "images": {"score": 85, "findings": []},
            "cognitive_load": {"score": 85, "findings": []},
            "teacher_experience": {
                "score": 85,
                "finding": "Mock review — enable AI for full evaluation.",
            },
        },
        "reviewed_at": datetime.now(UTC).isoformat(),
        "review_version": QUALITY_REVIEW_VERSION,
    }


def _fallback_report(artifact_type: str) -> dict[str, Any]:
    return {
        "overall_score": 0,
        "review_status": REVIEW_STATUS_NEEDS_REVIEW,
        "strengths": [],
        "suggestions": ["Quality review failed — manual teacher review recommended."],
        "corrections_applied": [],
        "teacher_attention_needed": ["Review failed. Please verify lesson quality manually."],
        "flags": {
            "missing_curriculum_resource": False,
            "image_review_needed": False,
            "continuity_issue": False,
            "placeholder_language": False,
            "incomplete_rhythm": False,
            "weak_teacher_experience": True,
        },
        "categories": {
            "curriculum_fidelity": {"score": 0, "findings": ["Review failed."]},
            "instructional_continuity": {"score": 0, "findings": ["Review failed."]},
            "classroom_authenticity": {"score": 0, "findings": ["Review failed."]},
            "instructional_rhythm": {"score": 0, "findings": ["Review failed."]},
            "lesson_variety": {"score": 0, "findings": ["Review failed."]},
            "images": {"score": 0, "findings": ["Review failed."]},
            "cognitive_load": {"score": 0, "findings": ["Review failed."]},
            "teacher_experience": {"score": 0, "finding": "Review failed — manual check needed."},
        },
        "reviewed_at": datetime.now(UTC).isoformat(),
    }

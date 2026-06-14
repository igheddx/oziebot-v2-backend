"""OpenAI generation for TeacherAssist v2 instructional package artifacts."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_mode import is_teacher_assist_real_ai_active
from oziebot_api.services.teacher_assist.ai_usage import (
    assert_teacher_assist_ai_cost_available,
    record_teacher_assist_ai_usage,
)
from oziebot_api.services.teacher_assist.openai_json_client import execute_openai_json_completion
from oziebot_api.services.teacher_assist.prompt_contracts import (
    V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
    V2_PACKAGE_ARTIFACT_FEATURES,
)
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_provider_model
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import resolve_pacing_day_plan

V2_PACKAGE_PROMPT_VERSION = "v2-instructional-package-v1"

DAILY_LESSON_PLAN_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "subjects": [
        {
            "subject_name": "string",
            "objective": "string",
            "mini_lesson": "string",
            "teacher_actions": ["string"],
            "student_activity": ["string"],
            "materials": ["string"],
            "assessment": "string",
            "notes": "string",
            "direct_instruction": "string",
            "guided_practice": "string",
            "independent_practice": "string",
            "checks_for_understanding": ["string"],
            "closure": "string",
        }
    ],
}

SLIDE_DECK_SCHEMA: dict[str, Any] = {
    "title": "string",
    "slides": [{"title": "string", "bullets": ["string"]}],
}

GENERIC_ARTIFACT_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

QUIZ_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "questions": [{"prompt": "string", "objective_id": "string"}],
    "answer_key": [{"prompt": "string", "answer": "string"}],
    "objective_mapping": [{"objective_id": "string", "question_prompt": "string"}],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

RUBRIC_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "criteria": [
        {
            "name": "string",
            "point_value": "number",
            "performance_levels": [{"label": "string", "description": "string", "points": "number"}],
        }
    ],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

NEWSLETTER_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "what_students_will_learn": ["string"],
    "reminders": ["string"],
    "upcoming_focus": ["string"],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

ASSIGNMENT_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "objective_alignment": "string",
    "passage_title": "string",
    "passage_text": "string",
    "student_instructions": ["string"],
    "questions": [{"prompt": "string", "type": "string"}],
    "success_criteria": ["string"],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

WRITING_RESPONSE_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "objective_alignment": "string",
    "writing_prompt": "string",
    "student_instructions": ["string"],
    "sentence_starters": ["string"],
    "success_criteria": ["string"],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

ARTIFACT_SCHEMAS: dict[str, dict[str, Any]] = {
    "daily_lesson_plan": DAILY_LESSON_PLAN_SCHEMA,
    "subject_slide_deck": SLIDE_DECK_SCHEMA,
    "quiz": QUIZ_SCHEMA,
    "rubric": RUBRIC_SCHEMA,
    "assignment": ASSIGNMENT_SCHEMA,
    "writing_response": WRITING_RESPONSE_SCHEMA,
    "parent_newsletter_summary": NEWSLETTER_SCHEMA,
}


def _schema_for_artifact(artifact_type: str) -> dict[str, Any]:
    return ARTIFACT_SCHEMAS.get(artifact_type, GENERIC_ARTIFACT_SCHEMA)


_OBJECTIVE_ALIGNMENT_DIRECTIVE = (
    "REQUIRED: Align ALL content strictly to the pacing guide objectives listed in resolved_objectives. "
    "Each objective's code and description must be reflected in the generated content. "
    "If resolved_daily_topics is provided, structure the content around those specific topics in order. "
    "If resolved_assessment_checks is provided, incorporate those checks into activities or questions. "
    "Do NOT use generic or invented objectives — only use what is in resolved_objectives."
)


def _instruction_for_artifact(artifact_type: str) -> str:
    instructions = {
        "daily_lesson_plan": (
            "Generate a teacher-usable daily lesson plan with objectives, materials, direct instruction, "
            "guided practice, independent practice, checks for understanding, closure, and teacher notes "
            "for each subject block. Use resolved_day_plan for the specific day's focus, daily_topic, "
            "objective_focus, and assessment_check. "
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE + " "
            "Only reference materials listed in pacing_context, district_document_context, and "
            "teacher_document_context. Do not invent textbook or curriculum names."
        ),
        "subject_slide_deck": (
            "Generate classroom-ready slide deck content with clear titles and concise bullet points. "
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE + " "
            "Build one slide per daily topic in resolved_daily_topics. "
            "Use extracted district curriculum content and teacher supplemental document content for slide body text. "
            "Do not invent textbook or curriculum names."
        ),
        "quiz": (
            "Generate a quiz whose questions directly test each objective in resolved_objectives. "
            "Include one or more questions per objective code; map each question to its objective_id. "
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE + " "
            "Base questions on the extracted document content — do not rely on filenames alone."
        ),
        "rubric": (
            "Generate a rubric whose criteria map directly to resolved_objectives. "
            "Name each criterion after the objective it measures. "
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE + " "
            "Use the extracted district and teacher document content to clarify performance expectations."
        ),
        "assignment": (
            "Generate a written assignment grounded in the pacing guide objectives and source materials. "
            "Set passage_text from extracted document content or district curriculum excerpts — do not invent text. "
            "Set objective_alignment to a clear sentence stating which objective(s) students are practicing. "
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE + " "
            "Student instructions and questions must reference the specific objective(s) students are working toward."
        ),
        "writing_response": (
            "Generate a writing response prompt that asks students to demonstrate the pacing guide objective(s). "
            "Set writing_prompt to a specific, objective-aligned writing task — not a generic prompt. "
            "Set objective_alignment to a clear sentence naming the objective(s) this addresses. "
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE + " "
            "Sentence starters should scaffold the specific objective language."
        ),
        "parent_newsletter_summary": (
            "Generate a parent-friendly weekly newsletter summary. "
            "what_students_will_learn must list the actual objectives from resolved_objectives in plain language. "
            "Use extracted document content only as instructional context; keep the tone family friendly."
        ),
    }
    default = (
        f"Generate a teacher-usable {artifact_type.replace('_', ' ')}. "
        + _OBJECTIVE_ALIGNMENT_DIRECTIVE
    )
    return instructions.get(artifact_type, default)


def _normalize_artifact_content(artifact_type: str, content: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(content)
    if artifact_type == "quiz" and not normalized.get("sections"):
        sections = []
        questions = normalized.get("questions") or []
        if questions:
            sections.append(
                {
                    "heading": "Questions",
                    "bullets": [str(item.get("prompt") or item) for item in questions],
                }
            )
        answer_key = normalized.get("answer_key") or []
        if answer_key:
            sections.append(
                {
                    "heading": "Answer Key",
                    "bullets": [
                        f"{item.get('prompt', 'Question')}: {item.get('answer', '')}"
                        for item in answer_key
                        if isinstance(item, dict)
                    ],
                }
            )
        normalized["sections"] = sections
    if artifact_type == "rubric" and not normalized.get("sections"):
        criteria = normalized.get("criteria") or []
        normalized["sections"] = [
            {
                "heading": "Rubric Criteria",
                "bullets": [
                    f"{item.get('name', 'Criterion')} ({item.get('point_value', 0)} pts)"
                    for item in criteria
                    if isinstance(item, dict)
                ],
            }
        ]
    if artifact_type == "parent_newsletter_summary" and not normalized.get("sections"):
        normalized["sections"] = [
            {"heading": "What students will learn", "bullets": normalized.get("what_students_will_learn") or []},
            {"heading": "Reminders", "bullets": normalized.get("reminders") or []},
            {"heading": "Upcoming focus", "bullets": normalized.get("upcoming_focus") or []},
        ]
    if not normalized.get("sections") and artifact_type not in {"daily_lesson_plan", "subject_slide_deck"}:
        normalized.setdefault(
            "sections",
            [{"heading": "Overview", "body": normalized.get("summary") or "Generated instructional resource."}],
        )
    return normalized


def generate_v2_instructional_artifact(
    db: Session,
    *,
    settings: Settings,
    user: User,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    artifact_type: str,
    generation_context: dict[str, Any],
    week: dict[str, Any],
    subject_meta: dict[str, Any] | None = None,
    week_subject: dict[str, Any] | None = None,
    day_label: str | None = None,
    title_hint: str | None = None,
) -> dict[str, Any] | None:
    if not is_teacher_assist_real_ai_active(db, settings):
        return None

    effective_settings = resolve_teacher_assist_settings(db, settings)
    assert_teacher_assist_ai_cost_available(db, effective_settings)
    model_name = get_teacher_assist_provider_model(effective_settings, provider_name="openai")
    feature = V2_PACKAGE_ARTIFACT_FEATURES.get(artifact_type, V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE)

    # Surface pacing guide objectives and grounding fields at the top level so the AI
    # instruction directives can reference them by name without the model having to
    # discover them buried inside week_subject.pacing_context.
    resolved_objectives = [
        {"code": obj.get("objective_code"), "description": obj.get("description")}
        for obj in (week_subject or {}).get("objectives") or []
        if obj.get("objective_code") or obj.get("description")
    ]
    pacing_ctx = (week_subject or {}).get("pacing_context") or {}
    resolved_daily_topics = [
        str(day["daily_topic"])
        for day in pacing_ctx.get("days") or []
        if day.get("daily_topic")
    ]
    resolved_assessment_checks = [
        str(day["assessment_check"])
        for day in pacing_ctx.get("days") or []
        if day.get("assessment_check")
    ]
    resolved_day_plan = resolve_pacing_day_plan(week_subject, day_label) if week_subject and day_label else None

    prompt_payload = {
        "prompt_version": V2_PACKAGE_PROMPT_VERSION,
        "artifact_type": artifact_type,
        "title_hint": title_hint,
        "day_label": day_label,
        "school_year": generation_context.get("school_year"),
        "state_id": generation_context.get("state_id"),
        "district_id": generation_context.get("district_id"),
        "school_id": generation_context.get("school_id"),
        "grade_id": generation_context.get("grade_id"),
        "subjects": generation_context.get("subjects"),
        "pacing_guide_ids": generation_context.get("pacing_guide_ids"),
        "week": week,
        "subject": subject_meta,
        "week_subject": week_subject,
        # Top-level grounding fields — referenced directly by the instruction directives.
        "resolved_objectives": resolved_objectives,
        "resolved_daily_topics": resolved_daily_topics,
        "resolved_assessment_checks": resolved_assessment_checks,
        "resolved_day_plan": resolved_day_plan,
        "pacing_materials": generation_context.get("pacing_materials"),
        "district_materials_summary": generation_context.get("district_materials_summary"),
        "district_document_context": generation_context.get("district_document_context"),
        "district_link_context": generation_context.get("district_link_context"),
        "teacher_supplemental_files": generation_context.get("teacher_supplemental_files"),
        "teacher_supplemental_links": generation_context.get("teacher_supplemental_links"),
        "teacher_supplemental_notes": generation_context.get("teacher_supplemental_notes"),
        "teacher_document_context": generation_context.get("teacher_document_context"),
        "teacher_link_context": generation_context.get("teacher_link_context"),
        "ai_readiness_summary": generation_context.get("ai_readiness_summary"),
        "selected_output_types": generation_context.get("selected_output_types"),
        "teaching_order": generation_context.get("teaching_order"),
        "generation_mode": generation_context.get("generation_mode"),
        "teacher_generation_notes": generation_context.get("teacher_generation_notes"),
        "existing_package_assignments": generation_context.get("existing_package_assignments"),
        "require_distinct_from_existing": generation_context.get("require_distinct_from_existing"),
    }

    instruction = _instruction_for_artifact(artifact_type)
    if generation_context.get("generation_mode") == "package_additional_assignment":
        instruction += (
            " This is an ADDITIONAL assignment for an existing instructional package. "
            "It must be clearly different from existing_package_assignments in focus, format, and tasks. "
            "Follow teacher_generation_notes closely."
        )
    result = execute_openai_json_completion(
        effective_settings,
        model_name=model_name,
        instruction=instruction,
        prompt_payload=prompt_payload,
        required_output_schema=_schema_for_artifact(artifact_type),
    )
    record_teacher_assist_ai_usage(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        feature=feature,
        provider=result.provider,
        model=result.model,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        estimated_cost_cents=result.estimated_cost_cents,
        metadata={
            "operation_type": feature,
            "artifact_type": artifact_type,
            "package_id": str(package_id),
            "related_entity_type": "instructional_package",
            "related_entity_id": str(package_id),
            "teacher_review_required": True,
        },
    )
    return _normalize_artifact_content(artifact_type, result.content_json)

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

ARTIFACT_SCHEMAS: dict[str, dict[str, Any]] = {
    "daily_lesson_plan": DAILY_LESSON_PLAN_SCHEMA,
    "subject_slide_deck": SLIDE_DECK_SCHEMA,
    "quiz": QUIZ_SCHEMA,
    "rubric": RUBRIC_SCHEMA,
    "parent_newsletter_summary": NEWSLETTER_SCHEMA,
}


def _schema_for_artifact(artifact_type: str) -> dict[str, Any]:
    return ARTIFACT_SCHEMAS.get(artifact_type, GENERIC_ARTIFACT_SCHEMA)


def _instruction_for_artifact(artifact_type: str) -> str:
    instructions = {
        "daily_lesson_plan": (
            "Generate a teacher-usable daily lesson plan with objectives, materials, direct instruction, "
            "guided practice, independent practice, checks for understanding, closure, and teacher notes "
            "for each subject block. Use the pacing guide day plan in week_subject.pacing_context.days "
            "that matches day_label. Only reference materials listed in pacing_context, catalog_resources, "
            "district_materials_summary, district_document_context, and teacher_document_context. "
            "Use extracted document content below when creating daily teaching plans, slide deck content, "
            "quiz questions, exit tickets, written assignments, rubrics, and newsletters. "
            "Do not invent textbook or curriculum names."
        ),
        "subject_slide_deck": (
            "Generate classroom-ready slide deck content with clear titles and concise bullet points. "
            "Use extracted district curriculum content and teacher supplemental document content when available."
        ),
        "quiz": (
            "Generate a quiz with questions, answer key, objective mapping, and teacher-facing sections. "
            "Use the extracted document content and do not rely on filenames alone."
        ),
        "rubric": (
            "Generate a rubric with criteria, point values, and performance levels aligned to objectives, "
            "using the extracted district and teacher document content when it clarifies expectations."
        ),
        "parent_newsletter_summary": (
            "Generate a parent-friendly weekly newsletter summary with what students will learn, reminders, "
            "and upcoming focus. Use the extracted document content only as instructional context; "
            "keep the final tone family friendly."
        ),
    }
    default = (
        f"Generate a teacher-usable {artifact_type.replace('_', ' ')} aligned to pacing guide objectives."
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
        "resolved_day_plan": resolve_pacing_day_plan(week_subject, day_label) if week_subject and day_label else None,
        "pacing_materials": generation_context.get("pacing_materials"),
        "district_materials_summary": generation_context.get("district_materials_summary"),
        "district_document_context": generation_context.get("district_document_context"),
        "teacher_supplemental_files": generation_context.get("teacher_supplemental_files"),
        "teacher_supplemental_links": generation_context.get("teacher_supplemental_links"),
        "teacher_supplemental_notes": generation_context.get("teacher_supplemental_notes"),
        "teacher_document_context": generation_context.get("teacher_document_context"),
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

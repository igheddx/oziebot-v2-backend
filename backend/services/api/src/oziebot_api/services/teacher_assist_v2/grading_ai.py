"""TeacherAssist v2 grading AI generation (mock + OpenAI)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

import httpx
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.constants import validate_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.prompt_contracts import (
    GRADING_ASSIST_FEATURE,
    GRADING_ASSIST_PROMPT_VERSION,
)
from oziebot_api.services.teacher_assist.provider_config import (
    TeacherAssistProviderCircuitBreaker,
    get_teacher_assist_provider_model,
)
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.grading_constants import (
    DEFAULT_RUBRIC_SECTIONS,
    GRADING_DRAFT_MAX_SCORE,
)


@dataclass(frozen=True)
class GradingDraftAIResult:
    score: float
    max_score: float
    percentage: float
    rubric_json: dict[str, Any]
    teacher_comment_draft: str
    strengths: list[str]
    improvements: list[str]
    objective_evidence: list[dict[str, Any]]
    confidence_score: float
    provider: str
    model: str | None
    input_tokens: int
    output_tokens: int
    estimated_cost_cents: int


GRADING_OUTPUT_SCHEMA: dict[str, Any] = {
    "score": "number",
    "max_score": "number",
    "teacher_comment_draft": "string",
    "strengths": ["string"],
    "improvements": ["string"],
    "rubric_sections": [
        {
            "name": "string",
            "score": "number",
            "max_score": "number",
            "feedback": "string",
        }
    ],
    "objective_evidence": [
        {
            "objective_id": "string",
            "evidence": "string",
        }
    ],
    "confidence_score": "number between 0 and 1",
}


def _confidence_from_context(*, student_number: int, assignment_title: str) -> float:
    digest = hashlib.sha256(f"{student_number}:{assignment_title}".encode("utf-8")).hexdigest()
    bucket = int(digest[:2], 16) / 255
    return round(0.55 + (0.4 * bucket), 2)


def _mock_rubric_sections(*, score_ratio: float) -> list[dict[str, Any]]:
    section_max = GRADING_DRAFT_MAX_SCORE / len(DEFAULT_RUBRIC_SECTIONS)
    sections: list[dict[str, Any]] = []
    for index, name in enumerate(DEFAULT_RUBRIC_SECTIONS):
        section_score = round(section_max * score_ratio * (0.9 + (index * 0.03)), 1)
        sections.append(
            {
                "name": name,
                "score": min(section_max, section_score),
                "max_score": section_max,
                "feedback": f"[MOCK AI] Draft feedback for {name.lower()} based on assignment expectations.",
            }
        )
    return sections


def generate_mock_grading_draft(*, context: dict[str, Any]) -> GradingDraftAIResult:
    assignment = context["assignment"]
    student_number = int(context["student_submission"]["student_number"] or 0)
    max_score = GRADING_DRAFT_MAX_SCORE
    score_ratio = 0.72 + ((student_number % 5) * 0.04)
    score = round(max_score * min(score_ratio, 0.95), 1)
    percentage = round((score / max_score) * 100, 1)
    objectives = context.get("objectives") or []
    primary_objective = objectives[0]["objective_id"] if objectives else "objective"
    objective_description = objectives[0]["description"] if objectives else "learning objective"
    teacher_notes = context.get("teacher_notes") or []
    note_suffix = f" Teacher notes considered: {teacher_notes[0][:120]}" if teacher_notes else ""

    return GradingDraftAIResult(
        score=score,
        max_score=max_score,
        percentage=percentage,
        rubric_json={"sections": _mock_rubric_sections(score_ratio=score / max_score)},
        teacher_comment_draft=(
            f"[MOCK AI] Draft comment for STUDENT #{student_number} on '{assignment['title']}'. "
            f"The response shows progress toward {primary_objective}.{note_suffix} "
            "This is a draft only — teacher review is required before any gradebook commit."
        ),
        strengths=[
            "Shows understanding of the assignment prompt and weekly objective.",
            "Includes enough detail for formative feedback and reteach decisions.",
        ],
        improvements=[
            "Strengthen textual evidence and explicit connections to the objective.",
            "Revise organization so the main inference or claim is easier to follow.",
        ],
        objective_evidence=[
            {
                "objective_id": primary_objective,
                "evidence": (
                    f"[MOCK AI] Student work appears to address {primary_objective}: "
                    f"{objective_description[:160]}"
                ),
            }
        ],
        confidence_score=_confidence_from_context(
            student_number=student_number,
            assignment_title=str(assignment["title"]),
        ),
        provider="mock",
        model="mock",
        input_tokens=0,
        output_tokens=0,
        estimated_cost_cents=0,
    )


def _estimate_cost_cents(model_name: str, *, input_tokens: int, output_tokens: int) -> int:
    pricing = {
        "gpt-4.1-mini": (0.40, 1.60),
        "gpt-4.1": (2.00, 8.00),
        "gpt-4o-mini": (0.15, 0.60),
    }.get(model_name)
    if pricing is None:
        return 0
    input_cost = (input_tokens / 1_000_000) * pricing[0]
    output_cost = (output_tokens / 1_000_000) * pricing[1]
    return max(0, round((input_cost + output_cost) * 100))


def generate_openai_grading_draft(
    *, settings: Settings, context: dict[str, Any], db: Session | None = None
) -> GradingDraftAIResult:
    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(effective_settings.teacher_assist_ai_provider)
    if provider_name != "openai":
        raise RuntimeError("OpenAI grading requires teacher_assist_ai_provider=openai")
    TeacherAssistProviderCircuitBreaker().assert_can_execute(effective_settings, provider_name)
    if not effective_settings.teacher_assist_openai_api_key:
        raise RuntimeError("TeacherAssist OpenAI API key is not configured")
    model_name = get_teacher_assist_provider_model(effective_settings, provider_name=provider_name)

    request_body = {
        "model": model_name,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a teacher grading assistant. Return JSON only. "
                    "Produce draft grading suggestions — never final grades. "
                    "Do not include student names or other PII."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "prompt_version": GRADING_ASSIST_PROMPT_VERSION,
                        "instruction": (
                            "Generate a draft grade suggestion, rubric section scores, strengths, "
                            "improvements, teacher comment draft, and objective evidence."
                        ),
                        "grading_context": context,
                        "required_output_schema": GRADING_OUTPUT_SCHEMA,
                    }
                ),
            },
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {effective_settings.teacher_assist_openai_api_key}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=90.0) as client:
        response = client.post(
            f"{effective_settings.teacher_assist_openai_base_url.rstrip('/')}/chat/completions",
            headers=headers,
            json=request_body,
        )
        response.raise_for_status()
        payload = response.json()

    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError("OpenAI grading returned no choices")
    message = choices[0].get("message") or {}
    raw_content = message.get("content")
    if not isinstance(raw_content, str) or not raw_content.strip():
        raise ValueError("OpenAI grading returned empty content")
    content = json.loads(raw_content)

    max_score = float(content.get("max_score") or GRADING_DRAFT_MAX_SCORE)
    score = float(content.get("score") or 0)
    percentage = round((score / max_score) * 100, 1) if max_score > 0 else 0.0
    rubric_sections = content.get("rubric_sections") or []
    usage = payload.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or 0)

    return GradingDraftAIResult(
        score=score,
        max_score=max_score,
        percentage=percentage,
        rubric_json={"sections": rubric_sections},
        teacher_comment_draft=str(content.get("teacher_comment_draft") or ""),
        strengths=[str(item) for item in (content.get("strengths") or [])],
        improvements=[str(item) for item in (content.get("improvements") or [])],
        objective_evidence=list(content.get("objective_evidence") or []),
        confidence_score=float(content.get("confidence_score") or 0.7),
        provider="openai",
        model=model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost_cents=_estimate_cost_cents(model_name, input_tokens=input_tokens, output_tokens=output_tokens),
    )


def generate_grading_draft_ai_result(
    *, settings: Settings, context: dict[str, Any], db: Session | None = None
) -> GradingDraftAIResult:
    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(effective_settings.teacher_assist_ai_provider)
    TeacherAssistProviderCircuitBreaker().assert_can_execute(effective_settings, provider_name)
    if provider_name == "mock":
        return generate_mock_grading_draft(context=context)
    if provider_name == "openai":
        return generate_openai_grading_draft(settings=settings, context=context, db=db)
    raise NotImplementedError(f"Grading AI provider '{provider_name}' is not implemented")

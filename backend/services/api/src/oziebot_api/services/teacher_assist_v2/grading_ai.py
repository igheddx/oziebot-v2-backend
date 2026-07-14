"""TeacherAssist v2 grading AI generation (mock + OpenAI)."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

import httpx
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ai_usage import assert_teacher_assist_ai_cost_available
from oziebot_api.services.teacher_assist.constants import validate_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.openai_pricing import estimate_openai_cost_cents
from oziebot_api.services.teacher_assist.prompt_contracts import (
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
from oziebot_api.services.teacher_assist_v2.grading_rubric import (
    grading_template_from_package_rubric,
    mock_sections_from_template,
    totals_from_grading_rubric,
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
    student_facing_feedback: dict[str, str]
    teacher_facing_explanation: str
    suspected_misconception: str | None
    recommended_next_step: str
    uncertainty_flags: list[str]
    evidence_used: str


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
    "student_facing_feedback": {
        "celebrate": (
            "string — one sentence, no TEKS codes, no jargon. Start with 'You'. "
            "Name something specific the student did well in their response."
        ),
        "correct": (
            "string — one sentence, no TEKS codes. The single most important thing to improve, "
            "phrased constructively. Be specific to the rubric criterion missed."
        ),
        "encourage": (
            "string — one sentence, no TEKS codes. Forward-looking motivation tied to the next step. "
            "Make it feel achievable."
        ),
    },
    "teacher_facing_explanation": (
        "string — 3–5 sentences for the teacher. Explain the score rationale: which rubric "
        "criteria were met or missed and why. Reference specific language or evidence from the "
        "student response. Note any extraction or OCR concerns that affected confidence."
    ),
    "suspected_misconception": (
        "string or null — Name the specific learning gap or pattern if detectable. "
        "Examples: 'Summarizing instead of analyzing', 'Missing text evidence to support claim', "
        "'Confusing topic sentence with thesis'. Null if no clear misconception is evident."
    ),
    "recommended_next_step": (
        "string — One concrete instructional suggestion for this specific student. "
        "Example: 'Brief conference on using direct quotes as evidence' or "
        "'Try the sentence frame: The author shows this when...'."
    ),
    "uncertainty_flags": [
        "string — Specific concern. Examples: 'Response text appears truncated', "
        "'OCR quality uncertain — handwriting may have been misread', "
        "'Student may have written outside the response box'. Empty list if none."
    ],
    "evidence_used": (
        "string — Summarize the 2–3 most specific pieces of evidence from the student's actual "
        "response text that most influenced the score. Quote or closely paraphrase student language."
    ),
}


def _confidence_from_context(*, student_number: int, assignment_title: str) -> float:
    digest = hashlib.sha256(f"{student_number}:{assignment_title}".encode("utf-8")).hexdigest()
    bucket = int(digest[:2], 16) / 255
    return round(0.55 + (0.4 * bucket), 2)


def _mock_rubric_sections(*, score_ratio: float, context: dict[str, Any]) -> list[dict[str, Any]]:
    template = context.get("rubric_template")
    if isinstance(template, dict) and template.get("sections"):
        return mock_sections_from_template(template, score_ratio=score_ratio)
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
    template = context.get("rubric_template")
    if not isinstance(template, dict):
        template = grading_template_from_package_rubric(context.get("rubric"))
    max_score = float(template.get("total_points") or GRADING_DRAFT_MAX_SCORE)
    score_ratio = 0.72 + ((student_number % 5) * 0.04)
    score = round(max_score * min(score_ratio, 0.95), 1)
    percentage = round((score / max_score) * 100, 1) if max_score > 0 else 0.0
    rubric_json = {
        "sections": _mock_rubric_sections(
            score_ratio=score / max_score if max_score else 0, context=context
        )
    }
    score, max_score = totals_from_grading_rubric(rubric_json)
    if max_score <= 0:
        max_score = float(template.get("total_points") or GRADING_DRAFT_MAX_SCORE)
        score = round(max_score * min(score_ratio, 0.95), 1)
    percentage = round((score / max_score) * 100, 1) if max_score > 0 else 0.0
    objectives = context.get("objectives") or []
    primary_objective = objectives[0]["objective_id"] if objectives else "objective"
    objective_description = objectives[0]["description"] if objectives else "learning objective"
    teacher_notes = context.get("teacher_notes") or []
    note_suffix = f" Teacher notes considered: {teacher_notes[0][:120]}" if teacher_notes else ""

    confidence = _confidence_from_context(
        student_number=student_number,
        assignment_title=str(assignment["title"]),
    )
    return GradingDraftAIResult(
        score=score,
        max_score=max_score,
        percentage=percentage,
        rubric_json=rubric_json,
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
        confidence_score=confidence,
        provider="mock",
        model="mock",
        input_tokens=0,
        output_tokens=0,
        estimated_cost_cents=0,
        student_facing_feedback={
            "celebrate": "You showed effort in responding to the assignment prompt.",
            "correct": "Add specific details or evidence from the text to support your main idea.",
            "encourage": "With one more round of revision, your response will be much stronger!",
        },
        teacher_facing_explanation=(
            f"[MOCK AI] Student #{student_number} scored {round(score, 1)}/{round(max_score, 1)} "
            f"({round(percentage, 1)}%) on '{assignment['title']}'. The rubric criteria were scored "
            "based on the mock response pattern. Teacher review is required before committing any "
            f"grade. Estimated AI confidence: {confidence}."
        ),
        suspected_misconception=None,
        recommended_next_step="Review the rubric criteria with the student and identify the area to target next.",
        uncertainty_flags=["[MOCK AI] This is a mock draft — no actual student text was analyzed."],
        evidence_used="[MOCK AI] No real student response was analyzed. Evidence fields populated with placeholder text.",
    )


def generate_openai_grading_draft(
    *, settings: Settings, context: dict[str, Any], db: Session | None = None
) -> GradingDraftAIResult:
    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(
        effective_settings.teacher_assist_ai_provider
    )
    if provider_name != "openai":
        raise RuntimeError("OpenAI grading requires teacher_assist_ai_provider=openai")
    TeacherAssistProviderCircuitBreaker().assert_can_execute(effective_settings, provider_name)
    if not effective_settings.teacher_assist_openai_api_key:
        raise RuntimeError("TeacherAssist OpenAI API key is not configured")
    assert_teacher_assist_ai_cost_available(db, effective_settings)
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
                    "Do not include student names, TEKS codes, or other PII in any student-facing field. "
                    "student_facing_feedback fields (celebrate, correct, encourage) must be written "
                    "directly to the student in plain, encouraging language appropriate for the grade level. "
                    "teacher_facing_explanation is for the teacher's eyes only and may reference rubric criteria."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "prompt_version": GRADING_ASSIST_PROMPT_VERSION,
                        "instruction": (
                            "Generate a draft grade suggestion using the rubric_template criteria names and "
                            "max_score values exactly. Evaluate only the provided student_submission.response_text. "
                            "Do not infer missing student work from filenames or metadata. Return rubric_sections "
                            "with one entry per criterion, matching criterion names and max_score from rubric_template. "
                            "Sum section scores for score. If extraction quality appears weak or incomplete, lower "
                            "confidence_score and mention the uncertainty in teacher_comment_draft. "
                            "For student_facing_feedback: write celebrate (what the student did well, cite specific "
                            "evidence), correct (the single most important improvement), and encourage (one "
                            "forward-looking motivational sentence). Never include TEKS codes or standard numbers "
                            "in student_facing_feedback. "
                            "For evidence_used: quote or closely paraphrase the 2–3 most specific pieces of student "
                            "response text that drove your score. "
                            "For suspected_misconception: name the learning gap if clearly detectable, else null."
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
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with httpx.Client(timeout=90.0) as client:
                response = client.post(
                    f"{effective_settings.teacher_assist_openai_base_url.rstrip('/')}/chat/completions",
                    headers=headers,
                    json=request_body,
                )
                response.raise_for_status()
                payload = response.json()
            break
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
            last_error = exc
            if attempt == 2:
                raise ValueError(
                    "TeacherAssist could not reach the OpenAI API for grading. Check the configured model, "
                    "base URL, API key, and outbound network access, then try again."
                ) from exc
            time.sleep(1.0 + attempt)
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text.strip()
            raise ValueError(
                f"TeacherAssist grading request failed ({exc.response.status_code}). "
                f"{detail or 'Check the provider configuration and try again.'}"
            ) from exc
    else:
        if last_error is not None:
            raise last_error

    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError("OpenAI grading returned no choices")
    message = choices[0].get("message") or {}
    raw_content = message.get("content")
    if not isinstance(raw_content, str) or not raw_content.strip():
        raise ValueError("OpenAI grading returned empty content")
    content = json.loads(raw_content)

    rubric_sections = content.get("rubric_sections") or []
    template = context.get("rubric_template")
    if isinstance(template, dict) and template.get("sections") and not rubric_sections:
        rubric_sections = [
            {
                "name": row.get("name"),
                "score": 0.0,
                "max_score": row.get("max_score"),
                "feedback": "",
            }
            for row in template.get("sections") or []
        ]
    rubric_json = {"sections": rubric_sections}
    max_score = float(content.get("max_score") or 0)
    score = float(content.get("score") or 0)
    if max_score <= 0:
        score, max_score = totals_from_grading_rubric(rubric_json)
    if max_score <= 0:
        max_score = float((template or {}).get("total_points") or GRADING_DRAFT_MAX_SCORE)
    if score <= 0 and rubric_json.get("sections"):
        score, max_score = totals_from_grading_rubric(rubric_json)
    percentage = round((score / max_score) * 100, 1) if max_score > 0 else 0.0
    usage = payload.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or 0)

    raw_feedback = content.get("student_facing_feedback") or {}
    student_facing_feedback = {
        "celebrate": str(raw_feedback.get("celebrate") or ""),
        "correct": str(raw_feedback.get("correct") or ""),
        "encourage": str(raw_feedback.get("encourage") or ""),
    }
    return GradingDraftAIResult(
        score=score,
        max_score=max_score,
        percentage=percentage,
        rubric_json=rubric_json,
        teacher_comment_draft=str(content.get("teacher_comment_draft") or ""),
        strengths=[str(item) for item in (content.get("strengths") or [])],
        improvements=[str(item) for item in (content.get("improvements") or [])],
        objective_evidence=list(content.get("objective_evidence") or []),
        confidence_score=float(content.get("confidence_score") or 0.7),
        provider="openai",
        model=model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost_cents=estimate_openai_cost_cents(
            model_name, input_tokens=input_tokens, output_tokens=output_tokens
        ),
        student_facing_feedback=student_facing_feedback,
        teacher_facing_explanation=str(content.get("teacher_facing_explanation") or ""),
        suspected_misconception=content.get("suspected_misconception") or None,
        recommended_next_step=str(content.get("recommended_next_step") or ""),
        uncertainty_flags=[str(f) for f in (content.get("uncertainty_flags") or [])],
        evidence_used=str(content.get("evidence_used") or ""),
    )


def generate_grading_draft_ai_result(
    *, settings: Settings, context: dict[str, Any], db: Session | None = None
) -> GradingDraftAIResult:
    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(
        effective_settings.teacher_assist_ai_provider
    )
    TeacherAssistProviderCircuitBreaker().assert_can_execute(effective_settings, provider_name)
    if provider_name == "mock":
        return generate_mock_grading_draft(context=context)
    if provider_name == "openai":
        return generate_openai_grading_draft(settings=settings, context=context, db=db)
    raise NotImplementedError(f"Grading AI provider '{provider_name}' is not implemented")

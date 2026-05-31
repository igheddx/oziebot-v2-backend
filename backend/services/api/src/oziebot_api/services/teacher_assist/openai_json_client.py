"""Shared OpenAI JSON completion helper for TeacherAssist."""

from __future__ import annotations

import json
from typing import Any

import httpx

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ai_provider import TeacherAssistAIProviderResult
from oziebot_api.services.teacher_assist.openai_pricing import estimate_openai_cost_cents


def execute_openai_json_completion(
    settings: Settings,
    *,
    model_name: str,
    instruction: str,
    prompt_payload: dict[str, Any],
    required_output_schema: dict[str, Any],
    system_prompt: str | None = None,
    timeout_seconds: float = 90.0,
) -> TeacherAssistAIProviderResult:
    if not (settings.teacher_assist_openai_api_key or "").strip():
        raise RuntimeError("TeacherAssist OpenAI API key is not configured")

    request_body = {
        "model": model_name,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": system_prompt
                or (
                    "You generate teacher-ready instructional content as structured JSON only. "
                    "Never include markdown, prose outside JSON, or personally identifying information."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "instruction": instruction,
                        "prompt_payload": prompt_payload,
                        "required_output_schema": required_output_schema,
                    }
                ),
            },
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {settings.teacher_assist_openai_api_key}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=timeout_seconds) as client:
        response = client.post(
            f"{settings.teacher_assist_openai_base_url.rstrip('/')}/chat/completions",
            headers=headers,
            json=request_body,
        )
        response.raise_for_status()
        payload = response.json()

    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError("TeacherAssist OpenAI provider returned no choices")
    message = choices[0].get("message") or {}
    raw_content = message.get("content")
    if isinstance(raw_content, list):
        raw_content = "".join(entry.get("text", "") for entry in raw_content if isinstance(entry, dict))
    if not isinstance(raw_content, str) or not raw_content.strip():
        raise ValueError("TeacherAssist OpenAI provider returned empty JSON content")
    try:
        content_json = json.loads(raw_content)
    except json.JSONDecodeError as exc:
        raise ValueError("TeacherAssist OpenAI provider returned malformed JSON") from exc

    usage = payload.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or 0)
    estimated_cost_cents = estimate_openai_cost_cents(
        model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    return TeacherAssistAIProviderResult(
        content_json=content_json,
        provider="openai",
        model=model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost_cents=estimated_cost_cents,
        metadata_json={
            "is_mock": False,
            "provider_mode": "real",
        },
    )

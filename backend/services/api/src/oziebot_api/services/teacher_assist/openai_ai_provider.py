from __future__ import annotations

import json
from typing import Any

import httpx

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ai_provider import (
    TeacherAssistAIProvider,
    TeacherAssistAIProviderResult,
)
from oziebot_api.services.teacher_assist.instructional_plan_prompt_builder import (
    build_instructional_plan_prompt,
    build_instructional_plan_section_regeneration_prompt,
    instructional_plan_output_schema,
    instructional_plan_section_output_schema,
)

from oziebot_api.services.teacher_assist.openai_pricing import estimate_openai_cost_cents


class OpenAITeacherAssistAIProvider(TeacherAssistAIProvider):
    provider_name = "openai"

    def __init__(self, settings: Settings, *, model_name: str) -> None:
        self._settings = settings
        self._model_name = model_name

    def _execute_json_request(
        self,
        *,
        prompt_payload: dict[str, Any],
        required_output_schema: dict[str, Any],
        instruction: str,
    ) -> TeacherAssistAIProviderResult:
        request_body = {
            "model": self._model_name,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You generate teacher-ready instructional planning content as structured JSON only. "
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
            "Authorization": f"Bearer {self._settings.teacher_assist_openai_api_key}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                f"{self._settings.teacher_assist_openai_base_url.rstrip('/')}/chat/completions",
                headers=headers,
                json=request_body,
            )
            response.raise_for_status()
            payload = response.json()

        choices = payload.get("choices") or []
        if not choices:
            raise RuntimeError("TeacherAssist real provider returned no choices")
        message = choices[0].get("message") or {}
        raw_content = message.get("content")
        if isinstance(raw_content, list):
            raw_content = "".join(
                entry.get("text", "") for entry in raw_content if isinstance(entry, dict)
            )
        if not isinstance(raw_content, str) or not raw_content.strip():
            raise ValueError("TeacherAssist real provider returned empty JSON content")
        try:
            content_json = json.loads(raw_content)
        except json.JSONDecodeError as exc:
            raise ValueError("TeacherAssist real provider returned malformed JSON") from exc

        usage = payload.get("usage") or {}
        input_tokens = int(usage.get("prompt_tokens") or 0)
        output_tokens = int(usage.get("completion_tokens") or 0)
        estimated_cost_cents = estimate_openai_cost_cents(
            self._model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )
        return TeacherAssistAIProviderResult(
            content_json=content_json,
            provider=self.provider_name,
            model=self._model_name,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            estimated_cost_cents=estimated_cost_cents,
            metadata_json={
                "is_mock": False,
                "provider_mode": "real",
                "prompt_version": prompt_payload["prompt_version"],
                "request_id": payload.get("id"),
                "finish_reason": choices[0].get("finish_reason"),
                "usage": usage,
            },
        )

    def generate_instructional_plan(
        self, context_preview: dict[str, Any]
    ) -> TeacherAssistAIProviderResult:
        prompt_payload = build_instructional_plan_prompt(context_preview)
        return self._execute_json_request(
            prompt_payload=prompt_payload,
            required_output_schema=instructional_plan_output_schema(),
            instruction=(
                "Return one JSON object that satisfies the supplied instructional plan schema "
                "and follows all safety and quality requirements."
            ),
        )

    def regenerate_instructional_plan_section(
        self,
        *,
        context_preview: dict[str, Any],
        current_plan_content: dict[str, Any],
        section_key: str,
        section_path: str | None = None,
        current_section_content: Any = None,
        teacher_instruction: str | None = None,
        preserve_existing_context: bool = True,
    ) -> TeacherAssistAIProviderResult:
        prompt_payload = build_instructional_plan_section_regeneration_prompt(
            context_preview=context_preview,
            current_plan_content=current_plan_content,
            section_key=section_key,
            section_path=section_path,
            current_section_content=current_section_content if preserve_existing_context else None,
            teacher_instruction=teacher_instruction,
            preserve_existing_context=preserve_existing_context,
        )
        return self._execute_json_request(
            prompt_payload=prompt_payload,
            required_output_schema=instructional_plan_section_output_schema(
                section_key, section_path=section_path
            ),
            instruction=(
                "Return one JSON object with only the section_content wrapper for the requested section "
                "regeneration. Do not return the full plan."
            ),
        )

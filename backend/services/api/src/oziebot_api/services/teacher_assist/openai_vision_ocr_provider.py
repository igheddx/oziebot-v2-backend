from __future__ import annotations

import base64
import json

import httpx

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ocr_errors import TeacherAssistOCRProviderError
from oziebot_api.services.teacher_assist.ocr_provider import TeacherAssistOCRProviderResult
from oziebot_api.services.teacher_assist.ocr_provider_config import (
    confidence_level_from_score,
    get_teacher_assist_ocr_provider_model,
)

OPENAI_VISION_COST_PER_MILLION_INPUT_TOKENS_USD = 0.15
OPENAI_VISION_COST_PER_MILLION_OUTPUT_TOKENS_USD = 0.60


def _estimate_openai_vision_cost_cents(*, input_tokens: int, output_tokens: int) -> int:
    input_cost = (input_tokens / 1_000_000) * OPENAI_VISION_COST_PER_MILLION_INPUT_TOKENS_USD
    output_cost = (output_tokens / 1_000_000) * OPENAI_VISION_COST_PER_MILLION_OUTPUT_TOKENS_USD
    return max(0, round((input_cost + output_cost) * 100))


class OpenAIVisionTeacherAssistOCRProvider:
    provider_name = "openai_vision"

    def extract_text(
        self,
        *,
        artifact_type: str,
        mime_type: str,
        original_filename: str,
        file_bytes: bytes,
        settings: Settings,
    ) -> TeacherAssistOCRProviderResult:
        model_name = get_teacher_assist_ocr_provider_model(settings, provider_name=self.provider_name)
        timeout_seconds = max(5, int(settings.teacher_assist_ocr_provider_timeout_seconds))
        normalized_mime = (mime_type or "application/octet-stream").strip().lower()
        image_data_url = (
            f"data:{normalized_mime};base64,{base64.b64encode(file_bytes).decode('ascii')}"
        )
        request_body = {
            "model": model_name,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Extract all readable text from the provided classroom artifact image. "
                        "Return JSON only with keys extracted_text, confidence_score (0-1), and page_count."
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": (
                                "Extract the full readable text for teacher review. "
                                "Do not summarize. Preserve line breaks where visible."
                            ),
                        },
                        {"type": "image_url", "image_url": {"url": image_data_url}},
                    ],
                },
            ],
            "temperature": 0.0,
        }
        headers = {
            "Authorization": f"Bearer {settings.teacher_assist_openai_api_key}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(timeout=float(timeout_seconds)) as client:
                response = client.post(
                    f"{settings.teacher_assist_openai_base_url.rstrip('/')}/chat/completions",
                    headers=headers,
                    json=request_body,
                )
        except httpx.TimeoutException as exc:
            raise TeacherAssistOCRProviderError(
                "TeacherAssist OpenAI vision OCR timed out",
                error_code="provider_timeout",
                metadata={"provider": self.provider_name},
            ) from exc
        except httpx.HTTPError as exc:
            raise TeacherAssistOCRProviderError(
                "TeacherAssist OpenAI vision OCR request failed",
                error_code="provider_not_configured",
                metadata={"provider": self.provider_name},
            ) from exc

        if response.status_code == 429:
            raise TeacherAssistOCRProviderError(
                "TeacherAssist OpenAI vision OCR rate limit exceeded",
                error_code="provider_quota_exceeded",
                metadata={"provider": self.provider_name, "status_code": response.status_code},
            )
        if response.status_code >= 500:
            raise TeacherAssistOCRProviderError(
                "TeacherAssist OpenAI vision OCR provider unavailable",
                error_code="provider_malformed_response",
                metadata={"provider": self.provider_name, "status_code": response.status_code},
            )
        if response.status_code >= 400:
            raise TeacherAssistOCRProviderError(
                "TeacherAssist OpenAI vision OCR rejected the request",
                error_code="provider_not_configured",
                metadata={"provider": self.provider_name, "status_code": response.status_code},
            )

        try:
            payload = response.json()
            content = payload["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            extracted_text = str(parsed.get("extracted_text") or "").strip()
            confidence_score = float(parsed.get("confidence_score") or 0.5)
            page_count = int(parsed.get("page_count") or 1)
            usage = payload.get("usage") or {}
            input_tokens = int(usage.get("prompt_tokens") or 0)
            output_tokens = int(usage.get("completion_tokens") or 0)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise TeacherAssistOCRProviderError(
                "TeacherAssist OpenAI vision OCR returned malformed JSON",
                error_code="provider_malformed_response",
                metadata={"provider": self.provider_name},
            ) from exc

        if not extracted_text:
            raise TeacherAssistOCRProviderError(
                "TeacherAssist OpenAI vision OCR returned no extractable text",
                error_code="provider_malformed_response",
                metadata={"provider": self.provider_name},
            )

        confidence_level = confidence_level_from_score(confidence_score)
        return TeacherAssistOCRProviderResult(
            extracted_text=extracted_text,
            provider=self.provider_name,
            model=model_name,
            metadata_json={
                "artifact_type": artifact_type,
                "mime_type": normalized_mime,
                "original_filename_present": bool(original_filename),
                "bytes_read": len(file_bytes),
                "is_mock": False,
                "provider_mode": "real",
                "provider_version": model_name,
                "provider_confidence_score": round(confidence_score, 4),
                "confidence_level": confidence_level,
                "page_count": max(1, page_count),
                "processing_timeout_seconds": timeout_seconds,
                "estimated_cost_cents": _estimate_openai_vision_cost_cents(
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                ),
                "low_confidence_output": confidence_level == "low",
                "llm_provider": True,
            },
        )

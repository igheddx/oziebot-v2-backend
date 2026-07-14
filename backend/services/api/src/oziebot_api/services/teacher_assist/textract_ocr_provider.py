from __future__ import annotations

from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ocr_errors import TeacherAssistOCRProviderError
from oziebot_api.services.teacher_assist.ocr_provider import TeacherAssistOCRProviderResult
from oziebot_api.services.teacher_assist.ocr_provider_config import (
    confidence_level_from_score,
    get_teacher_assist_ocr_provider_model,
)


def _resolve_textract_region(settings: Settings) -> str:
    return (
        (settings.teacher_assist_ocr_aws_region or "").strip()
        or (settings.teacher_assist_s3_region or "").strip()
        or "us-east-1"
    )


def _extract_textract_text_and_confidence(
    blocks: list[dict[str, Any]],
) -> tuple[str, float | None, int]:
    lines: list[str] = []
    confidences: list[float] = []
    page_numbers: set[int] = set()
    for block in blocks:
        block_type = block.get("BlockType")
        if block_type == "PAGE" and block.get("Page") is not None:
            page_numbers.add(int(block["Page"]))
        if block_type != "LINE":
            continue
        text = str(block.get("Text") or "").strip()
        if text:
            lines.append(text)
        confidence = block.get("Confidence")
        if confidence is not None:
            try:
                confidences.append(float(confidence) / 100.0)
            except (TypeError, ValueError):
                continue
    extracted_text = "\n".join(lines).strip()
    average_confidence = round(sum(confidences) / len(confidences), 4) if confidences else None
    page_count = len(page_numbers) if page_numbers else (1 if extracted_text else 0)
    return extracted_text, average_confidence, page_count


def _map_textract_error(exc: Exception) -> TeacherAssistOCRProviderError:
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {}) if exc.response else {}
        code = str(error.get("Code") or "")
        message = str(error.get("Message") or str(exc))
        lowered = code.lower()
        if (
            "throttl" in lowered
            or "limitexceeded" in lowered
            or lowered in {"provisionedthroughputexceededexception"}
        ):
            return TeacherAssistOCRProviderError(
                message or "TeacherAssist Textract rate limit exceeded",
                error_code="provider_quota_exceeded",
                metadata={"provider": "textract", "aws_error_code": code},
            )
        if "unsupported" in lowered or "invalid" in lowered:
            return TeacherAssistOCRProviderError(
                message or "TeacherAssist Textract rejected the artifact",
                error_code="unsupported_mime_type",
                metadata={"provider": "textract", "aws_error_code": code},
            )
        if "timeout" in lowered:
            return TeacherAssistOCRProviderError(
                message or "TeacherAssist Textract timed out",
                error_code="provider_timeout",
                metadata={"provider": "textract", "aws_error_code": code},
            )
        return TeacherAssistOCRProviderError(
            message or "TeacherAssist Textract request failed",
            error_code="provider_malformed_response",
            metadata={"provider": "textract", "aws_error_code": code},
        )
    if isinstance(exc, BotoCoreError):
        message = str(exc)
        if "timeout" in message.lower():
            return TeacherAssistOCRProviderError(
                message,
                error_code="provider_timeout",
                metadata={"provider": "textract"},
            )
        return TeacherAssistOCRProviderError(
            message,
            error_code="provider_not_configured",
            metadata={"provider": "textract"},
        )
    return TeacherAssistOCRProviderError(
        str(exc),
        error_code="provider_malformed_response",
        metadata={"provider": "textract"},
    )


class TextractTeacherAssistOCRProvider:
    provider_name = "textract"

    def extract_text(
        self,
        *,
        artifact_type: str,
        mime_type: str,
        original_filename: str,
        file_bytes: bytes,
        settings: Settings,
    ) -> TeacherAssistOCRProviderResult:
        import boto3

        model_name = get_teacher_assist_ocr_provider_model(
            settings, provider_name=self.provider_name
        )
        max_pages = max(1, int(settings.teacher_assist_ocr_max_pages))
        timeout_seconds = max(5, int(settings.teacher_assist_ocr_provider_timeout_seconds))
        client = boto3.client("textract", region_name=_resolve_textract_region(settings))
        try:
            response = client.detect_document_text(
                Document={"Bytes": file_bytes},
            )
        except Exception as exc:  # noqa: BLE001
            raise _map_textract_error(exc) from exc

        blocks = list(response.get("Blocks") or [])
        if not isinstance(blocks, list):
            raise TeacherAssistOCRProviderError(
                "TeacherAssist Textract returned an invalid response payload",
                error_code="provider_malformed_response",
                metadata={"provider": self.provider_name},
            )

        extracted_text, average_confidence, page_count = _extract_textract_text_and_confidence(
            blocks
        )
        if page_count > max_pages:
            raise TeacherAssistOCRProviderError(
                f"Artifact exceeds TeacherAssist OCR max page count ({max_pages})",
                error_code="provider_not_configured",
                metadata={"page_count": page_count, "max_pages": max_pages},
            )
        if not extracted_text:
            raise TeacherAssistOCRProviderError(
                "TeacherAssist Textract returned no extractable text",
                error_code="provider_malformed_response",
                metadata={"provider": self.provider_name},
            )

        confidence_score = average_confidence if average_confidence is not None else 0.35
        confidence_level = confidence_level_from_score(confidence_score)
        low_confidence = confidence_level == "low"
        return TeacherAssistOCRProviderResult(
            extracted_text=extracted_text,
            provider=self.provider_name,
            model=model_name,
            metadata_json={
                "artifact_type": artifact_type,
                "mime_type": mime_type or "application/octet-stream",
                "original_filename_present": bool(original_filename),
                "bytes_read": len(file_bytes),
                "is_mock": False,
                "provider_mode": "real",
                "provider_version": "detect_document_text",
                "provider_confidence_score": confidence_score,
                "confidence_level": confidence_level,
                "page_count": page_count,
                "processing_timeout_seconds": timeout_seconds,
                "estimated_cost_cents": None,
                "low_confidence_output": low_confidence,
            },
        )

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.constants import validate_teacher_assist_ocr_provider
from oziebot_api.services.teacher_assist.ocr_provider_config import (
    TeacherAssistOCRProviderCircuitBreaker,
)


@dataclass(frozen=True)
class TeacherAssistOCRProviderResult:
    extracted_text: str
    provider: str
    model: str
    metadata_json: dict[str, object] | None = None


class TeacherAssistOCRProvider(Protocol):
    provider_name: str

    def extract_text(
        self,
        *,
        artifact_type: str,
        mime_type: str,
        original_filename: str,
        file_bytes: bytes,
        settings: Settings,
    ) -> TeacherAssistOCRProviderResult: ...


def get_teacher_assist_ocr_provider(settings: Settings) -> TeacherAssistOCRProvider:
    provider_name = validate_teacher_assist_ocr_provider(settings.teacher_assist_ocr_provider)
    if provider_name == "mock":
        from oziebot_api.services.teacher_assist.mock_ocr_provider import (
            MockTeacherAssistOCRProvider,
        )

        return MockTeacherAssistOCRProvider()

    TeacherAssistOCRProviderCircuitBreaker().assert_can_execute(settings, provider_name)
    if provider_name == "textract":
        from oziebot_api.services.teacher_assist.textract_ocr_provider import (
            TextractTeacherAssistOCRProvider,
        )

        return TextractTeacherAssistOCRProvider()
    if provider_name == "openai_vision":
        from oziebot_api.services.teacher_assist.openai_vision_ocr_provider import (
            OpenAIVisionTeacherAssistOCRProvider,
        )

        return OpenAIVisionTeacherAssistOCRProvider()
    raise NotImplementedError(f"TeacherAssist OCR provider '{provider_name}' is not implemented")

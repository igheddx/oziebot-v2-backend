from __future__ import annotations

from dataclasses import dataclass

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.constants import (
    TEACHER_ASSIST_OCR_REAL_PROVIDERS,
    validate_teacher_assist_ocr_provider,
)
from oziebot_api.services.teacher_assist.ocr_errors import TeacherAssistOCRProviderError

TEACHER_ASSIST_OCR_SUPPORTED_MIME_TYPES = frozenset(
    {
        "application/pdf",
        "image/jpeg",
        "image/jpg",
        "image/png",
        "image/tiff",
        "image/webp",
        "image/gif",
    }
)

OPENAI_VISION_SUPPORTED_MIME_TYPES = frozenset(
    {
        "image/jpeg",
        "image/jpg",
        "image/png",
        "image/webp",
        "image/gif",
    }
)


def confidence_level_from_score(score: float) -> str:
    if score < 0.4:
        return "low"
    if score < 0.75:
        return "medium"
    return "high"


def get_teacher_assist_ocr_allowed_providers(settings: Settings) -> set[str]:
    raw = settings.teacher_assist_ocr_allowed_providers or ""
    providers = {item.strip() for item in raw.split(",") if item.strip()}
    return providers or {"mock"}


def get_teacher_assist_ocr_allowed_models(settings: Settings) -> set[str]:
    raw = settings.teacher_assist_ocr_allowed_models or ""
    models = {item.strip() for item in raw.split(",") if item.strip()}
    return models or {"mock-ocr"}


def resolve_teacher_assist_ocr_provider_mode(provider_name: str) -> str:
    return "mock" if provider_name == "mock" else "real"


@dataclass(frozen=True)
class TeacherAssistOCRProviderCircuitState:
    state: str
    reason: str | None = None


class TeacherAssistOCRProviderCircuitBreaker:
    def state_for_provider(self, settings: Settings, provider_name: str) -> TeacherAssistOCRProviderCircuitState:
        normalized = validate_teacher_assist_ocr_provider(provider_name)
        if normalized == "mock":
            return TeacherAssistOCRProviderCircuitState(state="closed")
        if normalized not in TEACHER_ASSIST_OCR_REAL_PROVIDERS:
            return TeacherAssistOCRProviderCircuitState(
                state="open",
                reason=f"TeacherAssist OCR provider '{normalized}' is not implemented",
            )
        if not settings.teacher_assist_real_ocr_enabled:
            return TeacherAssistOCRProviderCircuitState(
                state="open",
                reason="TeacherAssist real OCR providers are disabled",
            )
        allowed_providers = get_teacher_assist_ocr_allowed_providers(settings)
        if normalized not in allowed_providers:
            return TeacherAssistOCRProviderCircuitState(
                state="open",
                reason=f"TeacherAssist OCR provider '{normalized}' is not allowed",
            )
        if settings.teacher_assist_ocr_daily_cost_limit_cents <= 0:
            return TeacherAssistOCRProviderCircuitState(
                state="open",
                reason="TeacherAssist OCR daily cost limit is zero; real OCR disabled by policy",
            )
        credential_state = self._credential_state(settings, normalized)
        if credential_state.state != "closed":
            return credential_state
        return TeacherAssistOCRProviderCircuitState(state="closed")

    def _credential_state(self, settings: Settings, provider_name: str) -> TeacherAssistOCRProviderCircuitState:
        if provider_name == "textract":
            return TeacherAssistOCRProviderCircuitState(state="closed")
        if provider_name == "openai_vision":
            if not settings.teacher_assist_openai_api_key:
                return TeacherAssistOCRProviderCircuitState(
                    state="open",
                    reason="TeacherAssist OpenAI API key is not configured for OCR",
                )
            return TeacherAssistOCRProviderCircuitState(state="closed")
        return TeacherAssistOCRProviderCircuitState(
            state="open",
            reason=f"TeacherAssist OCR provider '{provider_name}' is not configured",
        )

    def assert_can_execute(self, settings: Settings, provider_name: str) -> None:
        state = self.state_for_provider(settings, provider_name)
        if state.state != "closed":
            error_code = "provider_disabled"
            if state.reason and "not configured" in state.reason.lower():
                error_code = "provider_not_configured"
            elif state.reason and "not allowed" in state.reason.lower():
                error_code = "provider_not_configured"
            raise TeacherAssistOCRProviderError(
                state.reason or "TeacherAssist OCR provider execution is blocked",
                error_code=error_code,
            )


def get_teacher_assist_ocr_provider_model(settings: Settings, *, provider_name: str) -> str:
    if provider_name == "mock":
        return "mock-ocr"
    if provider_name == "textract":
        model_name = "textract-detect-document-text"
    elif provider_name == "openai_vision":
        model_name = (settings.teacher_assist_ocr_openai_vision_model or "gpt-4o-mini").strip()
    else:
        raise TeacherAssistOCRProviderError(
            f"TeacherAssist OCR provider '{provider_name}' is not configured",
            error_code="provider_not_configured",
        )
    if model_name not in get_teacher_assist_ocr_allowed_models(settings):
        raise TeacherAssistOCRProviderError(
            f"TeacherAssist OCR model '{model_name}' is not allowed",
            error_code="provider_not_configured",
        )
    return model_name


def assert_ocr_artifact_supported(
    settings: Settings,
    *,
    mime_type: str,
    file_size: int,
    provider_name: str,
) -> None:
    normalized_mime = (mime_type or "application/octet-stream").strip().lower()
    max_bytes = max(1, int(settings.teacher_assist_ocr_max_file_bytes))
    if file_size > max_bytes:
        raise TeacherAssistOCRProviderError(
            f"Artifact exceeds TeacherAssist OCR max file size ({max_bytes} bytes)",
            error_code="provider_not_configured",
            metadata={"file_size": file_size, "max_file_bytes": max_bytes},
        )
    if provider_name == "mock":
        return
    if normalized_mime not in TEACHER_ASSIST_OCR_SUPPORTED_MIME_TYPES:
        raise TeacherAssistOCRProviderError(
            f"Unsupported MIME type for TeacherAssist OCR: {normalized_mime}",
            error_code="unsupported_mime_type",
            metadata={"mime_type": normalized_mime},
        )
    if provider_name == "openai_vision" and normalized_mime not in OPENAI_VISION_SUPPORTED_MIME_TYPES:
        raise TeacherAssistOCRProviderError(
            f"OpenAI vision OCR does not support MIME type: {normalized_mime}",
            error_code="unsupported_mime_type",
            metadata={"mime_type": normalized_mime, "provider": provider_name},
        )

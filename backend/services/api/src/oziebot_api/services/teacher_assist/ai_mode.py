"""TeacherAssist global AI mode resolution and connection testing."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.constants import validate_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.openai_json_client import execute_openai_json_completion
from oziebot_api.services.teacher_assist.provider_config import (
    TeacherAssistProviderCircuitBreaker,
    get_teacher_assist_allowed_models,
    get_teacher_assist_provider_model,
)
from oziebot_api.services.teacher_assist.ai_usage import get_effective_daily_cost_limit_cents
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings


_REAL_AI_PROVIDERS = frozenset({"openai", "gemini"})


def is_teacher_assist_real_ai_active(db: Session | None, settings: Settings) -> bool:
    effective = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(effective.teacher_assist_ai_provider)
    if provider_name not in _REAL_AI_PROVIDERS:
        return False
    real_provider_enabled = bool(
        effective.teacher_assist_real_provider_enabled
        or effective.teacher_assist_ai_enable_real_provider
    )
    if not real_provider_enabled:
        return False
    if provider_name == "openai" and not (effective.teacher_assist_openai_api_key or "").strip():
        return False
    if provider_name == "gemini" and not (effective.teacher_assist_gemini_api_key or "").strip():
        return False
    if get_effective_daily_cost_limit_cents(db, effective) <= 0:
        return False
    circuit = TeacherAssistProviderCircuitBreaker().state_for_provider(effective, provider_name)
    return circuit.state == "closed"


def get_teacher_assist_ai_mode_status(db: Session | None, settings: Settings) -> dict[str, Any]:
    effective = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(effective.teacher_assist_ai_provider)
    real_active = is_teacher_assist_real_ai_active(db, settings)
    ai_mode = f"real_{provider_name}" if real_active else "mock"
    if real_active:
        banner_message = "Real AI mode is enabled. Content should still be reviewed before use."
    else:
        banner_message = "Mock AI mode is currently enabled. Generated content is for testing."
    return {
        "ai_mode": ai_mode,
        "provider": provider_name,
        "model": effective.teacher_assist_real_provider_model if real_active else "mock",
        "banner_message": banner_message,
        "real_ai_enabled": real_active,
    }


def test_teacher_assist_openai_connection(db: Session, *, env_settings: Settings) -> dict[str, Any]:
    effective = resolve_teacher_assist_settings(db, env_settings)
    try:
        provider_name = validate_teacher_assist_ai_provider(effective.teacher_assist_ai_provider)
        if provider_name not in _REAL_AI_PROVIDERS:
            return {
                "success": False,
                "message": f"Provider must be set to openai or gemini to test connection (currently: {provider_name}).",
            }
        if (
            provider_name == "openai"
            and not (effective.teacher_assist_openai_api_key or "").strip()
        ):
            return {"success": False, "message": "OpenAI API key is not configured on the server."}
        if (
            provider_name == "gemini"
            and not (effective.teacher_assist_gemini_api_key or "").strip()
        ):
            return {"success": False, "message": "Gemini API key is not configured on the server."}
        model_name = get_teacher_assist_provider_model(effective, provider_name=provider_name)
    except RuntimeError as exc:
        return {"success": False, "message": str(exc)}

    api_key = (
        effective.teacher_assist_gemini_api_key
        if provider_name == "gemini"
        else effective.teacher_assist_openai_api_key
    )
    base_url = (
        effective.teacher_assist_gemini_base_url
        if provider_name == "gemini"
        else effective.teacher_assist_openai_base_url
    )
    try:
        result = execute_openai_json_completion(
            effective,
            model_name=model_name,
            instruction="Return a minimal JSON object confirming connectivity.",
            prompt_payload={"ping": True},
            required_output_schema={"ok": True},
            system_prompt='Return JSON only. Respond with {"ok": true}.',
            timeout_seconds=30.0,
            _api_key=api_key,
            _base_url=base_url,
            _provider=provider_name,
        )
        if result.content_json.get("ok") is not True:
            return {
                "success": False,
                "message": f"{provider_name} responded but did not return the expected test payload.",
            }
        return {
            "success": True,
            "message": f"{provider_name} connection successful.",
            "model": model_name,
            "input_tokens": result.input_tokens,
            "output_tokens": result.output_tokens,
        }
    except Exception as exc:
        return {"success": False, "message": str(exc)}


def list_teacher_assist_allowed_models_for_admin(settings: Settings) -> list[str]:
    allowed = sorted(get_teacher_assist_allowed_models(settings))
    return [model for model in allowed if model != "mock"]

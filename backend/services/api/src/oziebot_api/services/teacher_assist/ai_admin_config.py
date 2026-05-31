"""Root-admin TeacherAssist AI provider config read/write helpers."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker
from oziebot_api.services.teacher_assist.runtime_settings import (
    get_persisted_teacher_assist_ai_row,
    resolve_teacher_assist_settings,
    save_teacher_assist_ai_admin_config,
)


def _build_admin_config_payload(*, env_settings: Settings, effective_settings: Settings, db: Session) -> dict[str, Any]:
    provider_name = (effective_settings.teacher_assist_ai_provider or "mock").strip().lower() or "mock"
    real_provider_enabled = bool(
        effective_settings.teacher_assist_real_provider_enabled
        or effective_settings.teacher_assist_ai_enable_real_provider
    )
    openai_key_configured = bool((env_settings.teacher_assist_openai_api_key or "").strip())
    circuit = TeacherAssistProviderCircuitBreaker().state_for_provider(effective_settings, provider_name)
    openai_active = (
        provider_name == "openai"
        and real_provider_enabled
        and openai_key_configured
        and circuit.state == "closed"
    )
    blockers: list[str] = []
    if provider_name == "openai" and not real_provider_enabled:
        blockers.append("Real provider is disabled in admin settings.")
    if provider_name == "openai" and not openai_key_configured:
        blockers.append("TEACHER_ASSIST_OPENAI_API_KEY is not set on the server.")
    if circuit.state != "closed" and circuit.reason:
        blockers.append(circuit.reason)

    persisted_row = get_persisted_teacher_assist_ai_row(db)
    persisted_value = persisted_row.value if persisted_row else None

    return {
        "configured_provider": provider_name,
        "effective_mode": "openai" if openai_active else "mock",
        "real_provider_enabled": real_provider_enabled,
        "openai_api_key_configured": openai_key_configured,
        "real_provider_model": effective_settings.teacher_assist_real_provider_model,
        "daily_cost_limit_cents": env_settings.teacher_assist_ai_daily_cost_limit_cents,
        "ocr_provider": env_settings.teacher_assist_ocr_provider,
        "fixture_mode": env_settings.teacher_assist_ai_fixture_mode,
        "circuit_state": circuit.state,
        "circuit_reason": circuit.reason,
        "blockers": blockers,
        "config_source": "platform_settings" if persisted_row else "environment",
        "persisted_config": persisted_value,
        "persisted_updated_at": persisted_row.updated_at.isoformat() if persisted_row else None,
        "persisted_updated_by_user_id": str(persisted_row.updated_by_user_id)
        if persisted_row and persisted_row.updated_by_user_id
        else None,
        "env_defaults": {
            "ai_provider": env_settings.teacher_assist_ai_provider,
            "real_provider_enabled": bool(
                env_settings.teacher_assist_real_provider_enabled
                or env_settings.teacher_assist_ai_enable_real_provider
            ),
            "real_provider_model": env_settings.teacher_assist_real_provider_model,
        },
        "notes": [
            "Admin toggles persist immediately in platform_settings and apply without restarting the API.",
            "TEACHER_ASSIST_OPENAI_API_KEY remains server-side only and is never stored in the database.",
        ],
    }


def get_teacher_assist_ai_admin_config(db: Session, *, env_settings: Settings) -> dict[str, Any]:
    effective_settings = resolve_teacher_assist_settings(db, env_settings)
    return _build_admin_config_payload(
        env_settings=env_settings,
        effective_settings=effective_settings,
        db=db,
    )


def update_teacher_assist_ai_admin_config(
    db: Session,
    *,
    user: User,
    env_settings: Settings,
    ai_provider: str,
    real_provider_enabled: bool,
    real_provider_model: str | None,
) -> dict[str, Any]:
    save_teacher_assist_ai_admin_config(
        db,
        user_id=user.id,
        env_settings=env_settings,
        ai_provider=ai_provider,
        real_provider_enabled=real_provider_enabled,
        real_provider_model=real_provider_model,
    )
    db.flush()
    return get_teacher_assist_ai_admin_config(db, env_settings=env_settings)

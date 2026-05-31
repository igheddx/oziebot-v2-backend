"""Root-admin TeacherAssist AI provider config read/write helpers."""

from __future__ import annotations

import uuid
import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_mode import (
    get_teacher_assist_ai_mode_status,
    is_teacher_assist_real_ai_active,
    list_teacher_assist_allowed_models_for_admin,
)
from oziebot_api.services.teacher_assist.ai_usage import (
    get_effective_daily_cost_limit_cents,
    get_teacher_assist_ai_usage_summary,
    get_teacher_assist_daily_usage_cents,
)
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker
from oziebot_api.services.teacher_assist.runtime_settings import (
    get_persisted_teacher_assist_ai_row,
    resolve_teacher_assist_settings,
    save_teacher_assist_ai_admin_config,
)


def _resolve_updated_by_label(db: Session, user_id: str | None) -> str | None:
    if not user_id:
        return None
    from oziebot_api.models.user import User as UserModel

    try:
        user = db.get(UserModel, uuid.UUID(user_id))
    except ValueError:
        return user_id
    if user is None:
        return user_id
    return user.full_name or user.email


def _build_admin_config_payload(*, env_settings: Settings, effective_settings: Settings, db: Session) -> dict[str, Any]:
    mode_status = get_teacher_assist_ai_mode_status(db, env_settings)
    provider_name = (effective_settings.teacher_assist_ai_provider or "mock").strip().lower() or "mock"
    real_provider_enabled = bool(
        effective_settings.teacher_assist_real_provider_enabled
        or effective_settings.teacher_assist_ai_enable_real_provider
    )
    openai_key_configured = bool((env_settings.teacher_assist_openai_api_key or "").strip())
    circuit = TeacherAssistProviderCircuitBreaker().state_for_provider(effective_settings, provider_name)
    real_active = is_teacher_assist_real_ai_active(db, env_settings)
    effective_mode = "real_openai" if real_active else "mock"
    daily_cost_limit_cents = get_effective_daily_cost_limit_cents(db, env_settings)
    blockers: list[str] = []
    if provider_name == "openai" and not real_provider_enabled:
        blockers.append("Real provider is disabled in admin settings.")
    if provider_name == "openai" and not openai_key_configured:
        blockers.append("TEACHER_ASSIST_OPENAI_API_KEY is not set on the server.")
    if provider_name == "openai" and real_provider_enabled and daily_cost_limit_cents <= 0:
        blockers.append("Daily cost limit must be set before enabling real OpenAI mode.")
    if circuit.state != "closed" and circuit.reason:
        blockers.append(circuit.reason)

    persisted_row = get_persisted_teacher_assist_ai_row(db)
    persisted_value = persisted_row.value if persisted_row else None
    usage_summary = get_teacher_assist_ai_usage_summary(db, hours=24)
    updated_by_user_id = (
        str(persisted_row.updated_by_user_id)
        if persisted_row and persisted_row.updated_by_user_id
        else None
    )

    return {
        "ai_mode": effective_mode,
        "configured_provider": provider_name,
        "effective_mode": "openai" if real_active else "mock",
        "real_provider_enabled": real_provider_enabled,
        "openai_api_key_configured": openai_key_configured,
        "openai_api_key_status": "configured" if openai_key_configured else "missing",
        "real_provider_model": effective_settings.teacher_assist_real_provider_model,
        "allowed_models": list_teacher_assist_allowed_models_for_admin(env_settings),
        "daily_cost_limit_cents": daily_cost_limit_cents,
        "daily_usage_cents": get_teacher_assist_daily_usage_cents(db),
        "usage_summary": usage_summary,
        "ocr_provider": env_settings.teacher_assist_ocr_provider,
        "fixture_mode": env_settings.teacher_assist_ai_fixture_mode,
        "circuit_state": circuit.state,
        "circuit_reason": circuit.reason,
        "blockers": blockers,
        "config_source": "platform_settings" if persisted_row else "environment",
        "persisted_config": persisted_value,
        "persisted_updated_at": persisted_row.updated_at.isoformat() if persisted_row else None,
        "persisted_updated_by_user_id": updated_by_user_id,
        "last_updated_by": _resolve_updated_by_label(db, updated_by_user_id),
        "last_updated_at": persisted_row.updated_at.isoformat() if persisted_row else None,
        "teacher_banner_message": mode_status["banner_message"],
        "env_defaults": {
            "ai_provider": env_settings.teacher_assist_ai_provider,
            "real_provider_enabled": bool(
                env_settings.teacher_assist_real_provider_enabled
                or env_settings.teacher_assist_ai_enable_real_provider
            ),
            "real_provider_model": env_settings.teacher_assist_real_provider_model,
            "daily_cost_limit_cents": env_settings.teacher_assist_ai_daily_cost_limit_cents,
        },
        "notes": [
            "Admin toggles persist immediately in platform_settings and apply without restarting the API.",
            "TEACHER_ASSIST_OPENAI_API_KEY remains server-side only and is never stored in the database.",
            "Real OpenAI mode requires an API key, allowlisted model, and a daily cost limit.",
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
    daily_cost_limit_cents: int | None = None,
) -> dict[str, Any]:
    save_teacher_assist_ai_admin_config(
        db,
        user_id=user.id,
        env_settings=env_settings,
        ai_provider=ai_provider,
        real_provider_enabled=real_provider_enabled,
        real_provider_model=real_provider_model,
        daily_cost_limit_cents=daily_cost_limit_cents,
    )
    db.flush()
    return get_teacher_assist_ai_admin_config(db, env_settings=env_settings)

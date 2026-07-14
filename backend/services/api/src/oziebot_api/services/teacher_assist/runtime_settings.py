"""Persisted TeacherAssist runtime configuration (platform_settings)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.platform_setting import PlatformSetting
from oziebot_api.services.audit import record_admin_action
from oziebot_api.services.platform_management import upsert_setting
from oziebot_api.services.teacher_assist.constants import validate_teacher_assist_ai_provider

PLATFORM_SETTING_TEACHER_ASSIST_AI = "teacher_assist.ai_provider"


def get_persisted_teacher_assist_ai_row(db: Session) -> PlatformSetting | None:
    return db.get(PlatformSetting, PLATFORM_SETTING_TEACHER_ASSIST_AI)


def resolve_teacher_assist_settings(db: Session | None, settings: Settings) -> Settings:
    if db is None:
        return settings
    row = get_persisted_teacher_assist_ai_row(db)
    if row is None:
        return settings
    value = row.value or {}
    updates: dict[str, Any] = {}
    if value.get("ai_provider"):
        updates["teacher_assist_ai_provider"] = validate_teacher_assist_ai_provider(
            str(value["ai_provider"])
        )
    if "real_provider_enabled" in value:
        updates["teacher_assist_real_provider_enabled"] = bool(value["real_provider_enabled"])
    model = value.get("real_provider_model")
    if isinstance(model, str) and model.strip():
        updates["teacher_assist_real_provider_model"] = model.strip()
    if "daily_cost_limit_cents" in value:
        try:
            updates["teacher_assist_ai_daily_cost_limit_cents"] = max(
                0, int(value["daily_cost_limit_cents"])
            )
        except (TypeError, ValueError):
            pass
    return settings.model_copy(update=updates) if updates else settings


def save_teacher_assist_ai_admin_config(
    db: Session,
    *,
    user_id: uuid.UUID,
    env_settings: Settings,
    ai_provider: str,
    real_provider_enabled: bool,
    real_provider_model: str | None,
    daily_cost_limit_cents: int | None = None,
) -> PlatformSetting:
    normalized_provider = validate_teacher_assist_ai_provider(ai_provider)
    normalized_model = (real_provider_model or "").strip() or None

    if normalized_provider in ("openai", "gemini") and real_provider_enabled:
        if (
            normalized_provider == "openai"
            and not (env_settings.teacher_assist_openai_api_key or "").strip()
        ):
            raise ValueError(
                "TEACHER_ASSIST_OPENAI_API_KEY must be configured on the server before enabling OpenAI."
            )
        if (
            normalized_provider == "gemini"
            and not (env_settings.teacher_assist_gemini_api_key or "").strip()
        ):
            raise ValueError(
                "TEACHER_ASSIST_GEMINI_API_KEY must be configured on the server before enabling Gemini."
            )
        resolved_model = (
            normalized_model or (env_settings.teacher_assist_real_provider_model or "").strip()
        )
        if not resolved_model:
            raise ValueError(f"A model name is required when {normalized_provider} is enabled.")
        from oziebot_api.services.teacher_assist.provider_config import (
            get_teacher_assist_allowed_models,
        )

        allowed = get_teacher_assist_allowed_models(env_settings)
        if resolved_model not in allowed:
            raise ValueError(f"Model '{resolved_model}' is not in the allowed models list.")
        normalized_model = resolved_model
        resolved_limit = daily_cost_limit_cents
        if resolved_limit is None:
            persisted = get_persisted_teacher_assist_ai_row(db)
            persisted_limit = (
                (persisted.value or {}).get("daily_cost_limit_cents") if persisted else None
            )
            if persisted_limit is not None:
                resolved_limit = int(persisted_limit)
            else:
                resolved_limit = env_settings.teacher_assist_ai_daily_cost_limit_cents
        if int(resolved_limit or 0) <= 0:
            raise ValueError(
                f"Daily cost limit must be set before enabling real {normalized_provider} mode."
            )

    resolved_daily_limit = (
        daily_cost_limit_cents
        if daily_cost_limit_cents is not None
        else (
            int((previous.value or {}).get("daily_cost_limit_cents"))
            if (previous := get_persisted_teacher_assist_ai_row(db))
            and isinstance((previous.value or {}).get("daily_cost_limit_cents"), (int, float, str))
            else env_settings.teacher_assist_ai_daily_cost_limit_cents
        )
    )

    payload = {
        "ai_provider": normalized_provider,
        "real_provider_enabled": bool(real_provider_enabled),
        "real_provider_model": normalized_model,
        "daily_cost_limit_cents": max(0, int(resolved_daily_limit or 0)),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    previous = get_persisted_teacher_assist_ai_row(db)
    row = upsert_setting(
        db,
        key=PLATFORM_SETTING_TEACHER_ASSIST_AI,
        value=payload,
        updated_by_user_id=user_id,
    )
    record_admin_action(
        db,
        actor_user_id=user_id,
        action="teacher_assist.ai_provider.upsert",
        resource_type="platform_settings",
        resource_id=PLATFORM_SETTING_TEACHER_ASSIST_AI,
        details={"before": previous.value if previous else None, "after": payload},
    )
    return row

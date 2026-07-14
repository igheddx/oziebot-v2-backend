from __future__ import annotations

import uuid

import pytest

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ai_admin_config import (
    get_teacher_assist_ai_admin_config,
    update_teacher_assist_ai_admin_config,
)
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.runtime_settings import (
    PLATFORM_SETTING_TEACHER_ASSIST_AI,
    resolve_teacher_assist_settings,
    save_teacher_assist_ai_admin_config,
)
from tests.test_teacher_assist_v2_supporting_materials import _make_root_admin, _make_teacher


def test_v2_ai_provider_config_root_admin_read_and_update(client, db_session):
    token = _make_root_admin(db_session, client, "v2-ai-config-root@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    read = client.get("/v1/teacher-assist-v2/admin/ai-provider-config", headers=headers)
    assert read.status_code == 200, read.text
    payload = read.json()
    assert payload["configured_provider"] == "mock"
    assert payload["config_source"] == "environment"

    update = client.put(
        "/v1/teacher-assist-v2/admin/ai-provider-config",
        headers=headers,
        json={
            "ai_provider": "mock",
            "real_provider_enabled": False,
            "real_provider_model": None,
        },
    )
    assert update.status_code == 200, update.text
    updated = update.json()
    assert updated["config_source"] == "platform_settings"
    assert updated["persisted_config"]["ai_provider"] == "mock"
    assert updated["configured_provider"] == "mock"


def test_v2_ai_provider_config_teacher_forbidden(client, db_session):
    token = _make_teacher(db_session, client, "v2-ai-config-teacher@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    read = client.get("/v1/teacher-assist-v2/admin/ai-provider-config", headers=headers)
    assert read.status_code == 403

    update = client.put(
        "/v1/teacher-assist-v2/admin/ai-provider-config",
        headers=headers,
        json={
            "ai_provider": "mock",
            "real_provider_enabled": False,
            "real_provider_model": None,
        },
    )
    assert update.status_code == 403


def test_v2_ai_provider_config_openai_requires_api_key(client, db_session):
    token = _make_root_admin(db_session, client, "v2-ai-config-openai@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    update = client.put(
        "/v1/teacher-assist-v2/admin/ai-provider-config",
        headers=headers,
        json={
            "ai_provider": "openai",
            "real_provider_enabled": True,
            "real_provider_model": "gpt-4o-mini",
            "daily_cost_limit_cents": 500,
        },
    )
    assert update.status_code == 400
    assert "TEACHER_ASSIST_OPENAI_API_KEY" in update.json()["detail"]


def test_v2_ai_provider_config_openai_requires_daily_cost_limit(client, db_session):
    _make_root_admin(db_session, client, "v2-ai-config-limit@example.com")
    from sqlalchemy import select

    from oziebot_api.models.user import User

    user = db_session.scalar(select(User).where(User.email == "v2-ai-config-limit@example.com"))
    assert user is not None

    env_settings = Settings(
        teacher_assist_ai_provider="mock",
        teacher_assist_allowed_models="mock,gpt-4o-mini",
        teacher_assist_openai_api_key="sk-test",
    )
    with pytest.raises(ValueError, match="Daily cost limit"):
        save_teacher_assist_ai_admin_config(
            db_session,
            user_id=user.id,
            env_settings=env_settings,
            ai_provider="openai",
            real_provider_enabled=True,
            real_provider_model="gpt-4o-mini",
            daily_cost_limit_cents=0,
        )


def test_v2_teacher_ai_generation_status(client, db_session):
    token = _make_teacher(db_session, client, "v2-ai-mode-teacher@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    response = client.get("/v1/teacher-assist-v2/teacher/ai-generation-status", headers=headers)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["ai_mode"] == "mock"
    assert "banner_message" in payload


def test_v2_ai_test_connection_without_key(client, db_session):
    token = _make_root_admin(db_session, client, "v2-ai-test-root@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    response = client.post(
        "/v1/teacher-assist-v2/admin/ai-provider-config/test-connection", headers=headers
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["success"] is False


def test_resolve_teacher_assist_settings_overlays_platform_settings(db_session):
    env_settings = Settings(
        teacher_assist_ai_provider="mock", teacher_assist_real_provider_enabled=False
    )
    user_id = uuid.uuid4()
    save_teacher_assist_ai_admin_config(
        db_session,
        user_id=user_id,
        env_settings=env_settings,
        ai_provider="mock",
        real_provider_enabled=True,
        real_provider_model="mock",
    )
    db_session.flush()

    effective = resolve_teacher_assist_settings(db_session, env_settings)
    assert effective.teacher_assist_real_provider_enabled is True

    provider = get_teacher_assist_ai_provider(env_settings, db=db_session)
    assert provider.provider_name == "mock"


def test_ai_admin_config_service_round_trip(client, db_session):
    _make_root_admin(db_session, client, "v2-ai-config-service@example.com")
    from sqlalchemy import select

    from oziebot_api.models.user import User

    user = db_session.scalar(select(User).where(User.email == "v2-ai-config-service@example.com"))
    assert user is not None

    env_settings = Settings(
        teacher_assist_ai_provider="mock",
        teacher_assist_allowed_models="mock,gpt-4o-mini",
        teacher_assist_openai_api_key="sk-test",
    )
    config = update_teacher_assist_ai_admin_config(
        db_session,
        user=user,
        env_settings=env_settings,
        ai_provider="openai",
        real_provider_enabled=True,
        real_provider_model="gpt-4o-mini",
        daily_cost_limit_cents=500,
    )
    assert config["configured_provider"] == "openai"
    assert config["ai_mode"] == "real_openai"
    assert config["config_source"] == "platform_settings"
    assert config["real_provider_enabled"] is True

    reread = get_teacher_assist_ai_admin_config(db_session, env_settings=env_settings)
    assert reread["persisted_config"]["ai_provider"] == "openai"
    assert PLATFORM_SETTING_TEACHER_ASSIST_AI == "teacher_assist.ai_provider"

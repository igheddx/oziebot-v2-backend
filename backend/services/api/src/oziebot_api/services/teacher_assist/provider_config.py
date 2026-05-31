from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ai_provider import (
    TeacherAssistAIProvider,
    TeacherAssistAIProviderResult,
)
from oziebot_api.services.teacher_assist.constants import (
    validate_teacher_assist_ai_fixture_mode,
    validate_teacher_assist_ai_provider,
)
from oziebot_api.services.teacher_assist.fixture_store import TeacherAssistAIFixtureStore
from oziebot_api.services.teacher_assist.mock_ai_provider import MockTeacherAssistAIProvider
from oziebot_api.services.teacher_assist.openai_ai_provider import OpenAITeacherAssistAIProvider
from sqlalchemy.orm import Session

from oziebot_api.services.teacher_assist.prompt_contracts import (
    GRADING_ASSIST_FEATURE,
    GRADING_DRAFT_GENERATION_FEATURE,
    INSTRUCTIONAL_PLAN_GENERATION_FEATURE,
    V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
    V2_LESSON_PLAN_GENERATION_FEATURE,
)
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings

REAL_AI_WORKFLOWS = frozenset(
    {
        INSTRUCTIONAL_PLAN_GENERATION_FEATURE,
        GRADING_ASSIST_FEATURE,
        GRADING_DRAFT_GENERATION_FEATURE,
        V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
        V2_LESSON_PLAN_GENERATION_FEATURE,
    }
)


class FixtureAwareTeacherAssistAIProvider(TeacherAssistAIProvider):
    def __init__(
        self,
        *,
        inner: TeacherAssistAIProvider,
        fixture_store: TeacherAssistAIFixtureStore,
        fixture_mode: str,
    ) -> None:
        self.provider_name = inner.provider_name
        self._inner = inner
        self._fixture_store = fixture_store
        self._fixture_mode = fixture_mode

    def generate_instructional_plan(
        self, context_preview: dict[str, Any]
    ) -> TeacherAssistAIProviderResult:
        fixture_key = str(context_preview.get("draft", {}).get("id") or "teacher-assist-weekly-plan")
        if self._fixture_mode == "replay":
            return self._fixture_store.load(feature=INSTRUCTIONAL_PLAN_GENERATION_FEATURE, key=fixture_key)

        result = self._inner.generate_instructional_plan(context_preview)
        if self._fixture_mode == "record":
            self._fixture_store.save(
                feature=INSTRUCTIONAL_PLAN_GENERATION_FEATURE,
                key=fixture_key,
                result=result,
            )
        return result


def get_teacher_assist_allowed_models(settings: Settings) -> set[str]:
    raw = settings.teacher_assist_allowed_models or ""
    models = {item.strip() for item in raw.split(",") if item.strip()}
    return models or {"mock"}


@dataclass(frozen=True)
class TeacherAssistProviderCircuitState:
    state: str
    reason: str | None = None


class TeacherAssistProviderCircuitBreaker:
    def state_for_provider(self, settings: Settings, provider_name: str) -> TeacherAssistProviderCircuitState:
        real_provider_enabled = (
            settings.teacher_assist_real_provider_enabled or settings.teacher_assist_ai_enable_real_provider
        )
        if provider_name != "mock" and not real_provider_enabled:
            return TeacherAssistProviderCircuitState(
                state="open",
                reason="TeacherAssist real AI providers are disabled",
            )
        return TeacherAssistProviderCircuitState(state="closed")

    def assert_can_execute(self, settings: Settings, provider_name: str) -> None:
        state = self.state_for_provider(settings, provider_name)
        if state.state != "closed":
            raise RuntimeError(state.reason or "TeacherAssist provider execution is blocked")


def get_teacher_assist_provider_model(settings: Settings, *, provider_name: str) -> str:
    if provider_name == "mock":
        return "mock"
    model_name = (settings.teacher_assist_real_provider_model or "").strip()
    if not model_name:
        model_name = (settings.teacher_assist_openai_default_model or "").strip()
    if not model_name:
        raise RuntimeError("TeacherAssist real provider model is not configured")
    if model_name not in get_teacher_assist_allowed_models(settings):
        raise RuntimeError(f"TeacherAssist model '{model_name}' is not allowed")
    return model_name


def get_teacher_assist_ai_provider(
    settings: Settings,
    *,
    db: Session | None = None,
    workflow_type: str = INSTRUCTIONAL_PLAN_GENERATION_FEATURE,
) -> TeacherAssistAIProvider:
    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(effective_settings.teacher_assist_ai_provider)
    fixture_mode = validate_teacher_assist_ai_fixture_mode(effective_settings.teacher_assist_ai_fixture_mode)
    TeacherAssistProviderCircuitBreaker().assert_can_execute(effective_settings, provider_name)
    if provider_name != "mock" and workflow_type not in REAL_AI_WORKFLOWS:
        raise RuntimeError("TeacherAssist real provider execution is limited to supported workflows")

    if provider_name == "mock":
        provider: TeacherAssistAIProvider = MockTeacherAssistAIProvider()
    elif provider_name == "openai":
        if not effective_settings.teacher_assist_openai_api_key:
            raise RuntimeError("TeacherAssist OpenAI API key is not configured")
        provider = OpenAITeacherAssistAIProvider(
            effective_settings,
            model_name=get_teacher_assist_provider_model(effective_settings, provider_name=provider_name),
        )
    else:
        raise NotImplementedError(f"TeacherAssist AI provider '{provider_name}' is not implemented")

    if fixture_mode == "off":
        return provider
    return FixtureAwareTeacherAssistAIProvider(
        inner=provider,
        fixture_store=TeacherAssistAIFixtureStore(effective_settings.teacher_assist_ai_fixtures_root),
        fixture_mode=fixture_mode,
    )

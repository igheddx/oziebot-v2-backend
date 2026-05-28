from __future__ import annotations

from typing import Any

from oziebot_api.services.teacher_assist.mock_ai_provider import MockTeacherAssistAIProvider


def generate_mock_weekly_plan(context_snapshot: dict[str, Any]) -> dict[str, Any]:
    return MockTeacherAssistAIProvider().generate_instructional_plan(context_snapshot).content_json

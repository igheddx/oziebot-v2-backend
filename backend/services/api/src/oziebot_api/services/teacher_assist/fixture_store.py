from __future__ import annotations

import json
from pathlib import Path

from oziebot_api.services.teacher_assist.ai_provider import TeacherAssistAIProviderResult


class TeacherAssistAIFixtureStore:
    def __init__(self, root: str):
        self._root = Path(root)

    def _path(self, feature: str, key: str) -> Path:
        safe_key = key.replace("/", "_")
        return self._root / feature / f"{safe_key}.json"

    def load(self, *, feature: str, key: str) -> TeacherAssistAIProviderResult:
        path = self._path(feature, key)
        if not path.exists():
            raise FileNotFoundError(f"TeacherAssist fixture not found for {feature}:{key}")
        with path.open("r", encoding="utf-8") as handle:
            return TeacherAssistAIProviderResult.from_dict(json.load(handle))

    def save(self, *, feature: str, key: str, result: TeacherAssistAIProviderResult) -> None:
        path = self._path(feature, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(result.to_dict(), handle, indent=2, sort_keys=True)

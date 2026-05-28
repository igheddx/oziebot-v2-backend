from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(slots=True)
class TeacherAssistAIProviderResult:
    content_json: dict[str, Any]
    provider: str
    model: str | None
    input_tokens: int | None = 0
    output_tokens: int | None = 0
    estimated_cost_cents: int | None = 0
    metadata_json: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TeacherAssistAIProviderResult":
        return cls(
            content_json=dict(payload["content_json"]),
            provider=str(payload["provider"]),
            model=payload.get("model"),
            input_tokens=payload.get("input_tokens"),
            output_tokens=payload.get("output_tokens"),
            estimated_cost_cents=payload.get("estimated_cost_cents"),
            metadata_json=payload.get("metadata_json"),
        )


class TeacherAssistAIProvider(ABC):
    provider_name: str

    @abstractmethod
    def generate_instructional_plan(
        self, context_preview: dict[str, Any]
    ) -> TeacherAssistAIProviderResult:
        raise NotImplementedError

    @abstractmethod
    def regenerate_instructional_plan_section(
        self,
        *,
        context_preview: dict[str, Any],
        current_plan_content: dict[str, Any],
        section_key: str,
        section_path: str | None = None,
        current_section_content: Any = None,
        teacher_instruction: str | None = None,
        preserve_existing_context: bool = True,
    ) -> TeacherAssistAIProviderResult:
        raise NotImplementedError

    def generate_weekly_plan(self, context_preview: dict[str, Any]) -> TeacherAssistAIProviderResult:
        return self.generate_instructional_plan(context_preview)

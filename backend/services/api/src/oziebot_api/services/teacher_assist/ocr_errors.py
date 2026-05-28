from __future__ import annotations

from typing import Any


class TeacherAssistOCRProviderError(Exception):
    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.metadata = dict(metadata or {})

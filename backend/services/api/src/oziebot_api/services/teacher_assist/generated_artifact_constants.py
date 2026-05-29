from __future__ import annotations

GENERATED_ARTIFACT_TYPES = (
    "LESSON_PLAN",
    "ASSIGNMENT",
    "QUIZ",
    "RUBRIC",
    "NEWSLETTER",
    "PARENT_COMMUNICATION",
)

GENERATED_ARTIFACT_STATUSES = (
    "draft",
    "ready",
    "in_progress",
    "completed",
    "archived",
)


def validate_generated_artifact_type(value: str | None) -> str:
    normalized = (value or "").strip().upper()
    if normalized not in GENERATED_ARTIFACT_TYPES:
        raise ValueError("Unsupported generated artifact type")
    return normalized


def validate_generated_artifact_status(value: str | None) -> str:
    normalized = (value or "draft").strip().lower() or "draft"
    if normalized not in GENERATED_ARTIFACT_STATUSES:
        raise ValueError("Unsupported generated artifact status")
    return normalized

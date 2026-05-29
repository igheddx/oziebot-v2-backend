from __future__ import annotations

PACING_GUIDE_TYPES = ("DISTRICT", "GRADE_LEVEL", "TEACHER")
PACING_GUIDE_PERIOD_TYPES = ("YEAR", "GRADING_PERIOD", "UNIT", "WEEK")


def validate_pacing_guide_type(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in PACING_GUIDE_TYPES:
        raise ValueError(f"Unsupported pacing guide type '{value}'")
    return normalized


def validate_pacing_guide_period_type(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in PACING_GUIDE_PERIOD_TYPES:
        raise ValueError(f"Unsupported pacing guide period type '{value}'")
    return normalized

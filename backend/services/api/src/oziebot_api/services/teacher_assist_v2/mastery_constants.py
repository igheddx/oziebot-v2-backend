"""Mastery level thresholds for confirmed grade evidence."""

MASTERY_LEVELS = frozenset({"mastery", "developing", "beginning"})
MASTERY_THRESHOLD_MASTERY = 80.0
MASTERY_THRESHOLD_DEVELOPING = 60.0
EVIDENCE_TYPE_CONFIRMED_GRADE = "confirmed_grade"


def resolve_mastery_level(percentage: float) -> str:
    if percentage >= MASTERY_THRESHOLD_MASTERY:
        return "mastery"
    if percentage >= MASTERY_THRESHOLD_DEVELOPING:
        return "developing"
    return "beginning"

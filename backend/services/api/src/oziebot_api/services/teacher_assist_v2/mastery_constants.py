"""Mastery level thresholds for confirmed grade evidence."""

MASTERY_LEVELS = frozenset({"mastery", "developing", "beginning", "missing"})
MASTERY_THRESHOLD_MASTERY = 80.0
MASTERY_THRESHOLD_DEVELOPING = 60.0
EVIDENCE_TYPE_CONFIRMED_GRADE = "confirmed_grade"

MASTERY_LEVEL_LABELS = {
    "mastery": "Mastery",
    "developing": "Developing",
    "beginning": "Beginning",
    "missing": "Missing",
}

MASTERY_LEVEL_SHORT_LABELS = {
    "mastery": "M",
    "developing": "D",
    "beginning": "B",
    "missing": "Mi",
}


def resolve_mastery_level(percentage: float) -> str:
    if percentage >= MASTERY_THRESHOLD_MASTERY:
        return "mastery"
    if percentage >= MASTERY_THRESHOLD_DEVELOPING:
        return "developing"
    return "beginning"


def format_mastery_level_label(level: str) -> str:
    normalized = level.strip().lower()
    return MASTERY_LEVEL_LABELS.get(normalized, normalized.title())


def serialize_mastery_level_fields(
    *,
    percentage: float | None = None,
    mastery_level: str | None = None,
) -> dict[str, str]:
    if mastery_level and mastery_level.strip().lower() == "missing":
        return {
            "mastery_level": "missing",
            "mastery_level_label": "Missing",
        }
    if percentage is None:
        return {
            "mastery_level": "missing",
            "mastery_level_label": "Missing",
        }
    resolved = mastery_level.strip().lower() if mastery_level else resolve_mastery_level(percentage)
    if resolved not in MASTERY_LEVELS or resolved == "missing":
        resolved = resolve_mastery_level(percentage)
    return {
        "mastery_level": resolved,
        "mastery_level_label": format_mastery_level_label(resolved),
    }

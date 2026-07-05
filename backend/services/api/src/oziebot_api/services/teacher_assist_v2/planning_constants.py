from __future__ import annotations

REQUIRED_PACKAGE_OUTPUTS = ("daily_lesson_plan",)

OPTIONAL_PACKAGE_OUTPUTS = (
    "subject_slide_deck",
    "student_lesson_deck",
    "assignment",
    "writing_response",
    "quiz",
    "rubric",
    "exit_ticket",
    "bell_ringer",
    "vocabulary_list",
    "study_guide",
    "parent_newsletter_summary",
)

PACKAGE_OUTPUT_TYPES = REQUIRED_PACKAGE_OUTPUTS + OPTIONAL_PACKAGE_OUTPUTS

PACKAGE_ARTIFACT_TYPES = PACKAGE_OUTPUT_TYPES

PACKAGE_ARTIFACT_GROUPS = {
    "daily_teaching_plans": ("daily_lesson_plan",),
    "subject_slide_decks": ("subject_slide_deck",),
    "student_lesson_decks": ("student_lesson_deck",),
    "assessments": ("assignment", "writing_response", "quiz", "rubric", "exit_ticket"),
    "student_materials": ("bell_ringer", "vocabulary_list", "study_guide"),
    "communication": ("parent_newsletter_summary",),
}

WEEKDAY_LABELS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")

# Class period durations in minutes, keyed by subject_code.
# ELA is split: reading block and writing block taught separately.
# All other subjects default to a single instructional block.
SUBJECT_CLASS_MINUTES: dict[str, dict[str, int]] = {
    "ELA": {"reading": 35, "writing": 30},
}
DEFAULT_CLASS_MINUTES = 40

WEEK_RANGE_PRESETS = (
    (1, 1, "Week 1"),
    (1, 2, "Weeks 1–2"),
    (1, 4, "Weeks 1–4"),
)

# ── Learning Recovery Planner ──────────────────────────────────────────────────

RECOVERY_ARTIFACT_TYPES = (
    "recovery_bell_ringer",
    "recovery_mini_lesson",
    "recovery_small_group_packet",
    "recovery_conference_guide",
    "recovery_exit_ticket",
    "recovery_guided_practice",
    "recovery_assignment",
    "recovery_homework",
    "recovery_assessment",
    "recovery_spiral_review",
    "recovery_presentation",
)

RECOVERY_INTENT_TYPES = ("understanding", "skill", "vocabulary", "fluency", "confidence")

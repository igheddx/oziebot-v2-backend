from __future__ import annotations

REQUIRED_PACKAGE_OUTPUTS = ("daily_lesson_plan", "subject_slide_deck")

OPTIONAL_PACKAGE_OUTPUTS = (
    "assignment",
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
    "assessments": ("assignment", "quiz", "rubric", "exit_ticket"),
    "student_materials": ("bell_ringer", "vocabulary_list", "study_guide"),
    "communication": ("parent_newsletter_summary",),
}

WEEKDAY_LABELS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")

WEEK_RANGE_PRESETS = (
    (1, 1, "Week 1"),
    (1, 2, "Weeks 1–2"),
    (1, 4, "Weeks 1–4"),
)

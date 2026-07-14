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

# ── Incremental Generation — Prompt Version Registry ──────────────────────────
# Bump a version string whenever the corresponding prompt template changes.
# Artifacts store the version that generated them; stale artifacts (version mismatch)
# are flagged as dirty and regenerated on the next partial regen run.
ARTIFACT_PROMPT_VERSIONS: dict[str, str] = {
    # Tier-1 pipeline stages (package-level, very expensive)
    "curriculum_sequence_plan": "csp-v1",
    "instructional_design_plan": "idp-v1",
    "strand_journeys": "sj-v1",
    "validation": "val-v1",
    # Per-artifact types
    "quality_review": "qr-v1",
    "daily_lesson_plan": "dlp-v1",
    "subject_slide_deck": "ssd-v1",
    "student_lesson_deck": "sld-v1",
    "assignment": "asgn-v1",
    "writing_response": "wr-v1",
    "quiz": "quiz-v1",
    "rubric": "rubric-v1",
    "exit_ticket": "et-v1",
    "bell_ringer": "br-v1",
    "vocabulary_list": "vocab-v1",
    "study_guide": "sg-v1",
    "parent_newsletter_summary": "news-v1",
}

# Valid scopes for a partial regeneration job
REGEN_SCOPES = frozenset({"full", "dirty", "artifact_types", "quality_review", "images"})

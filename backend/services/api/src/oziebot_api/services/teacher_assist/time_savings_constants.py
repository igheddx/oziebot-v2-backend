from __future__ import annotations

TIME_SAVINGS_MINUTES = {
    "LESSON_PLAN": 30,
    "ASSIGNMENT": 20,
    "QUIZ": 15,
    "RUBRIC": 10,
    "NEWSLETTER": 15,
    "WEEK": 45,
    "TEMPLATE": 25,
    "ROLLOVER": 60,
}

REUSE_EVENT_TYPES = (
    "duplicate_week",
    "generate_next_week",
    "apply_template",
    "save_template",
    "rollover_v2",
    "reuse_artifact",
)

TEMPLATE_TYPES = ("TEACHER", "GRADE_TEAM", "SCHOOL", "DISTRICT")
TEMPLATE_VISIBILITY = ("PRIVATE", "TEAM", "SCHOOL", "DISTRICT")
OWNERSHIP_TYPES = ("TEACHER", "GRADE_TEAM", "SCHOOL", "DISTRICT")
GUIDE_VISIBILITY_SCOPES = ("PRIVATE", "TEAM", "SCHOOL", "DISTRICT")

REUSE_SOURCES = (
    "CURRENT_TEACHER",
    "GRADE_TEAM",
    "SCHOOL",
    "DISTRICT",
    "PRIOR_SCHOOL_YEAR",
    "SHARED_TEMPLATE",
)

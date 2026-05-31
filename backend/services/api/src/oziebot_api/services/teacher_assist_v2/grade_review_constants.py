"""TeacherAssist v2 teacher grade review and confirmation constants."""

from __future__ import annotations

ASSIGNMENT_GRADE_STATUSES = frozenset({"DRAFT", "CONFIRMED", "REVISED", "ARCHIVED"})

GRADE_REVIEW_ACTIONS = frozenset({"ACCEPT", "MODIFY", "REJECT", "SAVE_DRAFT"})

OFFICIAL_ASSIGNMENT_GRADE_STATUSES = frozenset({"CONFIRMED", "REVISED"})

GRADE_REVIEW_QUEUE_STATUSES = frozenset(
    {"PENDING", "READY_FOR_REVIEW", "REVIEWED", "CONFIRMED", "REJECTED", "REVISED", "DRAFT"}
)

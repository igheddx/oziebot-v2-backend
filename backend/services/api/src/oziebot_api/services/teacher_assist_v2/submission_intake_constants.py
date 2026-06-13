from __future__ import annotations

SUBMISSION_BATCH_STATUSES = (
    "UPLOADED",
    "PROCESSING",
    "READY_FOR_REVIEW",
    "COMPLETED",
    "FAILED",
    "ARCHIVED",
)

STUDENT_SUBMISSION_STATUSES = (
    "PROCESSING",
    "READY_FOR_REVIEW",
    "CONFIRMED",
    "NOT_UPLOADED",
    "INCOMPLETE",
    "ARCHIVED",
)

# Legacy statuses retained for reading older rows until data is migrated.
LEGACY_STUDENT_SUBMISSION_STATUSES = (
    "MATCHED",
    "MANUAL_MATCH",
    "NEEDS_REVIEW",
    "READY_FOR_GRADING",
)

GRADABLE_STUDENT_SUBMISSION_STATUSES = frozenset({"PROCESSING", "READY_FOR_REVIEW"})
REVIEWABLE_STUDENT_SUBMISSION_STATUSES = frozenset({"READY_FOR_REVIEW", "NOT_UPLOADED", "INCOMPLETE"})
TERMINAL_STUDENT_SUBMISSION_STATUSES = frozenset({"CONFIRMED", "INCOMPLETE", "ARCHIVED"})

SUBMISSION_MATCH_METHODS = (
    "QR",
    "MANUAL",
    "FILENAME",
    "GOOGLE_FORM",
    "UNKNOWN",
)

STUDENT_WORK_MIME_TYPES = frozenset(
    {
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "image/jpeg",
        "image/png",
        "image/webp",
        "image/gif",
        "text/plain",
    }
)

QR_PACKET_VERSION = "teacher_assist_v2_assignment_packet_v1"

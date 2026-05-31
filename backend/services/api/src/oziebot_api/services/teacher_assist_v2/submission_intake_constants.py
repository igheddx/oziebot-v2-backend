from __future__ import annotations

SUBMISSION_BATCH_STATUSES = (
    "UPLOADED",
    "PROCESSING",
    "MATCHED",
    "NEEDS_REVIEW",
    "FAILED",
    "ARCHIVED",
)

STUDENT_SUBMISSION_STATUSES = (
    "MATCHED",
    "MANUAL_MATCH",
    "NEEDS_REVIEW",
    "READY_FOR_GRADING",
    "ARCHIVED",
)

SUBMISSION_MATCH_METHODS = (
    "QR",
    "MANUAL",
    "FILENAME",
    "UNKNOWN",
)

STUDENT_WORK_MIME_TYPES = frozenset(
    {
        "application/pdf",
        "image/jpeg",
        "image/png",
        "image/webp",
        "image/gif",
    }
)

QR_PACKET_VERSION = "teacher_assist_v2_assignment_packet_v1"

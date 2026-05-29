from __future__ import annotations

from zoneinfo import ZoneInfo

GRADING_PERIOD_TYPES = ("nine_weeks", "six_weeks", "semester", "trimester", "custom")
STANDARD_TYPES = ("TEKS", "CUSTOM")
RESOURCE_TYPES = ("pdf", "pptx", "image", "worksheet", "spreadsheet", "link", "doc", "other")
ASSIGNMENT_TYPES = (
    "writing",
    "reading_response",
    "short_answer",
    "quiz",
    "exit_ticket",
    "project",
    "homework",
    "other",
)
ASSIGNMENT_STATUSES = (
    "draft",
    "ready",
    "assigned",
    "collected",
    "review_in_progress",
    "reviewed",
    "archived",
)
ASSIGNMENT_PRINT_PACKET_STATUSES = ("generated", "archived")
ASSIGNMENT_PRINT_TEMPLATE_TYPES = (
    "blank_writing_page",
    "lined_writing_page",
    "short_answer_page",
)
ASSIGNMENT_PRINT_OUTPUT_FORMATS = ("html",)
ASSIGNMENT_STUDENT_WORK_UPLOAD_STATUSES = ("uploaded", "archived")
ASSIGNMENT_STUDENT_WORK_PROCESSING_STATUSES = (
    "pending_review",
    "ready_for_processing",
    "processing_deferred",
    "archived",
)
ASSIGNMENT_GRADING_REVIEW_STATUSES = (
    "draft",
    "ai_suggested",
    "teacher_reviewing",
    "teacher_confirmed",
    "returned_for_revision",
    "archived",
)
ASSIGNMENT_GRADING_REVIEW_SOURCES = ("manual", "ai_placeholder")
ASSIGNMENT_GRADE_RECORD_STATUSES = ("active", "superseded", "reversed")
ASSIGNMENT_GRADEBOOK_COMMIT_TYPES = ("initial_commit", "correction", "reversal")
ASSIGNMENT_GRADEBOOK_COMMIT_STATUSES = ("active", "superseded", "reversed")
ASSIGNMENT_GRADEBOOK_AUDIT_EVENT_TYPES = (
    "commit_created",
    "commit_corrected",
    "commit_reversed",
    "commit_superseded",
    "export_generated",
)
ACTION_WORKSPACE_SEVERITIES = ("critical", "warning", "review", "ready", "info")
ACTION_WORKSPACE_SECTION_KEYS = (
    "extractions",
    "grading",
    "gradebook",
    "workflows_exports",
    "planning_assignments",
)
ALLOWED_ACTION_WORKSPACE_NAVIGATION_PREFIXES = ("/teacher-assist/",)
MASTERY_MATRIX_STATUSES = ("draft", "active", "archived")
MASTERY_LEVELS = ("not_assessed", "beginning", "developing", "mastery", "advanced")
MASTERY_EVALUATION_STATUSES = ("draft", "active", "reversed")
MASTERY_COMMIT_TYPES = ("initial_commit", "correction", "reversal")
MASTERY_COMMIT_STATUSES = ("active", "superseded", "reversed")
MASTERY_AUDIT_EVENT_TYPES = (
    "mastery_commit_created",
    "mastery_commit_corrected",
    "mastery_commit_reversed",
    "mastery_review_confirmed",
)
MASTERY_EVIDENCE_SOURCE_TYPES = (
    "assignment",
    "grading_review",
    "gradebook_commit",
    "manual_observation",
)
MASTERY_CONFIDENCE_LEVELS = ("low", "medium", "high")
MASTERY_LEVEL_RANK = {
    "not_assessed": 0,
    "beginning": 1,
    "developing": 2,
    "mastery": 3,
    "advanced": 4,
}
MASTERY_MASTERED_LEVELS = frozenset({"mastery", "advanced"})
RETEACH_OPERATIONAL_STATUSES = (
    "healthy",
    "monitor",
    "reteach_recommended",
    "critical_attention",
    "unassessed",
)
ASSIGNMENT_EFFECTIVENESS_STATUSES = (
    "effective",
    "mixed_results",
    "reteach_likely",
    "insufficient_data",
)
STUDENT_MASTERY_TRENDS = ("improving", "stable", "declining", "insufficient_data")
STANDARD_MASTERY_TRENDS = ("improving", "stable", "declining", "insufficient_data")
RETEACH_MASTERY_HEALTHY_THRESHOLD = 0.80
RETEACH_MASTERY_MONITOR_THRESHOLD = 0.50
RETEACH_MASTERY_RECOMMENDED_THRESHOLD = 0.25
ASSIGNMENT_EFFECTIVENESS_HEALTHY_THRESHOLD = 0.70
ASSIGNMENT_EFFECTIVENESS_MIXED_THRESHOLD = 0.40
MASTERY_ANALYTICS_RECENT_DAYS = 14
MASTERY_TREND_RANK_DELTA = 0.5
RETEACH_PLAN_STATUSES = ("draft", "ai_draft", "teacher_review", "archived")
RETEACH_PLAN_VERSION_SOURCES = ("initial", "ai_draft", "teacher_edit")
TEACHER_ASSIST_EXTRACTION_ARTIFACT_TYPES = ("resource", "student_work")
TEACHER_ASSIST_EXTRACTION_JOB_STATUSES = (
    "queued",
    "running",
    "completed",
    "failed",
    "cancelled",
    "skipped",
)
EXTRACTION_REVIEW_STATUSES = (
    "pending_review",
    "teacher_reviewing",
    "teacher_approved",
    "teacher_rejected",
    "reviewed",
    "issue_flagged",
    "needs_retry",
    "archived",
)
EXTRACTION_CONFIDENCE_LEVELS = ("low", "medium", "high", "unknown")
PLANNING_DRAFT_STATUSES = ("draft", "ready")
PLANNING_SCOPES = ("weekly", "multi_week", "module", "unit", "grading_period")
PLAN_VISIBILITY_SCOPES = ("private", "shared", "grade_team", "school", "district")
PLAN_REUSE_STATUSES = ("active", "archived", "reusable")
TEACHER_ASSIST_WORKFLOW_TYPES = (
    "weekly_plan_generation",
    "daily_deck_generation",
    "assessment_generation",
    "newsletter_generation",
    "grading_assist",
    "artifact_export",
)
TEACHER_ASSIST_WORKFLOW_STATUSES = ("queued", "running", "completed", "failed", "cancelled")
TEACHER_ASSIST_WORKFLOW_STEP_STATUSES = ("queued", "running", "completed", "failed", "skipped")
WEEKLY_PLAN_STATUSES = ("in_progress", "completed")
TEACHER_ASSIST_EXPORT_ARTIFACT_TYPES = (
    "lesson_slides",
    "guided_notes",
    "multiple_choice_quiz",
    "exit_ticket",
    "short_answer_quiz",
)
TEACHER_ASSIST_SLIDES_EXPORT_TYPES = ("lesson_slides", "guided_notes")
TEACHER_ASSIST_QUIZ_EXPORT_TYPES = ("multiple_choice_quiz", "exit_ticket", "short_answer_quiz")
TEACHER_ASSIST_EXPORT_ARTIFACT_STATUSES = (
    "queued",
    "generating",
    "ready",
    "failed",
    "archived",
)
TEACHER_ASSIST_EXPORT_FORMATS = ("pptx", "json", "printable_html")
ARTIFACT_EXPORT_WORKFLOW_TYPE = "artifact_export"
ARTIFACT_EXPORT_OUTPUT_REF_TYPE = "export_artifact"
EXPORT_PROMPT_VERSION = "export-artifacts-v1"
TEACHER_ASSIST_AI_PROVIDERS = ("mock", "openai")
TEACHER_ASSIST_OCR_PROVIDERS = ("mock", "textract", "openai_vision")
TEACHER_ASSIST_OCR_REAL_PROVIDERS = frozenset({"textract", "openai_vision"})
TEACHER_ASSIST_AI_FIXTURE_MODES = ("off", "record", "replay")
SUPPORTED_GRADE_LEVELS = ("Pre-K", "K", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12")

RESOURCE_TYPE_BY_EXTENSION = {
    ".pdf": "pdf",
    ".ppt": "pptx",
    ".pptx": "pptx",
    ".png": "image",
    ".jpg": "image",
    ".jpeg": "image",
    ".gif": "image",
    ".webp": "image",
    ".bmp": "image",
    ".svg": "image",
    ".doc": "doc",
    ".docx": "doc",
    ".rtf": "doc",
    ".txt": "doc",
    ".xls": "spreadsheet",
    ".xlsx": "spreadsheet",
    ".csv": "spreadsheet",
}


def validate_grading_period_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if normalized not in GRADING_PERIOD_TYPES:
        raise ValueError("Unsupported grading period type")
    return normalized


def validate_standard_type(value: str) -> str:
    normalized = value.strip()
    if normalized not in STANDARD_TYPES:
        raise ValueError("Unsupported standard type")
    return normalized


def validate_resource_type(value: str | None, *, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise ValueError("Resource type is required")
        return None
    normalized = value.strip()
    if not normalized:
        if required:
            raise ValueError("Resource type is required")
        return None
    if normalized not in RESOURCE_TYPES:
        raise ValueError("Unsupported resource type")
    return normalized


def validate_assignment_type(value: str | None) -> str:
    normalized = (value or "other").strip() or "other"
    if normalized not in ASSIGNMENT_TYPES:
        raise ValueError("Unsupported assignment type")
    return normalized


def validate_assignment_status(value: str | None) -> str:
    normalized = (value or "draft").strip() or "draft"
    if normalized not in ASSIGNMENT_STATUSES:
        raise ValueError("Unsupported assignment status")
    return normalized


def validate_assignment_print_packet_status(value: str | None) -> str:
    normalized = (value or "generated").strip() or "generated"
    if normalized not in ASSIGNMENT_PRINT_PACKET_STATUSES:
        raise ValueError("Unsupported assignment print packet status")
    return normalized


def validate_assignment_print_template_type(value: str | None) -> str:
    normalized = (value or "blank_writing_page").strip() or "blank_writing_page"
    if normalized not in ASSIGNMENT_PRINT_TEMPLATE_TYPES:
        raise ValueError("Unsupported assignment print template type")
    return normalized


def validate_assignment_print_output_format(value: str | None) -> str:
    normalized = (value or "html").strip() or "html"
    if normalized not in ASSIGNMENT_PRINT_OUTPUT_FORMATS:
        raise ValueError("Unsupported assignment print output format")
    return normalized


def validate_assignment_student_work_upload_status(value: str | None) -> str:
    normalized = (value or "uploaded").strip() or "uploaded"
    if normalized not in ASSIGNMENT_STUDENT_WORK_UPLOAD_STATUSES:
        raise ValueError("Unsupported assignment student-work upload status")
    return normalized


def validate_assignment_student_work_processing_status(value: str | None) -> str:
    normalized = (value or "pending_review").strip() or "pending_review"
    if normalized not in ASSIGNMENT_STUDENT_WORK_PROCESSING_STATUSES:
        raise ValueError("Unsupported assignment student-work processing status")
    return normalized


def validate_assignment_grading_review_status(value: str | None) -> str:
    normalized = (value or "draft").strip() or "draft"
    if normalized not in ASSIGNMENT_GRADING_REVIEW_STATUSES:
        raise ValueError("Unsupported assignment grading review status")
    return normalized


def validate_assignment_grading_review_source(value: str | None) -> str:
    normalized = (value or "manual").strip() or "manual"
    if normalized not in ASSIGNMENT_GRADING_REVIEW_SOURCES:
        raise ValueError("Unsupported assignment grading review source")
    return normalized


def validate_assignment_grade_record_status(value: str | None) -> str:
    normalized = (value or "active").strip() or "active"
    if normalized not in ASSIGNMENT_GRADE_RECORD_STATUSES:
        raise ValueError("Unsupported assignment grade record status")
    return normalized


def validate_assignment_gradebook_commit_type(value: str | None) -> str:
    normalized = (value or "initial_commit").strip() or "initial_commit"
    if normalized not in ASSIGNMENT_GRADEBOOK_COMMIT_TYPES:
        raise ValueError("Unsupported assignment gradebook commit type")
    return normalized


def validate_assignment_gradebook_commit_status(value: str | None) -> str:
    normalized = (value or "active").strip() or "active"
    if normalized not in ASSIGNMENT_GRADEBOOK_COMMIT_STATUSES:
        raise ValueError("Unsupported assignment gradebook commit status")
    return normalized


def validate_assignment_gradebook_audit_event_type(value: str | None) -> str:
    normalized = (value or "").strip()
    if normalized not in ASSIGNMENT_GRADEBOOK_AUDIT_EVENT_TYPES:
        raise ValueError("Unsupported assignment gradebook audit event type")
    return normalized


def validate_action_workspace_navigation_href(value: str) -> str:
    normalized = value.strip()
    if not normalized.startswith("/"):
        raise ValueError("Action workspace navigation href must be an absolute path")
    if "://" in normalized:
        raise ValueError("Action workspace navigation href must not include a scheme")
    if not any(normalized.startswith(prefix) for prefix in ALLOWED_ACTION_WORKSPACE_NAVIGATION_PREFIXES):
        raise ValueError("Action workspace navigation href must stay within TeacherAssist routes")
    return normalized


def validate_mastery_matrix_status(value: str | None) -> str:
    normalized = (value or "").strip()
    if normalized not in MASTERY_MATRIX_STATUSES:
        raise ValueError("Unsupported mastery matrix status")
    return normalized


def validate_mastery_level(value: str | None) -> str:
    normalized = (value or "").strip()
    if normalized not in MASTERY_LEVELS:
        raise ValueError("Unsupported mastery level")
    return normalized


def validate_mastery_evaluation_status(value: str | None) -> str:
    normalized = (value or "").strip()
    if normalized not in MASTERY_EVALUATION_STATUSES:
        raise ValueError("Unsupported mastery evaluation status")
    return normalized


def validate_mastery_commit_type(value: str | None) -> str:
    normalized = (value or "").strip()
    if normalized not in MASTERY_COMMIT_TYPES:
        raise ValueError("Unsupported mastery commit type")
    return normalized


def validate_mastery_commit_status(value: str | None) -> str:
    normalized = (value or "").strip()
    if normalized not in MASTERY_COMMIT_STATUSES:
        raise ValueError("Unsupported mastery commit status")
    return normalized


def validate_mastery_audit_event_type(value: str | None) -> str:
    normalized = (value or "").strip()
    if normalized not in MASTERY_AUDIT_EVENT_TYPES:
        raise ValueError("Unsupported mastery audit event type")
    return normalized


def validate_mastery_evidence_source_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized not in MASTERY_EVIDENCE_SOURCE_TYPES:
        raise ValueError("Unsupported mastery evidence source type")
    return normalized


def validate_mastery_confidence_level(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized not in MASTERY_CONFIDENCE_LEVELS:
        raise ValueError("Unsupported mastery confidence level")
    return normalized


def validate_reteach_plan_status(value: str | None) -> str:
    normalized = (value or "draft").strip().lower()
    if normalized not in RETEACH_PLAN_STATUSES:
        raise ValueError("Unsupported reteach plan status")
    return normalized


def validate_reteach_plan_version_source(value: str | None) -> str:
    normalized = (value or "initial").strip().lower()
    if normalized not in RETEACH_PLAN_VERSION_SOURCES:
        raise ValueError("Unsupported reteach plan version source")
    return normalized


def validate_teacher_assist_extraction_artifact_type(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_EXTRACTION_ARTIFACT_TYPES:
        raise ValueError("Unsupported TeacherAssist extraction artifact type")
    return normalized


def validate_teacher_assist_extraction_job_status(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_EXTRACTION_JOB_STATUSES:
        raise ValueError("Unsupported TeacherAssist extraction job status")
    return normalized


def validate_extraction_review_status(value: str) -> str:
    normalized = value.strip()
    if normalized not in EXTRACTION_REVIEW_STATUSES:
        raise ValueError("Unsupported TeacherAssist extraction review status")
    return normalized


def validate_extraction_confidence_level(value: str) -> str:
    normalized = value.strip()
    if normalized not in EXTRACTION_CONFIDENCE_LEVELS:
        raise ValueError("Unsupported TeacherAssist extraction confidence level")
    return normalized


def validate_planning_draft_status(value: str | None) -> str:
    normalized = (value or "draft").strip() or "draft"
    if normalized not in PLANNING_DRAFT_STATUSES:
        raise ValueError("Unsupported planning draft status")
    return normalized


def validate_planning_scope(value: str | None) -> str:
    normalized = (value or "weekly").strip() or "weekly"
    if normalized not in PLANNING_SCOPES:
        raise ValueError("Unsupported planning scope")
    return normalized


def validate_plan_visibility_scope(value: str | None) -> str:
    normalized = (value or "private").strip() or "private"
    if normalized not in PLAN_VISIBILITY_SCOPES:
        raise ValueError("Unsupported plan visibility scope")
    return normalized


def validate_plan_reuse_status(value: str | None) -> str:
    normalized = (value or "active").strip() or "active"
    if normalized not in PLAN_REUSE_STATUSES:
        raise ValueError("Unsupported plan reuse status")
    return normalized


def validate_teacher_assist_workflow_type(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_WORKFLOW_TYPES:
        raise ValueError("Unsupported TeacherAssist workflow type")
    return normalized


def validate_teacher_assist_workflow_status(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_WORKFLOW_STATUSES:
        raise ValueError("Unsupported TeacherAssist workflow status")
    return normalized


def validate_teacher_assist_workflow_step_status(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_WORKFLOW_STEP_STATUSES:
        raise ValueError("Unsupported TeacherAssist workflow step status")
    return normalized


def validate_weekly_plan_status(value: str) -> str:
    normalized = value.strip()
    if normalized not in WEEKLY_PLAN_STATUSES:
        raise ValueError("Unsupported weekly plan status")
    return normalized


def validate_teacher_assist_ai_provider(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_AI_PROVIDERS:
        raise ValueError("Unsupported TeacherAssist AI provider")
    return normalized


def validate_teacher_assist_ocr_provider(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_OCR_PROVIDERS:
        raise ValueError("Unsupported TeacherAssist OCR provider")
    return normalized


def validate_teacher_assist_export_artifact_type(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_EXPORT_ARTIFACT_TYPES:
        raise ValueError("Unsupported TeacherAssist export artifact type")
    return normalized


def validate_teacher_assist_export_artifact_status(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_EXPORT_ARTIFACT_STATUSES:
        raise ValueError("Unsupported TeacherAssist export artifact status")
    return normalized


def validate_teacher_assist_export_format(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_EXPORT_FORMATS:
        raise ValueError("Unsupported TeacherAssist export format")
    return normalized


def validate_teacher_assist_ai_fixture_mode(value: str) -> str:
    normalized = value.strip()
    if normalized not in TEACHER_ASSIST_AI_FIXTURE_MODES:
        raise ValueError("Unsupported TeacherAssist AI fixture mode")
    return normalized


def infer_resource_type(filename: str | None, mime_type: str | None) -> str:
    if filename:
        lower_filename = filename.lower()
        for suffix, resource_type in RESOURCE_TYPE_BY_EXTENSION.items():
            if lower_filename.endswith(suffix):
                return resource_type
    normalized_mime = (mime_type or "").lower()
    if normalized_mime.startswith("image/"):
        return "image"
    if "pdf" in normalized_mime:
        return "pdf"
    if "presentation" in normalized_mime or "powerpoint" in normalized_mime:
        return "pptx"
    if "spreadsheet" in normalized_mime or "excel" in normalized_mime or "csv" in normalized_mime:
        return "spreadsheet"
    if "word" in normalized_mime or "document" in normalized_mime or "text/" in normalized_mime:
        return "doc"
    return "other"


def validate_grade_level(value: str | None, *, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise ValueError("Grade level is required")
        return None
    normalized = value.strip()
    if not normalized:
        if required:
            raise ValueError("Grade level is required")
        return None
    if normalized not in SUPPORTED_GRADE_LEVELS:
        raise ValueError("Unsupported grade level")
    return normalized


def validate_timezone(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        ZoneInfo(normalized)
    except Exception as exc:  # pragma: no cover - ZoneInfo exceptions vary by runtime
        raise ValueError("Unsupported timezone") from exc
    return normalized

from __future__ import annotations

from datetime import UTC, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_export_artifact import TeacherAssistExportArtifact
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.services.teacher_assist.constants import (
    TEACHER_ASSIST_QUIZ_EXPORT_TYPES,
    TEACHER_ASSIST_SLIDES_EXPORT_TYPES,
    validate_teacher_assist_export_artifact_status,
    validate_teacher_assist_export_artifact_type,
    validate_teacher_assist_export_format,
)
from oziebot_api.services.teacher_assist.storage import get_teacher_assist_download_url
from oziebot_api.services.teacher_assist.workflow_service import get_visible_weekly_plan_or_404


def _default_export_format(*, artifact_type: str, export_format: str | None) -> str:
    if export_format:
        return validate_teacher_assist_export_format(export_format)
    if artifact_type in TEACHER_ASSIST_SLIDES_EXPORT_TYPES:
        return "pptx"
    if artifact_type in TEACHER_ASSIST_QUIZ_EXPORT_TYPES:
        return "json"
    raise ValueError("Unsupported export artifact type")


def _validate_export_format_for_artifact(*, artifact_type: str, export_format: str) -> str:
    normalized = validate_teacher_assist_export_format(export_format)
    if artifact_type in TEACHER_ASSIST_SLIDES_EXPORT_TYPES and normalized not in {
        "pptx",
        "printable_html",
    }:
        raise ValueError("Slide exports support pptx or printable_html formats")
    if artifact_type in TEACHER_ASSIST_QUIZ_EXPORT_TYPES and normalized not in {
        "json",
        "printable_html",
    }:
        raise ValueError("Quiz exports support json or printable_html formats")
    return normalized


def _artifact_download_filename(row: TeacherAssistExportArtifact) -> str:
    suffix = {
        "pptx": ".pptx",
        "json": ".json",
        "printable_html": ".html",
    }.get(row.export_format, ".bin")
    safe_title = "".join(
        char if char.isalnum() or char in {"-", "_"} else "-" for char in row.title
    ).strip("-")
    return f"{safe_title or row.artifact_type}{suffix}"


def _artifact_mime_type(export_format: str) -> str:
    return {
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "json": "application/json",
        "printable_html": "text/html",
    }.get(export_format, "application/octet-stream")


def get_export_artifact_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    export_artifact_id: uuid.UUID,
) -> TeacherAssistExportArtifact:
    row = db.scalars(
        select(TeacherAssistExportArtifact).where(
            TeacherAssistExportArtifact.id == export_artifact_id,
            TeacherAssistExportArtifact.tenant_id == tenant_id,
            TeacherAssistExportArtifact.user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Export artifact not found")
    return row


def list_export_artifacts(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    artifact_type: str | None = None,
    artifact_status: str | None = None,
    source_plan_id: uuid.UUID | None = None,
    limit: int = 50,
) -> list[TeacherAssistExportArtifact]:
    query = (
        select(TeacherAssistExportArtifact)
        .where(
            TeacherAssistExportArtifact.tenant_id == tenant_id,
            TeacherAssistExportArtifact.user_id == user_id,
        )
        .order_by(TeacherAssistExportArtifact.created_at.desc())
        .limit(max(1, min(limit, 200)))
    )
    if artifact_type:
        query = query.where(
            TeacherAssistExportArtifact.artifact_type
            == validate_teacher_assist_export_artifact_type(artifact_type)
        )
    if artifact_status:
        query = query.where(
            TeacherAssistExportArtifact.artifact_status
            == validate_teacher_assist_export_artifact_status(artifact_status)
        )
    if source_plan_id is not None:
        query = query.where(TeacherAssistExportArtifact.source_plan_id == source_plan_id)
    return list(db.scalars(query).all())


def build_export_artifact_detail(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    export_artifact_id: uuid.UUID,
) -> dict:
    row = get_export_artifact_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        export_artifact_id=export_artifact_id,
    )
    workflow = None
    if row.workflow_id is not None:
        workflow = db.scalars(
            select(TeacherAssistWorkflow).where(
                TeacherAssistWorkflow.id == row.workflow_id,
                TeacherAssistWorkflow.tenant_id == tenant_id,
            )
        ).one_or_none()
    download_url = None
    if row.artifact_status == "ready" and row.storage_key:
        download_url = get_teacher_assist_download_url(
            settings,
            storage_key=row.storage_key,
            original_filename=_artifact_download_filename(row),
            mime_type=_artifact_mime_type(row.export_format),
        )
    return {
        "artifact": row,
        "workflow_status": workflow.status if workflow is not None else None,
        "workflow_progress_percent": workflow.progress_percent if workflow is not None else None,
        "workflow_error_message": workflow.error_message if workflow is not None else None,
        "download_url": download_url,
    }


def get_export_artifact_download_url(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    export_artifact_id: uuid.UUID,
) -> dict[str, str]:
    row = get_export_artifact_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        export_artifact_id=export_artifact_id,
    )
    if row.artifact_status != "ready" or not row.storage_key:
        raise ValueError("Export artifact is not ready for download")
    return {
        "download_url": get_teacher_assist_download_url(
            settings,
            storage_key=row.storage_key,
            original_filename=_artifact_download_filename(row),
            mime_type=_artifact_mime_type(row.export_format),
        ),
        "filename": _artifact_download_filename(row),
        "mime_type": _artifact_mime_type(row.export_format),
    }


def validate_weekly_plan_export_eligibility(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan_id: uuid.UUID,
) -> None:
    plan = get_visible_weekly_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        weekly_plan_id=weekly_plan_id,
    )
    if plan.status not in {"in_progress", "completed"}:
        raise ValueError("Weekly plan must be in progress or completed before generating exports")


def normalize_export_request(
    *,
    artifact_type: str,
    export_format: str | None,
) -> tuple[str, str]:
    normalized_type = validate_teacher_assist_export_artifact_type(artifact_type)
    normalized_format = _validate_export_format_for_artifact(
        artifact_type=normalized_type,
        export_format=_default_export_format(
            artifact_type=normalized_type, export_format=export_format
        ),
    )
    return normalized_type, normalized_format


def touch_export_artifact_status(
    row: TeacherAssistExportArtifact,
    *,
    status: str,
) -> None:
    row.artifact_status = validate_teacher_assist_export_artifact_status(status)
    row.updated_at = datetime.now(UTC)

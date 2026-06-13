"""Shared helpers for persisting v2 instructional package artifacts."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.services.teacher_assist_v2.assignment_print_packets import generate_assignment_print_packet
from oziebot_api.services.teacher_assist_v2.student_packet_content import compute_pages_per_student
from oziebot_api.services.teacher_assist_v2.package_export import (
    render_artifact_preview_html,
    save_artifact_export,
)


def persist_package_artifact(
    db: Session,
    *,
    settings: Settings,
    package: TeacherAssistV2InstructionalPackage,
    artifact_type: str,
    content: dict[str, Any],
    provider_name: str,
    sequence_number: int,
    created_at: datetime,
    subject_id: uuid.UUID | None = None,
    period_id: uuid.UUID | None = None,
    day_label: str | None = None,
    export_note: str | None = None,
) -> TeacherAssistV2InstructionalPackageArtifact:
    artifact = TeacherAssistV2InstructionalPackageArtifact(
        id=uuid.uuid4(),
        tenant_id=package.tenant_id,
        package_id=package.id,
        artifact_type=artifact_type,
        subject_id=subject_id,
        period_id=period_id,
        day_label=day_label,
        sequence_number=sequence_number,
        title=str(content["title"]),
        status="ready",
        content_json=content,
        preview_html=render_artifact_preview_html(artifact_type=artifact_type, content=content),
        metadata_json={
            "provider": provider_name,
            "description": content.get("description") or content.get("summary"),
            "objective_mapping": content.get("objective_mapping"),
            "additional_exports": [],
            **({"export_note": export_note} if export_note else {}),
        },
        created_at=created_at,
        updated_at=created_at,
    )
    storage_key, export_format = save_artifact_export(
        settings=settings,
        tenant_id=package.tenant_id,
        artifact_id=artifact.id,
        artifact_type=artifact_type,
        content=content,
    )
    artifact.storage_key = storage_key
    artifact.export_format = export_format
    db.add(artifact)
    db.flush()
    return artifact


def refresh_package_artifact_exports(
    db: Session,
    *,
    settings: Settings,
    package: TeacherAssistV2InstructionalPackage,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    content: dict[str, Any],
    provider_name: str,
    updated_at: datetime,
    export_note: str | None = None,
) -> None:
    artifact.title = str(content["title"])
    artifact.content_json = content
    artifact.preview_html = render_artifact_preview_html(artifact_type=artifact.artifact_type, content=content)
    artifact.status = "ready"
    metadata = dict(artifact.metadata_json or {})
    metadata.update(
        {
            "provider": provider_name,
            "description": content.get("description") or content.get("summary"),
            "objective_mapping": content.get("objective_mapping"),
        }
    )
    if export_note:
        metadata["export_note"] = export_note
    storage_key, export_format = save_artifact_export(
        settings=settings,
        tenant_id=package.tenant_id,
        artifact_id=artifact.id,
        artifact_type=artifact.artifact_type,
        content=content,
    )
    artifact.storage_key = storage_key
    artifact.export_format = export_format
    if artifact.artifact_type in {"quiz", "assignment", "writing_response"}:
        from oziebot_api.services.teacher_assist_v2.assessment_student_exports import refresh_assessment_student_exports

        refresh_assessment_student_exports(
            db,
            settings=settings,
            package=package,
            artifact=artifact,
        )
        metadata = dict(artifact.metadata_json or {})
    else:
        metadata["additional_exports"] = metadata.get("additional_exports") or []
    artifact.metadata_json = metadata
    artifact.updated_at = updated_at


def attach_qr_student_packet(
    db: Session,
    *,
    settings: Settings,
    assignment_artifact: TeacherAssistV2InstructionalPackageArtifact,
    assignment_content: dict[str, Any],
) -> dict[str, Any] | None:
    if not assignment_artifact.assignment_id:
        return None
    assignment = db.get(TeacherAssistV2Assignment, assignment_artifact.assignment_id)
    if assignment is None:
        return None
    qr_packet = generate_assignment_print_packet(
        db,
        settings=settings,
        assignment=assignment,
        assignment_content=assignment_content,
        pages_per_student=compute_pages_per_student(
            artifact_type="assignment",
            content=assignment_content,
        ),
    )
    assignment_artifact.metadata_json = {
        **(assignment_artifact.metadata_json or {}),
        "qr_student_packet": qr_packet,
    }
    return qr_packet

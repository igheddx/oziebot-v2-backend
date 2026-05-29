from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_generated_artifact import TeacherAssistGeneratedArtifact
from oziebot_api.models.teacher_assist_newsletter_version import TeacherAssistNewsletterVersion
from oziebot_api.models.teacher_assist_weekly_plan_version import TeacherAssistWeeklyPlanVersion
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.generated_artifact_constants import (
    validate_generated_artifact_status,
    validate_generated_artifact_type,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _navigation_href(artifact: TeacherAssistGeneratedArtifact) -> str:
    if artifact.instructional_plan_id is not None:
        return f"/teacher-assist/weekly-planning/plans?id={artifact.instructional_plan_id}"
    if artifact.planning_draft_id is not None:
        return f"/teacher-assist/weekly-planning?draft_id={artifact.planning_draft_id}"
    if artifact.assignment_id is not None:
        return f"/teacher-assist/assignments?assignment_id={artifact.assignment_id}"
    if artifact.export_artifact_id is not None:
        return f"/teacher-assist/exports?id={artifact.export_artifact_id}"
    if artifact.newsletter_id is not None:
        return f"/teacher-assist/newsletters?id={artifact.newsletter_id}"
    return f"/teacher-assist/planning/weeks?period_id={artifact.pacing_guide_period_id}"


def _version_count(db: Session, artifact: TeacherAssistGeneratedArtifact) -> int:
    if artifact.instructional_plan_id is not None:
        return len(
            db.scalars(
                select(TeacherAssistWeeklyPlanVersion).where(
                    TeacherAssistWeeklyPlanVersion.weekly_plan_id == artifact.instructional_plan_id
                )
            ).all()
        )
    if artifact.newsletter_id is not None:
        return len(
            db.scalars(
                select(TeacherAssistNewsletterVersion).where(
                    TeacherAssistNewsletterVersion.newsletter_id == artifact.newsletter_id
                )
            ).all()
        )
    return 1


def register_generated_artifact(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    pacing_guide_id: uuid.UUID,
    pacing_guide_period_id: uuid.UUID,
    artifact_type: str,
    title: str,
    status: str = "draft",
    instructional_plan_id: uuid.UUID | None = None,
    planning_draft_id: uuid.UUID | None = None,
    assignment_id: uuid.UUID | None = None,
    export_artifact_id: uuid.UUID | None = None,
    newsletter_id: uuid.UUID | None = None,
    resource_links: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> TeacherAssistGeneratedArtifact:
    now = _now()
    from oziebot_api.services.teacher_assist.instructional_weeks import resolve_instructional_week_id_for_period

    instructional_week_id = resolve_instructional_week_id_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        pacing_guide_period_id=pacing_guide_period_id,
    )
    row = TeacherAssistGeneratedArtifact(
        tenant_id=tenant_id,
        created_by_user_id=user.id,
        pacing_guide_id=pacing_guide_id,
        pacing_guide_period_id=pacing_guide_period_id,
        instructional_week_id=instructional_week_id,
        artifact_type=validate_generated_artifact_type(artifact_type),
        title=title.strip(),
        status=validate_generated_artifact_status(status),
        instructional_plan_id=instructional_plan_id,
        planning_draft_id=planning_draft_id,
        assignment_id=assignment_id,
        export_artifact_id=export_artifact_id,
        newsletter_id=newsletter_id,
        resource_links_json=resource_links,
        metadata_json=metadata,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def list_generated_artifacts_for_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    pacing_guide_period_id: uuid.UUID,
) -> list[TeacherAssistGeneratedArtifact]:
    return list(
        db.scalars(
            select(TeacherAssistGeneratedArtifact)
            .where(
                TeacherAssistGeneratedArtifact.tenant_id == tenant_id,
                TeacherAssistGeneratedArtifact.created_by_user_id == user_id,
                TeacherAssistGeneratedArtifact.pacing_guide_period_id == pacing_guide_period_id,
            )
            .order_by(TeacherAssistGeneratedArtifact.created_at.desc())
        ).all()
    )


def serialize_generated_artifact(db: Session, artifact: TeacherAssistGeneratedArtifact) -> dict[str, Any]:
    creator = db.get(User, artifact.created_by_user_id)
    return {
        "id": str(artifact.id),
        "week_id": str(artifact.pacing_guide_period_id),
        "pacing_guide_id": str(artifact.pacing_guide_id),
        "pacing_guide_period_id": str(artifact.pacing_guide_period_id),
        "instructional_week_id": str(artifact.instructional_week_id) if artifact.instructional_week_id else None,
        "artifact_type": artifact.artifact_type,
        "title": artifact.title,
        "status": artifact.status,
        "created_by_user_id": str(artifact.created_by_user_id),
        "created_by_name": creator.full_name if creator else None,
        "created_at": artifact.created_at.isoformat(),
        "updated_at": artifact.updated_at.isoformat(),
        "instructional_plan_id": str(artifact.instructional_plan_id) if artifact.instructional_plan_id else None,
        "planning_draft_id": str(artifact.planning_draft_id) if artifact.planning_draft_id else None,
        "assignment_id": str(artifact.assignment_id) if artifact.assignment_id else None,
        "export_artifact_id": str(artifact.export_artifact_id) if artifact.export_artifact_id else None,
        "newsletter_id": str(artifact.newsletter_id) if artifact.newsletter_id else None,
        "resource_links": artifact.resource_links_json or [],
        "metadata": artifact.metadata_json or {},
        "version_count": _version_count(db, artifact),
        "navigation_href": _navigation_href(artifact),
    }


def build_generation_history(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    pacing_guide_period_id: uuid.UUID,
) -> list[dict[str, Any]]:
    rows = list_generated_artifacts_for_period(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        pacing_guide_period_id=pacing_guide_period_id,
    )
    return [serialize_generated_artifact(db, row) for row in rows]


def link_lesson_plan_artifact(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
    instructional_plan_id: uuid.UUID,
    status: str = "completed",
) -> TeacherAssistGeneratedArtifact | None:
    row = db.scalars(
        select(TeacherAssistGeneratedArtifact).where(
            TeacherAssistGeneratedArtifact.tenant_id == tenant_id,
            TeacherAssistGeneratedArtifact.created_by_user_id == user_id,
            TeacherAssistGeneratedArtifact.planning_draft_id == planning_draft_id,
        )
    ).first()
    if row is None:
        return None
    row.instructional_plan_id = instructional_plan_id
    row.status = validate_generated_artifact_status(status)
    row.updated_at = _now()
    db.flush()
    return row


def duplicate_generated_artifact(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    artifact_id: uuid.UUID,
) -> TeacherAssistGeneratedArtifact:
    source = db.scalars(
        select(TeacherAssistGeneratedArtifact).where(
            TeacherAssistGeneratedArtifact.id == artifact_id,
            TeacherAssistGeneratedArtifact.tenant_id == tenant_id,
            TeacherAssistGeneratedArtifact.created_by_user_id == user.id,
        )
    ).one_or_none()
    if source is None:
        raise LookupError("Generated artifact not found")
    return register_generated_artifact(
        db,
        tenant_id=tenant_id,
        user=user,
        pacing_guide_id=source.pacing_guide_id,
        pacing_guide_period_id=source.pacing_guide_period_id,
        artifact_type=source.artifact_type,
        title=f"{source.title} (Copy)",
        status="draft",
        instructional_plan_id=source.instructional_plan_id,
        planning_draft_id=source.planning_draft_id,
        assignment_id=source.assignment_id,
        export_artifact_id=source.export_artifact_id,
        newsletter_id=source.newsletter_id,
        resource_links=source.resource_links_json,
        metadata={
            **(source.metadata_json or {}),
            "duplicated_from_artifact_id": str(source.id),
        },
    )

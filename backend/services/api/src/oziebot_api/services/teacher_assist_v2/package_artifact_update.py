"""Teacher updates to generated instructional package artifacts."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.artifact_persistence import refresh_package_artifact_exports
from oziebot_api.services.teacher_assist_v2.planning_workflow import _assignment_context


def _now() -> datetime:
    return datetime.now(UTC)


def _get_owned_package(
    db: Session,
    *,
    user: User,
    package_id: uuid.UUID,
) -> TeacherAssistV2InstructionalPackage:
    base = _assignment_context(db, user=user)
    row = db.scalars(
        select(TeacherAssistV2InstructionalPackage).where(
            TeacherAssistV2InstructionalPackage.id == package_id,
            TeacherAssistV2InstructionalPackage.teacher_user_id == user.id,
            TeacherAssistV2InstructionalPackage.tenant_id == base["ctx"].tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Instructional package not found")
    return row


def _validate_rubric_content(content: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(content, dict):
        raise ValueError({"content_json": "Rubric content must be an object."})
    normalized = dict(content)
    criteria = normalized.get("criteria")
    if not isinstance(criteria, list) or not criteria:
        raise ValueError({"criteria": "At least one rubric criterion is required."})

    cleaned: list[dict[str, Any]] = []
    for index, item in enumerate(criteria, start=1):
        if not isinstance(item, dict):
            raise ValueError({"criteria": f"Criterion {index} must be an object."})
        name = str(item.get("name") or "").strip()
        if not name:
            raise ValueError({"criteria": f"Criterion {index} requires a name."})
        try:
            points = int(item.get("points") or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError({"criteria": f"Criterion {index} points must be a number."}) from exc
        if points < 0:
            raise ValueError({"criteria": f"Criterion {index} points cannot be negative."})
        levels = [
            str(level).strip()
            for level in (item.get("levels") or [])
            if str(level).strip()
        ]
        if len(levels) < 2:
            levels = ["Meets expectations", "Partially meets", "Does not meet"]
        cleaned.append({"name": name, "points": points, "levels": levels[:4]})

    normalized["criteria"] = cleaned
    normalized["total_points"] = sum(row["points"] for row in cleaned)
    title = str(normalized.get("title") or "").strip()
    if not title:
        raise ValueError({"title": "Rubric title is required."})
    normalized["title"] = title
    return normalized


def update_teacher_package_artifact_content(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package_id: uuid.UUID,
    artifact_id: uuid.UUID,
    content_json: dict[str, Any],
) -> TeacherAssistV2InstructionalPackageArtifact:
    package = _get_owned_package(db, user=user, package_id=package_id)
    artifact = db.scalars(
        select(TeacherAssistV2InstructionalPackageArtifact).where(
            TeacherAssistV2InstructionalPackageArtifact.id == artifact_id,
            TeacherAssistV2InstructionalPackageArtifact.package_id == package.id,
        )
    ).one_or_none()
    if artifact is None:
        raise LookupError("Package artifact not found")
    if artifact.artifact_type != "rubric":
        raise ValueError({"artifact_type": "Only rubric artifacts can be edited through this endpoint."})

    existing = artifact.content_json if isinstance(artifact.content_json, dict) else {}
    merged = {**existing, **content_json}
    content = _validate_rubric_content(merged)
    now = _now()
    refresh_package_artifact_exports(
        db,
        settings=settings,
        package=package,
        artifact=artifact,
        content=content,
        provider_name=str((artifact.metadata_json or {}).get("provider") or "teacher"),
        updated_at=now,
    )
    metadata = dict(artifact.metadata_json or {})
    metadata["teacher_edited"] = True
    metadata["teacher_edited_at"] = now.isoformat()
    artifact.metadata_json = metadata
    artifact.updated_at = now
    db.flush()
    return artifact

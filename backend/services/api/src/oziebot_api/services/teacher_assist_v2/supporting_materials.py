from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationObjective, EducationSchoolYear
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_objective import TeacherAssistPacingGuideObjective
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_supporting_material import (
    TeacherAssistPacingGuideSupportingMaterial,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.pacing_guide_foundation import get_catalog_pacing_guide_detail
from oziebot_api.services.teacher_assist.storage import (
    StoredTeacherAssistUpload,
    get_teacher_assist_download_url,
    save_teacher_assist_bytes,
    store_teacher_assist_upload,
)
from oziebot_api.services.teacher_assist_v2.supporting_materials_constants import (
    PACING_SUPPORTING_MATERIAL_KINDS,
    PACING_SUPPORTING_RESOURCE_TYPES,
    PACING_SUPPORTING_UPLOAD_EXTENSIONS,
)


def _field_errors(**errors: str) -> ValueError:
    return ValueError({key: value for key, value in errors.items() if value})


def _validate_resource_type(resource_type: str) -> str:
    normalized = resource_type.strip().lower()
    if normalized not in PACING_SUPPORTING_RESOURCE_TYPES:
        raise _field_errors(resource_type="Unsupported resource type")
    return normalized


def _validate_external_url(external_url: str) -> str:
    normalized = external_url.strip()
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise _field_errors(external_url="Enter a valid http or https URL")
    return normalized


def _validate_file_extension(filename: str) -> None:
    suffix = Path(filename).suffix.lower()
    if suffix not in PACING_SUPPORTING_UPLOAD_EXTENSIONS:
        raise _field_errors(file="Unsupported file type. Use PDF, DOCX, PPTX, TXT, or common image formats.")


def _get_guide_or_404(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID
) -> TeacherAssistPacingGuide:
    return get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)


def _resolve_platform_school_year_id(db: Session, *, guide: TeacherAssistPacingGuide) -> uuid.UUID | None:
    if not guide.school_year_label:
        return None
    row = db.scalars(
        select(EducationSchoolYear)
        .where(
            EducationSchoolYear.title == guide.school_year_label,
            EducationSchoolYear.state_id == guide.catalog_state_id,
        )
        .order_by(EducationSchoolYear.created_at.desc())
    ).first()
    return row.id if row is not None else None


def _assert_guide_hierarchy(guide: TeacherAssistPacingGuide) -> None:
    missing = []
    if guide.catalog_state_id is None:
        missing.append("state")
    if guide.catalog_district_id is None:
        missing.append("district")
    if guide.catalog_grade_id is None:
        missing.append("grade")
    if guide.catalog_subject_id is None:
        missing.append("subject")
    if missing:
        raise ValueError(f"Pacing guide is missing required catalog linkage: {', '.join(missing)}")


def _get_period_or_404(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    period_id: uuid.UUID,
) -> TeacherAssistPacingGuidePeriod:
    period = db.scalars(
        select(TeacherAssistPacingGuidePeriod).where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id,
        )
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide week not found")
    return period


def _validate_objective_linkage(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    education_objective_id: uuid.UUID,
    period_id: uuid.UUID | None,
) -> EducationObjective:
    objective = db.scalars(
        select(EducationObjective).where(EducationObjective.id == education_objective_id)
    ).one_or_none()
    if objective is None:
        raise LookupError("Learning objective not found")
    if guide.catalog_subject_id and objective.subject_id and objective.subject_id != guide.catalog_subject_id:
        raise _field_errors(education_objective_id="Objective subject does not match pacing guide subject")
    if period_id is not None:
        mapped = db.scalars(
            select(TeacherAssistPacingGuideObjective).where(
                TeacherAssistPacingGuideObjective.period_id == period_id,
                TeacherAssistPacingGuideObjective.objective_id == education_objective_id,
            )
        ).one_or_none()
        if mapped is None:
            raise _field_errors(education_objective_id="Objective is not mapped to the selected week")
    else:
        mapped = db.scalars(
            select(TeacherAssistPacingGuideObjective)
            .join(TeacherAssistPacingGuidePeriod)
            .where(
                TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id,
                TeacherAssistPacingGuideObjective.objective_id == education_objective_id,
            )
        ).first()
        if mapped is None:
            raise _field_errors(education_objective_id="Objective is not mapped to this pacing guide")
    return objective


def _validate_linkage(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    period_id: uuid.UUID | None,
    education_objective_id: uuid.UUID | None,
) -> None:
    if period_id is None and education_objective_id is None:
        raise _field_errors(linkage="Attach materials to a week or learning objective")
    if period_id is not None:
        _get_period_or_404(db, guide=guide, period_id=period_id)
    if education_objective_id is not None:
        _validate_objective_linkage(
            db,
            guide=guide,
            education_objective_id=education_objective_id,
            period_id=period_id,
        )


def _base_material_fields(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    period_id: uuid.UUID | None,
    education_objective_id: uuid.UUID | None,
) -> dict:
    _assert_guide_hierarchy(guide)
    _validate_linkage(
        db,
        guide=guide,
        period_id=period_id,
        education_objective_id=education_objective_id,
    )
    return {
        "tenant_id": guide.tenant_id,
        "pacing_guide_id": guide.id,
        "period_id": period_id,
        "education_objective_id": education_objective_id,
        "platform_school_year_id": _resolve_platform_school_year_id(db, guide=guide),
        "catalog_state_id": guide.catalog_state_id,
        "catalog_district_id": guide.catalog_district_id,
        "catalog_school_id": guide.catalog_school_id,
        "catalog_grade_id": guide.catalog_grade_id,
        "catalog_subject_id": guide.catalog_subject_id,
        "visibility_scope": "district",
    }


def serialize_supporting_material(
    row: TeacherAssistPacingGuideSupportingMaterial,
    *,
    settings: Settings | None = None,
) -> dict:
    download_url = None
    if row.material_kind == "file" and row.storage_key and settings is not None:
        download_url = get_teacher_assist_download_url(
            settings,
            storage_key=row.storage_key,
            original_filename=row.original_filename or row.title,
            mime_type=row.mime_type or "application/octet-stream",
        )
    return {
        "id": row.id,
        "pacing_guide_id": row.pacing_guide_id,
        "period_id": row.period_id,
        "education_objective_id": row.education_objective_id,
        "material_kind": row.material_kind,
        "resource_type": row.resource_type,
        "title": row.title,
        "description": row.description,
        "note_body": row.note_body,
        "external_url": row.external_url,
        "original_filename": row.original_filename,
        "mime_type": row.mime_type,
        "file_size": row.file_size,
        "storage_key": row.storage_key,
        "download_url": download_url,
        "visibility_scope": row.visibility_scope,
        "uploaded_by_user_id": row.uploaded_by_user_id,
        "active": row.active,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def list_supporting_materials(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID | None = None,
    education_objective_id: uuid.UUID | None = None,
    active_only: bool = True,
    settings: Settings | None = None,
) -> list[dict]:
    guide = _get_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    stmt = select(TeacherAssistPacingGuideSupportingMaterial).where(
        TeacherAssistPacingGuideSupportingMaterial.pacing_guide_id == guide.id,
        TeacherAssistPacingGuideSupportingMaterial.tenant_id == tenant_id,
    )
    if active_only:
        stmt = stmt.where(TeacherAssistPacingGuideSupportingMaterial.active.is_(True))
    if period_id is not None:
        stmt = stmt.where(TeacherAssistPacingGuideSupportingMaterial.period_id == period_id)
    if education_objective_id is not None:
        stmt = stmt.where(
            TeacherAssistPacingGuideSupportingMaterial.education_objective_id == education_objective_id
        )
    elif period_id is not None:
        stmt = stmt.where(TeacherAssistPacingGuideSupportingMaterial.education_objective_id.is_(None))
    rows = db.scalars(stmt.order_by(TeacherAssistPacingGuideSupportingMaterial.created_at.asc())).all()
    return [serialize_supporting_material(row, settings=settings) for row in rows]


def _create_material_row(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    actor: User,
    period_id: uuid.UUID | None,
    education_objective_id: uuid.UUID | None,
    material_kind: str,
    resource_type: str,
    title: str,
    description: str | None,
    note_body: str | None = None,
    external_url: str | None = None,
    stored: StoredTeacherAssistUpload | None = None,
) -> TeacherAssistPacingGuideSupportingMaterial:
    if material_kind not in PACING_SUPPORTING_MATERIAL_KINDS:
        raise ValueError(f"Unsupported material kind '{material_kind}'")
    normalized_title = title.strip()
    if not normalized_title:
        raise _field_errors(title="Title is required")
    now = datetime.now(UTC)
    row = TeacherAssistPacingGuideSupportingMaterial(
        **_base_material_fields(
            db,
            guide=guide,
            period_id=period_id,
            education_objective_id=education_objective_id,
        ),
        material_kind=material_kind,
        resource_type=_validate_resource_type(resource_type),
        title=normalized_title,
        description=description.strip() if description else None,
        note_body=note_body.strip() if note_body else None,
        external_url=external_url,
        storage_key=stored.storage_key if stored else None,
        original_filename=stored.original_filename if stored else None,
        mime_type=stored.mime_type if stored else None,
        file_size=stored.file_size if stored else None,
        uploaded_by_user_id=actor.id,
        active=True,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


async def upload_supporting_file(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    actor: User,
    pacing_guide_id: uuid.UUID,
    upload: UploadFile,
    title: str | None,
    description: str | None,
    resource_type: str,
    period_id: uuid.UUID | None,
    education_objective_id: uuid.UUID | None,
) -> TeacherAssistPacingGuideSupportingMaterial:
    guide = _get_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    if upload.filename is None or not upload.filename.strip():
        raise _field_errors(file="File is required")
    _validate_file_extension(upload.filename)
    if not resource_type.strip():
        raise _field_errors(resource_type="Resource type is required")
    stored = await store_teacher_assist_upload(
        settings,
        tenant_id=tenant_id,
        upload=upload,
        area="resources",
    )
    return _create_material_row(
        db,
        guide=guide,
        actor=actor,
        period_id=period_id,
        education_objective_id=education_objective_id,
        material_kind="file",
        resource_type=resource_type,
        title=title or stored.original_filename,
        description=description,
        stored=stored,
    )


def create_supporting_link(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    pacing_guide_id: uuid.UUID,
    title: str,
    external_url: str,
    resource_type: str,
    description: str | None,
    period_id: uuid.UUID | None,
    education_objective_id: uuid.UUID | None,
) -> TeacherAssistPacingGuideSupportingMaterial:
    guide = _get_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    if not external_url.strip():
        raise _field_errors(external_url="URL is required")
    if not resource_type.strip():
        raise _field_errors(resource_type="Resource type is required")
    return _create_material_row(
        db,
        guide=guide,
        actor=actor,
        period_id=period_id,
        education_objective_id=education_objective_id,
        material_kind="link",
        resource_type=resource_type,
        title=title,
        description=description,
        external_url=_validate_external_url(external_url),
    )


def create_supporting_note(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    pacing_guide_id: uuid.UUID,
    note_body: str,
    title: str | None,
    period_id: uuid.UUID | None,
    education_objective_id: uuid.UUID | None,
) -> TeacherAssistPacingGuideSupportingMaterial:
    guide = _get_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    normalized_body = note_body.strip()
    if not normalized_body:
        raise _field_errors(note_body="Note body is required")
    return _create_material_row(
        db,
        guide=guide,
        actor=actor,
        period_id=period_id,
        education_objective_id=education_objective_id,
        material_kind="note",
        resource_type="admin_note",
        title=(title.strip() if title and title.strip() else "District note"),
        description=None,
        note_body=normalized_body,
    )


def archive_supporting_material(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    material_id: uuid.UUID,
    actor: User,
) -> TeacherAssistPacingGuideSupportingMaterial:
    row = db.scalars(
        select(TeacherAssistPacingGuideSupportingMaterial).where(
            TeacherAssistPacingGuideSupportingMaterial.id == material_id,
            TeacherAssistPacingGuideSupportingMaterial.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Supporting material not found")
    row.active = False
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row


def get_pacing_guide_planning_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict:
    guide = _get_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    period = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id,
        )
        .options(
            selectinload(TeacherAssistPacingGuidePeriod.objectives).selectinload(
                TeacherAssistPacingGuideObjective.objective
            )
        )
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide week not found")
    materials = list_supporting_materials(
        db,
        tenant_id=tenant_id,
        pacing_guide_id=guide.id,
        period_id=period_id,
        active_only=True,
        settings=settings,
    )
    objective_materials = list_supporting_materials(
        db,
        tenant_id=tenant_id,
        pacing_guide_id=guide.id,
        active_only=True,
        settings=settings,
    )
    objective_ids = {str(row.objective_id) for row in period.objectives}
    objective_level = [
        row
        for row in objective_materials
        if row["education_objective_id"] is not None
        and str(row["education_objective_id"]) in objective_ids
    ]
    week_level = [row for row in materials if row["education_objective_id"] is None]

    objectives = []
    for mapped in period.objectives:
        objective = mapped.objective
        linked = [
            row
            for row in objective_level
            if row["education_objective_id"] == mapped.objective_id
        ]
        objectives.append(
            {
                "education_objective_id": mapped.objective_id,
                "objective_code": getattr(objective, "objective_id", None),
                "description": getattr(objective, "description", None),
                "curriculum_files": [row for row in linked if row["material_kind"] == "file"],
                "reference_links": [row for row in linked if row["material_kind"] == "link"],
                "notes": [row for row in linked if row["material_kind"] == "note"],
                "supporting_documents": [
                    row
                    for row in linked
                    if row["material_kind"] == "file"
                    and row["resource_type"] not in {"curriculum_file", "worksheet", "slide_deck"}
                ],
            }
        )

    return {
        "school_year": guide.school_year_label,
        "district_id": guide.catalog_district_id,
        "school_id": guide.catalog_school_id,
        "grade_id": guide.catalog_grade_id,
        "subject_id": guide.catalog_subject_id,
        "pacing_guide_id": guide.id,
        "period_id": period.id,
        "week_title": period.title,
        "week_description": period.description,
        "objectives": objectives,
        "curriculum_files": [row for row in week_level if row["material_kind"] == "file" and row["resource_type"] == "curriculum_file"],
        "reference_links": [row for row in week_level if row["material_kind"] == "link"],
        "notes": [row for row in week_level if row["material_kind"] == "note"],
        "supporting_documents": [
            row
            for row in week_level
            if row["material_kind"] == "file" and row["resource_type"] != "curriculum_file"
        ],
    }


def seed_placeholder_file_bytes(*, subject_code: str) -> tuple[bytes, str, str]:
    content = (
        f"Placeholder district curriculum overview for Grade 5 {subject_code}.\n"
        "Replace with district-owned materials your school is authorized to use.\n"
    ).encode("utf-8")
    filename = f"grade-5-{subject_code.lower().replace(' ', '-')}-overview.txt"
    return content, filename, "text/plain"


def create_seed_supporting_material_file(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    actor: User,
    guide: TeacherAssistPacingGuide,
    period_id: uuid.UUID,
    subject_code: str,
) -> TeacherAssistPacingGuideSupportingMaterial:
    contents, filename, mime_type = seed_placeholder_file_bytes(subject_code=subject_code)
    stored = save_teacher_assist_bytes(
        settings,
        tenant_id=tenant_id,
        area="resources",
        original_filename=filename,
        contents=contents,
        mime_type=mime_type,
    )
    return _create_material_row(
        db,
        guide=guide,
        actor=actor,
        period_id=period_id,
        education_objective_id=None,
        material_kind="file",
        resource_type="curriculum_file",
        title=f"Grade 5 {subject_code} curriculum overview (sample)",
        description="Sample placeholder file for local development.",
        stored=stored,
    )

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
from oziebot_api.models.teacher_assist_pacing_guide_objective import (
    TeacherAssistPacingGuideObjective,
)
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_resource import TeacherAssistPacingGuideResource
from oziebot_api.models.teacher_assist_pacing_guide_period_day import (
    TeacherAssistPacingGuidePeriodDay,
)
from oziebot_api.models.teacher_assist_pacing_guide_supporting_material import (
    TeacherAssistPacingGuideSupportingMaterial,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    get_catalog_pacing_guide_detail,
)
from oziebot_api.services.teacher_assist.storage import (
    StoredTeacherAssistUpload,
    get_teacher_assist_download_url,
    save_teacher_assist_bytes,
    store_teacher_assist_upload,
)
from oziebot_api.services.teacher_assist_v2.document_extraction import (
    ensure_supporting_material_extraction_record,
    load_supporting_material_extractions,
    run_document_extraction,
    serialize_document_extraction,
)
from oziebot_api.services.teacher_assist_v2.pacing_guide_period_days import load_period_days
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
        raise _field_errors(
            file="Unsupported file type. Use PDF, DOCX, PPTX, TXT, or common image formats."
        )


def _get_guide_or_404(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID
) -> TeacherAssistPacingGuide:
    return get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)


def _resolve_platform_school_year_id(
    db: Session, *, guide: TeacherAssistPacingGuide
) -> uuid.UUID | None:
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


def _get_period_day_or_404(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    period_day_id: uuid.UUID,
) -> TeacherAssistPacingGuidePeriodDay:
    day = db.scalars(
        select(TeacherAssistPacingGuidePeriodDay)
        .join(TeacherAssistPacingGuidePeriod)
        .where(
            TeacherAssistPacingGuidePeriodDay.id == period_day_id,
            TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id,
        )
    ).one_or_none()
    if day is None:
        raise LookupError("Pacing guide day not found")
    return day


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
    if (
        guide.catalog_subject_id
        and objective.subject_id
        and objective.subject_id != guide.catalog_subject_id
    ):
        raise _field_errors(
            education_objective_id="Objective subject does not match pacing guide subject"
        )
    if period_id is not None:
        mapped = db.scalars(
            select(TeacherAssistPacingGuideObjective).where(
                TeacherAssistPacingGuideObjective.period_id == period_id,
                TeacherAssistPacingGuideObjective.objective_id == education_objective_id,
            )
        ).one_or_none()
        if mapped is None:
            raise _field_errors(
                education_objective_id="Objective is not mapped to the selected week"
            )
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
            raise _field_errors(
                education_objective_id="Objective is not mapped to this pacing guide"
            )
    return objective


def _validate_linkage(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    period_id: uuid.UUID | None,
    period_day_id: uuid.UUID | None,
    education_objective_id: uuid.UUID | None,
    allow_guide_level: bool = False,
) -> uuid.UUID | None:
    resolved_period_id = period_id
    if period_day_id is not None:
        day = _get_period_day_or_404(db, guide=guide, period_day_id=period_day_id)
        if resolved_period_id is not None and resolved_period_id != day.period_id:
            raise _field_errors(period_day_id="Day does not belong to the selected week")
        resolved_period_id = day.period_id
        if education_objective_id is not None:
            _validate_objective_linkage(
                db,
                guide=guide,
                education_objective_id=education_objective_id,
                period_id=resolved_period_id,
            )
        return resolved_period_id
    if period_id is None and education_objective_id is None:
        if allow_guide_level:
            return None
        raise _field_errors(linkage="Attach materials to a week, day, or learning objective")
    if period_id is not None:
        _get_period_or_404(db, guide=guide, period_id=period_id)
    if education_objective_id is not None:
        _validate_objective_linkage(
            db,
            guide=guide,
            education_objective_id=education_objective_id,
            period_id=period_id,
        )
    return resolved_period_id


def _base_material_fields(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    period_id: uuid.UUID | None,
    period_day_id: uuid.UUID | None,
    education_objective_id: uuid.UUID | None,
    allow_guide_level: bool = False,
) -> dict:
    _assert_guide_hierarchy(guide)
    resolved_period_id = _validate_linkage(
        db,
        guide=guide,
        period_id=period_id,
        period_day_id=period_day_id,
        education_objective_id=education_objective_id,
        allow_guide_level=allow_guide_level,
    )
    return {
        "tenant_id": guide.tenant_id,
        "pacing_guide_id": guide.id,
        "period_id": resolved_period_id,
        "period_day_id": period_day_id,
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
    extraction: dict | None = None,
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
        "period_day_id": row.period_day_id,
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
        "source_resource_id": row.source_resource_id,
        "source_pacing_guide_id": row.source_pacing_guide_id,
        "active": row.active,
        "archived_at": row.archived_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "extraction": extraction,
    }


def list_supporting_materials(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID | None = None,
    period_day_id: uuid.UUID | None = None,
    education_objective_id: uuid.UUID | None = None,
    guide_level_only: bool = False,
    week_level_only: bool = False,
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
    if period_day_id is not None:
        stmt = stmt.where(TeacherAssistPacingGuideSupportingMaterial.period_day_id == period_day_id)
    elif period_id is not None:
        stmt = stmt.where(TeacherAssistPacingGuideSupportingMaterial.period_id == period_id)
    if education_objective_id is not None:
        stmt = stmt.where(
            TeacherAssistPacingGuideSupportingMaterial.education_objective_id
            == education_objective_id
        )
    elif period_day_id is not None:
        stmt = stmt.where(
            TeacherAssistPacingGuideSupportingMaterial.education_objective_id.is_(None)
        )
    elif period_id is not None and week_level_only:
        stmt = stmt.where(
            TeacherAssistPacingGuideSupportingMaterial.period_day_id.is_(None),
            TeacherAssistPacingGuideSupportingMaterial.education_objective_id.is_(None),
        )
    elif period_id is not None:
        stmt = stmt.where(
            TeacherAssistPacingGuideSupportingMaterial.education_objective_id.is_(None)
        )
    elif guide_level_only:
        stmt = stmt.where(
            TeacherAssistPacingGuideSupportingMaterial.period_id.is_(None),
            TeacherAssistPacingGuideSupportingMaterial.period_day_id.is_(None),
            TeacherAssistPacingGuideSupportingMaterial.education_objective_id.is_(None),
        )
    rows = db.scalars(
        stmt.order_by(TeacherAssistPacingGuideSupportingMaterial.created_at.asc())
    ).all()
    if settings is not None:
        for row in rows:
            if row.material_kind != "file" or not row.storage_key:
                continue
            record = ensure_supporting_material_extraction_record(db, row=row)
            if record.extraction_status == "not_started":
                run_document_extraction(db, settings=settings, record=record)
    extraction_map = load_supporting_material_extractions(db, material_ids=[row.id for row in rows])
    return [
        serialize_supporting_material(
            row,
            settings=settings,
            extraction=serialize_document_extraction(extraction_map.get(row.id)),
        )
        for row in rows
    ]


def _create_material_row(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    actor: User,
    period_id: uuid.UUID | None,
    period_day_id: uuid.UUID | None = None,
    education_objective_id: uuid.UUID | None = None,
    material_kind: str,
    resource_type: str,
    title: str,
    description: str | None,
    note_body: str | None = None,
    external_url: str | None = None,
    stored: StoredTeacherAssistUpload | None = None,
    source_resource_id: uuid.UUID | None = None,
    source_pacing_guide_id: uuid.UUID | None = None,
) -> TeacherAssistPacingGuideSupportingMaterial:
    if material_kind not in PACING_SUPPORTING_MATERIAL_KINDS:
        raise ValueError(f"Unsupported material kind '{material_kind}'")
    normalized_title = title.strip()
    if not normalized_title:
        raise _field_errors(title="Title is required")
    if not resource_type.strip():
        raise _field_errors(resource_type="Resource type is required")
    if material_kind == "file" and stored is None and source_resource_id is None:
        raise _field_errors(file="File is required")
    if material_kind == "link" and not external_url and source_resource_id is None:
        raise _field_errors(external_url="URL is required")
    if (
        material_kind == "note"
        and not (note_body and note_body.strip())
        and source_resource_id is None
    ):
        raise _field_errors(note_body="Note body is required")
    now = datetime.now(UTC)
    row = TeacherAssistPacingGuideSupportingMaterial(
        **_base_material_fields(
            db,
            guide=guide,
            period_id=period_id,
            period_day_id=period_day_id,
            education_objective_id=education_objective_id,
            allow_guide_level=period_id is None
            and period_day_id is None
            and education_objective_id is None,
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
        source_resource_id=source_resource_id,
        source_pacing_guide_id=source_pacing_guide_id,
        active=True,
        archived_at=None,
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
    period_day_id: uuid.UUID | None,
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
    row = _create_material_row(
        db,
        guide=guide,
        actor=actor,
        period_id=period_id,
        period_day_id=period_day_id,
        education_objective_id=education_objective_id,
        material_kind="file",
        resource_type=resource_type,
        title=title or stored.original_filename,
        description=description,
        stored=stored,
    )
    record = ensure_supporting_material_extraction_record(db, row=row)
    run_document_extraction(db, settings=settings, record=record)
    return row


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
    period_day_id: uuid.UUID | None = None,
    education_objective_id: uuid.UUID | None = None,
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
        period_day_id=period_day_id,
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
    period_day_id: uuid.UUID | None = None,
    education_objective_id: uuid.UUID | None = None,
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
        period_day_id=period_day_id,
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
    row.archived_at = datetime.now(UTC)
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
            ),
            selectinload(TeacherAssistPacingGuidePeriod.resources).selectinload(
                TeacherAssistPacingGuideResource.catalog_resource
            ),
        )
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide week not found")
    all_materials = list_supporting_materials(
        db,
        tenant_id=tenant_id,
        pacing_guide_id=guide.id,
        active_only=True,
        settings=settings,
    )
    guide_level = [
        row
        for row in all_materials
        if row["period_id"] is None
        and row["period_day_id"] is None
        and row["education_objective_id"] is None
    ]
    week_level = [
        row
        for row in all_materials
        if row["period_id"] == period_id
        and row["period_day_id"] is None
        and row["education_objective_id"] is None
    ]
    objective_ids = {str(row.objective_id) for row in period.objectives}
    objective_level = [
        row
        for row in all_materials
        if row["education_objective_id"] is not None
        and str(row["education_objective_id"]) in objective_ids
    ]
    day_level_by_id: dict[str, list[dict]] = {}
    for row in all_materials:
        if row["period_day_id"] is None:
            continue
        if row["period_id"] != period_id:
            continue
        day_level_by_id.setdefault(str(row["period_day_id"]), []).append(row)

    objectives = []
    for mapped in period.objectives:
        objective = mapped.objective
        linked = [
            row for row in objective_level if row["education_objective_id"] == mapped.objective_id
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

    period_metadata = (
        period.metadata_json if isinstance(getattr(period, "metadata_json", None), dict) else {}
    )
    guide_metadata = (
        guide.metadata_json if isinstance(getattr(guide, "metadata_json", None), dict) else {}
    )
    days = load_period_days(db, period_id=period.id)
    daily_context = []
    for day in days:
        day_materials = day_level_by_id.get(str(day.id), [])
        daily_context.append(
            {
                "day_id": str(day.id),
                "day_label": day.day_label,
                "sequence_number": day.sequence_number,
                "daily_topic": day.daily_topic,
                "objective_focus": day.objective_focus,
                "teacher_notes": day.teacher_notes,
                "materials_needed": day.materials_needed,
                "assessment_check": day.assessment_check,
                "attached_files": [row for row in day_materials if row["material_kind"] == "file"],
                "reference_links": [row for row in day_materials if row["material_kind"] == "link"],
                "notes": [row for row in day_materials if row["material_kind"] == "note"],
            }
        )

    catalog_resources = []
    for linked in period.resources:
        catalog = linked.catalog_resource
        title = getattr(catalog, "title", None) if catalog is not None else None
        catalog_resources.append(
            {
                "id": str(linked.id),
                "material_kind": "catalog_resource",
                "resource_type": getattr(catalog, "resource_type", None)
                if catalog is not None
                else "catalog_resource",
                "title": title or linked.notes or "Catalog resource",
                "description": getattr(catalog, "description", None)
                if catalog is not None
                else linked.notes,
                "notes": linked.notes,
                "is_primary": linked.is_primary,
                "catalog_resource_id": str(linked.catalog_resource_id)
                if linked.catalog_resource_id
                else None,
            }
        )

    return {
        "pacing_guide_title": guide.title,
        "pacing_guide_description": guide.description,
        "ownership_scope": guide_metadata.get("ownership_scope"),
        "unit_title": guide_metadata.get("unit_title"),
        "estimated_duration_weeks": guide_metadata.get("estimated_duration_weeks"),
        "start_week": guide_metadata.get("start_week"),
        "end_week": guide_metadata.get("end_week"),
        "school_year": guide.school_year_label,
        "district_id": guide.catalog_district_id,
        "school_id": guide.catalog_school_id,
        "grade_id": guide.catalog_grade_id,
        "subject_id": guide.catalog_subject_id,
        "pacing_guide_id": guide.id,
        "period_id": period.id,
        "week_title": period.title,
        "week_description": period.description,
        "week_unit_title": period_metadata.get("unit_title"),
        "daily_plans": period_metadata.get("daily_plans") or [],
        "days": daily_context,
        "objectives": objectives,
        "guide_level_materials": guide_level,
        "week_level_materials": week_level,
        "day_level_materials": [row for rows in day_level_by_id.values() for row in rows],
        "objective_level_materials": objective_level,
        "curriculum_files": [
            row
            for row in week_level
            if row["material_kind"] == "file"
            and row["resource_type"] in {"curriculum_file", "curriculum_reference"}
        ],
        "reference_links": [row for row in week_level if row["material_kind"] == "link"],
        "notes": [row for row in week_level if row["material_kind"] == "note"],
        "attached_curriculum_files": [row for row in week_level if row["material_kind"] == "file"],
        "catalog_resources": catalog_resources,
        "supporting_documents": [
            row
            for row in week_level
            if row["material_kind"] == "file"
            and row["resource_type"] not in {"curriculum_file", "curriculum_reference"}
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
        period_day_id=None,
        education_objective_id=None,
        material_kind="file",
        resource_type="curriculum_file",
        title=f"Grade 5 {subject_code} curriculum overview (sample)",
        description="Sample placeholder file for local development.",
        stored=stored,
    )


def copy_pacing_guide_supporting_materials(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    source_guide_id: uuid.UUID,
    target_guide_id: uuid.UUID,
    period_id_map: dict[str, uuid.UUID],
    day_id_map: dict[str, uuid.UUID],
) -> int:
    guide = _get_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=target_guide_id)
    rows = db.scalars(
        select(TeacherAssistPacingGuideSupportingMaterial).where(
            TeacherAssistPacingGuideSupportingMaterial.pacing_guide_id == source_guide_id,
            TeacherAssistPacingGuideSupportingMaterial.tenant_id == tenant_id,
            TeacherAssistPacingGuideSupportingMaterial.active.is_(True),
        )
    ).all()
    now = datetime.now(UTC)
    copied = 0
    for row in rows:
        mapped_period_id = period_id_map.get(str(row.period_id)) if row.period_id else None
        mapped_day_id = day_id_map.get(str(row.period_day_id)) if row.period_day_id else None
        if row.period_id is not None and mapped_period_id is None:
            continue
        if row.period_day_id is not None and mapped_day_id is None:
            continue
        db.add(
            TeacherAssistPacingGuideSupportingMaterial(
                tenant_id=guide.tenant_id,
                pacing_guide_id=target_guide_id,
                period_id=mapped_period_id,
                period_day_id=mapped_day_id,
                education_objective_id=row.education_objective_id,
                platform_school_year_id=row.platform_school_year_id,
                catalog_state_id=row.catalog_state_id,
                catalog_district_id=row.catalog_district_id,
                catalog_school_id=row.catalog_school_id,
                catalog_grade_id=row.catalog_grade_id,
                catalog_subject_id=row.catalog_subject_id,
                material_kind=row.material_kind,
                resource_type=row.resource_type,
                title=row.title,
                description=row.description,
                note_body=row.note_body,
                external_url=row.external_url,
                storage_key=row.storage_key,
                original_filename=row.original_filename,
                mime_type=row.mime_type,
                file_size=row.file_size,
                visibility_scope=guide.visibility_scope,
                uploaded_by_user_id=actor.id,
                source_resource_id=row.id,
                source_pacing_guide_id=source_guide_id,
                active=True,
                archived_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        copied += 1
    db.flush()
    return copied

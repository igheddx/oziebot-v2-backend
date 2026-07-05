"""Teacher planning form, review context, and supplemental materials."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlparse

from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationSubject
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
    TeacherAssistV2PlanningSupplementalMaterial,
)
from oziebot_api.models.teacher_assist_v2_onboarding import TeacherAssistV2PacingGuideAssignment
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.pacing_guide_foundation import get_catalog_pacing_guide_detail
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user
from oziebot_api.services.teacher_assist.storage import (
    StoredTeacherAssistUpload,
    get_teacher_assist_download_url,
    store_teacher_assist_upload,
)
from oziebot_api.services.teacher_assist_v2.pacing_guide_setup import (
    _load_guide_summary,
    _require_completed_onboarding,
    list_teacher_available_pacing_guides,
)
from oziebot_api.services.teacher_assist_v2.pacing_guides import ensure_tenant_school_year
from oziebot_api.services.teacher_assist_v2.planning_constants import WEEK_RANGE_PRESETS  # noqa: F401 kept for compat
from oziebot_api.services.teacher_assist_v2.package_lifecycle import resolve_default_plan_dates
from oziebot_api.services.teacher_assist_v2.platform_context import resolve_instructional_catalog_tenant_id
from oziebot_api.services.teacher_assist_v2.school_years import get_platform_school_year_or_404
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import (
    filter_excluded_pacing_materials,
    flatten_pacing_materials,
    resolve_week_daily_topic,
)
from oziebot_api.services.teacher_assist_v2.document_extraction import (
    build_ai_readiness_summary,
    ensure_planning_supplemental_extraction_record,
    load_planning_supplemental_extractions,
    run_document_extraction,
    serialize_document_extraction,
)
from oziebot_api.services.teacher_assist_v2.supporting_materials import (
    get_pacing_guide_planning_context,
)
from oziebot_api.services.teacher_assist_v2.supporting_materials_constants import (
    PACING_SUPPORTING_UPLOAD_EXTENSIONS,
)
from oziebot_api.services.teacher_assist_v2.output_recommender import (
    recommend_outputs_from_guide_detail,
)
from oziebot_api.services.teacher_assist_v2.teacher_onboarding import (
    get_v2_onboarding,
    is_v2_pacing_setup_complete,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _field_errors(**errors: str) -> ValueError:
    return ValueError({key: value for key, value in errors.items() if value})


def _require_planning_ready(db: Session, *, user: User):
    onboarding = get_v2_onboarding(db, user_id=user.id)
    if onboarding is None or not is_v2_pacing_setup_complete(onboarding):
        raise ValueError("Complete pacing guide setup before planning.")
    return _require_completed_onboarding(onboarding)


def _teacher_assignments(db: Session, *, user: User) -> list[TeacherAssistV2PacingGuideAssignment]:
    return db.scalars(
        select(TeacherAssistV2PacingGuideAssignment).where(
            TeacherAssistV2PacingGuideAssignment.user_id == user.id,
            TeacherAssistV2PacingGuideAssignment.active.is_(True),
        ).order_by(TeacherAssistV2PacingGuideAssignment.created_at.asc())
    ).all()


def _week_periods(guide) -> list:
    return sorted(
        (period for period in guide.periods if period.period_type == "WEEK"),
        key=lambda period: period.sequence_number,
    )


def _assignment_context(db: Session, *, user: User) -> dict:
    ctx = teacher_assist_context_for_user(db, user)
    onboarding = _require_planning_ready(db, user=user)
    assert onboarding.school_year_id is not None
    assert onboarding.grade_id is not None
    assert onboarding.state_id is not None
    assert onboarding.district_id is not None

    platform_year = get_platform_school_year_or_404(db, school_year_id=onboarding.school_year_id)
    platform_tenant_id = resolve_instructional_catalog_tenant_id(db)
    tenant_year = ensure_tenant_school_year(db, tenant_id=ctx.tenant_id, platform_year=platform_year)

    subject_ids = [uuid.UUID(value) for value in (onboarding.selected_subject_ids or [])]
    subjects = db.scalars(select(EducationSubject).where(EducationSubject.id.in_(subject_ids))).all()
    subject_by_id = {row.id: row for row in subjects}
    assignments = _teacher_assignments(db, user=user)

    guides = []
    loaded_guide_objects = []
    subject_codes: set[str] = set()
    min_periods = None
    for assignment in assignments:
        subject = subject_by_id.get(assignment.subject_id)
        if subject is None:
            continue
        guide = _load_guide_summary(
            db,
            tenant_id=ctx.tenant_id,
            platform_tenant_id=platform_tenant_id,
            pacing_guide_id=assignment.pacing_guide_id,
        )
        period_count = len(_week_periods(guide))
        min_periods = period_count if min_periods is None else min(min_periods, period_count)
        loaded_guide_objects.append(guide)
        subject_codes.add((subject.subject_code or "").strip().upper())
        available = list_teacher_available_pacing_guides(
            db,
            platform_tenant_id=platform_tenant_id,
            onboarding=onboarding,
            platform_year=platform_year,
            subject_id=subject.id,
        )
        guides.append(
            {
                "subject_id": str(subject.id),
                "subject_code": subject.subject_code,
                "subject_name": subject.display_name,
                "pacing_guide_id": str(guide.id),
                "pacing_guide_title": guide.title,
                "guide_scope": assignment.guide_scope,
                "period_count": period_count,
                "available_pacing_guides": [
                    {
                        "id": str(row.id),
                        "title": row.title,
                        "description": row.description,
                        "school_year_label": row.school_year_label,
                        "scope_label": "School" if row.catalog_school_id else "District",
                        "is_selected": str(row.id) == str(guide.id),
                    }
                    for row in available
                ],
            }
        )

    if not guides:
        raise ValueError("Adopt pacing guides for your subjects before planning.")

    n = min_periods or 1
    week_ranges: list[dict] = []
    # One option per individual week
    for i in range(1, n + 1):
        week_ranges.append({"week_start": i, "week_end": i, "label": f"Week {i}"})
    # "All weeks" option when the guide is more than one week
    if n > 1:
        week_ranges.append({"week_start": 1, "week_end": n, "label": f"All weeks (1–{n})"})

    recommended_outputs = recommend_outputs_from_guide_detail(
        loaded_guide_objects,
        total_guide_weeks=n,
        subject_codes=subject_codes,
    )

    return {
        "ctx": ctx,
        "onboarding": onboarding,
        "platform_year": platform_year,
        "tenant_year": tenant_year,
        "platform_tenant_id": platform_tenant_id,
        "subjects": guides,
        "default_teaching_order": [row["subject_id"] for row in sorted(
            guides,
            key=lambda g: next(
                (i for i, sid in enumerate(subject_ids) if str(sid) == g["subject_id"]),
                999,
            ),
        )],
        "week_ranges": week_ranges,
        "total_guide_weeks": n,
        "recommended_outputs": recommended_outputs,
    }


def build_planning_form(db: Session, *, user: User) -> dict:
    base = _assignment_context(db, user=user)
    onboarding = base["onboarding"]
    # Default to "All weeks" (last entry) so teachers generate the full guide at once.
    default_range = base["week_ranges"][-1] if base["week_ranges"] else None
    default_start, default_end = resolve_default_plan_dates(
        db,
        user=user,
        week_start=default_range["week_start"] if default_range else 1,
        week_end=default_range["week_end"] if default_range else 1,
    )
    return {
        "school_year": {
            "id": str(base["platform_year"].id),
            "title": base["platform_year"].title,
            "active": base["platform_year"].active,
        },
        "grade_id": str(onboarding.grade_id),
        "subjects": base["subjects"],
        "week_ranges": base["week_ranges"],
        "default_teaching_order": base["default_teaching_order"],
        "default_plan_start_date": default_start.isoformat(),
        "default_plan_end_date": default_end.isoformat(),
        "required_outputs": ["daily_lesson_plan", "subject_slide_deck"],
        "optional_outputs": [
            "assignment",
            "quiz",
            "rubric",
            "exit_ticket",
            "bell_ringer",
            "vocabulary_list",
            "study_guide",
            "parent_newsletter_summary",
        ],
        "recommended_outputs": base["recommended_outputs"],
    }


def _validate_week_range(*, week_start: int, week_end: int, max_periods: int) -> None:
    if week_start < 1 or week_end < week_start:
        raise _field_errors(week_range="Invalid week range")
    if week_end > max_periods:
        raise _field_errors(week_range="Selected weeks exceed pacing guide length")


def _guide_for_assignment(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    platform_tenant_id: uuid.UUID,
    assignment: TeacherAssistV2PacingGuideAssignment,
):
    return get_catalog_pacing_guide_detail(
        db,
        tenant_id=tenant_id if assignment.guide_scope == "teacher" else platform_tenant_id,
        pacing_guide_id=assignment.pacing_guide_id,
    )


def build_planning_review_context(
    db: Session,
    *,
    user: User,
    week_start: int,
    week_end: int,
    settings: Settings | None = None,
    excluded_pacing_material_ids: list[uuid.UUID] | None = None,
) -> dict:
    base = _assignment_context(db, user=user)
    min_periods = min(row["period_count"] for row in base["subjects"])
    _validate_week_range(week_start=week_start, week_end=week_end, max_periods=min_periods)
    excluded_ids = {str(value) for value in (excluded_pacing_material_ids or [])}

    ctx = base["ctx"]
    platform_tenant_id = base["platform_tenant_id"]
    assignments = {str(row.subject_id): row for row in _teacher_assignments(db, user=user)}

    weeks = []
    for subject_row in base["subjects"]:
        assignment = assignments[subject_row["subject_id"]]
        guide = _guide_for_assignment(
            db,
            tenant_id=ctx.tenant_id,
            platform_tenant_id=platform_tenant_id,
            assignment=assignment,
        )
        guide_tenant_id = ctx.tenant_id if assignment.guide_scope == "teacher" else platform_tenant_id
        for week_index, period in enumerate(_week_periods(guide), start=1):
            if week_index < week_start or week_index > week_end:
                continue
            week_entry = next((row for row in weeks if row["sequence_number"] == week_index), None)
            if week_entry is None:
                week_entry = {
                    "sequence_number": week_index,
                    "title": period.title,
                    "start_date": period.start_date.isoformat() if period.start_date else None,
                    "end_date": period.end_date.isoformat() if period.end_date else None,
                    "subjects": [],
                }
                weeks.append(week_entry)
            objectives = []
            for mapped in period.objectives:
                objective = mapped.objective
                objectives.append(
                    {
                        "education_objective_id": str(mapped.objective_id),
                        "objective_code": getattr(objective, "objective_id", None),
                        "description": getattr(objective, "description", None),
                        "is_required": bool(mapped.is_required),
                        "notes": mapped.notes or None,
                    }
                )
            pacing_context = get_pacing_guide_planning_context(
                db,
                tenant_id=guide_tenant_id,
                pacing_guide_id=guide.id,
                period_id=period.id,
                settings=settings,
            )
            week_entry["subjects"].append(
                {
                    "subject_id": subject_row["subject_id"],
                    "subject_name": subject_row["subject_name"],
                    "pacing_guide_id": str(guide.id),
                    "period_id": str(period.id),
                    "daily_topic": resolve_week_daily_topic({"pacing_context": pacing_context}),
                    "objectives": objectives,
                    "district_materials": filter_excluded_pacing_materials(
                        flatten_pacing_materials(
                            pacing_context,
                            excluded_material_ids=excluded_ids or None,
                        ),
                        excluded_ids or None,
                    ),
                    "pacing_days": [
                        {
                            "day_label": day.get("day_label"),
                            "sequence_number": day.get("sequence_number"),
                            "daily_topic": day.get("daily_topic"),
                            "objective_focus": day.get("objective_focus"),
                            "teacher_notes": day.get("teacher_notes"),
                            "materials_needed": day.get("materials_needed"),
                            "assessment_check": day.get("assessment_check"),
                        }
                        for day in (pacing_context.get("days") or [])
                    ],
                    "pacing_context": pacing_context,
                }
            )

    weeks.sort(key=lambda row: row["sequence_number"])
    supplemental = list_planning_supplemental_materials(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        settings=settings,
        unlinked_only=True,
    )
    from oziebot_api.services.teacher_assist_v2.package_lifecycle import default_plan_dates_for_week_range

    period_starts = [
        date.fromisoformat(str(week["start_date"])) for week in weeks if week.get("start_date")
    ]
    period_ends = [date.fromisoformat(str(week["end_date"])) for week in weeks if week.get("end_date")]
    plan_start, plan_end = default_plan_dates_for_week_range(
        week_start=week_start,
        week_end=week_end,
        period_start_dates=period_starts,
        period_end_dates=period_ends,
    )
    district_materials = [
        material
        for week in weeks
        for subject in week["subjects"]
        for material in subject["district_materials"]
    ]
    base_ctx = _assignment_context(db, user=user)
    total_guide_weeks = base_ctx.get("total_guide_weeks") or min_periods
    return {
        "week_start": week_start,
        "week_end": week_end,
        "total_guide_weeks": total_guide_weeks,
        "weeks": weeks,
        "teacher_supplemental_materials": supplemental,
        "ai_readiness_summary": build_ai_readiness_summary(
            district_materials=district_materials,
            teacher_materials=supplemental,
        ),
        "default_plan_start_date": plan_start.isoformat(),
        "default_plan_end_date": plan_end.isoformat(),
    }


def _supplemental_scope(db: Session, *, user: User, week_start: int, week_end: int) -> dict:
    base = _assignment_context(db, user=user)
    onboarding = base["onboarding"]
    min_periods = min(row["period_count"] for row in base["subjects"])
    _validate_week_range(week_start=week_start, week_end=week_end, max_periods=min_periods)
    return {
        "tenant_id": base["ctx"].tenant_id,
        "platform_school_year_id": base["platform_year"].id,
        "catalog_state_id": onboarding.state_id,
        "catalog_district_id": onboarding.district_id,
        "catalog_school_id": onboarding.school_id,
        "catalog_grade_id": onboarding.grade_id,
        "week_start": week_start,
        "week_end": week_end,
    }


def serialize_supplemental_material(
    row: TeacherAssistV2PlanningSupplementalMaterial,
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
        "material_kind": row.material_kind,
        "resource_type": row.resource_type,
        "title": row.title,
        "description": row.description,
        "note_body": row.note_body,
        "external_url": row.external_url,
        "original_filename": row.original_filename,
        "download_url": download_url,
        "week_start": row.week_start,
        "week_end": row.week_end,
        "created_at": row.created_at,
        "extraction": extraction,
    }


def list_planning_supplemental_materials(
    db: Session,
    *,
    user: User,
    week_start: int,
    week_end: int,
    settings: Settings | None = None,
    unlinked_only: bool = False,
    package_id: uuid.UUID | None = None,
) -> list[dict]:
    if package_id is None:
        _supplemental_scope(db, user=user, week_start=week_start, week_end=week_end)
    stmt = select(TeacherAssistV2PlanningSupplementalMaterial).where(
        TeacherAssistV2PlanningSupplementalMaterial.teacher_user_id == user.id,
        TeacherAssistV2PlanningSupplementalMaterial.active.is_(True),
    )
    if package_id is not None:
        stmt = stmt.where(TeacherAssistV2PlanningSupplementalMaterial.package_id == package_id)
    else:
        stmt = stmt.where(
            TeacherAssistV2PlanningSupplementalMaterial.week_start == week_start,
            TeacherAssistV2PlanningSupplementalMaterial.week_end == week_end,
        )
        if unlinked_only:
            stmt = stmt.where(TeacherAssistV2PlanningSupplementalMaterial.package_id.is_(None))
    rows = db.scalars(stmt.order_by(TeacherAssistV2PlanningSupplementalMaterial.created_at.asc())).all()
    if settings is not None:
        for row in rows:
            if row.material_kind != "file" or not row.storage_key:
                continue
            record = ensure_planning_supplemental_extraction_record(db, row=row)
            if record.extraction_status == "not_started":
                run_document_extraction(db, settings=settings, record=record)
    extraction_map = load_planning_supplemental_extractions(db, material_ids=[row.id for row in rows])
    return [
        serialize_supplemental_material(
            row,
            settings=settings,
            extraction=serialize_document_extraction(extraction_map.get(row.id)),
        )
        for row in rows
    ]


def _validate_file_extension(filename: str) -> None:
    suffix = Path(filename).suffix.lower()
    if suffix not in PACING_SUPPORTING_UPLOAD_EXTENSIONS:
        raise _field_errors(file="Unsupported file type.")


def _validate_url(external_url: str) -> str:
    normalized = external_url.strip()
    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise _field_errors(external_url="Enter a valid http or https URL")
    return normalized


def _create_supplemental_row(
    db: Session,
    *,
    user: User,
    week_start: int,
    week_end: int,
    material_kind: str,
    resource_type: str,
    title: str,
    description: str | None = None,
    note_body: str | None = None,
    external_url: str | None = None,
    stored: StoredTeacherAssistUpload | None = None,
) -> TeacherAssistV2PlanningSupplementalMaterial:
    scope = _supplemental_scope(db, user=user, week_start=week_start, week_end=week_end)
    now = _now()
    row = TeacherAssistV2PlanningSupplementalMaterial(
        tenant_id=scope["tenant_id"],
        teacher_user_id=user.id,
        platform_school_year_id=scope["platform_school_year_id"],
        catalog_state_id=scope["catalog_state_id"],
        catalog_district_id=scope["catalog_district_id"],
        catalog_school_id=scope["catalog_school_id"],
        catalog_grade_id=scope["catalog_grade_id"],
        week_start=week_start,
        week_end=week_end,
        material_kind=material_kind,
        resource_type=resource_type,
        title=title.strip(),
        description=description.strip() if description else None,
        note_body=note_body.strip() if note_body else None,
        external_url=external_url,
        storage_key=stored.storage_key if stored else None,
        original_filename=stored.original_filename if stored else None,
        mime_type=stored.mime_type if stored else None,
        file_size=stored.file_size if stored else None,
        uploaded_by_user_id=user.id,
        active=True,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


async def upload_planning_supplemental_file(
    db: Session,
    *,
    settings: Settings,
    user: User,
    week_start: int,
    week_end: int,
    upload: UploadFile,
    title: str | None,
    resource_type: str,
) -> TeacherAssistV2PlanningSupplementalMaterial:
    if upload.filename is None or not upload.filename.strip():
        raise _field_errors(file="File is required")
    _validate_file_extension(upload.filename)
    ctx = teacher_assist_context_for_user(db, user)
    stored = await store_teacher_assist_upload(
        settings,
        tenant_id=ctx.tenant_id,
        upload=upload,
        area="resources",
    )
    row = _create_supplemental_row(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        material_kind="file",
        resource_type=resource_type or "other",
        title=title or stored.original_filename,
        stored=stored,
    )
    record = ensure_planning_supplemental_extraction_record(db, row=row)
    run_document_extraction(db, settings=settings, record=record)
    return row


def create_planning_supplemental_link(
    db: Session,
    *,
    user: User,
    week_start: int,
    week_end: int,
    title: str,
    external_url: str,
    resource_type: str,
    description: str | None,
) -> TeacherAssistV2PlanningSupplementalMaterial:
    if not title.strip():
        raise _field_errors(title="Title is required")
    return _create_supplemental_row(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        material_kind="link",
        resource_type=resource_type or "reference_link",
        title=title,
        description=description,
        external_url=_validate_url(external_url),
    )


def create_planning_supplemental_note(
    db: Session,
    *,
    user: User,
    week_start: int,
    week_end: int,
    note_body: str,
    title: str | None,
) -> TeacherAssistV2PlanningSupplementalMaterial:
    if not note_body.strip():
        raise _field_errors(note_body="Note body is required")
    return _create_supplemental_row(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        material_kind="note",
        resource_type="teacher_note",
        title=title.strip() if title and title.strip() else "Teacher note",
        note_body=note_body,
    )


def archive_planning_supplemental_material(
    db: Session,
    *,
    user: User,
    material_id: uuid.UUID,
) -> TeacherAssistV2PlanningSupplementalMaterial:
    row = db.scalars(
        select(TeacherAssistV2PlanningSupplementalMaterial).where(
            TeacherAssistV2PlanningSupplementalMaterial.id == material_id,
            TeacherAssistV2PlanningSupplementalMaterial.teacher_user_id == user.id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Supplemental material not found")
    row.active = False
    row.updated_at = _now()
    db.flush()
    return row


def get_instructional_package_detail(
    db: Session,
    *,
    user: User,
    package_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict:
    row = db.scalars(
        select(TeacherAssistV2InstructionalPackage)
        .where(
            TeacherAssistV2InstructionalPackage.id == package_id,
            TeacherAssistV2InstructionalPackage.teacher_user_id == user.id,
        )
        .options(
            selectinload(TeacherAssistV2InstructionalPackage.artifacts).selectinload(
                TeacherAssistV2InstructionalPackageArtifact.slide_visual_assets
            )
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Instructional package not found")

    from oziebot_api.models.education_catalog import EducationSubject
    from oziebot_api.services.teacher_assist_v2.package_export import artifact_download_url, group_artifacts

    subject_ids = {artifact.subject_id for artifact in row.artifacts if artifact.subject_id}
    subject_names = {
        item.id: item.display_name
        for item in db.scalars(select(EducationSubject).where(EducationSubject.id.in_(subject_ids))).all()
    } if subject_ids else {}

    artifacts = []
    for artifact in sorted(row.artifacts, key=lambda item: (item.sequence_number, item.title)):
        metadata = artifact.metadata_json if isinstance(artifact.metadata_json, dict) else {}
        content = artifact.content_json if isinstance(artifact.content_json, dict) else {}
        entry = {
            "id": str(artifact.id),
            "artifact_type": artifact.artifact_type,
            "subject_id": str(artifact.subject_id) if artifact.subject_id else None,
            "subject_name": subject_names.get(artifact.subject_id) if artifact.subject_id else None,
            "title": artifact.title,
            "day_label": artifact.day_label,
            "status": artifact.status,
            "description": metadata.get("description") or content.get("description") or content.get("summary"),
            "objective_mapping": metadata.get("objective_mapping") or content.get("objective_mapping"),
            "objective_ids": metadata.get("objective_ids") or content.get("objective_ids") or [],
            "teks_ids": metadata.get("teks_ids") or content.get("teks_ids") or [],
            "alignment_summary": metadata.get("alignment_summary") or content.get("alignment_summary"),
            "preview_html": artifact.preview_html,
            "export_format": artifact.export_format,
            "download_url": artifact_download_url(artifact, settings=settings),
            "export_available": bool(artifact.storage_key),
            "additional_downloads": [],
            "slide_visual_assets": [
                {
                    "id": str(asset.id),
                    "slide_id": asset.slide_id,
                    "visual_type": asset.visual_type,
                    "title": asset.title,
                    "description": asset.description,
                    "source_type": asset.source_type,
                    "source_url": asset.source_url,
                    "attribution": asset.attribution,
                    "local_asset_key": asset.local_asset_key,
                    "prompt_hint": asset.prompt_hint,
                    "educational_purpose": asset.educational_purpose,
                    "suggested_placement": asset.suggested_placement,
                    "layout_template": asset.layout_template,
                    "visual_generation_status": asset.visual_generation_status,
                    "search_terms": list(asset.search_terms_json or []),
                    "suggested_sources": list(asset.suggested_sources_json or []),
                    "created_at": asset.created_at,
                }
                for asset in sorted(artifact.slide_visual_assets, key=lambda item: (item.slide_id, item.updated_at))
            ],
        }
        if content:
            entry["content_json"] = content
        if metadata.get("linked_rubric_artifact_id"):
            entry["linked_rubric_artifact_id"] = metadata["linked_rubric_artifact_id"]
        elif content.get("linked_rubric_artifact_id"):
            entry["linked_rubric_artifact_id"] = content["linked_rubric_artifact_id"]
        if metadata.get("linked_writing_response_artifact_id"):
            entry["linked_writing_response_artifact_id"] = metadata["linked_writing_response_artifact_id"]
        if metadata.get("teacher_edited"):
            entry["teacher_edited"] = True
        if metadata.get("package_additional"):
            entry["package_additional"] = True
        if (
            settings is not None
            and artifact.artifact_type in {"quiz", "assignment", "writing_response"}
            and metadata.get("additional_exports")
        ):
            from oziebot_api.services.teacher_assist.storage import teacher_assist_file_exists
            missing = any(
                isinstance(extra, dict)
                and extra.get("storage_key")
                and not teacher_assist_file_exists(settings, storage_key=extra["storage_key"])
                for extra in metadata["additional_exports"]
            )
            if missing:
                from oziebot_api.services.teacher_assist_v2.assessment_student_exports import refresh_assessment_student_exports
                try:
                    refresh_assessment_student_exports(db, settings=settings, package=row, artifact=artifact)
                    metadata = dict(artifact.metadata_json or {})
                except Exception:
                    pass
        for extra in metadata.get("additional_exports") or []:
            if not isinstance(extra, dict) or not extra.get("storage_key"):
                continue
            export_format = str(extra.get("format") or "html")
            mime_types = {
                "json": "application/json",
                "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "html": "text/html",
            }
            url = artifact_download_url(
                artifact,
                settings=settings,
                storage_key=extra["storage_key"],
                filename=str(
                    extra.get("original_filename")
                    or f"{artifact.title} {extra.get('label', 'download')}.{export_format}"
                ),
                mime_type=mime_types.get(export_format, "application/octet-stream"),
            )
            if url:
                entry["additional_downloads"].append(
                    {
                        "label": str(extra.get("label") or "Download"),
                        "format": str(extra.get("format") or "html"),
                        "download_url": url,
                    }
                )
        if artifact.artifact_type == "assignment" and metadata.get("qr_student_packet"):
            entry["qr_student_packet"] = metadata["qr_student_packet"]
        if artifact.artifact_type == "quiz":
            entry["assignment_id"] = str(artifact.assignment_id) if artifact.assignment_id else None
        artifacts.append(entry)

    daily_plans = [item for item in artifacts if item["artifact_type"] == "daily_lesson_plan"]
    subject_decks = [item for item in artifacts if item["artifact_type"] == "subject_slide_deck"]
    student_decks = [item for item in artifacts if item["artifact_type"] == "student_lesson_deck"]
    student_daily_decks = [item for item in student_decks if item.get("day_label")]
    student_subject_decks = [item for item in student_decks if not item.get("day_label")]

    # Derive the teacher-facing confidence indicator from the validation report.
    # Teachers see only the label and a single plain-language note (if Needs Review).
    # The full report is accessible to root admins via the admin endpoint only.
    _validation_report = row.instructional_validation_report_json or {}
    _confidence_label = _validation_report.get("confidence_label") or None
    _confidence_note: str | None = None
    if _confidence_label == "Needs Review":
        # Surface the highest-severity issue's recommendation as the teacher note.
        _severity_rank = {"critical": 4, "severe": 3, "moderate": 2, "warning": 1, "info": 0}
        _all_issues = [
            i
            for entry in (_validation_report.get("subjects") or [])
            for i in (entry.get("issues") or [])
        ]
        _top_issue = max(
            _all_issues,
            key=lambda i: _severity_rank.get(i.get("severity") or "info", 0),
            default=None,
        )
        if _top_issue:
            _week = _top_issue.get("week")
            _rec = _top_issue.get("recommendation") or ""
            _confidence_note = (
                f"Week {_week}: {_rec}" if _week and _rec
                else _rec or _validation_report.get("teacher_summary") or None
            )

    # Derive artifact alignment confidence from the cross-artifact alignment report.
    # Teachers see only Ready/Needs Review. The full report is for root admins only.
    _alignment_report = row.instructional_alignment_report_json or {}
    _alignment_weeks = _alignment_report.get("weeks") or []
    _alignment_confidence: str | None = None
    if _alignment_weeks:
        _alignment_confidence = (
            "Needs Review"
            if any(w.get("week_alignment_confidence") == "Needs Review" for w in _alignment_weeks)
            else "Ready"
        )

    return {
        "id": str(row.id),
        "status": row.status,
        "week_start": row.week_start,
        "week_end": row.week_end,
        "teaching_order": [str(value) for value in row.teaching_order_json],
        "selected_outputs": list(row.selected_outputs_json),
        "provider_name": row.provider_name,
        "instructional_confidence": _confidence_label,
        "instructional_confidence_note": _confidence_note,
        "instructional_alignment_confidence": _alignment_confidence,
        "generation_document_usage": (row.metadata_json or {}).get("generation_document_usage")
        if isinstance(row.metadata_json, dict)
        else None,
        "ai_readiness_summary": (row.metadata_json or {}).get("ai_readiness_summary")
        if isinstance(row.metadata_json, dict)
        else None,
        "created_at": row.created_at.isoformat(),
        "artifact_groups": group_artifacts(artifacts),
        "artifacts": artifacts,
        "teacher_teaching_brief": row.teacher_coaching_summary_json or None,
        "teaching_mode_available": bool(daily_plans or subject_decks or student_decks),
        "teaching_presentations": {
            "daily_plans": [
                {
                    "artifact_id": item["id"],
                    "day_label": item["day_label"],
                    "title": item["title"],
                }
                for item in daily_plans
            ],
            "subject_decks": [
                {
                    "artifact_id": item["id"],
                    "subject_id": item["subject_id"],
                    "subject_name": item["subject_name"],
                    "title": item["title"],
                }
                for item in subject_decks
            ],
            "student_daily_decks": [
                {
                    "artifact_id": item["id"],
                    "day_label": item["day_label"],
                    "title": item["title"],
                }
                for item in student_daily_decks
            ],
            "student_subject_decks": [
                {
                    "artifact_id": item["id"],
                    "subject_id": item["subject_id"],
                    "subject_name": item["subject_name"],
                    "title": item["title"],
                }
                for item in student_subject_decks
            ],
        },
    }

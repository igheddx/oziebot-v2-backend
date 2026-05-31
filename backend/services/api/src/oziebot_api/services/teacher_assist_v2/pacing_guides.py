from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationSchoolYear
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.user import User
from oziebot_api.schemas.pacing_guide import (
    CatalogPacingGuideDetailOut,
    CatalogPacingGuideObjectiveOut,
    CatalogPacingGuidePeriodOut,
    CatalogPacingGuideResourceOut,
    CatalogPacingGuideSummaryOut,
    CatalogPacingGuideUpdate,
)
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    copy_pacing_guide,
    deactivate_catalog_pacing_guide,
    get_catalog_pacing_guide_detail,
    list_catalog_pacing_guides,
    update_catalog_pacing_guide,
    update_pacing_guide_period,
)
from oziebot_api.services.teacher_assist.setup import create_school_year


def ensure_tenant_school_year(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    platform_year: EducationSchoolYear,
) -> TeacherAssistSchoolYear:
    existing = db.scalars(
        select(TeacherAssistSchoolYear).where(
            TeacherAssistSchoolYear.tenant_id == tenant_id,
            TeacherAssistSchoolYear.title == platform_year.title,
        )
    ).one_or_none()
    if existing is not None:
        existing.start_date = platform_year.start_date
        existing.end_date = platform_year.end_date
        existing.is_active = platform_year.active
        existing.is_template = False
        existing.updated_at = datetime.now(UTC)
        db.flush()
        return existing
    return create_school_year(
        db,
        tenant_id=tenant_id,
        title=platform_year.title,
        start_date=platform_year.start_date,
        end_date=platform_year.end_date,
        is_active=platform_year.active,
        is_template=False,
    )


def serialize_pacing_guide_summary(guide, *, period_count: int) -> CatalogPacingGuideSummaryOut:
    return CatalogPacingGuideSummaryOut(
        id=guide.id,
        tenant_id=guide.tenant_id,
        school_year_id=guide.school_year_id,
        school_year_label=guide.school_year_label,
        guide_type=guide.guide_type,
        title=guide.title,
        description=guide.description,
        catalog_state_id=guide.catalog_state_id,
        catalog_district_id=guide.catalog_district_id,
        catalog_school_id=guide.catalog_school_id,
        catalog_grade_id=guide.catalog_grade_id,
        catalog_subject_id=guide.catalog_subject_id,
        is_template=guide.is_template,
        is_active=guide.is_active,
        is_shared=guide.is_shared,
        created_by_user_id=guide.created_by_user_id,
        updated_by_user_id=guide.updated_by_user_id,
        period_count=period_count,
        created_at=guide.created_at,
        updated_at=guide.updated_at,
    )


def serialize_pacing_guide_detail(guide) -> CatalogPacingGuideDetailOut:
    summary = serialize_pacing_guide_summary(guide, period_count=len(guide.periods))
    periods: list[CatalogPacingGuidePeriodOut] = []
    for period in guide.periods:
        periods.append(
            CatalogPacingGuidePeriodOut(
                id=period.id,
                pacing_guide_id=period.pacing_guide_id,
                period_type=period.period_type,
                title=period.title,
                description=period.description,
                sequence_number=period.sequence_number,
                start_date=period.start_date,
                end_date=period.end_date,
                objectives=[
                    CatalogPacingGuideObjectiveOut(
                        id=row.id,
                        objective_id=row.objective_id,
                        is_required=row.is_required,
                        notes=row.notes,
                        objective_code=getattr(getattr(row, "objective", None), "objective_id", None),
                        objective_description=getattr(getattr(row, "objective", None), "description", None),
                    )
                    for row in period.objectives
                ],
                resources=[
                    CatalogPacingGuideResourceOut(
                        id=row.id,
                        catalog_resource_id=row.catalog_resource_id,
                        resource_library_item_id=row.resource_library_item_id,
                        is_primary=row.is_primary,
                        notes=row.notes,
                        resource_title=getattr(getattr(row, "catalog_resource", None), "title", None),
                        resource_type=getattr(getattr(row, "catalog_resource", None), "resource_type", None),
                    )
                    for row in period.resources
                ],
                created_at=period.created_at,
                updated_at=period.updated_at,
            )
        )
    return CatalogPacingGuideDetailOut(**summary.model_dump(), periods=periods)


def list_v2_district_pacing_guides(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    catalog_district_id: uuid.UUID | None = None,
    catalog_grade_id: uuid.UUID | None = None,
    active_only: bool = True,
):
    guides = list_catalog_pacing_guides(
        db,
        tenant_id=tenant_id,
        guide_type="DISTRICT",
        active_only=active_only,
    )
    if catalog_district_id is not None:
        guides = [guide for guide in guides if guide.catalog_district_id == catalog_district_id]
    if catalog_grade_id is not None:
        guides = [guide for guide in guides if guide.catalog_grade_id == catalog_grade_id]
    return guides


def get_v2_pacing_guide_detail(db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID):
    return get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)


def update_v2_pacing_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    pacing_guide_id: uuid.UUID,
    body: CatalogPacingGuideUpdate,
):
    return update_catalog_pacing_guide(
        db,
        tenant_id=tenant_id,
        pacing_guide_id=pacing_guide_id,
        actor=actor,
        school_year_id=body.school_year_id,
        guide_type=body.guide_type,
        title=body.title,
        description=body.description,
        catalog_state_id=body.catalog_state_id,
        catalog_district_id=body.catalog_district_id,
        catalog_school_id=body.catalog_school_id,
        catalog_grade_id=body.catalog_grade_id,
        catalog_subject_id=body.catalog_subject_id,
        is_template=body.is_template,
        is_active=body.is_active,
        is_shared=body.is_shared,
    )


def clone_v2_pacing_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    source_guide_id: uuid.UUID,
    title: str | None,
    school_year_id: uuid.UUID | None,
):
    return copy_pacing_guide(
        db,
        tenant_id=tenant_id,
        actor=actor,
        source_guide_id=source_guide_id,
        target_guide_type="DISTRICT",
        title=title,
        school_year_id=school_year_id,
    )


def archive_v2_pacing_guide(db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID, actor: User):
    return deactivate_catalog_pacing_guide(
        db,
        tenant_id=tenant_id,
        pacing_guide_id=pacing_guide_id,
        actor=actor,
    )


def update_v2_pacing_guide_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    period_id: uuid.UUID,
    title: str,
    description: str | None,
):
    from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
    from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod

    period = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
    ).one_or_none()
    if period is None:
        raise LookupError("Pacing guide period not found")
    return update_pacing_guide_period(
        db,
        tenant_id=tenant_id,
        period_id=period_id,
        period_type=period.period_type,
        title=title,
        description=description,
        sequence_number=period.sequence_number,
        start_date=period.start_date,
        end_date=period.end_date,
    )

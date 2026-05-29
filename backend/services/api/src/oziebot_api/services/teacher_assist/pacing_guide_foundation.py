from __future__ import annotations

from datetime import UTC, date, datetime
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.education_catalog import (
    EducationCurriculumResource,
    EducationGrade,
    EducationObjective,
    EducationSchool,
    EducationSubject,
)
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_objective import TeacherAssistPacingGuideObjective
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_resource import TeacherAssistPacingGuideResource
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.education_catalog import (
    get_district_or_404,
    get_grade_or_404,
    get_objective_or_404,
    get_school_or_404,
    get_state_or_404,
    get_subject_or_404,
)
from oziebot_api.services.teacher_assist.pacing_guide_constants import (
    validate_pacing_guide_period_type,
    validate_pacing_guide_type,
)
from oziebot_api.services.teacher_assist.planning import get_pacing_guide_or_404
from oziebot_api.services.teacher_assist.setup import get_school_year_or_404


def _now() -> datetime:
    return datetime.now(UTC)


def _validate_catalog_scope(
    db: Session,
    *,
    catalog_state_id: uuid.UUID | None,
    catalog_district_id: uuid.UUID | None,
    catalog_school_id: uuid.UUID | None,
    catalog_grade_id: uuid.UUID | None,
    catalog_subject_id: uuid.UUID | None,
) -> None:
    if catalog_school_id is not None:
        school = get_school_or_404(db, catalog_school_id)
        if catalog_district_id is not None and school.district_id != catalog_district_id:
            raise ValueError("School does not belong to the selected district")
        catalog_district_id = school.district_id
    if catalog_district_id is not None:
        district = get_district_or_404(db, catalog_district_id)
        if catalog_state_id is not None and district.state_id != catalog_state_id:
            raise ValueError("District does not belong to the selected state")
        catalog_state_id = district.state_id
    if catalog_state_id is not None:
        get_state_or_404(db, catalog_state_id)
    if catalog_grade_id is not None:
        get_grade_or_404(db, catalog_grade_id)
    if catalog_subject_id is not None:
        get_subject_or_404(db, catalog_subject_id)


def list_catalog_pacing_guides(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    guide_type: str | None = None,
    catalog_school_id: uuid.UUID | None = None,
    active_only: bool = True,
) -> list[TeacherAssistPacingGuide]:
    stmt = select(TeacherAssistPacingGuide).where(TeacherAssistPacingGuide.tenant_id == tenant_id)
    if active_only:
        stmt = stmt.where(TeacherAssistPacingGuide.is_active.is_(True))
    if guide_type:
        stmt = stmt.where(TeacherAssistPacingGuide.guide_type == validate_pacing_guide_type(guide_type))
    if catalog_school_id is not None:
        stmt = stmt.where(TeacherAssistPacingGuide.catalog_school_id == catalog_school_id)
    stmt = stmt.options(selectinload(TeacherAssistPacingGuide.periods))
    return db.scalars(stmt.order_by(TeacherAssistPacingGuide.updated_at.desc())).all()


def get_catalog_pacing_guide_detail(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID
) -> TeacherAssistPacingGuide:
    row = db.scalars(
        select(TeacherAssistPacingGuide)
        .where(
            TeacherAssistPacingGuide.id == pacing_guide_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
        .options(
            selectinload(TeacherAssistPacingGuide.periods)
            .selectinload(TeacherAssistPacingGuidePeriod.objectives)
            .selectinload(TeacherAssistPacingGuideObjective.objective),
            selectinload(TeacherAssistPacingGuide.periods)
            .selectinload(TeacherAssistPacingGuidePeriod.resources)
            .selectinload(TeacherAssistPacingGuideResource.catalog_resource),
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Pacing guide not found")
    row.periods.sort(key=lambda period: period.sequence_number)
    return row


def create_catalog_pacing_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    school_year_id: uuid.UUID,
    guide_type: str,
    title: str,
    description: str | None,
    catalog_state_id: uuid.UUID | None,
    catalog_district_id: uuid.UUID | None,
    catalog_school_id: uuid.UUID | None,
    catalog_grade_id: uuid.UUID | None,
    catalog_subject_id: uuid.UUID | None,
    is_template: bool = False,
    is_shared: bool = False,
) -> TeacherAssistPacingGuide:
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    normalized_type = validate_pacing_guide_type(guide_type)
    _validate_catalog_scope(
        db,
        catalog_state_id=catalog_state_id,
        catalog_district_id=catalog_district_id,
        catalog_school_id=catalog_school_id,
        catalog_grade_id=catalog_grade_id,
        catalog_subject_id=catalog_subject_id,
    )
    now = _now()
    row = TeacherAssistPacingGuide(
        tenant_id=tenant_id,
        school_year_id=school_year.id,
        school_year_label=school_year.title,
        title=title.strip(),
        description=description.strip() if description else None,
        guide_type=normalized_type,
        catalog_state_id=catalog_state_id,
        catalog_district_id=catalog_district_id,
        catalog_school_id=catalog_school_id,
        catalog_grade_id=catalog_grade_id,
        catalog_subject_id=catalog_subject_id,
        is_template=is_template,
        is_active=True,
        is_shared=is_shared or normalized_type in {"DISTRICT", "GRADE_LEVEL"},
        created_by_user_id=actor.id,
        updated_by_user_id=actor.id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_catalog_pacing_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
    actor: User,
    school_year_id: uuid.UUID,
    guide_type: str,
    title: str,
    description: str | None,
    catalog_state_id: uuid.UUID | None,
    catalog_district_id: uuid.UUID | None,
    catalog_school_id: uuid.UUID | None,
    catalog_grade_id: uuid.UUID | None,
    catalog_subject_id: uuid.UUID | None,
    is_template: bool,
    is_active: bool,
    is_shared: bool,
) -> TeacherAssistPacingGuide:
    row = get_pacing_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    normalized_type = validate_pacing_guide_type(guide_type)
    _validate_catalog_scope(
        db,
        catalog_state_id=catalog_state_id,
        catalog_district_id=catalog_district_id,
        catalog_school_id=catalog_school_id,
        catalog_grade_id=catalog_grade_id,
        catalog_subject_id=catalog_subject_id,
    )
    row.school_year_id = school_year.id
    row.school_year_label = school_year.title
    row.title = title.strip()
    row.description = description.strip() if description else None
    row.guide_type = normalized_type
    row.catalog_state_id = catalog_state_id
    row.catalog_district_id = catalog_district_id
    row.catalog_school_id = catalog_school_id
    row.catalog_grade_id = catalog_grade_id
    row.catalog_subject_id = catalog_subject_id
    row.is_template = is_template
    row.is_active = is_active
    row.is_shared = is_shared or normalized_type in {"DISTRICT", "GRADE_LEVEL"}
    row.updated_by_user_id = actor.id
    row.updated_at = _now()
    db.flush()
    return row


def deactivate_catalog_pacing_guide(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID, actor: User
) -> TeacherAssistPacingGuide:
    row = get_pacing_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    row.is_active = False
    row.updated_by_user_id = actor.id
    row.updated_at = _now()
    db.flush()
    return row


def create_pacing_guide_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
    period_type: str,
    title: str,
    description: str | None,
    sequence_number: int | None,
    start_date: date | None,
    end_date: date | None,
) -> TeacherAssistPacingGuidePeriod:
    guide = get_pacing_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    if sequence_number is None:
        max_sequence = db.scalar(
            select(func.max(TeacherAssistPacingGuidePeriod.sequence_number)).where(
                TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id
            )
        )
        sequence_number = (max_sequence or 0) + 1
    now = _now()
    row = TeacherAssistPacingGuidePeriod(
        pacing_guide_id=guide.id,
        period_type=validate_pacing_guide_period_type(period_type),
        title=title.strip(),
        description=description.strip() if description else None,
        sequence_number=sequence_number,
        start_date=start_date,
        end_date=end_date,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_pacing_guide_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    period_id: uuid.UUID,
    period_type: str,
    title: str,
    description: str | None,
    sequence_number: int,
    start_date: date | None,
    end_date: date | None,
) -> TeacherAssistPacingGuidePeriod:
    row = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Pacing guide period not found")
    row.period_type = validate_pacing_guide_period_type(period_type)
    row.title = title.strip()
    row.description = description.strip() if description else None
    row.sequence_number = sequence_number
    row.start_date = start_date
    row.end_date = end_date
    row.updated_at = _now()
    db.flush()
    return row


def delete_pacing_guide_period(db: Session, *, tenant_id: uuid.UUID, period_id: uuid.UUID) -> None:
    row = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
        .where(
            TeacherAssistPacingGuidePeriod.id == period_id,
            TeacherAssistPacingGuide.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Pacing guide period not found")
    db.delete(row)
    db.flush()


def reorder_pacing_guide_periods(
    db: Session, *, tenant_id: uuid.UUID, pacing_guide_id: uuid.UUID, ordered_period_ids: list[uuid.UUID]
) -> list[TeacherAssistPacingGuidePeriod]:
    guide = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    period_by_id = {period.id: period for period in guide.periods}
    if set(period_by_id.keys()) != set(ordered_period_ids):
        raise ValueError("Period reorder payload must include all guide periods exactly once")
    for index, period_id in enumerate(ordered_period_ids, start=1):
        period = period_by_id[period_id]
        period.sequence_number = index
        period.updated_at = _now()
    db.flush()
    return sorted(period_by_id.values(), key=lambda period: period.sequence_number)


def add_pacing_guide_objective(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    period_id: uuid.UUID,
    objective_id: uuid.UUID,
    is_required: bool,
    notes: str | None,
) -> TeacherAssistPacingGuideObjective:
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
    get_objective_or_404(db, objective_id)
    row = TeacherAssistPacingGuideObjective(
        period_id=period.id,
        objective_id=objective_id,
        is_required=is_required,
        notes=notes.strip() if notes else None,
        created_at=_now(),
    )
    db.add(row)
    db.flush()
    return row


def add_pacing_guide_resource(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    period_id: uuid.UUID,
    catalog_resource_id: uuid.UUID | None,
    resource_library_item_id: uuid.UUID | None,
    is_primary: bool,
    notes: str | None,
) -> TeacherAssistPacingGuideResource:
    if catalog_resource_id is None and resource_library_item_id is None:
        raise ValueError("A catalog resource or teacher resource is required")
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
    if catalog_resource_id is not None:
        resource = db.get(EducationCurriculumResource, catalog_resource_id)
        if resource is None:
            raise LookupError("Catalog resource not found")
    row = TeacherAssistPacingGuideResource(
        period_id=period.id,
        catalog_resource_id=catalog_resource_id,
        resource_library_item_id=resource_library_item_id,
        is_primary=is_primary,
        notes=notes.strip() if notes else None,
        created_at=_now(),
    )
    db.add(row)
    db.flush()
    return row


def _copy_guide_tree(
    db: Session,
    *,
    source: TeacherAssistPacingGuide,
    target: TeacherAssistPacingGuide,
) -> None:
    source_detail = db.scalars(
        select(TeacherAssistPacingGuide)
        .where(TeacherAssistPacingGuide.id == source.id)
        .options(
            selectinload(TeacherAssistPacingGuide.periods).selectinload(TeacherAssistPacingGuidePeriod.objectives),
            selectinload(TeacherAssistPacingGuide.periods).selectinload(TeacherAssistPacingGuidePeriod.resources),
        )
    ).one()
    for period in sorted(source_detail.periods, key=lambda row: row.sequence_number):
        new_period = TeacherAssistPacingGuidePeriod(
            pacing_guide_id=target.id,
            period_type=period.period_type,
            title=period.title,
            description=period.description,
            sequence_number=period.sequence_number,
            start_date=period.start_date,
            end_date=period.end_date,
            created_at=_now(),
            updated_at=_now(),
        )
        db.add(new_period)
        db.flush()
        for objective in period.objectives:
            db.add(
                TeacherAssistPacingGuideObjective(
                    period_id=new_period.id,
                    objective_id=objective.objective_id,
                    is_required=objective.is_required,
                    notes=objective.notes,
                    created_at=_now(),
                )
            )
        for resource in period.resources:
            db.add(
                TeacherAssistPacingGuideResource(
                    period_id=new_period.id,
                    catalog_resource_id=resource.catalog_resource_id,
                    resource_library_item_id=resource.resource_library_item_id,
                    is_primary=resource.is_primary,
                    notes=resource.notes,
                    created_at=_now(),
                )
            )
    db.flush()


def copy_pacing_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    source_guide_id: uuid.UUID,
    target_guide_type: str,
    title: str | None,
    school_year_id: uuid.UUID | None,
) -> TeacherAssistPacingGuide:
    source = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=source_guide_id)
    normalized_type = validate_pacing_guide_type(target_guide_type)
    next_school_year_id = school_year_id or source.school_year_id
    target = create_catalog_pacing_guide(
        db,
        tenant_id=tenant_id,
        actor=actor,
        school_year_id=next_school_year_id,
        guide_type=normalized_type,
        title=title or f"{source.title} Copy",
        description=source.description,
        catalog_state_id=source.catalog_state_id,
        catalog_district_id=source.catalog_district_id,
        catalog_school_id=source.catalog_school_id,
        catalog_grade_id=source.catalog_grade_id,
        catalog_subject_id=source.catalog_subject_id,
        is_template=source.is_template,
        is_shared=normalized_type in {"DISTRICT", "GRADE_LEVEL"},
    )
    _copy_guide_tree(db, source=source, target=target)
    return get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=target.id)


class PacingGuideRolloverService:
    @staticmethod
    def rollover_school_year(
        db: Session,
        *,
        tenant_id: uuid.UUID,
        actor: User,
        source_school_year_id: uuid.UUID,
        target_school_year_id: uuid.UUID,
        guide_ids: list[uuid.UUID] | None = None,
    ) -> list[TeacherAssistPacingGuide]:
        source_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=source_school_year_id)
        target_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=target_school_year_id)
        stmt = select(TeacherAssistPacingGuide).where(
            TeacherAssistPacingGuide.tenant_id == tenant_id,
            TeacherAssistPacingGuide.school_year_id == source_year.id,
            TeacherAssistPacingGuide.is_active.is_(True),
        )
        if guide_ids:
            stmt = stmt.where(TeacherAssistPacingGuide.id.in_(guide_ids))
        sources = db.scalars(stmt).all()
        created: list[TeacherAssistPacingGuide] = []
        for source in sources:
            created.append(
                copy_pacing_guide(
                    db,
                    tenant_id=tenant_id,
                    actor=actor,
                    source_guide_id=source.id,
                    target_guide_type=source.guide_type,
                    title=source.title,
                    school_year_id=target_year.id,
                )
            )
        return created

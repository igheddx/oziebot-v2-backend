"""Seed Texas demo data for teacher time savings (Phase 36)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationGrade, EducationSchool, EducationSubject
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_time_savings import (
    TeacherAssistPlanningGroup,
    TeacherAssistPlanningGroupMember,
    TeacherAssistReuseEvent,
    TeacherAssistWeekTemplate,
)
from oziebot_api.scripts.seed_pacing_guides import _build_week_schedule, _schedule_reference_date, _seed_actor, seed_pacing_guides
from oziebot_api.services.teacher_assist.pacing_guide_foundation import create_catalog_pacing_guide, create_pacing_guide_period
from oziebot_api.services.teacher_assist.setup import create_school_year
from oziebot_api.services.teacher_assist.time_savings_constants import TIME_SAVINGS_MINUTES
from oziebot_api.services.teacher_assist.week_context_service import WeekContextService


PRIOR_SCHOOL_YEARS = (
    ("2025-2026", date(2025, 8, 11), date(2026, 5, 29)),
    ("2026-2027", date(2026, 8, 10), date(2027, 5, 28)),
)


def _ensure_school_year(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    title: str,
    start_date: date,
    end_date: date,
    is_active: bool,
) -> TeacherAssistSchoolYear:
    existing = db.scalars(
        select(TeacherAssistSchoolYear).where(
            TeacherAssistSchoolYear.tenant_id == tenant_id,
            TeacherAssistSchoolYear.title == title,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    return create_school_year(
        db,
        tenant_id=tenant_id,
        title=title,
        start_date=start_date,
        end_date=end_date,
        is_active=is_active,
    )


def _ensure_planning_group(db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID) -> TeacherAssistPlanningGroup:
    existing = db.scalars(
        select(TeacherAssistPlanningGroup).where(
            TeacherAssistPlanningGroup.tenant_id == tenant_id,
            TeacherAssistPlanningGroup.name == "5th Grade Math Team",
        )
    ).one_or_none()
    if existing is not None:
        return existing
    now = datetime.now(UTC)
    group = TeacherAssistPlanningGroup(
        tenant_id=tenant_id,
        name="5th Grade Math Team",
        description="Shared planning group for Grade 5 math pacing and reuse.",
        subject="Math",
        grade_level="5",
        visibility="TEAM",
        created_by_user_id=user_id,
        created_at=now,
        updated_at=now,
    )
    db.add(group)
    db.flush()
    db.add(
        TeacherAssistPlanningGroupMember(
            group_id=group.id,
            user_id=user_id,
            role="owner",
            joined_at=now,
        )
    )
    db.flush()
    return group


def _ensure_shared_math_guide(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year: TeacherAssistSchoolYear,
    group: TeacherAssistPlanningGroup,
    grade_id: uuid.UUID,
    subject_id: uuid.UUID,
    state_id: uuid.UUID,
    district_id: uuid.UUID,
    school_id: uuid.UUID,
) -> TeacherAssistPacingGuide:
    existing = db.scalars(
        select(TeacherAssistPacingGuide).where(
            TeacherAssistPacingGuide.tenant_id == tenant_id,
            TeacherAssistPacingGuide.title == "Grade 5 Math Team Shared Pacing Guide",
            TeacherAssistPacingGuide.school_year_id == school_year.id,
        )
    ).one_or_none()
    if existing is not None:
        return existing

    guide = create_catalog_pacing_guide(
        db,
        tenant_id=tenant_id,
        created_by_user_id=user_id,
        school_year_id=school_year.id,
        guide_type="DISTRICT",
        title="Grade 5 Math Team Shared Pacing Guide",
        description="Team-shared pacing guide for reusable decimal operations weeks.",
        catalog_state_id=state_id,
        catalog_district_id=district_id,
        catalog_school_id=school_id,
        catalog_grade_id=grade_id,
        catalog_subject_id=subject_id,
        is_template=False,
        is_shared=True,
    )
    guide.ownership_type = "GRADE_TEAM"
    guide.visibility_scope = "TEAM"
    guide.planning_group_id = group.id
    db.flush()

    week_schedule = _build_week_schedule(_schedule_reference_date(school_year))
    for index, (week_title, focus) in enumerate(
        (
            ("Week 1", "Decimal place value review"),
            ("Week 2", "Decimal operations"),
            ("Week 3", "Multi-step problem solving"),
            ("Week 4", "Mixed review and application"),
        ),
        start=1,
    ):
        start_date, end_date = week_schedule[index - 1]
        create_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=guide.id,
            period_type="WEEK",
            title=f"{week_title}: {focus}",
            description=f"Reusable team week for {focus.lower()}.",
            sequence_number=index,
            start_date=start_date,
            end_date=end_date,
        )
    return guide


def _ensure_sample_templates(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user,
    guide: TeacherAssistPacingGuide,
) -> int:
    created = 0
    periods = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .where(
            TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id,
            TeacherAssistPacingGuidePeriod.period_type == "WEEK",
        )
        .order_by(TeacherAssistPacingGuidePeriod.sequence_number)
    ).all()
    samples = (
        ("Decimal Review Week Template", "WEEK", "TEAM", periods[0].id if periods else None),
        ("Decimal Operations Assignment Template", "ASSIGNMENT", "SCHOOL", periods[1].id if len(periods) > 1 else None),
        ("Problem Solving Quiz Template", "QUIZ", "DISTRICT", periods[2].id if len(periods) > 2 else None),
    )
    for name, artifact_type, visibility, period_id in samples:
        existing = db.scalars(
            select(TeacherAssistWeekTemplate).where(
                TeacherAssistWeekTemplate.tenant_id == tenant_id,
                TeacherAssistWeekTemplate.name == name,
            )
        ).one_or_none()
        if existing is not None or period_id is None:
            continue
        context = WeekContextService.build(db, tenant_id=tenant_id, user=user, period_id=period_id)
        payload = WeekContextService.serialize(context)
        now = datetime.now(UTC)
        db.add(
            TeacherAssistWeekTemplate(
                tenant_id=tenant_id,
                created_by_user_id=user.id,
                name=name,
                description=f"Sample reusable {artifact_type.lower()} template for Texas demo data.",
                subject="Math",
                grade_level="5",
                artifact_type=artifact_type,
                template_type="GRADE_TEAM" if visibility == "TEAM" else visibility,
                visibility=visibility,
                school_year_id=guide.school_year_id,
                source_period_id=period_id,
                template_data=payload,
                created_at=now,
                updated_at=now,
            )
        )
        created += 1
    db.flush()
    return created


def _ensure_reuse_events(db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID, period_id: uuid.UUID | None) -> int:
    existing = db.scalar(
        select(TeacherAssistReuseEvent.id).where(
            TeacherAssistReuseEvent.tenant_id == tenant_id,
            TeacherAssistReuseEvent.user_id == user_id,
            TeacherAssistReuseEvent.event_type == "duplicate_week",
        ).limit(1)
    )
    if existing is not None or period_id is None:
        return 0
    now = datetime.now(UTC)
    db.add(
        TeacherAssistReuseEvent(
            tenant_id=tenant_id,
            user_id=user_id,
            event_type="duplicate_week",
            artifact_type="WEEK",
            source_entity_type="pacing_guide_period",
            source_entity_id=period_id,
            target_entity_id=period_id,
            estimated_minutes_saved=TIME_SAVINGS_MINUTES["WEEK"],
            metadata_json={"seed": True},
            created_at=now,
        )
    )
    db.add(
        TeacherAssistReuseEvent(
            tenant_id=tenant_id,
            user_id=user_id,
            event_type="apply_template",
            artifact_type="WEEK",
            source_entity_type="week_template",
            source_entity_id=uuid.uuid4(),
            target_entity_id=period_id,
            estimated_minutes_saved=TIME_SAVINGS_MINUTES["TEMPLATE"],
            metadata_json={"seed": True},
            created_at=now - timedelta(days=2),
        )
    )
    db.flush()
    return 2


def seed_time_savings(db: Session) -> dict[str, int]:
    counts = {"school_years": 0, "planning_groups": 0, "shared_guides": 0, "templates": 0, "reuse_events": 0}
    seed_pacing_guides(db)

    actor, tenant_id = _seed_actor(db)
    state_id = district_id = school_id = grade_id = subject_id = None
    school = db.scalars(select(EducationSchool).where(EducationSchool.name == "Mason Elementary")).first()
    if school is not None:
        school_id = school.id
        district_id = school.district_id
        grade = db.scalars(
            select(EducationGrade).where(EducationGrade.school_id == school.id, EducationGrade.grade_code == "5")
        ).first()
        if grade is not None:
            grade_id = grade.id
            state_id = grade.state_id
            subject = db.scalars(
                select(EducationSubject).where(
                    EducationSubject.grade_id == grade.id,
                    EducationSubject.subject_code == "Math",
                )
            ).first()
            if subject is not None:
                subject_id = subject.id

    for index, (title, start_date, end_date) in enumerate(PRIOR_SCHOOL_YEARS):
        before = db.scalars(
            select(TeacherAssistSchoolYear).where(
                TeacherAssistSchoolYear.tenant_id == tenant_id,
                TeacherAssistSchoolYear.title == title,
            )
        ).one_or_none()
        _ensure_school_year(
            db,
            tenant_id=tenant_id,
            title=title,
            start_date=start_date,
            end_date=end_date,
            is_active=index == len(PRIOR_SCHOOL_YEARS) - 1,
        )
        if before is None:
            counts["school_years"] += 1

    group_before = db.scalars(
        select(TeacherAssistPlanningGroup).where(
            TeacherAssistPlanningGroup.tenant_id == tenant_id,
            TeacherAssistPlanningGroup.name == "5th Grade Math Team",
        )
    ).one_or_none()
    group = _ensure_planning_group(db, tenant_id=tenant_id, user_id=actor.id)
    if group_before is None:
        counts["planning_groups"] += 1

    if all(value is not None for value in (state_id, district_id, school_id, grade_id, subject_id)):
        school_year = db.scalars(
            select(TeacherAssistSchoolYear).where(
                TeacherAssistSchoolYear.tenant_id == tenant_id,
                TeacherAssistSchoolYear.title == "2026-2027",
            )
        ).one()
        guide_before = db.scalars(
            select(TeacherAssistPacingGuide).where(
                TeacherAssistPacingGuide.tenant_id == tenant_id,
                TeacherAssistPacingGuide.title == "Grade 5 Math Team Shared Pacing Guide",
            )
        ).one_or_none()
        guide = _ensure_shared_math_guide(
            db,
            tenant_id=tenant_id,
            user_id=actor.id,
            school_year=school_year,
            group=group,
            grade_id=grade_id,
            subject_id=subject_id,
            state_id=state_id,
            district_id=district_id,
            school_id=school_id,
        )
        if guide_before is None:
            counts["shared_guides"] += 1
        counts["templates"] += _ensure_sample_templates(db, tenant_id=tenant_id, user=actor, guide=guide)
        first_period = db.scalars(
            select(TeacherAssistPacingGuidePeriod)
            .where(
                TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id,
                TeacherAssistPacingGuidePeriod.period_type == "WEEK",
            )
            .order_by(TeacherAssistPacingGuidePeriod.sequence_number)
        ).first()
        counts["reuse_events"] += _ensure_reuse_events(
            db,
            tenant_id=tenant_id,
            user_id=actor.id,
            period_id=first_period.id if first_period else None,
        )

    return counts


def main() -> None:
    from oziebot_api.config import get_settings
    from oziebot_api.db.session import make_session_factory

    settings = get_settings()
    if not settings.database_url:
        raise SystemExit("DATABASE_URL is required")
    factory = make_session_factory(settings)
    if factory is None:
        raise SystemExit("Could not create session factory")

    session = factory()
    try:
        counts = seed_time_savings(session)
        session.commit()
        print("Time savings seed complete:", counts)
    finally:
        session.close()


if __name__ == "__main__":
    main()

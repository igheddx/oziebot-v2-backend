"""Seed catalog-aligned district pacing guides for Texas demo data."""

from __future__ import annotations

from datetime import date, timedelta
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationCurriculumResource,
    EducationDistrict,
    EducationGrade,
    EducationObjective,
    EducationSchool,
    EducationState,
    EducationSubject,
)
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.user import User
from oziebot_api.scripts.seed_education_catalog import _user_by_email, seed_education_catalog
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    add_pacing_guide_objective,
    add_pacing_guide_resource,
    create_catalog_pacing_guide,
    create_pacing_guide_period,
)
from oziebot_api.services.teacher_assist.setup import create_grading_period, create_school_year


def _build_week_schedule(reference: date | None = None) -> list[tuple[date, date]]:
    anchor = reference or date.today()
    monday = anchor - timedelta(days=anchor.weekday())
    return [(monday + timedelta(weeks=index), monday + timedelta(weeks=index, days=4)) for index in range(4)]


def _schedule_reference_date(school_year: TeacherAssistSchoolYear) -> date:
    today = date.today()
    if today < school_year.start_date:
        return school_year.start_date
    if today > school_year.end_date:
        return school_year.end_date
    return today


SCHOOL_YEAR_TITLE = "2026-2027"
SCHOOL_YEAR_START = date(2026, 8, 10)
SCHOOL_YEAR_END = date(2027, 5, 28)

SUBJECT_GUIDES: dict[str, dict[str, object]] = {
    "ELA": {
        "title": "Grade 5 ELA District Pacing Guide",
        "description": "Leander ISD Grade 5 ELA pacing for the opening four weeks of instruction.",
        "weeks": [
            ("Week 1", "Main idea launch", ["5.ELA.1"], "Establish annotation routines for informational text."),
            ("Week 2", "Summarizing informational texts", ["5.ELA.2"], "Model summary frames and partner rehearsal."),
            ("Week 3", "Main idea spiral review", ["5.ELA.1"], "Use short passages to reinforce supporting details."),
            ("Week 4", "Summary synthesis", ["5.ELA.1", "5.ELA.2"], "Combine main idea and summary skills in one response."),
        ],
    },
    "Math": {
        "title": "Grade 5 Math District Pacing Guide",
        "description": "Leander ISD Grade 5 Math pacing for decimal operations and problem solving.",
        "weeks": [
            ("Week 1", "Decimal place value review", ["5.MATH.1"], "Refresh place value before computation routines."),
            ("Week 2", "Decimal operations", ["5.MATH.1"], "Practice add/subtract/multiply decimals in context."),
            ("Week 3", "Multi-step problem solving", ["5.MATH.2"], "Introduce strip diagrams and estimation checks."),
            ("Week 4", "Mixed review and application", ["5.MATH.1", "5.MATH.2"], "Use station work to blend operations and reasoning."),
        ],
    },
    "Science": {
        "title": "Grade 5 Science District Pacing Guide",
        "description": "Leander ISD Grade 5 Science pacing for matter and force investigations.",
        "weeks": [
            ("Week 1", "Classifying matter", ["5.SCI.1"], "Sort materials by observable physical properties."),
            ("Week 2", "Properties in context", ["5.SCI.1"], "Connect classroom labs to vocabulary notebooks."),
            ("Week 3", "Force and motion intro", ["5.SCI.2"], "Use push/pull stations to build investigation language."),
            ("Week 4", "Investigation notebooking", ["5.SCI.1", "5.SCI.2"], "Compare matter observations with motion data."),
        ],
    },
    "Social Studies": {
        "title": "Grade 5 Social Studies District Pacing Guide",
        "description": "Leander ISD Grade 5 Social Studies pacing for historical analysis and civics.",
        "weeks": [
            ("Week 1", "Historical cause and effect", ["5.SS.1"], "Introduce primary-source analysis with guided prompts."),
            ("Week 2", "Major event timelines", ["5.SS.1"], "Build collaborative timelines from short text sets."),
            ("Week 3", "Civic responsibilities", ["5.SS.2"], "Discuss classroom and community citizenship examples."),
            ("Week 4", "Connecting history and civics", ["5.SS.1", "5.SS.2"], "Use a short inquiry task to link past and present."),
        ],
    },
}


def _seed_actor(db: Session) -> tuple[User, uuid.UUID]:
    for email in ("Dominic@oziebot.com", "Aweleu@yahoo.com", "Dvaten.1992@gmail.com"):
        user = _user_by_email(db, email)
        if user is None:
            continue
        membership = db.scalars(
            select(TenantMembership).where(TenantMembership.user_id == user.id).order_by(TenantMembership.created_at)
        ).first()
        if membership is None:
            continue
        return user, membership.tenant_id
    membership = db.scalars(select(TenantMembership).order_by(TenantMembership.created_at)).first()
    if membership is None:
        raise RuntimeError("No tenant membership found for pacing guide seed")
    user = db.get(User, membership.user_id)
    if user is None:
        raise RuntimeError("No user found for pacing guide seed")
    return user, membership.tenant_id


def _ensure_school_year(db: Session, *, tenant_id: uuid.UUID) -> TeacherAssistSchoolYear:
    existing = db.scalars(
        select(TeacherAssistSchoolYear).where(
            TeacherAssistSchoolYear.tenant_id == tenant_id,
            TeacherAssistSchoolYear.title == SCHOOL_YEAR_TITLE,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    return create_school_year(
        db,
        tenant_id=tenant_id,
        title=SCHOOL_YEAR_TITLE,
        start_date=SCHOOL_YEAR_START,
        end_date=SCHOOL_YEAR_END,
        is_active=True,
    )


def _ensure_grading_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year: TeacherAssistSchoolYear,
) -> TeacherAssistGradingPeriod:
    existing = db.scalars(
        select(TeacherAssistGradingPeriod).where(
            TeacherAssistGradingPeriod.school_year_id == school_year.id,
            TeacherAssistGradingPeriod.title == "Fall Semester",
        )
    ).one_or_none()
    if existing is not None:
        return existing
    week_schedule = _build_week_schedule(_schedule_reference_date(school_year))
    return create_grading_period(
        db,
        tenant_id=tenant_id,
        school_year_id=school_year.id,
        title="Fall Semester",
        grading_period_type="semester",
        start_date=week_schedule[0][0],
        end_date=week_schedule[-1][1],
        sort_order=1,
    )


def seed_pacing_guides(db: Session) -> dict[str, int]:
    counts = {"guides": 0, "periods": 0, "objectives": 0, "resources": 0}
    existing_state = db.scalars(select(EducationState).where(EducationState.abbreviation == "TX")).one_or_none()
    if existing_state is None:
        seed_education_catalog(db)

    state = db.scalars(select(EducationState).where(EducationState.abbreviation == "TX")).one()
    district = db.scalars(
        select(EducationDistrict).where(
            EducationDistrict.state_id == state.id,
            EducationDistrict.name == "Leander Independent School District",
        )
    ).one()
    school = db.scalars(
        select(EducationSchool).where(
            EducationSchool.district_id == district.id,
            EducationSchool.name == "Mason Elementary",
        )
    ).one()
    grade = db.scalars(
        select(EducationGrade).where(
            EducationGrade.school_id == school.id,
            EducationGrade.grade_code == "5",
        )
    ).one()

    actor, tenant_id = _seed_actor(db)
    school_year = _ensure_school_year(db, tenant_id=tenant_id)
    _ensure_grading_period(db, tenant_id=tenant_id, school_year=school_year)
    week_schedule = _build_week_schedule(_schedule_reference_date(school_year))

    subjects = {
        row.subject_code: row
        for row in db.scalars(
            select(EducationSubject).where(EducationSubject.grade_id == grade.id)
        ).all()
    }
    objectives = {
        row.objective_id: row
        for row in db.scalars(
            select(EducationObjective).where(
                EducationObjective.state_id == state.id,
                EducationObjective.grade_level == "5",
            )
        ).all()
    }
    resources_by_subject: dict[str, list[EducationCurriculumResource]] = {}
    for subject_code in SUBJECT_GUIDES:
        resources_by_subject[subject_code] = list(
            db.scalars(
                select(EducationCurriculumResource).where(
                    EducationCurriculumResource.school_id == school.id,
                    EducationCurriculumResource.grade_level == "5",
                    EducationCurriculumResource.subject_code == subject_code,
                )
            ).all()
        )

    for subject_code, guide_def in SUBJECT_GUIDES.items():
        subject = subjects.get(subject_code)
        if subject is None:
            continue
        existing = db.scalars(
            select(TeacherAssistPacingGuide).where(
                TeacherAssistPacingGuide.tenant_id == tenant_id,
                TeacherAssistPacingGuide.guide_type == "DISTRICT",
                TeacherAssistPacingGuide.catalog_subject_id == subject.id,
                TeacherAssistPacingGuide.school_year_id == school_year.id,
            )
        ).one_or_none()
        if existing is not None:
            continue

        guide = create_catalog_pacing_guide(
            db,
            tenant_id=tenant_id,
            actor=actor,
            school_year_id=school_year.id,
            guide_type="DISTRICT",
            title=str(guide_def["title"]),
            description=str(guide_def["description"]),
            catalog_state_id=state.id,
            catalog_district_id=district.id,
            catalog_school_id=school.id,
            catalog_grade_id=grade.id,
            catalog_subject_id=subject.id,
            is_template=False,
            is_shared=True,
        )
        counts["guides"] += 1

        create_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=guide.id,
            period_type="GRADING_PERIOD",
            title="Fall Semester",
            description="Opening semester pacing for Grade 5.",
            sequence_number=1,
            start_date=week_schedule[0][0],
            end_date=week_schedule[-1][1],
        )
        counts["periods"] += 1
        create_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=guide.id,
            period_type="UNIT",
            title="Unit 1: Launch Foundations",
            description="Establish routines and first instructional priorities.",
            sequence_number=2,
            start_date=week_schedule[0][0],
            end_date=week_schedule[1][1],
        )
        counts["periods"] += 1
        create_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=guide.id,
            period_type="UNIT",
            title="Unit 2: Apply and Extend",
            description="Extend skills with mixed review and application.",
            sequence_number=5,
            start_date=week_schedule[2][0],
            end_date=week_schedule[3][1],
        )
        counts["periods"] += 1

        weeks = guide_def["weeks"]
        for index, week_def in enumerate(weeks, start=1):
            week_title, focus, objective_codes, notes = week_def
            start_date, end_date = week_schedule[index - 1]
            period = create_pacing_guide_period(
                db,
                tenant_id=tenant_id,
                pacing_guide_id=guide.id,
                period_type="WEEK",
                title=f"{week_title}: {focus}",
                description=notes,
                sequence_number=index + 2 if index <= 2 else index + 3,
                start_date=start_date,
                end_date=end_date,
            )
            counts["periods"] += 1
            for objective_code in objective_codes:
                objective = objectives.get(objective_code)
                if objective is None:
                    continue
                add_pacing_guide_objective(
                    db,
                    tenant_id=tenant_id,
                    period_id=period.id,
                    objective_id=objective.id,
                    is_required=True,
                    notes=f"Focus objective for {week_title.lower()}.",
                )
                counts["objectives"] += 1
            linked_resources = resources_by_subject.get(subject_code, [])
            for resource_index, resource in enumerate(linked_resources):
                add_pacing_guide_resource(
                    db,
                    tenant_id=tenant_id,
                    period_id=period.id,
                    catalog_resource_id=resource.id,
                    resource_library_item_id=None,
                    is_primary=resource_index == 0,
                    notes=f"Reference for {week_title.lower()}.",
                )
                counts["resources"] += 1

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
        counts = seed_pacing_guides(session)
        session.commit()
        print("Pacing guide seed complete:", counts)
    finally:
        session.close()


if __name__ == "__main__":
    main()

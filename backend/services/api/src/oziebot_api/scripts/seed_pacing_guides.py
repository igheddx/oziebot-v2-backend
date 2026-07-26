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
from oziebot_api.models.teacher_assist_pacing_guide_objective import (
    TeacherAssistPacingGuideObjective,
)
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.user import User
from oziebot_api.scripts.seed_education_catalog import GOLDEN_PATH_ELA_OBJECTIVE_ID
from oziebot_api.scripts.seed_education_catalog import _user_by_email, seed_education_catalog
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    add_pacing_guide_objective,
    add_pacing_guide_resource,
    create_catalog_pacing_guide,
    create_pacing_guide_period,
)
from oziebot_api.services.teacher_assist.setup import create_grading_period, create_school_year

PACING_WEEK_COUNT = 9
GRADING_PERIOD_TITLE = "9 Weeks 1"

GOLDEN_PATH_ELA_WEEK1_FOCUS = "Inference and textual evidence"
GOLDEN_PATH_ELA_WEEK1_TITLE = f"Week 1: {GOLDEN_PATH_ELA_WEEK1_FOCUS}"
GOLDEN_PATH_ELA_WEEK1_DAILY_TOPICS = """Monday: Introduce inference and evidence.
Tuesday: Practice making inferences from short informational text.
Wednesday: Cite evidence to support an inference.
Thursday: Write short constructed responses using inference and evidence.
Friday: Assessment / exit ticket on inference and evidence."""
GOLDEN_PATH_ELA_WEEK1_DESCRIPTION = (
    "Focus on helping students infer meaning from informational text and cite textual evidence.\n\n"
    f"{GOLDEN_PATH_ELA_WEEK1_DAILY_TOPICS}"
)


def _build_week_schedule(
    reference: date | None = None, week_count: int = PACING_WEEK_COUNT
) -> list[tuple[date, date]]:
    anchor = reference or date.today()
    monday = anchor - timedelta(days=anchor.weekday())
    return [
        (monday + timedelta(weeks=index), monday + timedelta(weeks=index, days=4))
        for index in range(week_count)
    ]


def _schedule_reference_date(school_year: TeacherAssistSchoolYear) -> date:
    today = date.today()
    if today < school_year.start_date:
        return school_year.start_date
    if today > school_year.end_date:
        return school_year.end_date
    return today


def _cycle_objectives(*codes: str, count: int) -> list[list[str]]:
    if not codes:
        return [[] for _ in range(count)]
    rows: list[list[str]] = []
    for index in range(count):
        primary = codes[index % len(codes)]
        secondary = codes[(index + 1) % len(codes)] if len(codes) > 1 and index % 3 == 2 else None
        row = [primary]
        if secondary and secondary != primary:
            row.append(secondary)
        rows.append(row)
    return rows


def _subject_weeks(
    *,
    focuses: list[str],
    notes: list[str],
    objective_codes: tuple[str, ...],
) -> list[tuple[str, str, list[str], str]]:
    objectives_by_week = _cycle_objectives(*objective_codes, count=PACING_WEEK_COUNT)
    return [
        (f"Week {index + 1}", focus, objectives_by_week[index], note)
        for index, (focus, note) in enumerate(zip(focuses, notes, strict=True))
    ]


SCHOOL_YEAR_TITLE = "2026-2027"
SCHOOL_YEAR_START = date(2026, 8, 10)
SCHOOL_YEAR_END = date(2027, 5, 28)

SUBJECT_GUIDES: dict[str, dict[str, object]] = {
    "ELA": {
        "title": "Grade 5 ELA — 9-Week District Pacing Guide",
        "description": "Mason Elementary Grade 5 ELA nine-week pacing aligned to seeded TEKS objectives.",
        "weeks": _subject_weeks(
            objective_codes=(GOLDEN_PATH_ELA_OBJECTIVE_ID, "5.ELA.2"),
            focuses=[
                GOLDEN_PATH_ELA_WEEK1_FOCUS,
                "Summarizing informational texts",
                "Supporting details practice",
                "Summary frames",
                "Main idea spiral review",
                "Text structure comparison",
                "Summary synthesis",
                "Cross-text analysis",
                "Quarter checkpoint and reflection",
            ],
            notes=[
                GOLDEN_PATH_ELA_WEEK1_DESCRIPTION,
                "Model summary frames and partner rehearsal.",
                "Use short passages to reinforce supporting details.",
                "Practice concise summaries with evidence stems.",
                "Revisit main idea with mixed genres.",
                "Compare how structure supports meaning.",
                "Combine main idea and summary in one response.",
                "Connect two short texts around a shared theme.",
                "Use a brief performance task to review quarter skills.",
            ],
        ),
    },
    "Math": {
        "title": "Grade 5 Math — 9-Week District Pacing Guide",
        "description": "Mason Elementary Grade 5 Math nine-week pacing for decimals and multi-step problem solving.",
        "weeks": _subject_weeks(
            objective_codes=("5.MATH.1", "5.MATH.2"),
            focuses=[
                "Decimal place value review",
                "Decimal operations",
                "Estimation with decimals",
                "Multi-step problem solving intro",
                "Strip diagrams and models",
                "Mixed decimal operations",
                "Word problem strategies",
                "Application stations",
                "Nine-week assessment review",
            ],
            notes=[
                "Refresh place value before computation routines.",
                "Practice add, subtract, and multiply decimals in context.",
                "Use rounding and benchmarks to check reasonableness.",
                "Introduce multi-step routines with think-alouds.",
                "Model strip diagrams for unknown quantities.",
                "Blend operations in short daily practice sets.",
                "Focus on planning steps before calculating.",
                "Use collaborative stations for real-world tasks.",
                "Review both computation and reasoning objectives.",
            ],
        ),
    },
    "Science": {
        "title": "Grade 5 Science — 9-Week District Pacing Guide",
        "description": "Mason Elementary Grade 5 Science nine-week pacing for matter and force investigations.",
        "weeks": _subject_weeks(
            objective_codes=("5.SCI.1", "5.SCI.2"),
            focuses=[
                "Classifying matter",
                "Properties in context",
                "Matter vocabulary notebooks",
                "Force and motion intro",
                "Push and pull stations",
                "Investigation planning",
                "Data collection routines",
                "Matter and motion synthesis",
                "Lab notebook checkpoint",
            ],
            notes=[
                "Sort materials by observable physical properties.",
                "Connect classroom labs to vocabulary notebooks.",
                "Build academic language for property comparisons.",
                "Use push and pull stations to build investigation language.",
                "Compare how force changes motion in simple trials.",
                "Plan fair tests with student-generated questions.",
                "Record observations with labeled diagrams.",
                "Compare matter observations with motion data.",
                "Summarize quarter investigations in science notebooks.",
            ],
        ),
    },
    "Social Studies": {
        "title": "Grade 5 Social Studies — 9-Week District Pacing Guide",
        "description": "Mason Elementary Grade 5 Social Studies nine-week pacing for historical analysis and civics.",
        "weeks": _subject_weeks(
            objective_codes=("5.SS.1", "5.SS.2"),
            focuses=[
                "Historical cause and effect",
                "Primary source analysis",
                "Major event timelines",
                "Perspective and evidence",
                "Civic responsibilities",
                "Community citizenship",
                "Connecting history and civics",
                "Inquiry task launch",
                "Nine-week social studies checkpoint",
            ],
            notes=[
                "Introduce primary-source analysis with guided prompts.",
                "Practice sourcing and contextualizing short documents.",
                "Build collaborative timelines from text sets.",
                "Discuss how evidence supports historical claims.",
                "Discuss classroom and community citizenship examples.",
                "Connect local examples to civic responsibilities.",
                "Use a short inquiry task to link past and present.",
                "Draft and revise responses using rubric language.",
                "Review cause-and-effect and civic reasoning skills.",
            ],
        ),
    },
}


def _seed_actor(db: Session) -> tuple[User, uuid.UUID]:
    for email in ("Dvaten.1992@gmail.com", "Aweleu@yahoo.com", "Dominic@oziebot.com"):
        user = _user_by_email(db, email)
        if user is None:
            continue
        membership = db.scalars(
            select(TenantMembership)
            .where(TenantMembership.user_id == user.id)
            .order_by(TenantMembership.created_at)
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


OPERATIONAL_SEED_EMAILS = (
    "Dvaten.1992@gmail.com",
    "Aweleu@yahoo.com",
    "Dominic@oziebot.com",
)


def _operational_seed_tenants(db: Session) -> list[tuple[User, uuid.UUID]]:
    tenants: dict[uuid.UUID, User] = {}
    for email in OPERATIONAL_SEED_EMAILS:
        user = _user_by_email(db, email)
        if user is None:
            continue
        membership = db.scalars(
            select(TenantMembership)
            .where(TenantMembership.user_id == user.id)
            .order_by(TenantMembership.created_at)
        ).first()
        if membership is None:
            continue
        tenants[membership.tenant_id] = user
    if tenants:
        return [(user, tenant_id) for tenant_id, user in tenants.items()]
    actor, tenant_id = _seed_actor(db)
    return [(actor, tenant_id)]


def _ensure_school_year(db: Session, *, tenant_id: uuid.UUID) -> TeacherAssistSchoolYear:
    existing = db.scalars(
        select(TeacherAssistSchoolYear).where(
            TeacherAssistSchoolYear.tenant_id == tenant_id,
            TeacherAssistSchoolYear.title == SCHOOL_YEAR_TITLE,
        )
    ).first()
    if existing is not None:
        if not existing.is_template or existing.is_active:
            existing.is_template = True
            existing.is_active = False
            db.flush()
        return existing
    return create_school_year(
        db,
        tenant_id=tenant_id,
        title=SCHOOL_YEAR_TITLE,
        start_date=SCHOOL_YEAR_START,
        end_date=SCHOOL_YEAR_END,
        is_active=False,
        is_template=True,
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
            TeacherAssistGradingPeriod.title == GRADING_PERIOD_TITLE,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    week_schedule = _build_week_schedule(_schedule_reference_date(school_year))
    return create_grading_period(
        db,
        tenant_id=tenant_id,
        school_year_id=school_year.id,
        title=GRADING_PERIOD_TITLE,
        grading_period_type="nine_weeks",
        start_date=week_schedule[0][0],
        end_date=week_schedule[-1][1],
        sort_order=1,
    )


def seed_pacing_guides(db: Session) -> dict[str, int]:
    totals = {"guides": 0, "periods": 0, "objectives": 0, "resources": 0, "tenants": 0}
    existing_state = db.scalars(
        select(EducationState).where(EducationState.abbreviation == "TX")
    ).one_or_none()
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

    for actor, tenant_id in _operational_seed_tenants(db):
        counts = _seed_pacing_guides_for_tenant(
            db,
            actor=actor,
            tenant_id=tenant_id,
            state=state,
            district=district,
            school=school,
            grade=grade,
            subjects=subjects,
            objectives=objectives,
            resources_by_subject=resources_by_subject,
        )
        for key in ("guides", "periods", "objectives", "resources"):
            totals[key] += counts[key]
        align_counts = _align_golden_path_ela_week1(
            db,
            tenant_id=tenant_id,
            actor=actor,
            subjects=subjects,
            objectives=objectives,
        )
        for key in ("guides", "periods", "objectives"):
            totals[key] += align_counts[key]
        totals["tenants"] += 1
    return totals


def _seed_pacing_guides_for_tenant(
    db: Session,
    *,
    actor: User,
    tenant_id: uuid.UUID,
    state: EducationState,
    district: EducationDistrict,
    school: EducationSchool,
    grade: EducationGrade,
    subjects: dict[str, EducationSubject],
    objectives: dict[str, EducationObjective],
    resources_by_subject: dict[str, list[EducationCurriculumResource]],
) -> dict[str, int]:
    counts = {"guides": 0, "periods": 0, "objectives": 0, "resources": 0}
    school_year = _ensure_school_year(db, tenant_id=tenant_id)
    week_schedule = _build_week_schedule(_schedule_reference_date(school_year))

    for subject_code, guide_def in SUBJECT_GUIDES.items():
        subject = subjects.get(subject_code)
        if subject is None:
            continue
        guide_title = str(guide_def["title"])
        existing = db.scalars(
            select(TeacherAssistPacingGuide).where(
                TeacherAssistPacingGuide.tenant_id == tenant_id,
                TeacherAssistPacingGuide.guide_type == "DISTRICT",
                TeacherAssistPacingGuide.catalog_subject_id == subject.id,
                TeacherAssistPacingGuide.school_year_id == school_year.id,
                TeacherAssistPacingGuide.title == guide_title,
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
            title=guide_title,
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
            title=GRADING_PERIOD_TITLE,
            description="Nine-week pacing block for Mason Elementary Grade 5.",
            sequence_number=1,
            start_date=week_schedule[0][0],
            end_date=week_schedule[-1][1],
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
                sequence_number=index + 1,
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


def _week_periods_for_guide(
    db: Session, *, pacing_guide_id: uuid.UUID
) -> list[TeacherAssistPacingGuidePeriod]:
    return list(
        db.scalars(
            select(TeacherAssistPacingGuidePeriod)
            .where(
                TeacherAssistPacingGuidePeriod.pacing_guide_id == pacing_guide_id,
                TeacherAssistPacingGuidePeriod.period_type == "WEEK",
            )
            .order_by(TeacherAssistPacingGuidePeriod.sequence_number.asc())
        ).all()
    )


def _align_golden_path_ela_week1(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor: User,
    subjects: dict[str, EducationSubject],
    objectives: dict[str, EducationObjective],
) -> dict[str, int]:
    counts = {"guides": 0, "periods": 0, "objectives": 0}
    subject = subjects.get("ELA")
    objective = objectives.get(GOLDEN_PATH_ELA_OBJECTIVE_ID)
    if subject is None or objective is None:
        return counts

    guide_title = str(SUBJECT_GUIDES["ELA"]["title"])
    guide = db.scalars(
        select(TeacherAssistPacingGuide).where(
            TeacherAssistPacingGuide.tenant_id == tenant_id,
            TeacherAssistPacingGuide.guide_type == "DISTRICT",
            TeacherAssistPacingGuide.catalog_subject_id == subject.id,
            TeacherAssistPacingGuide.title == guide_title,
        )
    ).one_or_none()
    if guide is None:
        return counts

    week_periods = _week_periods_for_guide(db, pacing_guide_id=guide.id)
    if not week_periods:
        return counts

    week1 = week_periods[0]
    week1.title = GOLDEN_PATH_ELA_WEEK1_TITLE
    week1.description = GOLDEN_PATH_ELA_WEEK1_DESCRIPTION
    counts["periods"] += 1

    for mapping in db.scalars(
        select(TeacherAssistPacingGuideObjective).where(
            TeacherAssistPacingGuideObjective.period_id == week1.id
        )
    ).all():
        db.delete(mapping)
    db.flush()

    add_pacing_guide_objective(
        db,
        tenant_id=tenant_id,
        period_id=week1.id,
        objective_id=objective.id,
        is_required=True,
        notes="Golden path Week 1 ELA objective.",
    )
    counts["objectives"] += 1
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

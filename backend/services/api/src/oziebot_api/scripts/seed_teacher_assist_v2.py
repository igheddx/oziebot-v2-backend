"""Idempotent seed for TeacherAssist v2 academic hierarchy and root admin access."""

from __future__ import annotations


from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationDistrict,
    EducationGrade,
    EducationObjective,
    EducationSchool,
    EducationState,
)
from oziebot_api.scripts.seed_v2_instructional_foundation import seed_v2_instructional_foundation
from oziebot_api.scripts.seed_education_catalog import seed_education_catalog
from oziebot_api.services.product_access import (
    TEACHER_ASSIST_PRODUCT_KEY,
    TRADING_PRODUCT_KEY,
    grant_tenant_product_access,
    set_user_default_product,
)
from oziebot_api.services.teacher_assist.access_seed import (
    ensure_existing_user_teacher_assist_access,
    _get_user_by_email,
    _primary_membership,
)
from oziebot_api.scripts.seed_education_catalog import (
    GOLDEN_PATH_ELA_OBJECTIVE_DESCRIPTION,
    GOLDEN_PATH_ELA_OBJECTIVE_ID,
)
from oziebot_api.services.teacher_assist.education_catalog import (
    create_grade,
    create_objective,
    create_school,
    create_subject,
)
from oziebot_api.services.teacher_assist_v2.roles import ensure_v2_root_admin_role


GRADE_5_OBJECTIVES = [
    ("5", "Math", "5.MATH.1", "Students perform operations with decimals."),
    ("5", "Math", "5.MATH.2", "Students solve multi-step mathematical problems."),
    ("5", "Math", "5.MATH.3", "Students represent and interpret data using graphs and tables."),
    ("5", "Science", "5.SCI.1", "Students classify matter by physical properties."),
    ("5", "Science", "5.SCI.2", "Students investigate force and motion."),
    ("5", "Science", "5.SCI.3", "Students describe interactions in ecosystems."),
    ("5", "Social Studies", "5.SS.1", "Students examine causes of major historical events."),
    ("5", "Social Studies", "5.SS.2", "Students analyze civic responsibilities."),
    (
        "5",
        "Social Studies",
        "5.SS.3",
        "Students explain basic economic principles and free enterprise.",
    ),
    ("5", "ELA", GOLDEN_PATH_ELA_OBJECTIVE_ID, GOLDEN_PATH_ELA_OBJECTIVE_DESCRIPTION),
    (
        "5",
        "ELA",
        "5.ELA.1",
        "Students identify main idea and supporting details in informational texts.",
    ),
    ("5", "ELA", "5.ELA.2", "Students summarize literary and informational texts."),
    ("5", "ELA", "5.ELA.3", "Students plan, draft, revise, and edit written compositions."),
]

SCHOOL_TEMPLATES = [
    ("Example Middle School", "Middle School", [("6", "6"), ("7", "7"), ("8", "8")]),
    ("Example High School", "High School", [("9", "9"), ("10", "10"), ("11", "11"), ("12", "12")]),
]


def _ensure_objectives(db: Session, *, state_id, counts: dict) -> None:
    for grade_level, subject_code, objective_id, description in GRADE_5_OBJECTIVES:
        existing = db.scalars(
            select(EducationObjective).where(
                EducationObjective.state_id == state_id,
                EducationObjective.objective_id == objective_id,
            )
        ).one_or_none()
        if existing is None:
            create_objective(
                db,
                state_id=state_id,
                grade_level=grade_level,
                subject_code=subject_code,
                objective_type="TEKS",
                objective_id=objective_id,
                description=description,
                coverage_type="required",
            )
            counts["objectives"] = counts.get("objectives", 0) + 1
        elif objective_id == GOLDEN_PATH_ELA_OBJECTIVE_ID:
            existing.description = description
            existing.objective_type = "TEKS"
            existing.coverage_type = "required"
            existing.subject_code = subject_code
            existing.grade_level = grade_level


def _ensure_example_schools(db: Session, *, district_id, counts: dict) -> None:
    subject_defs = [
        ("Math", "Math"),
        ("Science", "Science"),
        ("Social Studies", "Social Studies"),
        ("ELA", "ELA"),
    ]
    for school_name, school_type, grade_codes in SCHOOL_TEMPLATES:
        school = db.scalars(
            select(EducationSchool).where(
                EducationSchool.district_id == district_id,
                EducationSchool.name == school_name,
            )
        ).one_or_none()
        if school is None:
            school = create_school(
                db,
                district_id=district_id,
                name=school_name,
                school_type=school_type,
            )
            counts["schools"] = counts.get("schools", 0) + 1
        for grade_code, display_name in grade_codes:
            grade = db.scalars(
                select(EducationGrade).where(
                    EducationGrade.school_id == school.id,
                    EducationGrade.grade_code == grade_code,
                )
            ).one_or_none()
            if grade is None:
                grade = create_grade(
                    db,
                    school_id=school.id,
                    grade_code=grade_code,
                    display_name=display_name,
                )
                counts["grades"] = counts.get("grades", 0) + 1
            for subject_code, subject_name in subject_defs:
                from oziebot_api.models.education_catalog import EducationSubject

                existing = db.scalars(
                    select(EducationSubject).where(
                        EducationSubject.grade_id == grade.id,
                        EducationSubject.subject_code == subject_code,
                    )
                ).one_or_none()
                if existing is None:
                    create_subject(
                        db,
                        grade_id=grade.id,
                        subject_code=subject_code,
                        display_name=subject_name,
                    )
                    counts["subjects"] = counts.get("subjects", 0) + 1


def seed_teacher_assist_v2(db: Session) -> dict:
    counts = seed_education_catalog(db)
    state = db.scalars(select(EducationState).where(EducationState.abbreviation == "TX")).one()
    district = db.scalars(
        select(EducationDistrict).where(
            EducationDistrict.state_id == state.id,
            EducationDistrict.name == "Leander Independent School District",
        )
    ).one()
    _ensure_objectives(db, state_id=state.id, counts=counts)
    _ensure_example_schools(db, district_id=district.id, counts=counts)
    counts.update(seed_v2_instructional_foundation(db))

    dominic_email = "dominic@oziebot.com"
    dominic = _get_user_by_email(db, dominic_email)
    if dominic is not None:
        ensure_existing_user_teacher_assist_access(db, email=dominic_email)
        ensure_v2_root_admin_role(db, email=dominic_email)
        membership = _primary_membership(db, user_id=dominic.id)
        if membership is not None:
            grant_tenant_product_access(
                db, tenant_id=membership.tenant_id, product_key=TRADING_PRODUCT_KEY, status="active"
            )
            grant_tenant_product_access(
                db,
                tenant_id=membership.tenant_id,
                product_key=TEACHER_ASSIST_PRODUCT_KEY,
                status="active",
            )
            set_user_default_product(db, user=dominic, product_key=TEACHER_ASSIST_PRODUCT_KEY)
        counts["v2_root_admin"] = 1

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
        counts = seed_teacher_assist_v2(session)
        session.commit()
        print("TeacherAssist v2 seed complete:", counts)
    finally:
        session.close()


if __name__ == "__main__":
    main()

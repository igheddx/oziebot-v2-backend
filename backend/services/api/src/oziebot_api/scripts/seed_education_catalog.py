"""Seed platform education catalog foundation data."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationCurriculumResource,
    EducationDistrict,
    EducationGrade,
    EducationObjective,
    EducationObjectiveResourceMapping,
    EducationResourceLink,
    EducationSchool,
    EducationState,
    EducationSubject,
    TeacherSchoolAssignment,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.education_catalog import (
    create_curriculum_resource,
    create_district,
    create_grade,
    create_objective,
    create_resource_link,
    create_school,
    create_state,
    create_subject,
    create_teacher_assignment,
)

GOLDEN_PATH_ELA_OBJECTIVE_ID = "5.6E"
GOLDEN_PATH_ELA_OBJECTIVE_DESCRIPTION = "Students will make inferences from informational text and support their conclusions with textual evidence."


def _user_by_email(db: Session, email: str) -> User | None:
    return db.scalars(
        select(User).where(func.lower(User.email) == email.strip().lower())
    ).one_or_none()


def seed_education_catalog(db: Session) -> dict[str, int]:
    counts = {
        "states": 0,
        "districts": 0,
        "schools": 0,
        "grades": 0,
        "subjects": 0,
        "objectives": 0,
        "resources": 0,
        "links": 0,
        "mappings": 0,
        "assignments": 0,
    }

    state = db.scalars(
        select(EducationState).where(EducationState.abbreviation == "TX")
    ).one_or_none()
    if state is None:
        state = create_state(db, name="Texas", abbreviation="TX")
        counts["states"] += 1

    district = db.scalars(
        select(EducationDistrict).where(
            EducationDistrict.state_id == state.id,
            EducationDistrict.name == "Leander Independent School District",
        )
    ).one_or_none()
    if district is None:
        district = create_district(
            db,
            state_id=state.id,
            name="Leander Independent School District",
            district_code="LISD",
        )
        counts["districts"] += 1
    elif district.district_code is None:
        district.district_code = "LISD"

    school = db.scalars(
        select(EducationSchool).where(
            EducationSchool.district_id == district.id,
            EducationSchool.name == "Mason Elementary",
        )
    ).one_or_none()
    if school is None:
        school = create_school(
            db,
            district_id=district.id,
            name="Mason Elementary",
            school_type="Elementary",
        )
        counts["schools"] += 1

    grade_codes = [
        ("K", "Kindergarten"),
        ("1", "1"),
        ("2", "2"),
        ("3", "3"),
        ("4", "4"),
        ("5", "5"),
    ]
    grade_rows: dict[str, EducationGrade] = {}
    for grade_code, display_name in grade_codes:
        existing = db.scalars(
            select(EducationGrade).where(
                EducationGrade.school_id == school.id,
                EducationGrade.grade_code == grade_code,
            )
        ).one_or_none()
        if existing is None:
            existing = create_grade(
                db,
                school_id=school.id,
                grade_code=grade_code,
                display_name=display_name,
            )
            counts["grades"] += 1
        grade_rows[grade_code] = existing

    subject_defs = [
        ("ELA", "ELA"),
        ("Math", "Math"),
        ("Science", "Science"),
        ("Social Studies", "Social Studies"),
    ]
    for grade_code, grade_row in grade_rows.items():
        for subject_code, display_name in subject_defs:
            existing = db.scalars(
                select(EducationSubject).where(
                    EducationSubject.grade_id == grade_row.id,
                    EducationSubject.subject_code == subject_code,
                )
            ).one_or_none()
            if existing is None:
                create_subject(
                    db,
                    grade_id=grade_row.id,
                    subject_code=subject_code,
                    display_name=display_name,
                )
                counts["subjects"] += 1

    objective_defs = [
        ("5", "ELA", GOLDEN_PATH_ELA_OBJECTIVE_ID, GOLDEN_PATH_ELA_OBJECTIVE_DESCRIPTION),
        ("5", "ELA", "5.ELA.1", "Students identify main idea and supporting details."),
        ("5", "ELA", "5.ELA.2", "Students summarize informational texts."),
        ("5", "Math", "5.MATH.1", "Students perform operations with decimals."),
        ("5", "Math", "5.MATH.2", "Students solve multi-step mathematical problems."),
        ("5", "Science", "5.SCI.1", "Students classify matter by physical properties."),
        ("5", "Science", "5.SCI.2", "Students investigate force and motion."),
        ("5", "Social Studies", "5.SS.1", "Students examine causes of major historical events."),
        ("5", "Social Studies", "5.SS.2", "Students analyze civic responsibilities."),
    ]
    for grade_level, subject_code, objective_id, description in objective_defs:
        existing = db.scalars(
            select(EducationObjective).where(
                EducationObjective.state_id == state.id,
                EducationObjective.objective_id == objective_id,
            )
        ).one_or_none()
        if existing is None:
            create_objective(
                db,
                state_id=state.id,
                grade_level=grade_level,
                subject_code=subject_code,
                objective_type="TEKS",
                objective_id=objective_id,
                description=description,
                coverage_type="required",
            )
            counts["objectives"] += 1
        elif objective_id == GOLDEN_PATH_ELA_OBJECTIVE_ID:
            existing.description = description
            existing.objective_type = "TEKS"
            existing.coverage_type = "required"
            existing.subject_code = subject_code
            existing.grade_level = grade_level

    curriculum_titles = {
        "ELA": "5th Grade ELA Curriculum Guide",
        "Math": "5th Grade Math Curriculum Guide",
        "Science": "5th Grade Science Curriculum Guide",
        "Social Studies": "5th Grade Social Studies Curriculum Guide",
    }
    textbook_titles = {
        "ELA": "ELA Textbook",
        "Math": "Math Textbook",
        "Science": "Science Textbook",
        "Social Studies": "Social Studies Textbook",
    }
    curriculum_resources: dict[str, EducationCurriculumResource] = {}
    for subject_code, title in curriculum_titles.items():
        existing = db.scalars(
            select(EducationCurriculumResource).where(
                EducationCurriculumResource.school_id == school.id,
                EducationCurriculumResource.grade_level == "5",
                EducationCurriculumResource.subject_code == subject_code,
                EducationCurriculumResource.resource_type == "curriculum",
            )
        ).one_or_none()
        if existing is None:
            existing = create_curriculum_resource(
                db,
                state_id=state.id,
                district_id=district.id,
                school_id=school.id,
                grade_level="5",
                subject_code=subject_code,
                resource_type="curriculum",
                title=title,
                description=f"Placeholder curriculum guide for {subject_code}.",
                storage_key=f"education-catalog/placeholders/{subject_code.lower().replace(' ', '-')}-curriculum.pdf",
            )
            counts["resources"] += 1
        curriculum_resources[subject_code] = existing

    textbook_resources: dict[str, EducationCurriculumResource] = {}
    for subject_code, title in textbook_titles.items():
        existing = db.scalars(
            select(EducationCurriculumResource).where(
                EducationCurriculumResource.school_id == school.id,
                EducationCurriculumResource.grade_level == "5",
                EducationCurriculumResource.subject_code == subject_code,
                EducationCurriculumResource.resource_type == "textbook",
            )
        ).one_or_none()
        if existing is None:
            existing = create_curriculum_resource(
                db,
                state_id=state.id,
                district_id=district.id,
                school_id=school.id,
                grade_level="5",
                subject_code=subject_code,
                resource_type="textbook",
                title=title,
                description=f"Placeholder textbook record for {subject_code}.",
                storage_key=f"education-catalog/placeholders/{subject_code.lower().replace(' ', '-')}-textbook.pdf",
            )
            counts["resources"] += 1
        textbook_resources[subject_code] = existing

    for subject_code, resource in curriculum_resources.items():
        existing_link = db.scalars(
            select(EducationResourceLink).where(
                EducationResourceLink.curriculum_resource_id == resource.id,
                EducationResourceLink.url == "https://tea.texas.gov",
            )
        ).one_or_none()
        if existing_link is None:
            create_resource_link(
                db,
                curriculum_resource_id=resource.id,
                link_title=f"{subject_code} reference",
                url="https://tea.texas.gov",
            )
            counts["links"] += 1

    grade_5_objectives = db.scalars(
        select(EducationObjective).where(
            EducationObjective.state_id == state.id,
            EducationObjective.grade_level == "5",
            EducationObjective.active.is_(True),
        )
    ).all()
    now = datetime.now(UTC)
    for objective in grade_5_objectives:
        subject_code = objective.subject_code
        linked_resources = [
            curriculum_resources.get(subject_code),
            textbook_resources.get(subject_code),
        ]
        for resource in linked_resources:
            if resource is None:
                continue
            existing_mapping = db.scalars(
                select(EducationObjectiveResourceMapping).where(
                    EducationObjectiveResourceMapping.objective_id == objective.id,
                    EducationObjectiveResourceMapping.resource_id == resource.id,
                )
            ).one_or_none()
            if existing_mapping is None:
                db.add(
                    EducationObjectiveResourceMapping(
                        objective_id=objective.id,
                        resource_id=resource.id,
                        created_at=now,
                    )
                )
                counts["mappings"] += 1

    for email in ("Aweleu@yahoo.com", "Dvaten.1992@gmail.com"):
        user = _user_by_email(db, email)
        if user is None:
            continue
        existing_assignment = db.scalars(
            select(TeacherSchoolAssignment).where(
                TeacherSchoolAssignment.user_id == user.id,
                TeacherSchoolAssignment.school_id == school.id,
            )
        ).one_or_none()
        if existing_assignment is None:
            create_teacher_assignment(
                db,
                user_id=user.id,
                state_id=state.id,
                district_id=district.id,
                school_id=school.id,
            )
            counts["assignments"] += 1

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
        counts = seed_education_catalog(session)
        session.commit()
        print("Education catalog seed complete:", counts)
    finally:
        session.close()


if __name__ == "__main__":
    main()

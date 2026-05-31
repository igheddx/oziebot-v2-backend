from __future__ import annotations

from sqlalchemy import select

from oziebot_api.models.education_catalog import (
    EducationDistrict,
    EducationGrade,
    EducationObjective,
    EducationSchool,
    EducationSchoolYear,
    EducationState,
    EducationSubject,
)
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_objective import TeacherAssistPacingGuideObjective
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_supporting_material import (
    TeacherAssistPacingGuideSupportingMaterial,
)
from oziebot_api.scripts.seed_education_catalog import GOLDEN_PATH_ELA_OBJECTIVE_ID
from oziebot_api.scripts.seed_pacing_guides import (
    GOLDEN_PATH_ELA_WEEK1_DAILY_TOPICS,
    GOLDEN_PATH_ELA_WEEK1_TITLE,
    SUBJECT_GUIDES,
)
from oziebot_api.scripts.seed_teacher_assist_v2 import seed_teacher_assist_v2
from oziebot_api.scripts.seed_v2_pacing_supporting_materials import (
    GOLDEN_PATH_ELA_CURRICULUM_TITLE,
    GOLDEN_PATH_ELA_LINK_TITLE,
    GOLDEN_PATH_ELA_LINK_URL,
    GOLDEN_PATH_ELA_NOTE_TITLE,
)
from oziebot_api.services.teacher_assist.access_seed import _get_user_by_email, _primary_membership
from oziebot_api.services.teacher_assist_v2.supporting_materials import get_pacing_guide_planning_context
from tests.test_teacher_assist_v2_supporting_materials import _make_root_admin


def test_golden_path_ela_seed_alignment(db_session, client):
    _make_root_admin(db_session, client, "v2-golden-root@example.com")
    seed_teacher_assist_v2(db_session)
    db_session.commit()

    state = db_session.scalar(select(EducationState).where(EducationState.abbreviation == "TX"))
    district = db_session.scalar(
        select(EducationDistrict).where(
            EducationDistrict.state_id == state.id,
            EducationDistrict.name == "Leander Independent School District",
        )
    )
    school = db_session.scalar(
        select(EducationSchool).where(
            EducationSchool.district_id == district.id,
            EducationSchool.name == "Mason Elementary",
        )
    )
    grade = db_session.scalar(
        select(EducationGrade).where(
            EducationGrade.school_id == school.id,
            EducationGrade.grade_code == "5",
        )
    )
    ela = db_session.scalar(
        select(EducationSubject).where(
            EducationSubject.grade_id == grade.id,
            EducationSubject.subject_code == "ELA",
        )
    )
    school_year = db_session.scalar(
        select(EducationSchoolYear).where(EducationSchoolYear.title == "2026-2027")
    )
    objective = db_session.scalar(
        select(EducationObjective).where(
            EducationObjective.state_id == state.id,
            EducationObjective.objective_id == GOLDEN_PATH_ELA_OBJECTIVE_ID,
        )
    )

    assert state is not None
    assert district is not None
    assert school is not None
    assert grade is not None
    assert ela is not None
    assert school_year is not None
    assert objective is not None
    assert objective.objective_type == "TEKS"
    assert objective.coverage_type == "required"
    assert "inferences from informational text" in objective.description.lower()

    user = _get_user_by_email(db_session, "v2-golden-root@example.com")
    membership = _primary_membership(db_session, user_id=user.id) if user else None
    assert membership is not None

    guide = db_session.scalar(
        select(TeacherAssistPacingGuide).where(
            TeacherAssistPacingGuide.tenant_id == membership.tenant_id,
            TeacherAssistPacingGuide.title == SUBJECT_GUIDES["ELA"]["title"],
        )
    )
    assert guide is not None

    week1 = db_session.scalar(
        select(TeacherAssistPacingGuidePeriod).where(
            TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id,
            TeacherAssistPacingGuidePeriod.period_type == "WEEK",
        ).order_by(TeacherAssistPacingGuidePeriod.sequence_number.asc())
    )
    assert week1 is not None
    assert week1.title == GOLDEN_PATH_ELA_WEEK1_TITLE
    assert "Monday: Introduce inference and evidence." in (week1.description or "")
    assert "Friday: Assessment / exit ticket on inference and evidence." in (week1.description or "")

    mapped = db_session.scalars(
        select(TeacherAssistPacingGuideObjective).where(
            TeacherAssistPacingGuideObjective.period_id == week1.id
        )
    ).all()
    assert len(mapped) == 1
    assert mapped[0].objective_id == objective.id
    assert mapped[0].is_required is True

    materials = db_session.scalars(
        select(TeacherAssistPacingGuideSupportingMaterial).where(
            TeacherAssistPacingGuideSupportingMaterial.pacing_guide_id == guide.id,
            TeacherAssistPacingGuideSupportingMaterial.period_id == week1.id,
        )
    ).all()
    titles = {row.title for row in materials}
    assert GOLDEN_PATH_ELA_CURRICULUM_TITLE in titles
    assert GOLDEN_PATH_ELA_LINK_TITLE in titles
    assert GOLDEN_PATH_ELA_NOTE_TITLE in titles

    link = next(row for row in materials if row.title == GOLDEN_PATH_ELA_LINK_TITLE)
    assert link.external_url == GOLDEN_PATH_ELA_LINK_URL

    context = get_pacing_guide_planning_context(
        db_session,
        tenant_id=membership.tenant_id,
        pacing_guide_id=guide.id,
        period_id=week1.id,
        settings=None,
    )
    assert context["week_title"] == GOLDEN_PATH_ELA_WEEK1_TITLE
    assert len(context["objectives"]) == 1
    assert context["objectives"][0]["objective_code"] == GOLDEN_PATH_ELA_OBJECTIVE_ID
    objective_materials = (
        context["objectives"][0]["curriculum_files"]
        + context["objectives"][0]["reference_links"]
        + context["objectives"][0]["notes"]
    )
    material_titles = {item["title"] for item in objective_materials}
    assert GOLDEN_PATH_ELA_CURRICULUM_TITLE in material_titles
    assert GOLDEN_PATH_ELA_NOTE_TITLE in material_titles
    assert GOLDEN_PATH_ELA_WEEK1_DAILY_TOPICS.splitlines()[0] in (week1.description or "")

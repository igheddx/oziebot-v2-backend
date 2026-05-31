"""Seed district pacing guide supporting materials for Grade 5 LISD guides."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import get_settings
from oziebot_api.models.education_catalog import (
    EducationDistrict,
    EducationGrade,
    EducationObjective,
    EducationSchool,
    EducationState,
)
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_supporting_material import (
    TeacherAssistPacingGuideSupportingMaterial,
)
from oziebot_api.models.user import User
from oziebot_api.scripts.seed_education_catalog import GOLDEN_PATH_ELA_OBJECTIVE_ID
from oziebot_api.scripts.seed_pacing_guides import SUBJECT_GUIDES
from oziebot_api.services.teacher_assist.access_seed import _get_user_by_email, _primary_membership
from oziebot_api.services.teacher_assist.pacing_guide_foundation import get_catalog_pacing_guide_detail
from oziebot_api.services.teacher_assist_v2.supporting_materials import (
    create_seed_supporting_material_file,
    create_supporting_link,
    create_supporting_note,
    save_teacher_assist_bytes,
)
from oziebot_api.services.teacher_assist_v2.supporting_materials import _create_material_row

GOLDEN_PATH_ELA_CURRICULUM_TITLE = "Grade 5 ELA Inference Curriculum Guide"
GOLDEN_PATH_ELA_LINK_TITLE = "Inference and Text Evidence Reference"
GOLDEN_PATH_ELA_LINK_URL = "https://tea.texas.gov/"
GOLDEN_PATH_ELA_NOTE_TITLE = "Instructional Focus"
GOLDEN_PATH_ELA_NOTE_BODY = (
    "Focus on helping students infer meaning from informational text and cite at least two pieces "
    "of evidence to support their thinking."
)


def _week_one_period(db: Session, *, guide_id) -> TeacherAssistPacingGuidePeriod | None:
    return db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .where(
            TeacherAssistPacingGuidePeriod.pacing_guide_id == guide_id,
            TeacherAssistPacingGuidePeriod.period_type == "WEEK",
        )
        .order_by(TeacherAssistPacingGuidePeriod.sequence_number.asc())
    ).first()


def _upsert_material(
    db: Session,
    *,
    guide: TeacherAssistPacingGuide,
    period_id,
    education_objective_id,
    title: str,
    updater,
    creator,
) -> bool:
    existing = db.scalars(
        select(TeacherAssistPacingGuideSupportingMaterial).where(
            TeacherAssistPacingGuideSupportingMaterial.pacing_guide_id == guide.id,
            TeacherAssistPacingGuideSupportingMaterial.period_id == period_id,
            TeacherAssistPacingGuideSupportingMaterial.title == title,
        )
    ).one_or_none()
    if existing is not None:
        updater(existing)
        return False
    creator()
    return True


def _ensure_golden_path_ela_supporting_materials(
    db: Session,
    *,
    settings,
    tenant_id,
    actor: User,
    guide: TeacherAssistPacingGuide,
    period_id,
    objective_id,
) -> int:
    created = 0

    def _ensure_file() -> None:
        nonlocal created

        def update_file(row: TeacherAssistPacingGuideSupportingMaterial) -> None:
            row.resource_type = "curriculum_file"
            row.material_kind = "file"
            row.description = "District curriculum guide placeholder for Week 1 inference instruction."
            row.education_objective_id = objective_id

        added = _upsert_material(
            db,
            guide=guide,
            period_id=period_id,
            education_objective_id=objective_id,
            title=GOLDEN_PATH_ELA_CURRICULUM_TITLE,
            updater=update_file,
            creator=lambda: _create_material_row(
                db,
                guide=guide,
                actor=actor,
                period_id=period_id,
                education_objective_id=objective_id,
                material_kind="file",
                resource_type="curriculum_file",
                title=GOLDEN_PATH_ELA_CURRICULUM_TITLE,
                description="District curriculum guide placeholder for Week 1 inference instruction.",
                stored=save_teacher_assist_bytes(
                    settings,
                    tenant_id=tenant_id,
                    area="resources",
                    original_filename="grade-5-ela-inference-curriculum-guide.txt",
                    contents=b"Grade 5 ELA inference and evidence curriculum guide placeholder.",
                    mime_type="text/plain",
                ),
            ),
        )
        if added:
            created += 1

    def _ensure_link() -> None:
        nonlocal created

        def update_link(row: TeacherAssistPacingGuideSupportingMaterial) -> None:
            row.resource_type = "reference_link"
            row.material_kind = "link"
            row.external_url = GOLDEN_PATH_ELA_LINK_URL
            row.description = "District-approved TEKS reference for inference and evidence."
            row.education_objective_id = objective_id

        added = _upsert_material(
            db,
            guide=guide,
            period_id=period_id,
            education_objective_id=objective_id,
            title=GOLDEN_PATH_ELA_LINK_TITLE,
            updater=update_link,
            creator=lambda: create_supporting_link(
                db,
                tenant_id=tenant_id,
                actor=actor,
                pacing_guide_id=guide.id,
                title=GOLDEN_PATH_ELA_LINK_TITLE,
                external_url=GOLDEN_PATH_ELA_LINK_URL,
                resource_type="reference_link",
                description="District-approved TEKS reference for inference and evidence.",
                period_id=period_id,
                education_objective_id=objective_id,
            ),
        )
        if added:
            created += 1

    def _ensure_note() -> None:
        nonlocal created

        def update_note(row: TeacherAssistPacingGuideSupportingMaterial) -> None:
            row.resource_type = "note"
            row.material_kind = "note"
            row.note_body = GOLDEN_PATH_ELA_NOTE_BODY
            row.education_objective_id = objective_id

        added = _upsert_material(
            db,
            guide=guide,
            period_id=period_id,
            education_objective_id=objective_id,
            title=GOLDEN_PATH_ELA_NOTE_TITLE,
            updater=update_note,
            creator=lambda: create_supporting_note(
                db,
                tenant_id=tenant_id,
                actor=actor,
                pacing_guide_id=guide.id,
                title=GOLDEN_PATH_ELA_NOTE_TITLE,
                note_body=GOLDEN_PATH_ELA_NOTE_BODY,
                period_id=period_id,
                education_objective_id=objective_id,
            ),
        )
        if added:
            created += 1

    _ensure_file()
    _ensure_link()
    _ensure_note()
    return created


def seed_v2_pacing_supporting_materials(db: Session) -> dict[str, int]:
    counts = {"materials": 0, "materials_updated": 0}
    state = db.scalars(select(EducationState).where(EducationState.abbreviation == "TX")).one_or_none()
    if state is None:
        return counts
    district = db.scalars(
        select(EducationDistrict).where(
            EducationDistrict.state_id == state.id,
            EducationDistrict.name == "Leander Independent School District",
        )
    ).one_or_none()
    if district is None:
        return counts
    school = db.scalars(
        select(EducationSchool).where(
            EducationSchool.district_id == district.id,
            EducationSchool.name == "Mason Elementary",
        )
    ).one_or_none()
    if school is None:
        return counts
    grade = db.scalars(
        select(EducationGrade).where(
            EducationGrade.school_id == school.id,
            EducationGrade.grade_code == "5",
        )
    ).one_or_none()
    if grade is None:
        return counts

    actor = _get_user_by_email(db, "dominic@oziebot.com")
    if actor is None:
        actor = db.scalars(select(User).order_by(User.created_at.asc())).first()
    if actor is None:
        return counts
    membership = _primary_membership(db, user_id=actor.id)
    if membership is None:
        return counts

    settings = get_settings()
    tenant_id = membership.tenant_id
    golden_objective = db.scalars(
        select(EducationObjective).where(
            EducationObjective.state_id == state.id,
            EducationObjective.objective_id == GOLDEN_PATH_ELA_OBJECTIVE_ID,
        )
    ).one_or_none()

    for subject_code in SUBJECT_GUIDES:
        guide_title = str(SUBJECT_GUIDES[subject_code]["title"])
        guide = db.scalars(
            select(TeacherAssistPacingGuide).where(
                TeacherAssistPacingGuide.tenant_id == tenant_id,
                TeacherAssistPacingGuide.guide_type == "DISTRICT",
                TeacherAssistPacingGuide.catalog_grade_id == grade.id,
                TeacherAssistPacingGuide.title == guide_title,
            )
        ).one_or_none()
        if guide is None:
            continue

        if subject_code == "ELA" and golden_objective is not None:
            period = _week_one_period(db, guide_id=guide.id)
            if period is None:
                continue
            created = _ensure_golden_path_ela_supporting_materials(
                db,
                settings=settings,
                tenant_id=tenant_id,
                actor=actor,
                guide=guide,
                period_id=period.id,
                objective_id=golden_objective.id,
            )
            counts["materials"] += created
            counts["materials_updated"] += 3 - created
            continue

        guide = get_catalog_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide.id)
        period = _week_one_period(db, guide_id=guide.id)
        if period is None:
            continue
        existing = db.scalars(
            select(TeacherAssistPacingGuideSupportingMaterial).where(
                TeacherAssistPacingGuideSupportingMaterial.pacing_guide_id == guide.id,
                TeacherAssistPacingGuideSupportingMaterial.period_id == period.id,
            )
        ).first()
        if existing is not None:
            continue

        create_seed_supporting_material_file(
            db,
            settings=settings,
            tenant_id=tenant_id,
            actor=actor,
            guide=guide,
            period_id=period.id,
            subject_code=subject_code,
        )
        counts["materials"] += 1

        create_supporting_link(
            db,
            tenant_id=tenant_id,
            actor=actor,
            pacing_guide_id=guide.id,
            title=f"Grade 5 {subject_code} planning reference (sample)",
            external_url=f"https://example.com/district/grade-5/{subject_code.lower().replace(' ', '-')}",
            resource_type="reference_link",
            description="Sample district-approved reference link placeholder.",
            period_id=period.id,
            education_objective_id=None,
        )
        counts["materials"] += 1

        create_supporting_note(
            db,
            tenant_id=tenant_id,
            actor=actor,
            pacing_guide_id=guide.id,
            title=f"Week 1 {subject_code} district note",
            note_body=(
                f"Sample district instructional note for Grade 5 {subject_code} Week 1. "
                "Use this area for pacing reminders and approved instructional guidance."
            ),
            period_id=period.id,
            education_objective_id=None,
        )
        counts["materials"] += 1

    return counts

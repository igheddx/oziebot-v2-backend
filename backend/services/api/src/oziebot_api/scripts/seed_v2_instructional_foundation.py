"""Seed platform school years, linked objectives, and district pacing guides for v2."""

from __future__ import annotations

from datetime import date

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationDistrict,
    EducationGrade,
    EducationObjective,
    EducationSchool,
    EducationSchoolYear,
    EducationState,
    EducationSubject,
)
from oziebot_api.scripts.seed_pacing_guides import seed_pacing_guides
from oziebot_api.services.teacher_assist.access_seed import _get_user_by_email, _primary_membership
from oziebot_api.services.teacher_assist_v2.pacing_guides import ensure_tenant_school_year
from oziebot_api.services.teacher_assist_v2.school_years import create_platform_school_year
from oziebot_api.scripts.seed_v2_pacing_supporting_materials import seed_v2_pacing_supporting_materials


def _ensure_platform_school_year(db: Session, *, state_id, district_id, counts: dict) -> EducationSchoolYear:
    existing = db.scalars(
        select(EducationSchoolYear)
        .where(EducationSchoolYear.title == "2026-2027")
        .order_by(EducationSchoolYear.created_at.asc())
    ).first()
    if existing is not None:
        if not existing.active:
            existing.active = True
            for row in db.scalars(select(EducationSchoolYear)).all():
                if row.id != existing.id:
                    row.active = False
        return existing
    row = create_platform_school_year(
        db,
        state_id=state_id,
        district_id=district_id,
        school_id=None,
        title="2026-2027",
        start_date=date(2026, 8, 1),
        end_date=date(2027, 6, 30),
        active=True,
    )
    counts["platform_school_years"] = counts.get("platform_school_years", 0) + 1
    return row


def _backfill_objectives(
    db: Session,
    *,
    state_id,
    district_id,
    school_id,
    grade_id,
    school_year_id,
    counts: dict,
) -> None:
    subjects = {
        row.subject_code: row
        for row in db.scalars(select(EducationSubject).where(EducationSubject.grade_id == grade_id)).all()
    }
    objectives = db.scalars(
        select(EducationObjective).where(
            EducationObjective.state_id == state_id,
            EducationObjective.grade_level == "5",
        )
    ).all()
    for objective in objectives:
        subject = subjects.get(objective.subject_code)
        changed = False
        if objective.district_id is None:
            objective.district_id = district_id
            changed = True
        if objective.school_id is None:
            objective.school_id = school_id
            changed = True
        if objective.grade_id is None and grade_id is not None:
            objective.grade_id = grade_id
            changed = True
        if objective.subject_id is None and subject is not None:
            objective.subject_id = subject.id
            changed = True
        if objective.school_year_id is None:
            objective.school_year_id = school_year_id
            changed = True
        if changed:
            counts["objectives_linked"] = counts.get("objectives_linked", 0) + 1


def seed_v2_instructional_foundation(db: Session) -> dict[str, int]:
    counts: dict[str, int] = {}
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
    platform_year = _ensure_platform_school_year(db, state_id=state.id, district_id=district.id, counts=counts)
    _backfill_objectives(
        db,
        state_id=state.id,
        district_id=district.id,
        school_id=school.id,
        grade_id=grade.id,
        school_year_id=platform_year.id,
        counts=counts,
    )

    dominic = _get_user_by_email(db, "dominic@oziebot.com")
    if dominic is not None:
        membership = _primary_membership(db, user_id=dominic.id)
        if membership is not None:
            ensure_tenant_school_year(db, tenant_id=membership.tenant_id, platform_year=platform_year)

    pacing_counts = seed_pacing_guides(db)
    counts.update({f"pacing_{key}": value for key, value in pacing_counts.items()})
    supporting_counts = seed_v2_pacing_supporting_materials(db)
    counts.update({f"supporting_{key}": value for key, value in supporting_counts.items()})
    return counts

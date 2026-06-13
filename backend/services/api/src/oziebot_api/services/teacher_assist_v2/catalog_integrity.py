from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationDistrict,
    EducationGrade,
    EducationObjective,
    EducationSchool,
    EducationState,
    EducationSubject,
    TeacherSchoolAssignment,
)
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.user import User
from oziebot_api.config import get_settings
from oziebot_api.services.teacher_assist.ai_mode import get_teacher_assist_ai_mode_status
from oziebot_api.services.teacher_assist.education_catalog import (
    get_district_or_404,
    get_grade_or_404,
    get_school_or_404,
    get_state_or_404,
    get_subject_or_404,
    list_districts,
    list_grades,
    list_schools,
    list_states,
    list_subjects,
    update_district,
    update_grade,
    update_school,
    update_state,
    update_subject,
)
from oziebot_api.services.teacher_assist_v2.roles import resolve_teacher_assist_role


class CatalogArchiveError(ValueError):
    def __init__(self, message: str, *, dependencies: list[str]) -> None:
        super().__init__(message)
        self.dependencies = dependencies


def _active_count(db: Session, stmt) -> int:
    return db.scalar(stmt) or 0


def state_archive_dependencies(db: Session, *, state_id: uuid.UUID) -> list[str]:
    get_state_or_404(db, state_id)
    deps: list[str] = []
    if _active_count(
        db,
        select(func.count())
        .select_from(EducationDistrict)
        .where(EducationDistrict.state_id == state_id, EducationDistrict.active.is_(True)),
    ):
        deps.append("active districts")
    if _active_count(
        db,
        select(func.count())
        .select_from(EducationObjective)
        .where(EducationObjective.state_id == state_id, EducationObjective.active.is_(True)),
    ):
        deps.append("learning objectives")
    if _active_count(
        db,
        select(func.count())
        .select_from(TeacherSchoolAssignment)
        .where(TeacherSchoolAssignment.state_id == state_id, TeacherSchoolAssignment.active.is_(True)),
    ):
        deps.append("teacher assignments")
    return deps


def district_archive_dependencies(db: Session, *, district_id: uuid.UUID) -> list[str]:
    get_district_or_404(db, district_id)
    deps: list[str] = []
    if _active_count(
        db,
        select(func.count())
        .select_from(EducationSchool)
        .where(EducationSchool.district_id == district_id, EducationSchool.active.is_(True)),
    ):
        deps.append("active schools")
    if _active_count(
        db,
        select(func.count())
        .select_from(TeacherSchoolAssignment)
        .where(TeacherSchoolAssignment.district_id == district_id, TeacherSchoolAssignment.active.is_(True)),
    ):
        deps.append("teacher assignments")
    return deps


def school_archive_dependencies(db: Session, *, school_id: uuid.UUID) -> list[str]:
    get_school_or_404(db, school_id)
    deps: list[str] = []
    if _active_count(
        db,
        select(func.count())
        .select_from(EducationGrade)
        .where(EducationGrade.school_id == school_id, EducationGrade.active.is_(True)),
    ):
        deps.append("grade assignments")
    if _active_count(
        db,
        select(func.count())
        .select_from(TeacherSchoolAssignment)
        .where(TeacherSchoolAssignment.school_id == school_id, TeacherSchoolAssignment.active.is_(True)),
    ):
        deps.append("teacher assignments")
    return deps


def grade_archive_dependencies(db: Session, *, grade_id: uuid.UUID) -> list[str]:
    grade = get_grade_or_404(db, grade_id)
    deps: list[str] = []
    if _active_count(
        db,
        select(func.count())
        .select_from(EducationSubject)
        .where(EducationSubject.grade_id == grade_id, EducationSubject.active.is_(True)),
    ):
        deps.append("subjects")
    if grade.school_id is not None:
        school = get_school_or_404(db, grade.school_id)
        district = get_district_or_404(db, school.district_id)
        state = get_state_or_404(db, district.state_id)
        if _active_count(
            db,
            select(func.count())
            .select_from(EducationObjective)
            .where(
                EducationObjective.state_id == state.id,
                EducationObjective.grade_level == grade.grade_code,
                EducationObjective.active.is_(True),
            ),
        ):
            deps.append("learning objectives for this grade level")
        if _active_count(
            db,
            select(func.count())
            .select_from(TeacherAssistPacingGuide)
            .where(
                TeacherAssistPacingGuide.grade_level == grade.grade_code,
                TeacherAssistPacingGuide.is_active.is_(True),
            ),
        ):
            deps.append("pacing guides")
    return deps


def archive_state(db: Session, *, state_id: uuid.UUID):
    row = get_state_or_404(db, state_id)
    deps = state_archive_dependencies(db, state_id=state_id)
    if deps:
        raise CatalogArchiveError(
            f"Cannot archive {row.name} because it is still linked to: {', '.join(deps)}.",
            dependencies=deps,
        )
    return update_state(
        db,
        state_id=state_id,
        name=row.name,
        abbreviation=row.abbreviation,
        active=False,
    )


def archive_district(db: Session, *, district_id: uuid.UUID):
    row = get_district_or_404(db, district_id)
    deps = district_archive_dependencies(db, district_id=district_id)
    if deps:
        raise CatalogArchiveError(
            f"Cannot archive {row.name} because it is still linked to: {', '.join(deps)}.",
            dependencies=deps,
        )
    return update_district(
        db,
        district_id=district_id,
        state_id=row.state_id,
        name=row.name,
        district_code=row.district_code,
        active=False,
    )


def archive_school(db: Session, *, school_id: uuid.UUID):
    row = get_school_or_404(db, school_id)
    deps = school_archive_dependencies(db, school_id=school_id)
    if deps:
        raise CatalogArchiveError(
            f"Cannot archive {row.name} because it is still linked to: {', '.join(deps)}.",
            dependencies=deps,
        )
    return update_school(
        db,
        school_id=school_id,
        district_id=row.district_id,
        name=row.name,
        school_type=row.school_type,
        active=False,
    )


def archive_grade(db: Session, *, grade_id: uuid.UUID):
    row = get_grade_or_404(db, grade_id)
    deps = grade_archive_dependencies(db, grade_id=grade_id)
    if deps:
        label = row.display_name or row.grade_code
        raise CatalogArchiveError(
            f"Cannot archive grade {label} because it is still linked to: {', '.join(deps)}.",
            dependencies=deps,
        )
    return update_grade(
        db,
        grade_id=grade_id,
        school_id=row.school_id,
        grade_code=row.grade_code,
        display_name=row.display_name,
        active=False,
    )


def archive_subject(db: Session, *, subject_id: uuid.UUID):
    row = get_subject_or_404(db, subject_id)
    return update_subject(
        db,
        subject_id=subject_id,
        grade_id=row.grade_id,
        subject_code=row.subject_code,
        display_name=row.display_name,
        active=False,
    )


def build_v2_context(db: Session, *, user: User) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist_v2.teacher_onboarding import (
        get_v2_onboarding,
        is_v2_onboarding_complete,
        is_v2_pacing_setup_complete,
    )

    role = resolve_teacher_assist_role(db, user=user)
    if role is None:
        return {
            "has_access": False,
            "role": None,
            "landing_route": "/teacher-assist-v2/access-denied",
            "onboarding_complete": False,
            "pacing_guide_setup_complete": False,
            "requires_password_change": False,
            "feature_locked": True,
            "feature_lock_message": None,
            "allowed_routes": [],
        }
    if role == "root_admin":
        return {
            "has_access": True,
            "role": role,
            "landing_route": "/teacher-assist-v2/admin",
            "onboarding_complete": True,
            "pacing_guide_setup_complete": True,
            "requires_password_change": False,
            "feature_locked": False,
            "feature_lock_message": None,
            "allowed_routes": [],
            "ai_generation": get_teacher_assist_ai_mode_status(db, get_settings()),
        }

    onboarding = get_v2_onboarding(db, user_id=user.id)
    requires_password_change = bool(getattr(user, "must_change_password", False))
    onboarding_complete = is_v2_onboarding_complete(onboarding)
    pacing_guide_setup_complete = is_v2_pacing_setup_complete(onboarding)

    if requires_password_change:
        landing_route = "/teacher-assist-v2/reset-password"
    elif not onboarding_complete:
        landing_route = "/teacher-assist-v2/onboarding"
    elif not pacing_guide_setup_complete:
        landing_route = "/teacher-assist-v2/pacing-guide-setup"
    else:
        landing_route = "/teacher-assist-v2/planning"

    allowed_routes = ["/teacher-assist-v2/access-denied"]
    if requires_password_change:
        allowed_routes.append("/teacher-assist-v2/reset-password")
    else:
        allowed_routes.append("/teacher-assist-v2/onboarding")
        if onboarding_complete:
            allowed_routes.append("/teacher-assist-v2/pacing-guide-setup")
        if pacing_guide_setup_complete:
            allowed_routes.append("/teacher-assist-v2/home")
            allowed_routes.append("/teacher-assist-v2/pacing-guide-setup")
            allowed_routes.append("/teacher-assist-v2/planning")
            allowed_routes.append("/teacher-assist-v2/packages")
            allowed_routes.append("/teacher-assist-v2/teach")
            allowed_routes.append("/teacher-assist-v2/assignments")

    feature_locked = requires_password_change or not onboarding_complete or not pacing_guide_setup_complete
    feature_lock_message = None
    if feature_locked:
        if requires_password_change:
            feature_lock_message = "Create a new password to continue."
        elif not onboarding_complete:
            feature_lock_message = "Complete onboarding to unlock TeacherAssist."
        elif not pacing_guide_setup_complete:
            feature_lock_message = "Set up your pacing guides to unlock TeacherAssist."

    progress_percent = 0
    if onboarding_complete and pacing_guide_setup_complete:
        progress_percent = 100
    elif onboarding_complete:
        progress_percent = 75
    elif requires_password_change:
        progress_percent = 0
    else:
        progress_percent = 25

    return {
        "has_access": True,
        "role": role,
        "landing_route": landing_route,
        "onboarding_complete": onboarding_complete,
        "pacing_guide_setup_complete": pacing_guide_setup_complete,
        "onboarding_progress_percent": progress_percent,
        "requires_password_change": requires_password_change,
        "feature_locked": feature_locked,
        "feature_lock_message": feature_lock_message,
        "allowed_routes": allowed_routes,
        "ai_generation": get_teacher_assist_ai_mode_status(db, get_settings()),
    }


def build_admin_dashboard(db: Session) -> dict[str, int]:
    return {
        "states": db.scalar(
            select(func.count()).select_from(EducationState).where(EducationState.active.is_(True))
        )
        or 0,
        "districts": db.scalar(
            select(func.count()).select_from(EducationDistrict).where(EducationDistrict.active.is_(True))
        )
        or 0,
        "schools": db.scalar(
            select(func.count()).select_from(EducationSchool).where(EducationSchool.active.is_(True))
        )
        or 0,
        "grades": db.scalar(
            select(func.count()).select_from(EducationGrade).where(EducationGrade.active.is_(True))
        )
        or 0,
        "subjects": db.scalar(
            select(func.count()).select_from(EducationSubject).where(EducationSubject.active.is_(True))
        )
        or 0,
    }


def build_hierarchy_explorer(db: Session, *, active_only: bool = True) -> list[dict[str, Any]]:
    states = list_states(db, active_only=active_only)
    tree: list[dict[str, Any]] = []
    for state in states:
        state_node = {
            "id": str(state.id),
            "type": "state",
            "name": state.name,
            "abbreviation": state.abbreviation,
            "active": state.active,
            "districts": [],
        }
        districts = list_districts(db, state_id=state.id, active_only=active_only)
        for district in districts:
            district_node = {
                "id": str(district.id),
                "type": "district",
                "name": district.name,
                "district_code": district.district_code,
                "active": district.active,
                "schools": [],
            }
            schools = list_schools(db, district_id=district.id, active_only=active_only)
            for school in schools:
                school_node = {
                    "id": str(school.id),
                    "type": "school",
                    "name": school.name,
                    "school_type": school.school_type,
                    "active": school.active,
                    "grades": [],
                }
                grades = list_grades(db, school_id=school.id, active_only=active_only)
                for grade in grades:
                    grade_node = {
                        "id": str(grade.id),
                        "type": "grade",
                        "grade_code": grade.grade_code,
                        "display_name": grade.display_name,
                        "active": grade.active,
                        "subjects": [],
                    }
                    subjects = list_subjects(db, grade_id=grade.id, active_only=active_only)
                    for subject in subjects:
                        grade_node["subjects"].append(
                            {
                                "id": str(subject.id),
                                "type": "subject",
                                "subject_code": subject.subject_code,
                                "display_name": subject.display_name,
                                "active": subject.active,
                            }
                        )
                    school_node["grades"].append(grade_node)
                district_node["schools"].append(school_node)
            state_node["districts"].append(district_node)
        tree.append(state_node)
    return tree

"""TeacherAssist v2 — clean role-based API."""

from __future__ import annotations

import uuid
from datetime import date

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser, RootAdminUser
from oziebot_api.schemas.education_catalog import (
    EducationDistrictCreate,
    EducationDistrictOut,
    EducationGradeCreate,
    EducationGradeOut,
    EducationSchoolCreate,
    EducationSchoolOut,
    EducationStateCreate,
    EducationStateOut,
    EducationSubjectCreate,
    EducationSubjectOut,
)
from oziebot_api.schemas.pacing_guide import CatalogPacingGuideDetailOut, CatalogPacingGuideSummaryOut, CatalogPacingGuideUpdate
from oziebot_api.schemas.teacher_assist_v2 import (
    EducationObjectiveV2Create,
    EducationObjectiveV2Out,
    EducationSchoolYearCreate,
    EducationSchoolYearOut,
    V2PacingGuideCopyIn,
    V2PacingGuidePeriodUpdate,
    V2PacingGuideSetupSaveIn,
    V2PlanningGenerateIn,
    V2PlanningSupplementalLinkCreate,
    V2PlanningSupplementalNoteCreate,
    V2GradeReviewModifyIn,
    V2GradeReviewRejectIn,
    V2GradeReviewSaveIn,
    V2TeacherAssistAiProviderConfigIn,
    V2StudentSubmissionManualMatchIn,
    V2StudentSubmissionStatusIn,
    V2PackageCloseOutIn,
    V2SupportingLinkCreate,
    V2SupportingMaterialOut,
    V2SupportingNoteCreate,
    V2TeacherOnboardingSaveIn,
    V2TeacherProvisionIn,
)
from oziebot_api.services.teacher_assist.education_catalog import (
    create_district,
    create_grade,
    create_school,
    create_state,
    create_subject,
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
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user
from oziebot_api.services.teacher_assist.storage import save_teacher_assist_bytes
from oziebot_api.services.teacher_assist_v2.catalog_integrity import (
    CatalogArchiveError,
    archive_district,
    archive_grade,
    archive_school,
    archive_state,
    archive_subject,
    build_admin_dashboard,
    build_hierarchy_explorer,
    build_v2_context,
    district_archive_dependencies,
    grade_archive_dependencies,
    school_archive_dependencies,
    state_archive_dependencies,
)
from oziebot_api.services.teacher_assist_v2.objectives import (
    archive_v2_objective,
    create_v2_objective,
    list_v2_objectives,
    update_v2_objective,
)
from oziebot_api.services.teacher_assist_v2.pacing_guides import (
    archive_v2_pacing_guide,
    clone_v2_pacing_guide,
    ensure_tenant_school_year,
    get_v2_pacing_guide_detail,
    list_v2_district_pacing_guides,
    serialize_pacing_guide_detail,
    serialize_pacing_guide_summary,
    update_v2_pacing_guide,
    update_v2_pacing_guide_period,
)
from oziebot_api.services.teacher_assist.teacher_assignment_provisioning import (
    list_teacher_assignment_rows,
    provision_teacher_school_assignment,
)
from oziebot_api.services.teacher_assist.ai_admin_config import (
    get_teacher_assist_ai_admin_config,
    update_teacher_assist_ai_admin_config,
)
from oziebot_api.services.teacher_assist.ai_mode import (
    get_teacher_assist_ai_mode_status,
    test_teacher_assist_openai_connection,
)
from oziebot_api.services.teacher_assist_v2.assignments import (
    get_teacher_assignment_detail,
    list_recent_assignments,
    list_teacher_assignments,
)
from oziebot_api.services.teacher_assist_v2.instructional_package_generation import generate_instructional_package
from oziebot_api.services.teacher_assist_v2.package_dashboard import (
    build_package_dashboard,
    close_out_instructional_package,
    get_instructional_package_detail_enriched,
    list_teacher_instructional_packages,
)
from oziebot_api.services.teacher_assist_v2.planning_context import get_pacing_guide_planning_context_for_teacher
from oziebot_api.services.teacher_assist_v2.planning_workflow import (
    archive_planning_supplemental_material,
    build_planning_form,
    build_planning_review_context,
    create_planning_supplemental_link,
    create_planning_supplemental_note,
    get_instructional_package_detail,
    list_planning_supplemental_materials,
    serialize_supplemental_material,
    upload_planning_supplemental_file,
)
from oziebot_api.services.teacher_assist_v2.pacing_guide_setup import (
    build_pacing_guide_setup_form,
    build_teacher_home_summary,
    save_pacing_guide_setup,
)
from oziebot_api.services.teacher_assist_v2.platform_context import resolve_v2_platform_context
from oziebot_api.services.teacher_assist_v2.grading_drafts import (
    create_grading_job_for_submission,
    generate_all_grading_drafts,
    get_latest_grading_draft,
)
from oziebot_api.services.teacher_assist_v2.gradebook_workspace import (
    list_gradebook_records,
    list_mastery_evidence,
)
from oziebot_api.services.teacher_assist_v2.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist_v2.grade_reviews import (
    accept_all_viewed_drafts,
    accept_grading_draft,
    get_assignment_grade_for_submission,
    get_grade_audit_history,
    list_assignment_grade_reviews,
    modify_grading_draft,
    record_submission_review_view,
    reject_grading_draft,
    save_grade_review_draft,
)
from oziebot_api.services.teacher_assist_v2.submission_intake import (
    create_assignment_submission_batch,
    get_student_submission_detail,
    get_student_submission_or_404,
    list_assignment_student_submissions,
    list_assignment_submission_batches,
    manually_match_student_submission,
    update_student_submission_status,
)
from oziebot_api.services.teacher_assist_v2.teacher_onboarding import (
    build_teacher_onboarding_form,
    get_v2_onboarding,
    save_teacher_onboarding,
    serialize_teacher_onboarding_status,
)
from oziebot_api.services.teacher_assist_v2.roles import resolve_teacher_assist_role
from oziebot_api.services.teacher_assist_v2.school_years import (
    archive_platform_school_year,
    create_platform_school_year,
    get_platform_school_year_or_404,
    list_platform_school_years,
    update_platform_school_year,
)
from oziebot_api.services.teacher_assist_v2.supporting_materials import (
    archive_supporting_material,
    create_supporting_link,
    create_supporting_note,
    get_pacing_guide_planning_context,
    list_supporting_materials,
    serialize_supporting_material,
    upload_supporting_file,
)

router = APIRouter(prefix="/teacher-assist-v2", tags=["teacher_assist_v2"])


def _require_v2_access(db: DbSession, user: CurrentUser):
    role = resolve_teacher_assist_role(db, user=user)
    if role is None:
        raise HTTPException(status_code=403, detail="TeacherAssist access denied")
    return role


def _require_root_admin(db: DbSession, user: CurrentUser):
    role = _require_v2_access(db, user)
    if role != "root_admin":
        raise HTTPException(status_code=403, detail="Root admin access required")
    return role


def _handle(action):
    try:
        return action()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except CatalogArchiveError as exc:
        raise HTTPException(
            status_code=409,
            detail={"message": str(exc), "dependencies": exc.dependencies},
        ) from exc
    except ValueError as exc:
        if exc.args and isinstance(exc.args[0], dict):
            raise HTTPException(status_code=400, detail={"field_errors": exc.args[0]}) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/context")
def read_v2_context(user: CurrentUser, db: DbSession) -> dict:
    try:
        teacher_assist_context_for_user(db, user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    return build_v2_context(db, user=user)


@router.get("/admin/hierarchy")
def read_hierarchy_explorer(user: RootAdminUser, db: DbSession, active_only: bool = True) -> list[dict]:
    _require_root_admin(db, user)
    return build_hierarchy_explorer(db, active_only=active_only)


@router.get("/admin/dashboard")
def read_admin_dashboard(user: RootAdminUser, db: DbSession) -> dict:
    _require_root_admin(db, user)
    return build_admin_dashboard(db)


@router.get("/admin/ai-provider-config")
def read_admin_ai_provider_config(
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_root_admin(db, user)
    return get_teacher_assist_ai_admin_config(db, env_settings=settings)


@router.put("/admin/ai-provider-config")
def update_admin_ai_provider_config(
    body: V2TeacherAssistAiProviderConfigIn,
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_root_admin(db, user)
    return _handle(
        lambda: update_teacher_assist_ai_admin_config(
            db,
            user=user,
            env_settings=settings,
            ai_provider=body.ai_provider,
            real_provider_enabled=body.real_provider_enabled,
            real_provider_model=body.real_provider_model,
            daily_cost_limit_cents=body.daily_cost_limit_cents,
        )
    )


@router.post("/admin/ai-provider-config/test-connection")
def test_admin_ai_provider_connection(
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_root_admin(db, user)
    return test_teacher_assist_openai_connection(db, env_settings=settings)


@router.get("/teacher/ai-generation-status")
def read_teacher_ai_generation_status(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_v2_access(db, user)
    return get_teacher_assist_ai_mode_status(db, settings)


@router.get("/catalog/states", response_model=list[EducationStateOut])
def read_v2_states(user: RootAdminUser, db: DbSession, active_only: bool = False) -> list[EducationStateOut]:
    _require_root_admin(db, user)
    return [EducationStateOut.model_validate(row) for row in list_states(db, active_only=active_only)]


@router.post("/catalog/states", response_model=EducationStateOut, status_code=201)
def create_v2_state(body: EducationStateCreate, user: RootAdminUser, db: DbSession) -> EducationStateOut:
    _require_root_admin(db, user)
    row = _handle(lambda: create_state(db, name=body.name, abbreviation=body.abbreviation, active=body.active))
    return EducationStateOut.model_validate(row)


@router.put("/catalog/states/{state_id}", response_model=EducationStateOut)
def update_v2_state(
    state_id: uuid.UUID, body: EducationStateCreate, user: RootAdminUser, db: DbSession
) -> EducationStateOut:
    _require_root_admin(db, user)
    row = _handle(
        lambda: update_state(
            db,
            state_id=state_id,
            name=body.name,
            abbreviation=body.abbreviation,
            active=body.active,
        )
    )
    return EducationStateOut.model_validate(row)


@router.post("/catalog/states/{state_id}/archive", response_model=EducationStateOut)
def archive_v2_state(state_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> EducationStateOut:
    _require_root_admin(db, user)
    row = _handle(lambda: archive_state(db, state_id=state_id))
    return EducationStateOut.model_validate(row)


@router.get("/catalog/states/{state_id}/archive-check")
def check_archive_state(state_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> dict:
    _require_root_admin(db, user)
    deps = _handle(lambda: state_archive_dependencies(db, state_id=state_id))
    return {"can_archive": len(deps) == 0, "dependencies": deps}


@router.get("/catalog/districts", response_model=list[EducationDistrictOut])
def read_v2_districts(
    user: RootAdminUser,
    db: DbSession,
    state_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationDistrictOut]:
    _require_root_admin(db, user)
    return [
        EducationDistrictOut.model_validate(row)
        for row in list_districts(db, state_id=state_id, active_only=active_only)
    ]


@router.post("/catalog/districts", response_model=EducationDistrictOut, status_code=201)
def create_v2_district(body: EducationDistrictCreate, user: RootAdminUser, db: DbSession) -> EducationDistrictOut:
    _require_root_admin(db, user)
    row = _handle(
        lambda: create_district(
            db,
            state_id=body.state_id,
            name=body.name,
            district_code=body.district_code,
            active=body.active,
        )
    )
    return EducationDistrictOut.model_validate(row)


@router.put("/catalog/districts/{district_id}", response_model=EducationDistrictOut)
def update_v2_district(
    district_id: uuid.UUID, body: EducationDistrictCreate, user: RootAdminUser, db: DbSession
) -> EducationDistrictOut:
    _require_root_admin(db, user)
    row = _handle(
        lambda: update_district(
            db,
            district_id=district_id,
            state_id=body.state_id,
            name=body.name,
            district_code=body.district_code,
            active=body.active,
        )
    )
    return EducationDistrictOut.model_validate(row)


@router.post("/catalog/districts/{district_id}/archive", response_model=EducationDistrictOut)
def archive_v2_district(district_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> EducationDistrictOut:
    _require_root_admin(db, user)
    row = _handle(lambda: archive_district(db, district_id=district_id))
    return EducationDistrictOut.model_validate(row)


@router.get("/catalog/districts/{district_id}/archive-check")
def check_archive_district(district_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> dict:
    _require_root_admin(db, user)
    deps = _handle(lambda: district_archive_dependencies(db, district_id=district_id))
    return {"can_archive": len(deps) == 0, "dependencies": deps}


@router.get("/catalog/schools", response_model=list[EducationSchoolOut])
def read_v2_schools(
    user: RootAdminUser,
    db: DbSession,
    district_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationSchoolOut]:
    _require_root_admin(db, user)
    return [
        EducationSchoolOut.model_validate(row)
        for row in list_schools(db, district_id=district_id, active_only=active_only)
    ]


@router.post("/catalog/schools", response_model=EducationSchoolOut, status_code=201)
def create_v2_school(body: EducationSchoolCreate, user: RootAdminUser, db: DbSession) -> EducationSchoolOut:
    _require_root_admin(db, user)
    row = _handle(
        lambda: create_school(
            db,
            district_id=body.district_id,
            name=body.name,
            school_type=body.school_type,
            active=body.active,
        )
    )
    return EducationSchoolOut.model_validate(row)


@router.put("/catalog/schools/{school_id}", response_model=EducationSchoolOut)
def update_v2_school(
    school_id: uuid.UUID, body: EducationSchoolCreate, user: RootAdminUser, db: DbSession
) -> EducationSchoolOut:
    _require_root_admin(db, user)
    row = _handle(
        lambda: update_school(
            db,
            school_id=school_id,
            district_id=body.district_id,
            name=body.name,
            school_type=body.school_type,
            active=body.active,
        )
    )
    return EducationSchoolOut.model_validate(row)


@router.post("/catalog/schools/{school_id}/archive", response_model=EducationSchoolOut)
def archive_v2_school(school_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> EducationSchoolOut:
    _require_root_admin(db, user)
    row = _handle(lambda: archive_school(db, school_id=school_id))
    return EducationSchoolOut.model_validate(row)


@router.get("/catalog/schools/{school_id}/archive-check")
def check_archive_school(school_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> dict:
    _require_root_admin(db, user)
    deps = _handle(lambda: school_archive_dependencies(db, school_id=school_id))
    return {"can_archive": len(deps) == 0, "dependencies": deps}


@router.get("/catalog/grades", response_model=list[EducationGradeOut])
def read_v2_grades(
    user: RootAdminUser,
    db: DbSession,
    school_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationGradeOut]:
    _require_root_admin(db, user)
    return [EducationGradeOut.model_validate(row) for row in list_grades(db, school_id=school_id, active_only=active_only)]


@router.post("/catalog/grades", response_model=EducationGradeOut, status_code=201)
def create_v2_grade(body: EducationGradeCreate, user: RootAdminUser, db: DbSession) -> EducationGradeOut:
    _require_root_admin(db, user)
    if body.school_id is None:
        raise HTTPException(status_code=400, detail="Grade requires a school")
    row = _handle(
        lambda: create_grade(
            db,
            school_id=body.school_id,
            grade_code=body.grade_code,
            display_name=body.display_name,
            active=body.active,
        )
    )
    return EducationGradeOut.model_validate(row)


@router.put("/catalog/grades/{grade_id}", response_model=EducationGradeOut)
def update_v2_grade(
    grade_id: uuid.UUID, body: EducationGradeCreate, user: RootAdminUser, db: DbSession
) -> EducationGradeOut:
    _require_root_admin(db, user)
    if body.school_id is None:
        raise HTTPException(status_code=400, detail="Grade requires a school")
    row = _handle(
        lambda: update_grade(
            db,
            grade_id=grade_id,
            school_id=body.school_id,
            grade_code=body.grade_code,
            display_name=body.display_name,
            active=body.active,
        )
    )
    return EducationGradeOut.model_validate(row)


@router.post("/catalog/grades/{grade_id}/archive", response_model=EducationGradeOut)
def archive_v2_grade(grade_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> EducationGradeOut:
    _require_root_admin(db, user)
    row = _handle(lambda: archive_grade(db, grade_id=grade_id))
    return EducationGradeOut.model_validate(row)


@router.get("/catalog/grades/{grade_id}/archive-check")
def check_archive_grade(grade_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> dict:
    _require_root_admin(db, user)
    deps = _handle(lambda: grade_archive_dependencies(db, grade_id=grade_id))
    return {"can_archive": len(deps) == 0, "dependencies": deps}


@router.get("/catalog/subjects", response_model=list[EducationSubjectOut])
def read_v2_subjects(
    user: RootAdminUser,
    db: DbSession,
    grade_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationSubjectOut]:
    _require_root_admin(db, user)
    return [
        EducationSubjectOut.model_validate(row)
        for row in list_subjects(db, grade_id=grade_id, active_only=active_only)
    ]


@router.post("/catalog/subjects", response_model=EducationSubjectOut, status_code=201)
def create_v2_subject(body: EducationSubjectCreate, user: RootAdminUser, db: DbSession) -> EducationSubjectOut:
    _require_root_admin(db, user)
    if body.grade_id is None:
        raise HTTPException(status_code=400, detail="Subject requires a grade")
    row = _handle(
        lambda: create_subject(
            db,
            grade_id=body.grade_id,
            subject_code=body.subject_code,
            display_name=body.display_name,
            active=body.active,
        )
    )
    return EducationSubjectOut.model_validate(row)


@router.put("/catalog/subjects/{subject_id}", response_model=EducationSubjectOut)
def update_v2_subject(
    subject_id: uuid.UUID, body: EducationSubjectCreate, user: RootAdminUser, db: DbSession
) -> EducationSubjectOut:
    _require_root_admin(db, user)
    if body.grade_id is None:
        raise HTTPException(status_code=400, detail="Subject requires a grade")
    row = _handle(
        lambda: update_subject(
            db,
            subject_id=subject_id,
            grade_id=body.grade_id,
            subject_code=body.subject_code,
            display_name=body.display_name,
            active=body.active,
        )
    )
    return EducationSubjectOut.model_validate(row)


@router.post("/catalog/subjects/{subject_id}/archive", response_model=EducationSubjectOut)
def archive_v2_subject(subject_id: uuid.UUID, user: RootAdminUser, db: DbSession) -> EducationSubjectOut:
    _require_root_admin(db, user)
    row = _handle(lambda: archive_subject(db, subject_id=subject_id))
    return EducationSubjectOut.model_validate(row)


@router.get("/instructional/school-years", response_model=list[EducationSchoolYearOut])
def read_v2_school_years(
    user: RootAdminUser,
    db: DbSession,
    state_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationSchoolYearOut]:
    _require_root_admin(db, user)
    return [
        EducationSchoolYearOut.model_validate(row)
        for row in list_platform_school_years(db, state_id=state_id, active_only=active_only)
    ]


@router.post("/instructional/school-years", response_model=EducationSchoolYearOut, status_code=201)
def create_v2_school_year(
    body: EducationSchoolYearCreate, user: RootAdminUser, db: DbSession
) -> EducationSchoolYearOut:
    _require_root_admin(db, user)
    row = _handle(
        lambda: create_platform_school_year(
            db,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            title=body.title,
            start_date=body.start_date,
            end_date=body.end_date,
            active=body.active,
        )
    )
    return EducationSchoolYearOut.model_validate(row)


@router.put("/instructional/school-years/{school_year_id}", response_model=EducationSchoolYearOut)
def update_v2_school_year(
    school_year_id: uuid.UUID, body: EducationSchoolYearCreate, user: RootAdminUser, db: DbSession
) -> EducationSchoolYearOut:
    _require_root_admin(db, user)
    row = _handle(
        lambda: update_platform_school_year(
            db,
            school_year_id=school_year_id,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            title=body.title,
            start_date=body.start_date,
            end_date=body.end_date,
            active=body.active,
        )
    )
    return EducationSchoolYearOut.model_validate(row)


@router.post("/instructional/school-years/{school_year_id}/archive", response_model=EducationSchoolYearOut)
def archive_v2_school_year(
    school_year_id: uuid.UUID, user: RootAdminUser, db: DbSession
) -> EducationSchoolYearOut:
    _require_root_admin(db, user)
    row = _handle(lambda: archive_platform_school_year(db, school_year_id=school_year_id))
    return EducationSchoolYearOut.model_validate(row)


@router.get("/instructional/objectives", response_model=list[EducationObjectiveV2Out])
def read_v2_objectives(
    user: RootAdminUser,
    db: DbSession,
    state_id: uuid.UUID | None = None,
    grade_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    school_year_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationObjectiveV2Out]:
    _require_root_admin(db, user)
    return [
        EducationObjectiveV2Out.model_validate(row)
        for row in list_v2_objectives(
            db,
            state_id=state_id,
            grade_id=grade_id,
            subject_id=subject_id,
            school_year_id=school_year_id,
            active_only=active_only,
        )
    ]


@router.post("/instructional/objectives", response_model=EducationObjectiveV2Out, status_code=201)
def create_v2_objective_route(
    body: EducationObjectiveV2Create, user: RootAdminUser, db: DbSession
) -> EducationObjectiveV2Out:
    _require_root_admin(db, user)
    row = _handle(
        lambda: create_v2_objective(
            db,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            grade_id=body.grade_id,
            subject_id=body.subject_id,
            school_year_id=body.school_year_id,
            objective_type=body.objective_type,
            objective_id=body.objective_id,
            description=body.description,
            is_required=body.is_required,
            active=body.active,
        )
    )
    return EducationObjectiveV2Out.model_validate(row)


@router.put("/instructional/objectives/{objective_id}", response_model=EducationObjectiveV2Out)
def update_v2_objective_route(
    objective_id: uuid.UUID, body: EducationObjectiveV2Create, user: RootAdminUser, db: DbSession
) -> EducationObjectiveV2Out:
    _require_root_admin(db, user)
    row = _handle(
        lambda: update_v2_objective(
            db,
            row_id=objective_id,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            grade_id=body.grade_id,
            subject_id=body.subject_id,
            school_year_id=body.school_year_id,
            objective_type=body.objective_type,
            objective_id=body.objective_id,
            description=body.description,
            is_required=body.is_required,
            active=body.active,
        )
    )
    return EducationObjectiveV2Out.model_validate(row)


@router.post("/instructional/objectives/{objective_id}/archive", response_model=EducationObjectiveV2Out)
def archive_v2_objective_route(
    objective_id: uuid.UUID, user: RootAdminUser, db: DbSession
) -> EducationObjectiveV2Out:
    _require_root_admin(db, user)
    row = _handle(lambda: archive_v2_objective(db, row_id=objective_id))
    return EducationObjectiveV2Out.model_validate(row)


@router.get("/instructional/pacing-guides", response_model=list[CatalogPacingGuideSummaryOut])
def read_v2_pacing_guides(
    user: RootAdminUser,
    db: DbSession,
    catalog_district_id: uuid.UUID | None = None,
    catalog_grade_id: uuid.UUID | None = None,
    active_only: bool = True,
) -> list[CatalogPacingGuideSummaryOut]:
    _require_root_admin(db, user)
    tenant_id, _ = resolve_v2_platform_context(db, user=user)
    guides = list_v2_district_pacing_guides(
        db,
        tenant_id=tenant_id,
        catalog_district_id=catalog_district_id,
        catalog_grade_id=catalog_grade_id,
        active_only=active_only,
    )
    return [serialize_pacing_guide_summary(guide, period_count=len(guide.periods)) for guide in guides]


@router.get("/instructional/pacing-guides/{pacing_guide_id}", response_model=CatalogPacingGuideDetailOut)
def read_v2_pacing_guide_detail(
    pacing_guide_id: uuid.UUID, user: RootAdminUser, db: DbSession
) -> CatalogPacingGuideDetailOut:
    _require_root_admin(db, user)
    tenant_id, _ = resolve_v2_platform_context(db, user=user)
    guide = _handle(lambda: get_v2_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id))
    return serialize_pacing_guide_detail(guide)


@router.put("/instructional/pacing-guides/{pacing_guide_id}", response_model=CatalogPacingGuideDetailOut)
def update_v2_pacing_guide_route(
    pacing_guide_id: uuid.UUID,
    body: CatalogPacingGuideUpdate,
    user: RootAdminUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    guide = _handle(
        lambda: update_v2_pacing_guide(
            db,
            tenant_id=tenant_id,
            actor=actor,
            pacing_guide_id=pacing_guide_id,
            body=body,
        )
    )
    detail = get_v2_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide.id)
    return serialize_pacing_guide_detail(detail)


@router.post("/instructional/pacing-guides/{pacing_guide_id}/clone", response_model=CatalogPacingGuideDetailOut)
def clone_v2_pacing_guide_route(
    pacing_guide_id: uuid.UUID,
    body: V2PacingGuideCopyIn,
    user: RootAdminUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    tenant_school_year_id = body.school_year_id
    if tenant_school_year_id is not None:
        platform_year = get_platform_school_year_or_404(db, school_year_id=tenant_school_year_id)
        tenant_school_year_id = ensure_tenant_school_year(
            db, tenant_id=tenant_id, platform_year=platform_year
        ).id
    guide = _handle(
        lambda: clone_v2_pacing_guide(
            db,
            tenant_id=tenant_id,
            actor=actor,
            source_guide_id=pacing_guide_id,
            title=body.title,
            school_year_id=tenant_school_year_id,
        )
    )
    return serialize_pacing_guide_detail(guide)


@router.post("/instructional/pacing-guides/{pacing_guide_id}/archive", response_model=CatalogPacingGuideSummaryOut)
def archive_v2_pacing_guide_route(
    pacing_guide_id: uuid.UUID, user: RootAdminUser, db: DbSession
) -> CatalogPacingGuideSummaryOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    guide = _handle(
        lambda: archive_v2_pacing_guide(
            db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id, actor=actor
        )
    )
    detail = get_v2_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=guide.id)
    return serialize_pacing_guide_summary(detail, period_count=len(detail.periods))


@router.put(
    "/instructional/pacing-guides/periods/{period_id}",
    response_model=CatalogPacingGuideDetailOut,
)
def update_v2_pacing_guide_period_route(
    period_id: uuid.UUID,
    body: V2PacingGuidePeriodUpdate,
    user: RootAdminUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    _require_root_admin(db, user)
    tenant_id, _ = resolve_v2_platform_context(db, user=user)
    period = _handle(
        lambda: update_v2_pacing_guide_period(
            db,
            tenant_id=tenant_id,
            period_id=period_id,
            title=body.title,
            description=body.description,
        )
    )
    detail = get_v2_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=period.pacing_guide_id)
    return serialize_pacing_guide_detail(detail)


def _material_out(row, settings: Settings) -> V2SupportingMaterialOut:
    return V2SupportingMaterialOut.model_validate(serialize_supporting_material(row, settings=settings))


@router.get(
    "/instructional/pacing-guides/{pacing_guide_id}/supporting-materials",
    response_model=list[V2SupportingMaterialOut],
)
def read_v2_pacing_guide_supporting_materials(
    pacing_guide_id: uuid.UUID,
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    period_id: uuid.UUID | None = None,
    education_objective_id: uuid.UUID | None = None,
) -> list[V2SupportingMaterialOut]:
    _require_root_admin(db, user)
    tenant_id, _ = resolve_v2_platform_context(db, user=user)
    rows = _handle(
        lambda: list_supporting_materials(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=pacing_guide_id,
            period_id=period_id,
            education_objective_id=education_objective_id,
            settings=settings,
        )
    )
    return [V2SupportingMaterialOut.model_validate(row) for row in rows]


@router.post(
    "/instructional/pacing-guides/{pacing_guide_id}/supporting-materials/upload",
    response_model=V2SupportingMaterialOut,
    status_code=201,
)
async def upload_v2_pacing_guide_supporting_file(
    pacing_guide_id: uuid.UUID,
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    file: UploadFile = File(...),
    title: str | None = Form(default=None),
    description: str | None = Form(default=None),
    resource_type: str = Form(...),
    period_id: uuid.UUID | None = Form(default=None),
    education_objective_id: uuid.UUID | None = Form(default=None),
) -> V2SupportingMaterialOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    try:
        row = await upload_supporting_file(
            db,
            settings=settings,
            tenant_id=tenant_id,
            actor=actor,
            pacing_guide_id=pacing_guide_id,
            upload=file,
            title=title,
            description=description,
            resource_type=resource_type,
            period_id=period_id,
            education_objective_id=education_objective_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        if exc.args and isinstance(exc.args[0], dict):
            raise HTTPException(status_code=400, detail={"field_errors": exc.args[0]}) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _material_out(row, settings)


@router.post(
    "/instructional/pacing-guides/{pacing_guide_id}/supporting-materials/links",
    response_model=V2SupportingMaterialOut,
    status_code=201,
)
def create_v2_pacing_guide_supporting_link(
    pacing_guide_id: uuid.UUID,
    body: V2SupportingLinkCreate,
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> V2SupportingMaterialOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    row = _handle(
        lambda: create_supporting_link(
            db,
            tenant_id=tenant_id,
            actor=actor,
            pacing_guide_id=pacing_guide_id,
            title=body.title,
            external_url=body.external_url,
            resource_type=body.resource_type,
            description=body.description,
            period_id=body.period_id,
            education_objective_id=body.education_objective_id,
        )
    )
    return _material_out(row, settings)


@router.post(
    "/instructional/pacing-guides/{pacing_guide_id}/supporting-materials/notes",
    response_model=V2SupportingMaterialOut,
    status_code=201,
)
def create_v2_pacing_guide_supporting_note(
    pacing_guide_id: uuid.UUID,
    body: V2SupportingNoteCreate,
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> V2SupportingMaterialOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    row = _handle(
        lambda: create_supporting_note(
            db,
            tenant_id=tenant_id,
            actor=actor,
            pacing_guide_id=pacing_guide_id,
            note_body=body.note_body,
            title=body.title,
            period_id=body.period_id,
            education_objective_id=body.education_objective_id,
        )
    )
    return _material_out(row, settings)


@router.post(
    "/instructional/pacing-guides/supporting-materials/{material_id}/archive",
    response_model=V2SupportingMaterialOut,
)
def archive_v2_pacing_guide_supporting_material(
    material_id: uuid.UUID,
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> V2SupportingMaterialOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    row = _handle(
        lambda: archive_supporting_material(
            db,
            tenant_id=tenant_id,
            material_id=material_id,
            actor=actor,
        )
    )
    return _material_out(row, settings)


@router.get("/instructional/pacing-guides/{pacing_guide_id}/planning-context")
def read_v2_pacing_guide_planning_context(
    pacing_guide_id: uuid.UUID,
    period_id: uuid.UUID,
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_root_admin(db, user)
    tenant_id, _ = resolve_v2_platform_context(db, user=user)
    return _handle(
        lambda: get_pacing_guide_planning_context(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=pacing_guide_id,
            period_id=period_id,
            settings=settings,
        )
    )


def _require_teacher(db: DbSession, user: CurrentUser):
    role = _require_v2_access(db, user)
    if role != "teacher":
        raise HTTPException(status_code=403, detail="Teacher access required")
    return role


def _ensure_teacher_route_allowed(db: DbSession, user: CurrentUser, route_suffix: str) -> None:
    context = build_v2_context(db, user=user)
    allowed = context.get("allowed_routes") or []
    full_route = f"/teacher-assist-v2{route_suffix}"
    if full_route not in allowed and context.get("feature_locked"):
        raise HTTPException(status_code=403, detail=context.get("feature_lock_message") or "Access locked")


@router.get("/teacher/onboarding")
def read_teacher_onboarding_form(
    user: CurrentUser,
    db: DbSession,
    grade_id: uuid.UUID | None = None,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/onboarding")
    return _handle(lambda: build_teacher_onboarding_form(db, user=user, grade_id=grade_id))


@router.post("/teacher/onboarding")
def save_teacher_onboarding_route(
    body: V2TeacherOnboardingSaveIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/onboarding")
    row = _handle(
        lambda: save_teacher_onboarding(
            db,
            user=user,
            school_year_id=body.school_year_id,
            grade_id=body.grade_id,
            student_count=body.student_count,
            selected_subject_ids=body.selected_subject_ids,
        )
    )
    return {
        "onboarding": serialize_teacher_onboarding_status(row),
        "landing_route": "/teacher-assist-v2/pacing-guide-setup",
    }


@router.get("/teacher/pacing-guide-setup")
def read_pacing_guide_setup(user: CurrentUser, db: DbSession) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/pacing-guide-setup")
    return _handle(lambda: build_pacing_guide_setup_form(db, user=user))


@router.post("/teacher/pacing-guide-setup")
def save_pacing_guide_setup_route(
    body: V2PacingGuideSetupSaveIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/pacing-guide-setup")
    row = _handle(
        lambda: save_pacing_guide_setup(
            db,
            user=user,
            selections=[selection.model_dump() for selection in body.selections],
        )
    )
    return {
        "onboarding": serialize_teacher_onboarding_status(row),
        "landing_route": "/teacher-assist-v2/planning",
    }


@router.get("/teacher/home")
def read_teacher_home(user: CurrentUser, db: DbSession, settings: Settings = Depends(settings_dep)) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/home")

    def _payload() -> dict:
        summary = build_teacher_home_summary(db, user=user)
        summary["package_dashboard"] = build_package_dashboard(db, user=user, settings=settings)
        summary["recent_assignments"] = list_recent_assignments(db, user=user)
        return summary

    return _handle(_payload)


@router.get("/teacher/planning/form")
def read_teacher_planning_form(user: CurrentUser, db: DbSession) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    return _handle(lambda: build_planning_form(db, user=user))


@router.get("/teacher/planning/review")
def read_teacher_planning_review(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    week_start: int = 1,
    week_end: int = 1,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    return _handle(
        lambda: build_planning_review_context(
            db,
            user=user,
            week_start=week_start,
            week_end=week_end,
            settings=settings,
        )
    )


@router.get("/teacher/planning/context")
def read_teacher_planning_context(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    week_start: int = 1,
    week_end: int = 1,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    return _handle(
        lambda: get_pacing_guide_planning_context_for_teacher(
            db,
            user=user,
            week_start=week_start,
            week_end=week_end,
            settings=settings,
        )
    )


@router.get("/teacher/planning/supplemental-materials")
def read_teacher_planning_supplemental_materials(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    week_start: int = 1,
    week_end: int = 1,
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    return _handle(
        lambda: list_planning_supplemental_materials(
            db,
            user=user,
            week_start=week_start,
            week_end=week_end,
            settings=settings,
        )
    )


@router.post("/teacher/planning/supplemental-materials/upload", status_code=201)
async def upload_teacher_planning_supplemental_file(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    file: UploadFile = File(...),
    week_start: int = Form(...),
    week_end: int = Form(...),
    title: str | None = Form(default=None),
    resource_type: str = Form(default="other"),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    try:
        row = await upload_planning_supplemental_file(
            db,
            settings=settings,
            user=user,
            week_start=week_start,
            week_end=week_end,
            upload=file,
            title=title,
            resource_type=resource_type,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        if exc.args and isinstance(exc.args[0], dict):
            raise HTTPException(status_code=400, detail={"field_errors": exc.args[0]}) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return serialize_supplemental_material(row, settings=settings)


@router.post("/teacher/planning/supplemental-materials/links", status_code=201)
def create_teacher_planning_supplemental_link(
    body: V2PlanningSupplementalLinkCreate,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    row = _handle(
        lambda: create_planning_supplemental_link(
            db,
            user=user,
            week_start=body.week_start,
            week_end=body.week_end,
            title=body.title,
            external_url=body.external_url,
            resource_type=body.resource_type,
            description=body.description,
        )
    )
    return serialize_supplemental_material(row, settings=settings)


@router.post("/teacher/planning/supplemental-materials/notes", status_code=201)
def create_teacher_planning_supplemental_note(
    body: V2PlanningSupplementalNoteCreate,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    row = _handle(
        lambda: create_planning_supplemental_note(
            db,
            user=user,
            week_start=body.week_start,
            week_end=body.week_end,
            note_body=body.note_body,
            title=body.title,
        )
    )
    return serialize_supplemental_material(row, settings=settings)


@router.post("/teacher/planning/supplemental-materials/{material_id}/archive")
def archive_teacher_planning_supplemental_material(
    material_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    row = _handle(lambda: archive_planning_supplemental_material(db, user=user, material_id=material_id))
    return serialize_supplemental_material(row)


@router.post("/teacher/planning/packages/generate", status_code=201)
def generate_teacher_instructional_package(
    body: V2PlanningGenerateIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    package = _handle(
        lambda: generate_instructional_package(
            db,
            settings=settings,
            user=user,
            week_start=body.week_start,
            week_end=body.week_end,
            teaching_order=body.teaching_order,
            selected_outputs=body.selected_outputs,
            plan_start_date=body.plan_start_date,
            plan_end_date=body.plan_end_date,
        )
    )
    return get_instructional_package_detail_enriched(db, user=user, package_id=package.id, settings=settings)


@router.get("/teacher/packages")
def read_teacher_instructional_packages(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    status: str | None = None,
    school_year_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/packages")
    return _handle(
        lambda: list_teacher_instructional_packages(
            db,
            user=user,
            settings=settings,
            status=status,
            school_year_id=school_year_id,
            subject_id=subject_id,
            date_from=date_from,
            date_to=date_to,
        )
    )


@router.get("/teacher/planning/packages/{package_id}")
def read_teacher_instructional_package(
    package_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/packages")
    return _handle(
        lambda: get_instructional_package_detail_enriched(
            db,
            user=user,
            package_id=package_id,
            settings=settings,
        )
    )


@router.post("/teacher/planning/packages/{package_id}/close-out")
def close_out_teacher_instructional_package(
    package_id: uuid.UUID,
    body: V2PackageCloseOutIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/packages")
    _handle(
        lambda: close_out_instructional_package(
            db,
            user=user,
            package_id=package_id,
            close_out_notes=body.close_out_notes,
            completed_date=body.completed_date,
        )
    )
    return get_instructional_package_detail_enriched(db, user=user, package_id=package_id, settings=settings)


@router.get("/teacher/assignments")
def read_teacher_assignments(
    user: CurrentUser,
    db: DbSession,
    status: str | None = None,
    assignment_type: str | None = None,
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: list_teacher_assignments(
            db,
            user=user,
            status=status,
            assignment_type=assignment_type,
        )
    )


@router.get("/teacher/assignments/{assignment_id}")
def read_teacher_assignment(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: get_teacher_assignment_detail(
            db,
            user=user,
            assignment_id=assignment_id,
            settings=settings,
        )
    )


@router.get("/teacher/assignments/{assignment_id}/submission-batches")
def read_assignment_submission_batches(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: list_assignment_submission_batches(db, user=user, assignment_id=assignment_id)
    )


@router.get("/teacher/assignments/{assignment_id}/submissions")
def read_assignment_student_submissions(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: list_assignment_student_submissions(db, user=user, assignment_id=assignment_id)
    )


@router.post("/teacher/assignments/{assignment_id}/submission-batches", status_code=201)
async def upload_assignment_submission_batch(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    file: UploadFile = File(...),
    student_number: int | None = Form(default=None),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    ctx = teacher_assist_context_for_user(db, user)
    contents = await file.read()
    await file.close()
    stored = save_teacher_assist_bytes(
        settings,
        tenant_id=ctx.tenant_id,
        area="student-work",
        original_filename=file.filename or "upload.bin",
        contents=contents,
        mime_type=file.content_type or "application/octet-stream",
    )
    return _handle(
        lambda: create_assignment_submission_batch(
            db,
            user=user,
            assignment_id=assignment_id,
            stored=stored,
            file_bytes=contents,
            student_number=student_number,
        )
    )


@router.get("/teacher/submissions/{submission_id}")
def read_student_submission(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")

    def _payload() -> dict:
        row = get_student_submission_or_404(db, user=user, submission_id=submission_id)
        record_submission_review_view(db, user=user, submission=row)
        return get_student_submission_detail(
            db,
            user=user,
            submission_id=submission_id,
            settings=settings,
        )

    return _handle(_payload)


@router.post("/teacher/submissions/{submission_id}/manual-match")
def manual_match_student_submission_route(
    submission_id: uuid.UUID,
    body: V2StudentSubmissionManualMatchIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")

    def action() -> dict:
        manually_match_student_submission(
            db,
            user=user,
            submission_id=submission_id,
            student_number=body.student_number,
        )
        return get_student_submission_detail(
            db,
            user=user,
            submission_id=submission_id,
            settings=settings,
        )

    return _handle(action)


@router.patch("/teacher/submissions/{submission_id}/status")
def patch_student_submission_status(
    submission_id: uuid.UUID,
    body: V2StudentSubmissionStatusIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")

    def action() -> dict:
        update_student_submission_status(
            db,
            user=user,
            submission_id=submission_id,
            status=body.status,
        )
        return get_student_submission_detail(
            db,
            user=user,
            submission_id=submission_id,
            settings=settings,
        )

    return _handle(action)


@router.post("/teacher/assignments/{assignment_id}/grading-jobs/generate-all")
def generate_assignment_grading_drafts(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: generate_all_grading_drafts(
            db,
            user=user,
            assignment_id=assignment_id,
            settings=settings,
        )
    )


@router.post("/teacher/submissions/{submission_id}/grading-jobs", status_code=201)
def generate_submission_grading_draft(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: create_grading_job_for_submission(
            db,
            user=user,
            submission_id=submission_id,
            settings=settings,
        )
    )


@router.get("/teacher/submissions/{submission_id}/grading-draft")
def read_submission_grading_draft(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    draft = _handle(
        lambda: get_latest_grading_draft(db, user=user, submission_id=submission_id)
    )
    if draft is None:
        raise HTTPException(status_code=404, detail="Grading draft not found")
    return draft




@router.get("/teacher/gradebook")
def read_teacher_gradebook(
    user: CurrentUser,
    db: DbSession,
    school_year_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    assignment_id: uuid.UUID | None = None,
    objective_id: uuid.UUID | None = None,
) -> list[dict]:
    _require_v2_access(db, user)
    return list_gradebook_records(
        db,
        user=user,
        school_year_id=school_year_id,
        subject_id=subject_id,
        assignment_id=assignment_id,
        objective_id=objective_id,
    )


@router.get("/teacher/mastery")
def read_teacher_mastery(
    user: CurrentUser,
    db: DbSession,
    objective_id: uuid.UUID | None = None,
    student_number: int | None = None,
    assignment_id: uuid.UUID | None = None,
) -> list[dict]:
    _require_v2_access(db, user)
    return list_mastery_evidence(
        db,
        user=user,
        objective_id=objective_id,
        student_number=student_number,
        assignment_id=assignment_id,
    )


@router.get("/teacher/objectives/{objective_id}/performance")
def read_objective_performance(
    objective_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    assignment_id: uuid.UUID | None = None,
) -> dict:
    _require_v2_access(db, user)
    return _handle(
        lambda: ObjectivePerformanceService.summarize_objective(
            db,
            user=user,
            objective_id=objective_id,
            assignment_id=assignment_id,
        )
    )

@router.get("/teacher/assignments/{assignment_id}/grade-reviews")
def read_assignment_grade_reviews(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(lambda: list_assignment_grade_reviews(db, user=user, assignment_id=assignment_id))


@router.get("/teacher/submissions/{submission_id}/assignment-grade")
def read_submission_assignment_grade(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    grade = _handle(
        lambda: get_assignment_grade_for_submission(db, user=user, submission_id=submission_id)
    )
    if grade is None:
        raise HTTPException(status_code=404, detail="Assignment grade not found")
    return grade


@router.get("/teacher/submissions/{submission_id}/grade-audit-history")
def read_submission_grade_audit_history(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(lambda: get_grade_audit_history(db, user=user, submission_id=submission_id))


@router.post("/teacher/submissions/{submission_id}/grade-review/accept", status_code=201)
def accept_submission_grade_review(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(lambda: accept_grading_draft(db, user=user, submission_id=submission_id))


@router.post("/teacher/submissions/{submission_id}/grade-review/modify", status_code=201)
def modify_submission_grade_review(
    submission_id: uuid.UUID,
    body: V2GradeReviewModifyIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: modify_grading_draft(
            db,
            user=user,
            submission_id=submission_id,
            score=body.score,
            max_score=body.max_score,
            teacher_comment=body.teacher_comment,
            rubric_json=body.rubric_json,
            teacher_override_reason=body.teacher_override_reason,
        )
    )


@router.post("/teacher/submissions/{submission_id}/grade-review/reject", status_code=201)
def reject_submission_grade_review(
    submission_id: uuid.UUID,
    body: V2GradeReviewRejectIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: reject_grading_draft(
            db,
            user=user,
            submission_id=submission_id,
            score=body.score,
            max_score=body.max_score,
            teacher_comment=body.teacher_comment,
            rubric_json=body.rubric_json,
            teacher_override_reason=body.teacher_override_reason,
        )
    )


@router.post("/teacher/submissions/{submission_id}/grade-review/save", status_code=201)
def save_submission_grade_review(
    submission_id: uuid.UUID,
    body: V2GradeReviewSaveIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: save_grade_review_draft(
            db,
            user=user,
            submission_id=submission_id,
            score=body.score,
            max_score=body.max_score,
            teacher_comment=body.teacher_comment,
            rubric_json=body.rubric_json,
            teacher_override_reason=body.teacher_override_reason,
        )
    )


@router.post("/teacher/assignments/{assignment_id}/grade-review/accept-all-viewed")
def accept_all_viewed_submission_grades(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(lambda: accept_all_viewed_drafts(db, user=user, assignment_id=assignment_id))


@router.get("/admin/teachers")
def read_admin_teachers(user: RootAdminUser, db: DbSession) -> list[dict]:
    _require_root_admin(db, user)
    rows = list_teacher_assignment_rows(db, active_only=True)
    return [
        {
            "assignment_id": str(row.assignment.id),
            "user_id": str(row.assignment.user_id),
            "email": row.user_email,
            "full_name": row.user_full_name,
            "state_name": row.state_name,
            "district_name": row.district_name,
            "school_name": row.school_name,
            "onboarding": serialize_teacher_onboarding_status(
                get_v2_onboarding(db, user_id=row.assignment.user_id)
            ),
        }
        for row in rows
    ]


@router.post("/admin/teachers/provision", status_code=201)
def provision_admin_teacher(body: V2TeacherProvisionIn, user: RootAdminUser, db: DbSession) -> dict:
    _require_root_admin(db, user)
    result = _handle(
        lambda: provision_teacher_school_assignment(
            db,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            user_id=body.user_id,
            email=body.email,
            full_name=body.full_name,
            tenant_name=body.tenant_name,
            catalog_grade_id=body.catalog_grade_id,
        )
    )
    return {
        "user_id": str(result.user_id),
        "email": result.email,
        "full_name": result.full_name,
        "created_user": result.created_user,
        "temporary_password": result.temporary_password,
        "grade_setup_applied": result.grade_setup_applied,
        "assignment_id": str(result.assignment.id),
    }

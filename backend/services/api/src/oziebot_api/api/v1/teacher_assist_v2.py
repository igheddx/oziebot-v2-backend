"""TeacherAssist v2 — clean role-based API."""

from __future__ import annotations

import uuid
from datetime import date

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import RedirectResponse, Response
from sqlalchemy.engine import make_url

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
    V2PacingGuideBuilderIn,
    V2PacingGuideCopyIn,
    V2PacingGuidePeriodUpdate,
    V2PacingGuideSetupSaveIn,
    V2PlanningGenerateIn,
    V2PlanningSupplementalLinkCreate,
    V2PlanningSupplementalNoteCreate,
    V2GradeReviewAcceptIn,
    V2GradeReviewModifyIn,
    V2GradeReviewRejectIn,
    V2GradeReviewSaveIn,
    V2GradeEntryAssignmentCreateIn,
    V2GradebookGridCellIn,
    V2ManualAssignmentCreateIn,
    V2TeacherAssistAiProviderConfigIn,
    V2StudentSubmissionManualMatchIn,
    V2StudentSubmissionResponseTextIn,
    V2StudentSubmissionStatusIn,
    V2PackageCloseOutIn,
    V2ArtifactDevLockIn,
    V2PackageRegenIn,
    V2PackageRubricUpdateIn,
    V2PackageAdditionalAssignmentGenerateIn,
    V2SupportingLinkCreate,
    V2SupportingMaterialOut,
    V2SupportingNoteCreate,
    V2TeacherOnboardingSaveIn,
    V2TeacherProvisionIn,
    V2RecoveryQueueCreateIn,
    V2RecoveryQueueUpdateIn,
    V2RecoveryArtifactGenerateIn,
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
from oziebot_api.services.teacher_assist.storage import open_teacher_assist_stream, save_teacher_assist_bytes
from oziebot_api.models.teacher_assist_v2_slide_visual_asset import TeacherAssistV2SlideVisualAsset
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.services.teacher_assist_v2.image_search import fetch_slide_images_for_artifact
from oziebot_api.services.teacher_assist_v2.package_export import render_slide_deck_html
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
from oziebot_api.services.teacher_assist_v2.objective_performance import ObjectivePerformanceService
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
from oziebot_api.services.teacher_assist_v2.instructional_package_generation import (
    generate_instructional_package,
    prepare_instructional_package_generation,
    queue_package_partial_regen,
)
from oziebot_api.services.teacher_assist_v2.package_additional_assignments import (
    build_additional_assignment_form,
    generate_additional_package_assignment,
)
from oziebot_api.services.teacher_assist_v2.package_artifact_update import update_teacher_package_artifact_content
from oziebot_api.services.teacher_assist_v2.package_artifact_refresh import regenerate_package_artifacts
from oziebot_api.services.teacher_assist_v2.google_oauth import (
    build_google_integration_status,
    build_oauth_authorization_url,
    complete_oauth_callback,
    disconnect_teacher_google,
)
from oziebot_api.services.teacher_assist_v2.google_form_quizzes import (
    build_teacher_google_status,
    create_google_form_for_assignment,
    import_google_form_results,
    import_google_form_results_csv,
)
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2PlanningSupplementalMaterial,
)
from sqlalchemy import select
from sqlalchemy.orm import selectinload
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
    generate_all_grading_drafts,
    get_latest_grading_draft,
    queue_grading_job_for_submission,
    run_grading_job_inline,
    run_grading_job,
)
from oziebot_api.services.teacher_assist_v2.document_extraction import (
    ensure_planning_supplemental_extraction_record,
    ensure_student_submission_extraction_record,
    run_document_extraction,
    save_student_submission_response_text,
)
from oziebot_api.services.teacher_assist_v2.gradebook_workspace import (
    list_gradebook_records,
    list_mastery_evidence,
)
from oziebot_api.services.teacher_assist_v2.gradebook_grid import (
    build_gradebook_grid_form,
    build_subject_gradebook_grid,
    export_subject_gradebook_grid_csv,
)
from oziebot_api.services.teacher_assist_v2.gradebook_manual_entry import (
    create_teacher_grade_entry_assignment,
    save_gradebook_grid_cell,
)
from oziebot_api.services.teacher_assist_v2.manual_assignments import (
    build_manual_assignment_form,
    create_teacher_manual_assignment,
    list_manual_assignment_objectives,
)
from oziebot_api.services.teacher_assist_v2.assignment_print_packets import generate_assignment_cover_sheets
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
from oziebot_api.services.teacher_assist_v2.grading_class_insight import build_assignment_class_insight
from oziebot_api.services.teacher_assist_v2.recovery_artifacts import (
    generate_recovery_artifact,
    list_recovery_artifacts,
)
from oziebot_api.services.teacher_assist_v2.recovery_budget import compute_recovery_budget
from oziebot_api.services.teacher_assist_v2.recovery_decision import build_recovery_decision
from oziebot_api.services.teacher_assist_v2.today_classroom import build_today_classroom
from oziebot_api.services.teacher_assist_v2.recovery_queue import (
    create_queue_item,
    list_queue_items,
    update_queue_item,
)
from oziebot_api.services.teacher_assist_v2.rubric_score_exports import (
    RUBRIC_SCORE_REPORT_DOCX_MIME,
    build_assignment_rubric_score_report_docx,
    build_submission_rubric_scorecard_html,
)
from oziebot_api.services.teacher_assist_v2.submission_intake import (
    create_assignment_submission_batch,
    get_student_submission_detail,
    get_student_submission_or_404,
    list_assignment_student_submissions,
    list_assignment_submission_batches,
    manually_match_student_submission,
    mark_student_submission_incomplete,
    update_student_submission_status,
    upload_supplement_for_submission,
)
from oziebot_api.services.teacher_assist_v2.submission_workflow import list_assignment_review_queue
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
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        if exc.args and isinstance(exc.args[0], dict):
            raise HTTPException(status_code=400, detail={"field_errors": exc.args[0]}) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _should_run_background(settings: Settings) -> bool:
    if not settings.database_url:
        return False
    url = make_url(settings.database_url)
    if url.drivername.startswith("sqlite") and url.database in {None, "", ":memory:"}:
        return False
    return True


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


@router.get("/teacher/ai-readiness")
def read_teacher_ai_readiness(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_v2_access(db, user)
    ai_status = get_teacher_assist_ai_mode_status(db, settings)
    ocr_real_enabled = bool(settings.teacher_assist_real_ocr_enabled)
    return {
        "ai_generation": ai_status,
        "supported_document_types": ["pdf", "docx", "txt", "pptx", "images_via_ocr"],
        "checks": [
            {
                "key": "real_ai_mode_enabled",
                "label": "Real AI mode enabled",
                "status": "green" if ai_status.get("real_ai_enabled") else "yellow",
                "detail": ai_status.get("banner_message"),
            },
            {
                "key": "document_extraction_available",
                "label": "Document extraction available",
                "status": "green",
                "detail": "Local extraction is available for PDF, DOCX, TXT, and PPTX files.",
            },
            {
                "key": "ocr_available",
                "label": "Image OCR available",
                "status": "green" if ocr_real_enabled else "yellow",
                "detail": (
                    "Real OCR is enabled for image-based uploads."
                    if ocr_real_enabled
                    else "Image-based uploads may need manual response text until real OCR is enabled."
                ),
            },
            {
                "key": "planning_context_uses_extracted_text",
                "label": "Planning context includes extracted text",
                "status": "green",
                "detail": "Package generation now includes extracted district and teacher document excerpts when available.",
            },
            {
                "key": "grading_requires_student_text",
                "label": "Grading requires actual student text",
                "status": "green",
                "detail": "Real AI grading is blocked until extracted or teacher-entered student response text exists.",
            },
            {
                "key": "usage_tracking_enabled",
                "label": "Usage tracking enabled",
                "status": "green",
                "detail": "Package generation and grading usage events are recorded with provider and model metadata.",
            },
        ],
    }


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
    return [
        serialize_pacing_guide_summary(guide, period_count=len(guide.periods), db=db) for guide in guides
    ]


@router.post("/instructional/pacing-guides/builder", response_model=CatalogPacingGuideDetailOut)
def create_v2_pacing_guide_builder_route(
    body: V2PacingGuideBuilderIn,
    user: RootAdminUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    from oziebot_api.services.teacher_assist_v2.pacing_guide_builder import create_v2_pacing_guide_from_builder

    guide = _handle(
        lambda: create_v2_pacing_guide_from_builder(
            db,
            tenant_id=tenant_id,
            actor=actor,
            body=body.model_dump(mode="json"),
        )
    )
    db.commit()
    return serialize_pacing_guide_detail(guide, db=db)


@router.put("/instructional/pacing-guides/{pacing_guide_id}/builder", response_model=CatalogPacingGuideDetailOut)
def update_v2_pacing_guide_builder_route(
    pacing_guide_id: uuid.UUID,
    body: V2PacingGuideBuilderIn,
    user: RootAdminUser,
    db: DbSession,
) -> CatalogPacingGuideDetailOut:
    _require_root_admin(db, user)
    tenant_id, actor = resolve_v2_platform_context(db, user=user)
    from oziebot_api.services.teacher_assist_v2.pacing_guide_builder import update_v2_pacing_guide_from_builder

    guide = _handle(
        lambda: update_v2_pacing_guide_from_builder(
            db,
            tenant_id=tenant_id,
            actor=actor,
            pacing_guide_id=pacing_guide_id,
            body=body.model_dump(mode="json"),
        )
    )
    db.commit()
    return serialize_pacing_guide_detail(guide, db=db)


@router.get("/instructional/pacing-guides/{pacing_guide_id}", response_model=CatalogPacingGuideDetailOut)
def read_v2_pacing_guide_detail(
    pacing_guide_id: uuid.UUID, user: RootAdminUser, db: DbSession
) -> CatalogPacingGuideDetailOut:
    _require_root_admin(db, user)
    tenant_id, _ = resolve_v2_platform_context(db, user=user)
    guide = _handle(lambda: get_v2_pacing_guide_detail(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id))
    return serialize_pacing_guide_detail(guide, db=db)


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
    return serialize_pacing_guide_detail(detail, db=db)


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
            target_guide_type=body.target_guide_type,
        )
    )
    return serialize_pacing_guide_detail(guide, db=db)


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
    return serialize_pacing_guide_summary(detail, period_count=len(detail.periods), db=db)


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
    return serialize_pacing_guide_detail(detail, db=db)


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
    period_day_id: uuid.UUID | None = None,
    education_objective_id: uuid.UUID | None = None,
    guide_level_only: bool = False,
    week_level_only: bool = False,
) -> list[V2SupportingMaterialOut]:
    _require_root_admin(db, user)
    tenant_id, _ = resolve_v2_platform_context(db, user=user)
    rows = _handle(
        lambda: list_supporting_materials(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=pacing_guide_id,
            period_id=period_id,
            period_day_id=period_day_id,
            education_objective_id=education_objective_id,
            guide_level_only=guide_level_only,
            week_level_only=week_level_only,
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
    period_day_id: uuid.UUID | None = Form(default=None),
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
            period_day_id=period_day_id,
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
            period_day_id=body.period_day_id,
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
            period_day_id=body.period_day_id,
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
    if role not in ("teacher", "root_admin"):
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


@router.get("/teacher/today")
def read_teacher_today(user: CurrentUser, db: DbSession) -> dict:
    _require_teacher(db, user)
    return _handle(lambda: build_today_classroom(db, user=user))


@router.get("/teacher/home")
def read_teacher_home(user: CurrentUser, db: DbSession, settings: Settings = Depends(settings_dep)) -> dict:
    role = _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/home")

    if role == "root_admin":
        return {"active_pacing_guides": [], "package_dashboard": {}, "recent_assignments": []}

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
            unlinked_only=True,
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


@router.post("/teacher/planning/supplemental-materials/{material_id}/extract")
def extract_teacher_planning_supplemental_material(
    material_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")

    def action() -> dict:
        row = db.scalars(
            select(TeacherAssistV2PlanningSupplementalMaterial).where(
                TeacherAssistV2PlanningSupplementalMaterial.id == material_id,
                TeacherAssistV2PlanningSupplementalMaterial.teacher_user_id == user.id,
            )
        ).one_or_none()
        if row is None:
            raise LookupError("Supplemental material not found")
        if row.material_kind != "file" or not row.storage_key:
            raise ValueError("Only uploaded files can be extracted")
        record = ensure_planning_supplemental_extraction_record(db, row=row)
        run_document_extraction(db, settings=settings, record=record)
        return serialize_supplemental_material(row, settings=settings)

    return _handle(action)


@router.post("/teacher/planning/packages/generate", status_code=201)
def generate_teacher_instructional_package(
    body: V2PlanningGenerateIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/planning")
    if not _should_run_background(settings):
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
                excluded_pacing_material_ids=body.excluded_pacing_material_ids,
                instructional_delivery_profile=body.instructional_delivery_profile,
                lost_instructional_days=body.lost_instructional_days,
            )
        )
        return get_instructional_package_detail_enriched(
            db, user=user, package_id=package.id, settings=settings
        )
    package, _context, _outputs, _excluded_ids, _final_status = _handle(
        lambda: prepare_instructional_package_generation(
            db,
            settings=settings,
            user=user,
            week_start=body.week_start,
            week_end=body.week_end,
            teaching_order=body.teaching_order,
            selected_outputs=body.selected_outputs,
            plan_start_date=body.plan_start_date,
            plan_end_date=body.plan_end_date,
            excluded_pacing_material_ids=body.excluded_pacing_material_ids,
            instructional_delivery_profile=body.instructional_delivery_profile,
            lost_instructional_days=body.lost_instructional_days,
            quality_review_enabled=body.quality_review_enabled,
        )
    )
    db.commit()
    # Job is now picked up by the teacher-assist-worker (claim_and_run_next_queued_v2_package).
    # No BackgroundTask here — the worker process is separate and survives API restarts.
    return get_instructional_package_detail_enriched(db, user=user, package_id=package.id, settings=settings)


@router.post("/teacher/planning/packages/{package_id}/actions/regenerate", status_code=200)
def queue_teacher_package_regen(
    package_id: uuid.UUID,
    body: V2PackageRegenIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    """Queue a partial or full regeneration of an existing instructional package.

    The package is marked processing/queued immediately; the teacher-assist-worker
    picks it up and regenerates only the requested artifact types (or everything for
    scope='full').  The package ID stays the same — the teacher's URL does not change.
    """
    _require_teacher(db, user)
    from oziebot_api.models.teacher_assist_v2_instructional_package import (
        TeacherAssistV2InstructionalPackage as _Pkg,
    )
    pkg = db.get(_Pkg, package_id)
    if pkg is None or str(pkg.teacher_user_id) != str(user.id):
        from fastapi import HTTPException as _HTTPException
        raise _HTTPException(404, "Package not found")

    current_state = (pkg.metadata_json or {}).get("generation_state")
    if current_state in ("queued", "running"):
        from fastapi import HTTPException as _HTTPException
        raise _HTTPException(409, "Package is already being processed")

    _handle(
        lambda: queue_package_partial_regen(
            db,
            package=pkg,
            regen_scope=body.scope,
            regen_artifact_types=body.artifact_types or None,
        )
    )
    db.commit()
    return {"status": "queued", "package_id": str(package_id), "scope": body.scope}


@router.put("/teacher/planning/artifacts/{artifact_id}/dev-lock", status_code=200)
def set_artifact_dev_lock(
    artifact_id: uuid.UUID,
    body: V2ArtifactDevLockIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    """Toggle the developer lock on a single artifact.

    Only root_admin users may lock/unlock artifacts.  Locked artifacts are excluded
    from regeneration even when their type appears in regen_artifact_types — they
    must be explicitly unlocked before they can be regenerated.
    """
    if getattr(user, "role", None) != "root_admin":
        raise HTTPException(403, "Developer lock requires root_admin role")

    artifact = db.get(TeacherAssistV2InstructionalPackageArtifact, artifact_id)
    if artifact is None:
        raise HTTPException(404, "Artifact not found")

    # Verify the artifact belongs to a package owned by this user (or any for root_admin).
    existing_meta = dict(artifact.metadata_json or {})
    existing_meta["dev_locked"] = body.locked
    artifact.metadata_json = existing_meta
    db.commit()
    return {
        "artifact_id": str(artifact_id),
        "artifact_type": artifact.artifact_type,
        "locked": body.locked,
    }


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
@router.get("/teacher/packages/{package_id}")
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


@router.delete("/teacher/planning/packages/{package_id}", status_code=204)
@router.delete("/teacher/packages/{package_id}", status_code=204)
def delete_teacher_instructional_package(
    package_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> None:
    """Permanently delete a package and all its artifacts. Cache is preserved."""
    _require_teacher(db, user)
    def _delete():
        package: TeacherAssistV2InstructionalPackage | None = (
            db.query(TeacherAssistV2InstructionalPackage)
            .filter_by(id=package_id)
            .first()
        )
        if package is None:
            raise LookupError("Instructional package not found.")
        if package.teacher_user_id != user.id and not user.is_root_admin:
            raise PermissionError("You do not have permission to delete this package.")
        if package.status in ("processing", "queued"):
            raise ValueError("Cannot delete a package that is currently being generated. Wait for it to complete or fail first.")
        db.delete(package)
        db.commit()
    return _handle(_delete)


@router.get("/teacher/artifacts/{artifact_id}/slide-image/{slide_id}")
def get_slide_image(
    artifact_id: uuid.UUID,
    slide_id: str,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> Response:
    _require_teacher(db, user)
    asset: TeacherAssistV2SlideVisualAsset | None = (
        db.query(TeacherAssistV2SlideVisualAsset)
        .filter_by(artifact_id=artifact_id, slide_id=slide_id)
        .order_by(TeacherAssistV2SlideVisualAsset.updated_at.desc())
        .first()
    )
    if asset is None or not asset.local_asset_key:
        raise HTTPException(status_code=404, detail="Slide image not available")
    try:
        stream = open_teacher_assist_stream(settings, storage_key=asset.local_asset_key)
        try:
            image_bytes = stream.read()
        finally:
            stream.close()
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=404, detail="Slide image file not found") from exc
    mime_type = "image/jpeg"
    if asset.local_asset_key.endswith(".png"):
        mime_type = "image/png"
    elif asset.local_asset_key.endswith(".webp"):
        mime_type = "image/webp"
    elif asset.local_asset_key.endswith(".gif"):
        mime_type = "image/gif"
    return Response(
        content=image_bytes,
        media_type=mime_type,
        headers={"Cache-Control": "private, max-age=3600"},
    )


@router.post("/teacher/artifacts/{artifact_id}/fetch-images")
def trigger_artifact_image_fetch(
    artifact_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    """Trigger Pixabay image fetch for all pending slides in a student lesson deck."""
    _require_teacher(db, user)

    if not settings.teacher_assist_pixabay_api_key:
        raise HTTPException(
            status_code=422,
            detail="Pixabay API key is not configured. Images will appear automatically once the key is added.",
        )

    artifact: TeacherAssistV2InstructionalPackageArtifact | None = (
        db.query(TeacherAssistV2InstructionalPackageArtifact)
        .filter_by(id=artifact_id, tenant_id=user.tenant_id)
        .first()
    )
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")

    try:
        fetch_slide_images_for_artifact(
            db,
            artifact=artifact,
            settings=settings,
            api_key=settings.teacher_assist_pixabay_api_key,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Image fetch failed") from exc

    assets = (
        db.query(TeacherAssistV2SlideVisualAsset)
        .filter_by(artifact_id=artifact_id)
        .all()
    )
    fetched = sum(1 for a in assets if a.visual_generation_status == "fetched")
    pending = sum(1 for a in assets if a.visual_generation_status == "pending")
    failed = sum(1 for a in assets if a.visual_generation_status == "failed")
    total = len(assets)

    _image_map = {a.slide_id: a.source_url for a in assets if a.source_url and a.visual_generation_status == "fetched"}
    if _image_map:
        artifact.preview_html = render_slide_deck_html(artifact.content_json or {}, image_map=_image_map)

    db.commit()

    return {
        "fetched": fetched,
        "pending": pending,
        "failed": failed,
        "total": total,
        "message": f"{fetched} of {total} slide images ready.",
    }


@router.post("/teacher/planning/packages/{package_id}/fetch-images", status_code=200)
def trigger_package_image_fetch(
    package_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    """Re-fetch Pixabay images for all slide visual assets across a package."""
    _require_teacher(db, user)

    if not settings.teacher_assist_pixabay_api_key:
        raise HTTPException(
            status_code=422,
            detail="Pixabay API key is not configured.",
        )

    _is_root_admin = bool(getattr(user, "is_root_admin", False))
    _pkg_filter = [TeacherAssistV2InstructionalPackage.id == package_id]
    if not _is_root_admin:
        _pkg_filter.append(TeacherAssistV2InstructionalPackage.teacher_user_id == user.id)

    pkg = db.scalars(select(TeacherAssistV2InstructionalPackage).where(*_pkg_filter)).one_or_none()
    if pkg is None:
        raise HTTPException(status_code=404, detail="Package not found")

    artifacts = db.scalars(
        select(TeacherAssistV2InstructionalPackageArtifact).where(
            TeacherAssistV2InstructionalPackageArtifact.package_id == package_id
        )
    ).all()

    total_fetched = 0
    total_failed = 0
    for artifact in artifacts:
        if not artifact.slide_visual_assets:
            continue
        try:
            fetch_slide_images_for_artifact(
                db,
                artifact=artifact,
                settings=settings,
                api_key=settings.teacher_assist_pixabay_api_key,
            )
            _fetched_assets = (
                db.query(TeacherAssistV2SlideVisualAsset)
                .filter_by(artifact_id=artifact.id)
                .filter(TeacherAssistV2SlideVisualAsset.visual_generation_status == "fetched")
                .all()
            )
            _image_map = {a.slide_id: a.source_url for a in _fetched_assets if a.source_url}
            if _image_map:
                artifact.preview_html = render_slide_deck_html(artifact.content_json or {}, image_map=_image_map)
            db.commit()
            total_fetched += 1
        except Exception:
            total_failed += 1

    return {"artifacts_processed": total_fetched, "artifacts_failed": total_failed}


@router.put("/teacher/planning/packages/{package_id}/artifacts/{artifact_id}/rubric")
def update_teacher_package_rubric(
    package_id: uuid.UUID,
    artifact_id: uuid.UUID,
    body: V2PackageRubricUpdateIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/packages")
    _handle(
        lambda: update_teacher_package_artifact_content(
            db,
            settings=settings,
            user=user,
            package_id=package_id,
            artifact_id=artifact_id,
            content_json=body.model_dump(),
        )
    )
    db.commit()
    return get_instructional_package_detail_enriched(db, user=user, package_id=package_id, settings=settings)


@router.get("/teacher/planning/packages/{package_id}/additional-assignments/form")
def read_package_additional_assignment_form(
    package_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/packages")
    return _handle(lambda: build_additional_assignment_form(db, user=user, package_id=package_id))


@router.post("/teacher/planning/packages/{package_id}/additional-assignments/generate", status_code=201)
def generate_package_additional_assignment(
    package_id: uuid.UUID,
    body: V2PackageAdditionalAssignmentGenerateIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/packages")
    _handle(
        lambda: generate_additional_package_assignment(
            db,
            settings=settings,
            user=user,
            package_id=package_id,
            subject_id=body.subject_id,
            artifact_type=body.artifact_type,
            teacher_notes=body.teacher_notes,
            title_hint=body.title_hint,
        )
    )
    db.commit()
    return get_instructional_package_detail_enriched(db, user=user, package_id=package_id, settings=settings)


@router.get("/admin/packages/{package_id}/validation-report")
def admin_get_package_validation_report(
    package_id: uuid.UUID,
    user: RootAdminUser,
    db: DbSession,
) -> dict:
    """Return the full instructional validation report for a package.

    Teachers receive only confidence_label and confidence_note via the package detail
    endpoint. Root admins receive the complete report here: per-subject scores,
    all issues separated by educational_issues vs generation_issues, improvement_source
    tags, revision history, and the report_meta with model and prompt version.
    """
    _require_root_admin(db, user)
    package = db.scalars(
        select(TeacherAssistV2InstructionalPackage)
        .where(TeacherAssistV2InstructionalPackage.id == package_id)
    ).one_or_none()
    if package is None:
        raise HTTPException(status_code=404, detail="Instructional package not found")
    report = package.instructional_validation_report_json
    if report is None:
        return {
            "package_id": str(package_id),
            "validation_report": None,
            "message": "No validation report available for this package. "
                       "Validation runs during package generation for AI-generated packages.",
        }
    return {
        "package_id": str(package_id),
        "instructional_design_plan_locked_at": (
            package.instructional_design_plan_locked_at.isoformat()
            if package.instructional_design_plan_locked_at
            else None
        ),
        "validation_report": report,
    }


@router.get("/admin/packages/{package_id}/alignment-report")
def admin_get_package_alignment_report(
    package_id: uuid.UUID,
    user: RootAdminUser,
    db: DbSession,
) -> dict:
    """Return the full cross-artifact alignment report for a package.

    Provides per-week alignment checks — exit_ticket_stem_compliance,
    rubric_criterion_alignment, quiz_objective_coverage, vocabulary_sequence,
    and student_prohibition_scan — each with an alignment_explanation and
    week_alignment_confidence. Also includes per-artifact version metadata
    (instructional_plan_version, validation_version, artifact_generation_version)
    accessible via individual artifact metadata_json["version"].

    Root admin only. Teachers see only instructional_alignment_confidence via
    the package detail endpoint.
    """
    _require_root_admin(db, user)
    package = db.scalars(
        select(TeacherAssistV2InstructionalPackage)
        .where(TeacherAssistV2InstructionalPackage.id == package_id)
    ).one_or_none()
    if package is None:
        raise HTTPException(status_code=404, detail="Instructional package not found")
    report = package.instructional_alignment_report_json
    if report is None:
        return {
            "package_id": str(package_id),
            "alignment_report": None,
            "message": (
                "No alignment report available for this package. "
                "Cross-artifact alignment validation runs during package generation "
                "for AI-generated packages."
            ),
        }
    return {
        "package_id": str(package_id),
        "instructional_design_plan_locked_at": (
            package.instructional_design_plan_locked_at.isoformat()
            if package.instructional_design_plan_locked_at
            else None
        ),
        "alignment_report": report,
    }


@router.post("/admin/packages/{package_id}/regenerate-artifacts")
def admin_regenerate_package_artifacts(
    package_id: uuid.UUID,
    user: RootAdminUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    """Idempotent refresh of package artifacts (deterministic or ELA demo profile)."""
    _require_root_admin(db, user)
    package = db.scalars(
        select(TeacherAssistV2InstructionalPackage)
        .where(TeacherAssistV2InstructionalPackage.id == package_id)
        .options(selectinload(TeacherAssistV2InstructionalPackage.artifacts))
    ).one_or_none()
    if package is None:
        raise HTTPException(status_code=404, detail="Instructional package not found")
    result = _handle(lambda: regenerate_package_artifacts(db, settings=settings, package=package))
    db.commit()
    return result


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
    db.commit()
    return get_instructional_package_detail_enriched(db, user=user, package_id=package_id, settings=settings)


@router.get("/teacher/assignments/manual/form")
def read_manual_assignment_form(user: CurrentUser, db: DbSession) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(lambda: build_manual_assignment_form(db, user=user))


@router.get("/teacher/assignments/manual/objectives")
def read_manual_assignment_objectives(
    user: CurrentUser,
    db: DbSession,
    week_number: int = Query(ge=1),
    subject_id: uuid.UUID = Query(...),
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: list_manual_assignment_objectives(
            db,
            user=user,
            week_number=week_number,
            subject_id=subject_id,
        )
    )


@router.post("/teacher/assignments/manual", status_code=201)
def create_manual_assignment_route(
    body: V2ManualAssignmentCreateIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(
        lambda: create_teacher_manual_assignment(
            db,
            settings=settings,
            user=user,
            title=body.title,
            description=body.description,
            week_number=body.week_number,
            subject_id=body.subject_id,
            education_objective_ids=body.education_objective_ids,
            assignment_type=body.assignment_type,
            generate_cover_sheets=body.generate_cover_sheets,
        )
    )


@router.post("/teacher/assignments/{assignment_id}/cover-sheets/generate")
def generate_assignment_cover_sheets_route(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment

    assignment = db.scalars(
        select(TeacherAssistV2Assignment).where(
            TeacherAssistV2Assignment.id == assignment_id,
            TeacherAssistV2Assignment.teacher_user_id == user.id,
        )
    ).one_or_none()
    if assignment is None:
        raise HTTPException(status_code=404, detail="Assignment not found.")
    return _handle(lambda: generate_assignment_cover_sheets(db, settings=settings, assignment=assignment))


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


@router.get("/admin/google-settings")
def read_admin_google_settings(user: RootAdminUser, db: DbSession, settings: Settings = Depends(settings_dep)) -> dict:
    _require_root_admin(db, user)
    return build_google_integration_status(settings)


@router.get("/teacher/google/connection")
def read_teacher_google_connection(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    return build_teacher_google_status(db, user=user, settings=settings)


@router.get("/teacher/google/oauth/start")
def start_teacher_google_oauth(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    return _handle(lambda: build_oauth_authorization_url(settings, teacher_user_id=user.id))


@router.get("/teacher/google/oauth/callback")
def complete_teacher_google_oauth_callback(
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
):
    if error:
        return RedirectResponse(
            f"{settings.teacher_assist_google_oauth_frontend_redirect}?error={error}"
        )
    if not code or not state:
        raise HTTPException(status_code=400, detail="Missing Google OAuth code or state.")
    try:
        complete_oauth_callback(db, settings=settings, code=code, state=state)
        db.commit()
    except ValueError as exc:
        db.rollback()
        return RedirectResponse(
            f"{settings.teacher_assist_google_oauth_frontend_redirect}?error={str(exc)}"
        )
    return RedirectResponse(f"{settings.teacher_assist_google_oauth_frontend_redirect}?connected=1")


@router.delete("/teacher/google/connection", status_code=204)
def delete_teacher_google_connection(user: CurrentUser, db: DbSession) -> None:
    _require_teacher(db, user)
    disconnect_teacher_google(db, teacher_user_id=user.id)
    db.commit()


@router.post("/teacher/assignments/{assignment_id}/google-form/create")
def create_assignment_google_form(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/packages")
    result = _handle(
        lambda: create_google_form_for_assignment(
            db, settings=settings, user=user, assignment_id=assignment_id
        )
    )
    db.commit()
    return result


@router.post("/teacher/assignments/{assignment_id}/google-form/import-results")
def import_assignment_google_form_results(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    result = _handle(
        lambda: import_google_form_results(
            db, settings=settings, user=user, assignment_id=assignment_id
        )
    )
    db.commit()
    return result


@router.post("/teacher/assignments/{assignment_id}/google-form/import-csv")
async def import_assignment_google_form_csv(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    file: UploadFile = File(...),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    csv_text = (await file.read()).decode("utf-8", errors="replace")
    await file.close()
    try:
        result = import_google_form_results_csv(
            db, user=user, assignment_id=assignment_id, csv_text=csv_text
        )
        db.commit()
        return result
    except ValueError as exc:
        db.rollback()
        if isinstance(exc.args[0], dict) and "row_errors" in exc.args[0]:
            raise HTTPException(status_code=400, detail=exc.args[0]) from exc
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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
            settings=settings,
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


@router.post("/teacher/submissions/{submission_id}/extract")
def extract_student_submission_text(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")

    def action() -> dict:
        row = get_student_submission_or_404(db, user=user, submission_id=submission_id)
        if row.status == "NOT_UPLOADED":
            raise ValueError("No uploaded student work is available for extraction.")
        record = ensure_student_submission_extraction_record(db, row=row)
        run_document_extraction(db, settings=settings, record=record)
        return get_student_submission_detail(
            db,
            user=user,
            submission_id=submission_id,
            settings=settings,
        )

    return _handle(action)


@router.put("/teacher/submissions/{submission_id}/response-text")
def save_student_submission_response_text_route(
    submission_id: uuid.UUID,
    body: V2StudentSubmissionResponseTextIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")

    def action() -> dict:
        row = get_student_submission_or_404(db, user=user, submission_id=submission_id)
        save_student_submission_response_text(db, submission=row, response_text=body.response_text)
        return get_student_submission_detail(
            db,
            user=user,
            submission_id=submission_id,
            settings=settings,
        )

    return _handle(action)


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


@router.get("/teacher/assignments/{assignment_id}/review-queue")
def read_assignment_review_queue(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[dict]:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(lambda: list_assignment_review_queue(db, user=user, assignment_id=assignment_id))


@router.post("/teacher/submissions/{submission_id}/incomplete")
def mark_submission_incomplete_route(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")

    def action() -> dict:
        mark_student_submission_incomplete(db, user=user, submission_id=submission_id)
        return get_student_submission_detail(
            db,
            user=user,
            submission_id=submission_id,
            settings=settings,
        )

    return _handle(action)


@router.post("/teacher/submissions/{submission_id}/supplement-upload", status_code=200)
async def upload_submission_supplement(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    file: UploadFile = File(...),
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
        lambda: upload_supplement_for_submission(
            db,
            user=user,
            submission_id=submission_id,
            stored=stored,
            file_bytes=contents,
            settings=settings,
        )
    )


@router.post("/teacher/assignments/{assignment_id}/grading-jobs/generate-all")
def generate_assignment_grading_drafts(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    background_tasks: BackgroundTasks,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    if not _should_run_background(settings):
        result = _handle(
            lambda: generate_all_grading_drafts(
                db,
                user=user,
                assignment_id=assignment_id,
                settings=settings,
            )
        )
        completed_jobs = []
        for job_id in result.get("queued_job_ids", []):
            completed_jobs.append(
                _handle(
                    lambda job_id=job_id: run_grading_job_inline(
                        db,
                        settings=settings,
                        user=user,
                        job_id=uuid.UUID(job_id),
                    )
                )
            )
        result["jobs"] = completed_jobs
        return result
    result = _handle(
        lambda: generate_all_grading_drafts(
            db,
            user=user,
            assignment_id=assignment_id,
            settings=settings,
        )
    )
    db.commit()
    for job_id in result.get("queued_job_ids", []):
        background_tasks.add_task(
            run_grading_job,
            settings=settings,
            user_id=user.id,
            job_id=uuid.UUID(job_id),
        )
    return result


@router.post("/teacher/submissions/{submission_id}/grading-jobs", status_code=201)
def generate_submission_grading_draft(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    background_tasks: BackgroundTasks,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")

    submission = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    if submission.status != "READY_FOR_GRADING":
        raise HTTPException(status_code=400, detail="Submission is not ready for auto-grading.")

    if not _should_run_background(settings):
        result = _handle(
            lambda: queue_grading_job_for_submission(
                db,
                user=user,
                submission_id=submission_id,
                settings=settings,
            )
        )
        if not result.get("already_processing"):
            return _handle(
                lambda: run_grading_job_inline(
                    db,
                    settings=settings,
                    user=user,
                    job_id=uuid.UUID(result["id"]),
                )
            )
        return result
    result = _handle(
        lambda: queue_grading_job_for_submission(
            db,
            user=user,
            submission_id=submission_id,
            settings=settings,
        )
    )
    db.commit()
    if not result.get("already_processing"):
        background_tasks.add_task(
            run_grading_job,
            settings=settings,
            user_id=user.id,
            job_id=uuid.UUID(result["id"]),
        )
    return result


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


@router.get("/teacher/gradebook/grid/form")
def read_gradebook_grid_form(user: CurrentUser, db: DbSession) -> dict:
    _require_v2_access(db, user)
    return _handle(lambda: build_gradebook_grid_form(db, user=user))


@router.get("/teacher/gradebook/grid")
def read_gradebook_grid(
    user: CurrentUser,
    db: DbSession,
    subject_id: uuid.UUID,
    grading_period_id: uuid.UUID | None = None,
) -> dict:
    _require_v2_access(db, user)
    return _handle(
        lambda: build_subject_gradebook_grid(
            db,
            user=user,
            subject_id=subject_id,
            grading_period_id=grading_period_id,
        )
    )


@router.get("/teacher/gradebook/grid/export")
def export_gradebook_grid(
    user: CurrentUser,
    db: DbSession,
    subject_id: uuid.UUID,
    grading_period_id: uuid.UUID | None = None,
):
    from fastapi.responses import Response

    _require_v2_access(db, user)
    filename, payload = export_subject_gradebook_grid_csv(
        db,
        user=user,
        subject_id=subject_id,
        grading_period_id=grading_period_id,
    )
    return Response(
        content=payload,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/teacher/gradebook/grid/assignments", status_code=201)
def create_gradebook_grid_assignment_route(
    body: V2GradeEntryAssignmentCreateIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/gradebook")
    return _handle(
        lambda: create_teacher_grade_entry_assignment(
            db,
            settings=settings,
            user=user,
            title=body.title,
            week_number=body.week_number,
            subject_id=body.subject_id,
            education_objective_ids=body.education_objective_ids,
            description=body.description,
        )
    )


@router.post("/teacher/gradebook/grid/grades", status_code=201)
def save_gradebook_grid_grade_route(
    body: V2GradebookGridCellIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/gradebook")
    return _handle(
        lambda: save_gradebook_grid_cell(
            db,
            user=user,
            assignment_id=body.assignment_id,
            student_number=body.student_number,
            score=body.score,
            max_score=body.max_score,
            teacher_comment=body.teacher_comment,
        )
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


@router.get("/teacher/submissions/{submission_id}/rubric-scorecard")
def read_submission_rubric_scorecard(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
):
    from fastapi.responses import HTMLResponse

    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    html = _handle(lambda: build_submission_rubric_scorecard_html(db, user=user, submission_id=submission_id))
    return HTMLResponse(content=html)


@router.get("/teacher/assignments/{assignment_id}/rubric-score-report")
def read_assignment_rubric_score_report(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
):
    from fastapi.responses import Response

    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    filename, payload = _handle(lambda: build_assignment_rubric_score_report_docx(db, user=user, assignment_id=assignment_id))
    return Response(
        content=payload,
        media_type=RUBRIC_SCORE_REPORT_DOCX_MIME,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/teacher/submissions/{submission_id}/grade-review/accept", status_code=201)
def accept_submission_grade_review(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    body: V2GradeReviewAcceptIn | None = None,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    payload = body or V2GradeReviewAcceptIn()
    return _handle(
        lambda: accept_grading_draft(
            db,
            user=user,
            submission_id=submission_id,
            score=payload.score,
            max_score=payload.max_score,
            teacher_comment=payload.teacher_comment,
            rubric_json=payload.rubric_json,
            student_facing_feedback=payload.student_facing_feedback,
        )
    )


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
            student_facing_feedback=body.student_facing_feedback,
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


@router.get("/teacher/assignments/{assignment_id}/class-insight")
def read_assignment_class_insight(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    _ensure_teacher_route_allowed(db, user, "/assignments")
    return _handle(lambda: build_assignment_class_insight(db, user=user, assignment_id=assignment_id))


# ── Recovery Queue ─────────────────────────────────────────────────────────────

@router.post("/teacher/recovery-queue")
def create_recovery_queue_item(
    payload: V2RecoveryQueueCreateIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    return _handle(
        lambda: create_queue_item(
            db,
            user=user,
            recommendation_type=payload.recommendation_type,
            reason=payload.reason,
            students_affected=payload.students_affected,
            assignment_id=payload.assignment_id,
            instructional_package_id=payload.instructional_package_id,
            education_objective_id=payload.education_objective_id,
            misconception_text=payload.misconception_text,
            evidence_snapshot=payload.evidence_snapshot,
            mastery_snapshot=payload.mastery_snapshot,
            priority=payload.priority,
        )
    )


@router.get("/teacher/recovery-queue")
def read_recovery_queue(
    user: CurrentUser,
    db: DbSession,
    assignment_id: uuid.UUID | None = None,
    instructional_package_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list:
    _require_teacher(db, user)
    statuses = [s.strip() for s in status.split(",")] if status else None
    return _handle(
        lambda: list_queue_items(
            db,
            user=user,
            statuses=statuses,
            assignment_id=assignment_id,
            instructional_package_id=instructional_package_id,
        )
    )


@router.patch("/teacher/recovery-queue/{item_id}")
def update_recovery_queue_item(
    item_id: uuid.UUID,
    payload: V2RecoveryQueueUpdateIn,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    return _handle(
        lambda: update_queue_item(
            db,
            user=user,
            item_id=item_id,
            teacher_response=payload.teacher_response,
            teacher_notes=payload.teacher_notes,
            scheduled_for=payload.scheduled_for,
            priority=payload.priority,
            status=payload.status,
            post_recovery_mastery_snapshot=payload.post_recovery_mastery_snapshot,
        )
    )


@router.get("/teacher/packages/{package_id}/recovery-budget")
def read_recovery_budget(
    package_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    return _handle(lambda: compute_recovery_budget(db, user=user, package_id=package_id))


# ── Learning Recovery Planner — Phase 8 ───────────────────────────────────────

@router.get("/teacher/assignments/{assignment_id}/recovery-decision")
def get_recovery_decision(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> dict:
    _require_teacher(db, user)
    return _handle(lambda: build_recovery_decision(db, user=user, assignment_id=assignment_id))


@router.post("/teacher/recovery-queue/{item_id}/artifacts")
def create_recovery_artifact(
    item_id: uuid.UUID,
    payload: V2RecoveryArtifactGenerateIn,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> dict:
    _require_teacher(db, user)
    return _handle(
        lambda: generate_recovery_artifact(
            db,
            settings=settings,
            user=user,
            queue_item_id=item_id,
            artifact_type=payload.artifact_type,
        )
    )


@router.get("/teacher/recovery-queue/{item_id}/artifacts")
def read_recovery_artifacts(
    item_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list:
    _require_teacher(db, user)
    return _handle(lambda: list_recovery_artifacts(db, user=user, queue_item_id=item_id))


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

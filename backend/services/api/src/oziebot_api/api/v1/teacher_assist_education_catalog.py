"""TeacherAssist education catalog routes."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query

from oziebot_api.deps.auth import CurrentUser, RootAdminUser
from oziebot_api.deps import DbSession
from oziebot_api.schemas.education_catalog import (
    CatalogImportCommitOut,
    CatalogImportPreviewIn,
    CatalogImportPreviewOut,
    CatalogImportRowErrorOut,
    CatalogObjectiveImportCommitIn,
    EducationCurriculumResourceCreate,
    EducationCurriculumResourceOut,
    EducationDistrictCreate,
    EducationDistrictOut,
    EducationGradeCreate,
    EducationGradeOut,
    EducationObjectiveCreate,
    EducationObjectiveOut,
    EducationResourceLinkCreate,
    EducationResourceLinkOut,
    EducationSchoolCreate,
    EducationSchoolOut,
    EducationStateCreate,
    EducationStateOut,
    EducationSubjectCreate,
    EducationSubjectOut,
    TeacherCatalogContextOut,
    TeacherSchoolAssignmentCreate,
    TeacherSchoolAssignmentListOut,
    TeacherSchoolAssignmentOut,
    TeacherSchoolAssignmentProvision,
    TeacherSchoolAssignmentProvisionOut,
    AvailableTeacherOut,
    TeacherMySchoolSetupOut,
    TeacherMySchoolSetupUpdate,
)
from oziebot_api.services.teacher_assist.education_catalog import (
    build_teacher_catalog_context,
    commit_objectives_import,
    create_curriculum_resource,
    create_district,
    create_grade,
    create_objective,
    create_resource_link,
    create_school,
    create_state,
    create_subject,
    create_teacher_assignment,
    list_curriculum_resources,
    list_districts,
    list_grades,
    list_objectives,
    list_resource_links,
    list_schools,
    list_states,
    list_subjects,
    list_teacher_assignments,
    preview_objectives_import,
    update_curriculum_resource,
    update_district,
    update_grade,
    update_objective,
    update_resource_link,
    update_school,
    update_state,
    update_subject,
    update_teacher_assignment,
)
from oziebot_api.services.teacher_assist.teacher_assignment_provisioning import (
    list_teacher_assignment_rows,
    provision_teacher_school_assignment,
    search_available_teachers_for_school,
)
from oziebot_api.services.teacher_assist.teacher_school_setup import (
    build_my_school_setup,
    sync_my_teaching_subjects,
    upsert_my_school_assignment,
)
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user

router = APIRouter(prefix="/teacher-assist/education-catalog", tags=["teacher_assist_education_catalog"])


def _tenant_id(db: DbSession, user: CurrentUser) -> uuid.UUID:
    return teacher_assist_context_for_user(db, user).tenant_id


def _require_teacher_assist(db: DbSession, user: CurrentUser) -> None:
    try:
        teacher_assist_context_for_user(db, user)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def _handle_value_errors(action):
    try:
        return action()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/my-context", response_model=TeacherCatalogContextOut)
def read_my_catalog_context(user: CurrentUser, db: DbSession) -> TeacherCatalogContextOut:
    """Legacy flat snapshot for backward compatibility.

    New browse flows should use ``/v1/teacher-assist/catalog/*``.
    Future catalog-aware planning should consume ``catalog_access`` rather than
    this endpoint's flat snapshot payload.
    """
    _require_teacher_assist(db, user)
    return TeacherCatalogContextOut(**build_teacher_catalog_context(db, user_id=user.id))


@router.get("/my-school-setup", response_model=TeacherMySchoolSetupOut)
def read_my_school_setup(user: CurrentUser, db: DbSession) -> TeacherMySchoolSetupOut:
    _require_teacher_assist(db, user)
    tenant_id = _tenant_id(db, user)
    return TeacherMySchoolSetupOut(**build_my_school_setup(db, tenant_id=tenant_id, user_id=user.id))


@router.put("/my-school-setup", response_model=TeacherMySchoolSetupOut)
def update_my_school_setup(
    body: TeacherMySchoolSetupUpdate,
    user: CurrentUser,
    db: DbSession,
) -> TeacherMySchoolSetupOut:
    _require_teacher_assist(db, user)
    tenant_id = _tenant_id(db, user)

    def action() -> dict:
        upsert_my_school_assignment(
            db,
            user_id=user.id,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
        )
        sync_my_teaching_subjects(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            catalog_grade_id=body.catalog_grade_id,
            catalog_subject_ids=body.catalog_subject_ids,
        )
        db.flush()
        return build_my_school_setup(db, tenant_id=tenant_id, user_id=user.id)

    return TeacherMySchoolSetupOut(**_handle_value_errors(action))


@router.get("/states", response_model=list[EducationStateOut])
def read_states(
    user: CurrentUser,
    db: DbSession,
    q: str | None = None,
    active_only: bool = False,
) -> list[EducationStateOut]:
    _require_teacher_assist(db, user)
    return [EducationStateOut.model_validate(row) for row in list_states(db, q=q, active_only=active_only)]


@router.post("/states", response_model=EducationStateOut, status_code=201)
def create_catalog_state(body: EducationStateCreate, user: RootAdminUser, db: DbSession) -> EducationStateOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(lambda: create_state(db, name=body.name, abbreviation=body.abbreviation, active=body.active))
    return EducationStateOut.model_validate(row)


@router.put("/states/{state_id}", response_model=EducationStateOut)
def update_catalog_state(
    state_id: uuid.UUID, body: EducationStateCreate, user: RootAdminUser, db: DbSession
) -> EducationStateOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: update_state(
            db,
            state_id=state_id,
            name=body.name,
            abbreviation=body.abbreviation,
            active=body.active,
        )
    )
    return EducationStateOut.model_validate(row)


@router.get("/districts", response_model=list[EducationDistrictOut])
def read_districts(
    user: CurrentUser,
    db: DbSession,
    state_id: uuid.UUID | None = None,
    q: str | None = None,
    active_only: bool = False,
) -> list[EducationDistrictOut]:
    _require_teacher_assist(db, user)
    return [
        EducationDistrictOut.model_validate(row)
        for row in list_districts(db, state_id=state_id, q=q, active_only=active_only)
    ]


@router.post("/districts", response_model=EducationDistrictOut, status_code=201)
def create_catalog_district(body: EducationDistrictCreate, user: RootAdminUser, db: DbSession) -> EducationDistrictOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: create_district(
            db,
            state_id=body.state_id,
            name=body.name,
            district_code=body.district_code,
            active=body.active,
        )
    )
    return EducationDistrictOut.model_validate(row)


@router.put("/districts/{district_id}", response_model=EducationDistrictOut)
def update_catalog_district(
    district_id: uuid.UUID, body: EducationDistrictCreate, user: RootAdminUser, db: DbSession
) -> EducationDistrictOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
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


@router.get("/schools", response_model=list[EducationSchoolOut])
def read_schools(
    user: CurrentUser,
    db: DbSession,
    district_id: uuid.UUID | None = None,
    q: str | None = None,
    active_only: bool = False,
) -> list[EducationSchoolOut]:
    _require_teacher_assist(db, user)
    return [
        EducationSchoolOut.model_validate(row)
        for row in list_schools(db, district_id=district_id, q=q, active_only=active_only)
    ]


@router.post("/schools", response_model=EducationSchoolOut, status_code=201)
def create_catalog_school(body: EducationSchoolCreate, user: RootAdminUser, db: DbSession) -> EducationSchoolOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: create_school(
            db,
            district_id=body.district_id,
            name=body.name,
            school_type=body.school_type,
            active=body.active,
        )
    )
    return EducationSchoolOut.model_validate(row)


@router.put("/schools/{school_id}", response_model=EducationSchoolOut)
def update_catalog_school(
    school_id: uuid.UUID, body: EducationSchoolCreate, user: RootAdminUser, db: DbSession
) -> EducationSchoolOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
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


@router.get("/grades", response_model=list[EducationGradeOut])
def read_grades(
    user: CurrentUser,
    db: DbSession,
    school_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationGradeOut]:
    _require_teacher_assist(db, user)
    return [EducationGradeOut.model_validate(row) for row in list_grades(db, school_id=school_id, active_only=active_only)]


@router.post("/grades", response_model=EducationGradeOut, status_code=201)
def create_catalog_grade(body: EducationGradeCreate, user: RootAdminUser, db: DbSession) -> EducationGradeOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: create_grade(
            db,
            school_id=body.school_id,
            grade_code=body.grade_code,
            display_name=body.display_name,
            active=body.active,
        )
    )
    return EducationGradeOut.model_validate(row)


@router.put("/grades/{grade_id}", response_model=EducationGradeOut)
def update_catalog_grade(
    grade_id: uuid.UUID, body: EducationGradeCreate, user: RootAdminUser, db: DbSession
) -> EducationGradeOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
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


@router.get("/subjects", response_model=list[EducationSubjectOut])
def read_subjects(
    user: CurrentUser,
    db: DbSession,
    grade_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationSubjectOut]:
    _require_teacher_assist(db, user)
    return [
        EducationSubjectOut.model_validate(row) for row in list_subjects(db, grade_id=grade_id, active_only=active_only)
    ]


@router.post("/subjects", response_model=EducationSubjectOut, status_code=201)
def create_catalog_subject(body: EducationSubjectCreate, user: RootAdminUser, db: DbSession) -> EducationSubjectOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: create_subject(
            db,
            grade_id=body.grade_id,
            subject_code=body.subject_code,
            display_name=body.display_name,
            active=body.active,
        )
    )
    return EducationSubjectOut.model_validate(row)


@router.put("/subjects/{subject_id}", response_model=EducationSubjectOut)
def update_catalog_subject(
    subject_id: uuid.UUID, body: EducationSubjectCreate, user: RootAdminUser, db: DbSession
) -> EducationSubjectOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
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


@router.get("/objectives", response_model=list[EducationObjectiveOut])
def read_objectives(
    user: CurrentUser,
    db: DbSession,
    state_id: uuid.UUID | None = None,
    grade_level: str | None = None,
    subject_code: str | None = None,
    q: str | None = None,
    active_only: bool = Query(default=True),
) -> list[EducationObjectiveOut]:
    _require_teacher_assist(db, user)
    return [
        EducationObjectiveOut.model_validate(row)
        for row in list_objectives(
            db,
            state_id=state_id,
            grade_level=grade_level,
            subject_code=subject_code,
            q=q,
            active_only=active_only,
        )
    ]


@router.post("/objectives", response_model=EducationObjectiveOut, status_code=201)
def create_catalog_objective(body: EducationObjectiveCreate, user: RootAdminUser, db: DbSession) -> EducationObjectiveOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: create_objective(
            db,
            state_id=body.state_id,
            grade_level=body.grade_level,
            subject_code=body.subject_code,
            objective_type=body.objective_type,
            objective_id=body.objective_id,
            description=body.description,
            coverage_type=body.coverage_type,
            active=body.active,
        )
    )
    return EducationObjectiveOut.model_validate(row)


@router.put("/objectives/{objective_id}", response_model=EducationObjectiveOut)
def update_catalog_objective(
    objective_id: uuid.UUID, body: EducationObjectiveCreate, user: RootAdminUser, db: DbSession
) -> EducationObjectiveOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: update_objective(
            db,
            row_id=objective_id,
            state_id=body.state_id,
            grade_level=body.grade_level,
            subject_code=body.subject_code,
            objective_type=body.objective_type,
            objective_id=body.objective_id,
            description=body.description,
            coverage_type=body.coverage_type,
            active=body.active,
        )
    )
    return EducationObjectiveOut.model_validate(row)


@router.post("/objectives/import/preview", response_model=CatalogImportPreviewOut)
def preview_catalog_objectives_import(
    body: CatalogImportPreviewIn, user: RootAdminUser, db: DbSession
) -> CatalogImportPreviewOut:
    _require_teacher_assist(db, user)
    try:
        preview = preview_objectives_import(db, csv_content=body.csv_content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return CatalogImportPreviewOut(
        total_rows=preview.total_rows,
        valid_count=preview.valid_count,
        invalid_count=preview.invalid_count,
        duplicate_count=preview.duplicate_count,
        errors=[CatalogImportRowErrorOut(row_number=e.row_number, message=e.message, field=e.field) for e in preview.errors],
    )


@router.post("/objectives/import/commit", response_model=CatalogImportCommitOut)
def commit_catalog_objectives_import(
    body: CatalogObjectiveImportCommitIn, user: RootAdminUser, db: DbSession
) -> CatalogImportCommitOut:
    _require_teacher_assist(db, user)
    result = commit_objectives_import(
        db,
        rows=[row.model_dump() for row in body.rows],
    )
    return CatalogImportCommitOut(
        created_count=result.created_count,
        skipped_duplicate_count=result.skipped_duplicate_count,
        errors=[CatalogImportRowErrorOut(row_number=e.row_number, message=e.message, field=e.field) for e in result.errors],
    )


@router.get("/curriculum-resources", response_model=list[EducationCurriculumResourceOut])
def read_curriculum_resources(
    user: CurrentUser,
    db: DbSession,
    school_id: uuid.UUID | None = None,
    grade_level: str | None = None,
    subject_code: str | None = None,
    resource_type: str | None = None,
    active_only: bool = Query(default=True),
) -> list[EducationCurriculumResourceOut]:
    _require_teacher_assist(db, user)
    return [
        EducationCurriculumResourceOut.model_validate(row)
        for row in list_curriculum_resources(
            db,
            school_id=school_id,
            grade_level=grade_level,
            subject_code=subject_code,
            resource_type=resource_type,
            active_only=active_only,
        )
    ]


@router.post("/curriculum-resources", response_model=EducationCurriculumResourceOut, status_code=201)
def create_catalog_curriculum_resource(
    body: EducationCurriculumResourceCreate, user: RootAdminUser, db: DbSession
) -> EducationCurriculumResourceOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: create_curriculum_resource(
            db,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            grade_level=body.grade_level,
            subject_code=body.subject_code,
            resource_type=body.resource_type,
            title=body.title,
            description=body.description,
            storage_key=body.storage_key,
            active=body.active,
        )
    )
    return EducationCurriculumResourceOut.model_validate(row)


@router.put("/curriculum-resources/{resource_id}", response_model=EducationCurriculumResourceOut)
def update_catalog_curriculum_resource(
    resource_id: uuid.UUID, body: EducationCurriculumResourceCreate, user: RootAdminUser, db: DbSession
) -> EducationCurriculumResourceOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: update_curriculum_resource(
            db,
            resource_id=resource_id,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            grade_level=body.grade_level,
            subject_code=body.subject_code,
            resource_type=body.resource_type,
            title=body.title,
            description=body.description,
            storage_key=body.storage_key,
            active=body.active,
        )
    )
    return EducationCurriculumResourceOut.model_validate(row)


@router.get("/resource-links", response_model=list[EducationResourceLinkOut])
def read_resource_links(
    user: CurrentUser,
    db: DbSession,
    curriculum_resource_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationResourceLinkOut]:
    _require_teacher_assist(db, user)
    return [
        EducationResourceLinkOut.model_validate(row)
        for row in list_resource_links(db, curriculum_resource_id=curriculum_resource_id, active_only=active_only)
    ]


@router.post("/resource-links", response_model=EducationResourceLinkOut, status_code=201)
def create_catalog_resource_link(
    body: EducationResourceLinkCreate, user: RootAdminUser, db: DbSession
) -> EducationResourceLinkOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: create_resource_link(
            db,
            curriculum_resource_id=body.curriculum_resource_id,
            link_title=body.link_title,
            url=body.url,
            active=body.active,
        )
    )
    return EducationResourceLinkOut.model_validate(row)


@router.put("/resource-links/{link_id}", response_model=EducationResourceLinkOut)
def update_catalog_resource_link(
    link_id: uuid.UUID, body: EducationResourceLinkCreate, user: RootAdminUser, db: DbSession
) -> EducationResourceLinkOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: update_resource_link(
            db,
            link_id=link_id,
            curriculum_resource_id=body.curriculum_resource_id,
            link_title=body.link_title,
            url=body.url,
            active=body.active,
        )
    )
    return EducationResourceLinkOut.model_validate(row)


@router.get("/teacher-assignments", response_model=list[TeacherSchoolAssignmentListOut])
def read_teacher_assignments(
    user: CurrentUser,
    db: DbSession,
    user_id: uuid.UUID | None = None,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[TeacherSchoolAssignmentListOut]:
    _require_teacher_assist(db, user)
    return [
        TeacherSchoolAssignmentListOut(
            **TeacherSchoolAssignmentOut.model_validate(row.assignment).model_dump(),
            user_email=row.user_email,
            user_full_name=row.user_full_name,
            state_name=row.state_name,
            district_name=row.district_name,
            school_name=row.school_name,
        )
        for row in list_teacher_assignment_rows(
            db,
            user_id=user_id,
            state_id=state_id,
            district_id=district_id,
            school_id=school_id,
            active_only=active_only,
        )
    ]


@router.get("/teacher-assignments/available-teachers", response_model=list[AvailableTeacherOut])
def read_available_teachers_for_school(
    user: RootAdminUser,
    db: DbSession,
    school_id: uuid.UUID = Query(...),
    q: str | None = Query(default=None, max_length=120),
    limit: int = Query(default=25, ge=1, le=50),
) -> list[AvailableTeacherOut]:
    _require_teacher_assist(db, user)
    return [
        AvailableTeacherOut(user_id=row.user_id, email=row.email, full_name=row.full_name)
        for row in search_available_teachers_for_school(db, school_id=school_id, q=q, limit=limit)
    ]


@router.post(
    "/teacher-assignments/provision",
    response_model=TeacherSchoolAssignmentProvisionOut,
    status_code=201,
)
def provision_catalog_teacher_assignment(
    body: TeacherSchoolAssignmentProvision, user: RootAdminUser, db: DbSession
) -> TeacherSchoolAssignmentProvisionOut:
    _require_teacher_assist(db, user)
    if body.user_id is None and not body.email:
        raise HTTPException(status_code=422, detail="Provide user_id or email for teacher assignment")
    row = _handle_value_errors(
        lambda: provision_teacher_school_assignment(
            db,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            active=body.active,
            user_id=body.user_id,
            email=body.email,
            full_name=body.full_name,
            tenant_name=body.tenant_name,
            catalog_grade_id=body.catalog_grade_id,
        )
    )
    return TeacherSchoolAssignmentProvisionOut(
        assignment=TeacherSchoolAssignmentOut.model_validate(row.assignment),
        user_id=row.user_id,
        email=row.email,
        full_name=row.full_name,
        created_user=row.created_user,
        temporary_password=row.temporary_password,
        grade_setup_applied=row.grade_setup_applied,
    )


@router.post("/teacher-assignments", response_model=TeacherSchoolAssignmentOut, status_code=201)
def create_catalog_teacher_assignment(
    body: TeacherSchoolAssignmentCreate, user: RootAdminUser, db: DbSession
) -> TeacherSchoolAssignmentOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: create_teacher_assignment(
            db,
            user_id=body.user_id,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            active=body.active,
        )
    )
    return TeacherSchoolAssignmentOut.model_validate(row)


@router.put("/teacher-assignments/{assignment_id}", response_model=TeacherSchoolAssignmentOut)
def update_catalog_teacher_assignment(
    assignment_id: uuid.UUID, body: TeacherSchoolAssignmentCreate, user: RootAdminUser, db: DbSession
) -> TeacherSchoolAssignmentOut:
    _require_teacher_assist(db, user)
    row = _handle_value_errors(
        lambda: update_teacher_assignment(
            db,
            assignment_id=assignment_id,
            user_id=body.user_id,
            state_id=body.state_id,
            district_id=body.district_id,
            school_id=body.school_id,
            active=body.active,
        )
    )
    return TeacherSchoolAssignmentOut.model_validate(row)

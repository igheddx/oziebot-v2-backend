"""Teacher self-service school placement and catalog-aligned subject selection."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationGrade,
    EducationSubject,
    TeacherSchoolAssignment,
)
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.education_catalog import (
    get_active_teacher_assignment,
    get_district_or_404,
    get_grade_or_404,
    get_school_or_404,
    get_state_or_404,
)
from oziebot_api.services.teacher_assist.setup import (
    create_subject,
    get_teacher_profile,
    upsert_teacher_profile,
)
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create


def _now() -> datetime:
    return datetime.now(UTC)


def upsert_my_school_assignment(
    db: Session,
    *,
    user_id: uuid.UUID,
    state_id: uuid.UUID,
    district_id: uuid.UUID,
    school_id: uuid.UUID,
) -> TeacherSchoolAssignment:
    get_state_or_404(db, state_id)
    district = get_district_or_404(db, district_id)
    if district.state_id != state_id:
        raise ValueError("District does not belong to the selected state")
    school = get_school_or_404(db, school_id)
    if school.district_id != district_id:
        raise ValueError("School does not belong to the selected district")

    existing_rows = db.scalars(
        select(TeacherSchoolAssignment).where(TeacherSchoolAssignment.user_id == user_id)
    ).all()
    now = _now()
    active_row: TeacherSchoolAssignment | None = None
    for row in existing_rows:
        if (
            row.state_id == state_id
            and row.district_id == district_id
            and row.school_id == school_id
        ):
            row.active = True
            row.updated_at = now
            active_row = row
        else:
            row.active = False
            row.updated_at = now
    if active_row is None:
        active_row = TeacherSchoolAssignment(
            user_id=user_id,
            state_id=state_id,
            district_id=district_id,
            school_id=school_id,
            active=True,
            created_at=now,
            updated_at=now,
        )
        db.add(active_row)
    db.flush()
    return active_row


def _tenant_subject_for_catalog(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    catalog_subject: EducationSubject,
) -> TeacherAssistSubject:
    code = catalog_subject.subject_code.strip()
    existing = db.scalars(
        select(TeacherAssistSubject).where(
            TeacherAssistSubject.tenant_id == tenant_id,
            TeacherAssistSubject.code == code,
        )
    ).first()
    if existing is not None:
        if existing.name != catalog_subject.display_name:
            existing.name = catalog_subject.display_name
            existing.updated_at = _now()
            db.flush()
        return existing
    return create_subject(
        db,
        tenant_id=tenant_id,
        code=code,
        name=catalog_subject.display_name,
    )


def _store_catalog_subject_selection(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    catalog_grade_id: uuid.UUID,
    catalog_subject_ids: list[str],
) -> None:
    prefs = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    progress = dict(prefs.onboarding_progress_json or {})
    progress["school_setup"] = {
        "catalog_grade_id": str(catalog_grade_id),
        "catalog_subject_ids": catalog_subject_ids,
    }
    prefs.onboarding_progress_json = progress
    prefs.updated_at = _now()
    db.flush()


def _load_catalog_subject_selection(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> tuple[str | None, list[str]]:
    prefs = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    school_setup = (prefs.onboarding_progress_json or {}).get("school_setup") or {}
    grade_id = school_setup.get("catalog_grade_id")
    subject_ids = school_setup.get("catalog_subject_ids") or []
    if not isinstance(subject_ids, list):
        return grade_id if isinstance(grade_id, str) else None, []
    return grade_id if isinstance(grade_id, str) else None, [
        str(subject_id) for subject_id in subject_ids
    ]


def _prune_deselected_tenant_subjects(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    catalog_subjects: list[EducationSubject],
    selected_subjects: list[EducationSubject],
) -> None:
    selected_codes = {subject.subject_code for subject in selected_subjects}
    deselected_codes = {
        subject.subject_code
        for subject in catalog_subjects
        if subject.subject_code not in selected_codes
    }
    if not deselected_codes:
        return
    tenant_subjects = db.scalars(
        select(TeacherAssistSubject).where(
            TeacherAssistSubject.tenant_id == tenant_id,
            TeacherAssistSubject.code.in_(deselected_codes),
        )
    ).all()
    for tenant_subject in tenant_subjects:
        db.delete(tenant_subject)
    db.flush()


def sync_my_teaching_subjects(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    catalog_grade_id: uuid.UUID,
    catalog_subject_ids: list[uuid.UUID] | None = None,
) -> tuple[EducationGrade, list[dict[str, str]]]:
    assignment = get_active_teacher_assignment(db, user_id=user_id)
    if assignment is None:
        raise ValueError("Save your school placement before syncing subjects")

    grade = get_grade_or_404(db, catalog_grade_id)
    if grade.school_id != assignment.school_id:
        raise ValueError("Selected grade does not belong to your assigned school")

    catalog_subjects = db.scalars(
        select(EducationSubject)
        .where(EducationSubject.grade_id == grade.id, EducationSubject.active.is_(True))
        .order_by(EducationSubject.subject_code.asc())
    ).all()
    if not catalog_subjects:
        raise ValueError("No catalog subjects are published for this grade yet")

    if catalog_subject_ids is None:
        selected_subjects = catalog_subjects
    else:
        selected_ids = {str(subject_id) for subject_id in catalog_subject_ids}
        selected_subjects = [
            subject for subject in catalog_subjects if str(subject.id) in selected_ids
        ]
        if not selected_subjects:
            raise ValueError("Select at least one district subject for your grade")
        invalid_ids = selected_ids - {str(subject.id) for subject in selected_subjects}
        if invalid_ids:
            raise ValueError("One or more selected subjects do not belong to this grade")

    synced: list[dict[str, str]] = []
    for catalog_subject in selected_subjects:
        tenant_subject = _tenant_subject_for_catalog(
            db,
            tenant_id=tenant_id,
            catalog_subject=catalog_subject,
        )
        synced.append(
            {
                "catalog_subject_id": str(catalog_subject.id),
                "tenant_subject_id": str(tenant_subject.id),
                "subject_code": catalog_subject.subject_code,
                "display_name": catalog_subject.display_name,
            }
        )

    if catalog_subject_ids is not None:
        _prune_deselected_tenant_subjects(
            db,
            tenant_id=tenant_id,
            catalog_subjects=catalog_subjects,
            selected_subjects=selected_subjects,
        )

    _store_catalog_subject_selection(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        catalog_grade_id=grade.id,
        catalog_subject_ids=[str(subject.id) for subject in selected_subjects],
    )

    user = db.get(User, user_id)
    if user is None:
        raise LookupError("User not found")
    profile = get_teacher_profile(db, user_id=user_id)
    upsert_teacher_profile(
        db,
        user=user,
        preferred_grade_level=grade.grade_code,
        default_student_count=profile.default_student_count if profile else None,
        preferred_grading_period_type=profile.preferred_grading_period_type if profile else None,
        timezone=profile.timezone if profile else None,
    )
    _sync_homeroom_after_subject_change(db, tenant_id=tenant_id, user_id=user_id)
    return grade, synced


def _sync_homeroom_after_subject_change(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID
) -> None:
    from oziebot_api.services.teacher_assist.teacher_classroom import (
        sync_homeroom_class_subjects_from_school_setup,
    )

    sync_homeroom_class_subjects_from_school_setup(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
    )


def build_my_school_setup(db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID) -> dict:
    assignment = get_active_teacher_assignment(db, user_id=user_id)
    profile = get_teacher_profile(db, user_id=user_id)
    if assignment is None:
        return {
            "assignment": None,
            "catalog_grade_id": None,
            "catalog_grade_code": profile.preferred_grade_level if profile else None,
            "selected_catalog_subject_ids": [],
            "synced_subjects": [],
        }

    state = get_state_or_404(db, assignment.state_id)
    district = get_district_or_404(db, assignment.district_id)
    school = get_school_or_404(db, assignment.school_id)
    grades = db.scalars(
        select(EducationGrade)
        .where(EducationGrade.school_id == school.id, EducationGrade.active.is_(True))
        .order_by(EducationGrade.grade_code.asc())
    ).all()
    selected_grade = next(
        (
            grade
            for grade in grades
            if grade.grade_code == (profile.preferred_grade_level if profile else None)
        ),
        None,
    )

    catalog_subjects: list[EducationSubject] = []
    if selected_grade is not None:
        catalog_subjects = db.scalars(
            select(EducationSubject)
            .where(
                EducationSubject.grade_id == selected_grade.id, EducationSubject.active.is_(True)
            )
            .order_by(EducationSubject.subject_code.asc())
        ).all()

    tenant_subjects = db.scalars(
        select(TeacherAssistSubject).where(TeacherAssistSubject.tenant_id == tenant_id)
    ).all()
    tenant_by_code = {row.code: row for row in tenant_subjects if row.code}

    stored_grade_id, stored_subject_ids = _load_catalog_subject_selection(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    valid_catalog_ids = {str(subject.id) for subject in catalog_subjects}
    if (
        selected_grade is not None
        and stored_grade_id == str(selected_grade.id)
        and stored_subject_ids
    ):
        selected_catalog_subject_ids = [
            subject_id for subject_id in stored_subject_ids if subject_id in valid_catalog_ids
        ]
    else:
        selected_catalog_subject_ids = [
            str(subject.id)
            for subject in catalog_subjects
            if subject.subject_code in tenant_by_code
        ]

    selected_catalog_id_set = set(selected_catalog_subject_ids)
    synced_subjects = [
        {
            "catalog_subject_id": str(subject.id),
            "tenant_subject_id": str(tenant_by_code[subject.subject_code].id),
            "subject_code": subject.subject_code,
            "display_name": subject.display_name,
        }
        for subject in catalog_subjects
        if str(subject.id) in selected_catalog_id_set and subject.subject_code in tenant_by_code
    ]

    return {
        "assignment": {
            "id": str(assignment.id),
            "state_id": str(state.id),
            "state_name": state.name,
            "state_abbreviation": state.abbreviation,
            "district_id": str(district.id),
            "district_name": district.name,
            "school_id": str(school.id),
            "school_name": school.name,
            "school_type": school.school_type,
        },
        "catalog_grade_id": str(selected_grade.id) if selected_grade else None,
        "catalog_grade_code": selected_grade.grade_code if selected_grade else None,
        "selected_catalog_subject_ids": selected_catalog_subject_ids,
        "synced_subjects": synced_subjects,
    }

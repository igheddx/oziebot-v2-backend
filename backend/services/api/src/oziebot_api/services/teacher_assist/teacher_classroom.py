"""Teacher homeroom setup derived from catalog placement."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_class_subject import TeacherAssistClassSubject
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.education_catalog import get_active_teacher_assignment
from oziebot_api.services.teacher_assist.setup import (
    attach_class_subject,
    create_class,
    get_teacher_profile,
    update_class,
    upsert_teacher_profile,
)
from oziebot_api.services.teacher_assist.teacher_school_setup import build_my_school_setup


def get_active_school_year(db: Session, *, tenant_id: uuid.UUID) -> TeacherAssistSchoolYear | None:
    return db.scalars(
        select(TeacherAssistSchoolYear).where(
            TeacherAssistSchoolYear.tenant_id == tenant_id,
            TeacherAssistSchoolYear.is_active.is_(True),
            TeacherAssistSchoolYear.is_template.is_(False),
        )
    ).first()


def _resolve_homeroom_class(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grade_level: str,
) -> TeacherAssistClass | None:
    classes = db.scalars(
        select(TeacherAssistClass).where(
            TeacherAssistClass.tenant_id == tenant_id,
            TeacherAssistClass.school_year_id == school_year_id,
            TeacherAssistClass.grade_level == grade_level,
        )
    ).all()
    if not classes:
        return None
    if len(classes) == 1:
        return classes[0]
    homeroom = next((row for row in classes if "homeroom" in row.name.lower()), None)
    return homeroom or classes[0]


def _sync_class_subjects(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    class_id: uuid.UUID,
    subject_ids: list[uuid.UUID] | None = None,
) -> None:
    if subject_ids is None:
        subjects = db.scalars(
            select(TeacherAssistSubject).where(TeacherAssistSubject.tenant_id == tenant_id)
        ).all()
        subject_ids = [subject.id for subject in subjects]

    existing = db.scalars(
        select(TeacherAssistClassSubject).where(TeacherAssistClassSubject.class_id == class_id)
    ).all()
    desired = set(subject_ids)
    for row in existing:
        if row.subject_id not in desired:
            db.delete(row)
    for subject_id in subject_ids:
        attach_class_subject(
            db,
            tenant_id=tenant_id,
            class_id=class_id,
            subject_id=subject_id,
        )
    db.flush()


def sync_homeroom_class_subjects_from_school_setup(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> None:
    school_setup = build_my_school_setup(db, tenant_id=tenant_id, user_id=user_id)
    synced_subjects = school_setup.get("synced_subjects") or []
    grade_level = school_setup.get("catalog_grade_code")
    active_school_year = get_active_school_year(db, tenant_id=tenant_id)
    if active_school_year is None or not grade_level:
        return

    homeroom = _resolve_homeroom_class(
        db,
        tenant_id=tenant_id,
        school_year_id=active_school_year.id,
        grade_level=grade_level,
    )
    if homeroom is None:
        return

    subject_ids = [uuid.UUID(row["tenant_subject_id"]) for row in synced_subjects]
    _sync_class_subjects(
        db,
        tenant_id=tenant_id,
        class_id=homeroom.id,
        subject_ids=subject_ids,
    )


def build_my_classroom(db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID) -> dict:
    school_setup = build_my_school_setup(db, tenant_id=tenant_id, user_id=user_id)
    profile = get_teacher_profile(db, user_id=user_id)
    active_school_year = get_active_school_year(db, tenant_id=tenant_id)
    grade_level = school_setup.get("catalog_grade_code") or (
        profile.preferred_grade_level if profile else None
    )
    homeroom: TeacherAssistClass | None = None
    if active_school_year is not None and grade_level:
        homeroom = _resolve_homeroom_class(
            db,
            tenant_id=tenant_id,
            school_year_id=active_school_year.id,
            grade_level=grade_level,
        )

    default_name = f"Grade {grade_level} Homeroom" if grade_level else "Homeroom"
    return {
        "grade_level": grade_level,
        "grade_display_name": school_setup.get("catalog_grade_code"),
        "homeroom_name": homeroom.name if homeroom else default_name,
        "student_count": (
            homeroom.student_count
            if homeroom is not None
            else (profile.default_student_count if profile else None)
        ),
        "timezone": profile.timezone if profile else None,
        "class_id": str(homeroom.id) if homeroom else None,
        "synced_subjects": school_setup.get("synced_subjects") or [],
        "has_active_school_year": active_school_year is not None,
        "requires_school_setup": school_setup.get("assignment") is None,
        "active_school_year_id": str(active_school_year.id) if active_school_year else None,
        "active_school_year_title": active_school_year.title if active_school_year else None,
        "active_school_year_start_date": (
            active_school_year.start_date.isoformat() if active_school_year and active_school_year.start_date else None
        ),
        "active_school_year_end_date": (
            active_school_year.end_date.isoformat() if active_school_year and active_school_year.end_date else None
        ),
    }


def upsert_my_classroom(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    homeroom_name: str,
    student_count: int,
    timezone: str | None,
) -> dict:
    if student_count <= 0:
        raise ValueError("Student count must be greater than zero")
    if not homeroom_name.strip():
        raise ValueError("Homeroom name is required")

    assignment = get_active_teacher_assignment(db, user_id=user.id)
    if assignment is None:
        raise ValueError("Complete school & district setup before configuring your classroom")

    profile = get_teacher_profile(db, user_id=user.id)
    grade_level = profile.preferred_grade_level if profile else None
    if not grade_level:
        raise ValueError("Grade level is not set. Save school & district setup first")

    active_school_year = get_active_school_year(db, tenant_id=tenant_id)
    if active_school_year is None:
        raise ValueError("Create an active school year before configuring your classroom")

    upsert_teacher_profile(
        db,
        user=user,
        preferred_grade_level=grade_level,
        default_student_count=student_count,
        preferred_grading_period_type=profile.preferred_grading_period_type if profile else None,
        timezone=timezone,
    )

    homeroom = _resolve_homeroom_class(
        db,
        tenant_id=tenant_id,
        school_year_id=active_school_year.id,
        grade_level=grade_level,
    )
    if homeroom is None:
        homeroom = create_class(
            db,
            tenant_id=tenant_id,
            school_year_id=active_school_year.id,
            name=homeroom_name.strip(),
            grade_level=grade_level,
            student_count=student_count,
        )
    else:
        homeroom = update_class(
            db,
            tenant_id=tenant_id,
            class_id=homeroom.id,
            school_year_id=active_school_year.id,
            name=homeroom_name.strip(),
            grade_level=grade_level,
            student_count=student_count,
        )

    school_setup = build_my_school_setup(db, tenant_id=tenant_id, user_id=user.id)
    synced_subjects = school_setup.get("synced_subjects") or []
    _sync_class_subjects(
        db,
        tenant_id=tenant_id,
        class_id=homeroom.id,
        subject_ids=[uuid.UUID(row["tenant_subject_id"]) for row in synced_subjects],
    )
    return build_my_classroom(db, tenant_id=tenant_id, user_id=user.id)

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_class_subject import TeacherAssistClassSubject
from oziebot_api.models.teacher_assist_mastery_matrix import TeacherAssistMasteryMatrix
from oziebot_api.models.teacher_assist_mastery_matrix_standard import TeacherAssistMasteryMatrixStandard
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.services.teacher_assist.constants import (
    validate_mastery_level,
    validate_mastery_matrix_status,
)
from oziebot_api.services.teacher_assist.planning import get_standard_or_404
from oziebot_api.services.teacher_assist.setup import (
    get_class_or_404,
    get_grading_period_or_404,
    get_school_year_or_404,
    get_subject_or_404,
)


def _normalize_string(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _validate_matrix_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
) -> None:
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    teacher_class = get_class_or_404(db, tenant_id=tenant_id, class_id=class_id)
    subject = get_subject_or_404(db, tenant_id=tenant_id, subject_id=subject_id)

    if teacher_class.school_year_id != school_year.id:
        raise ValueError("Class must belong to the selected school year")

    if grading_period_id is not None:
        grading_period = get_grading_period_or_404(
            db,
            tenant_id=tenant_id,
            grading_period_id=grading_period_id,
        )
        if grading_period.school_year_id != school_year.id:
            raise ValueError("Grading period must belong to the selected school year")

    class_subject_ids = {
        row.subject_id
        for row in db.scalars(
            select(TeacherAssistClassSubject).where(TeacherAssistClassSubject.class_id == teacher_class.id)
        ).all()
    }
    if class_subject_ids and subject.id not in class_subject_ids:
        raise ValueError("Selected subject must be attached to the selected class")


def _validate_matrix_standards(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    subject_id: uuid.UUID,
    school_year_id: uuid.UUID,
    standard_ids: list[uuid.UUID],
) -> list[TeacherAssistStandard]:
    if not standard_ids:
        raise ValueError("At least one standard is required for a mastery matrix")
    standards = [get_standard_or_404(db, tenant_id=tenant_id, standard_id=standard_id) for standard_id in standard_ids]
    for standard in standards:
        if standard.subject_id is not None and standard.subject_id != subject_id:
            raise ValueError("Standards must belong to the selected subject")
        if standard.school_year_id is not None and standard.school_year_id != school_year_id:
            raise ValueError("Standards must belong to the selected school year")
    return standards


def get_mastery_matrix_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    load_standards: bool = False,
) -> TeacherAssistMasteryMatrix:
    query = select(TeacherAssistMasteryMatrix).where(
        TeacherAssistMasteryMatrix.id == mastery_matrix_id,
        TeacherAssistMasteryMatrix.tenant_id == tenant_id,
        TeacherAssistMasteryMatrix.owner_user_id == user_id,
    )
    if load_standards:
        query = query.options(
            selectinload(TeacherAssistMasteryMatrix.matrix_standards).selectinload(
                TeacherAssistMasteryMatrixStandard.standard
            )
        )
    row = db.scalars(query).one_or_none()
    if row is None:
        raise LookupError("Mastery matrix not found")
    return row


def get_matrix_standard_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    standard_id: uuid.UUID,
) -> TeacherAssistMasteryMatrixStandard:
    get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
    )
    row = db.scalars(
        select(TeacherAssistMasteryMatrixStandard).where(
            TeacherAssistMasteryMatrixStandard.tenant_id == tenant_id,
            TeacherAssistMasteryMatrixStandard.mastery_matrix_id == mastery_matrix_id,
            TeacherAssistMasteryMatrixStandard.standard_id == standard_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Standard is not tracked in this mastery matrix")
    return row


def list_mastery_matrices(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list[TeacherAssistMasteryMatrix]:
    query = select(TeacherAssistMasteryMatrix).where(
        TeacherAssistMasteryMatrix.tenant_id == tenant_id,
        TeacherAssistMasteryMatrix.owner_user_id == user_id,
    )
    if school_year_id is not None:
        query = query.where(TeacherAssistMasteryMatrix.school_year_id == school_year_id)
    if grading_period_id is not None:
        query = query.where(TeacherAssistMasteryMatrix.grading_period_id == grading_period_id)
    if class_id is not None:
        query = query.where(TeacherAssistMasteryMatrix.class_id == class_id)
    if subject_id is not None:
        query = query.where(TeacherAssistMasteryMatrix.subject_id == subject_id)
    if status is not None:
        query = query.where(TeacherAssistMasteryMatrix.status == validate_mastery_matrix_status(status))
    return db.scalars(query.order_by(TeacherAssistMasteryMatrix.updated_at.desc())).all()


def create_mastery_matrix(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    title: str,
    status: str = "active",
    standard_ids: list[uuid.UUID],
    target_mastery_level: str = "mastery",
) -> TeacherAssistMasteryMatrix:
    normalized_title = _normalize_string(title)
    if not normalized_title:
        raise ValueError("Mastery matrix title is required")

    _validate_matrix_context(
        db,
        tenant_id=tenant_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
    )
    standards = _validate_matrix_standards(
        db,
        tenant_id=tenant_id,
        subject_id=subject_id,
        school_year_id=school_year_id,
        standard_ids=standard_ids,
    )
    normalized_target = validate_mastery_level(target_mastery_level)
    now = datetime.now(UTC)

    matrix = TeacherAssistMasteryMatrix(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        title=normalized_title,
        status=validate_mastery_matrix_status(status),
        created_at=now,
        updated_at=now,
    )
    db.add(matrix)
    db.flush()

    for index, standard in enumerate(standards):
        db.add(
            TeacherAssistMasteryMatrixStandard(
                tenant_id=tenant_id,
                mastery_matrix_id=matrix.id,
                standard_id=standard.id,
                display_order=index,
                target_mastery_level=normalized_target,
                assessment_count=0,
                created_at=now,
                updated_at=now,
            )
        )
    db.flush()
    return get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=matrix.id,
        load_standards=True,
    )


def update_mastery_matrix(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    title: str | None = None,
    status: str | None = None,
    standard_ids: list[uuid.UUID] | None = None,
    target_mastery_level: str | None = None,
) -> TeacherAssistMasteryMatrix:
    matrix = get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        load_standards=True,
    )
    now = datetime.now(UTC)

    if title is not None:
        normalized_title = _normalize_string(title)
        if not normalized_title:
            raise ValueError("Mastery matrix title is required")
        matrix.title = normalized_title

    if status is not None:
        matrix.status = validate_mastery_matrix_status(status)

    if standard_ids is not None:
        standards = _validate_matrix_standards(
            db,
            tenant_id=tenant_id,
            subject_id=matrix.subject_id,
            school_year_id=matrix.school_year_id,
            standard_ids=standard_ids,
        )
        existing_by_standard = {row.standard_id: row for row in matrix.matrix_standards}
        desired_ids = [standard.id for standard in standards]
        for row in list(matrix.matrix_standards):
            if row.standard_id not in desired_ids:
                db.delete(row)
        normalized_target = validate_mastery_level(target_mastery_level or "mastery")
        for index, standard in enumerate(standards):
            existing = existing_by_standard.get(standard.id)
            if existing is None:
                db.add(
                    TeacherAssistMasteryMatrixStandard(
                        tenant_id=tenant_id,
                        mastery_matrix_id=matrix.id,
                        standard_id=standard.id,
                        display_order=index,
                        target_mastery_level=normalized_target,
                        assessment_count=0,
                        created_at=now,
                        updated_at=now,
                    )
                )
            else:
                existing.display_order = index
                existing.updated_at = now
                if target_mastery_level is not None:
                    existing.target_mastery_level = normalized_target
    elif target_mastery_level is not None:
        normalized_target = validate_mastery_level(target_mastery_level)
        for row in matrix.matrix_standards:
            row.target_mastery_level = normalized_target
            row.updated_at = now

    matrix.updated_at = now
    db.flush()
    return get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=matrix.id,
        load_standards=True,
    )


def serialize_mastery_matrix(matrix: TeacherAssistMasteryMatrix) -> dict[str, Any]:
    return {
        "id": matrix.id,
        "tenant_id": matrix.tenant_id,
        "owner_user_id": matrix.owner_user_id,
        "school_year_id": matrix.school_year_id,
        "grading_period_id": matrix.grading_period_id,
        "class_id": matrix.class_id,
        "subject_id": matrix.subject_id,
        "title": matrix.title,
        "status": matrix.status,
        "created_at": matrix.created_at,
        "updated_at": matrix.updated_at,
        "standards": [
            {
                "id": row.id,
                "standard_id": row.standard_id,
                "display_order": row.display_order,
                "target_mastery_level": row.target_mastery_level,
                "assessment_count": row.assessment_count,
                "standard_code": row.standard.code if row.standard else None,
                "standard_description": row.standard.description if row.standard else None,
            }
            for row in sorted(matrix.matrix_standards, key=lambda item: item.display_order)
        ],
    }

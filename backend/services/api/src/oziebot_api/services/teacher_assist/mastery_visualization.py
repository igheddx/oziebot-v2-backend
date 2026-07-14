from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
from oziebot_api.models.teacher_assist_mastery_matrix import TeacherAssistMasteryMatrix
from oziebot_api.models.teacher_assist_mastery_matrix_standard import (
    TeacherAssistMasteryMatrixStandard,
)
from oziebot_api.services.teacher_assist.constants import MASTERY_LEVEL_RANK, MASTERY_LEVELS
from oziebot_api.services.teacher_assist.mastery_commit_service import list_mastery_evaluations
from oziebot_api.services.teacher_assist.mastery_matrix import get_mastery_matrix_or_404


def _level_distribution() -> dict[str, int]:
    return {level: 0 for level in MASTERY_LEVELS}


def _needs_reteach(mastery_level: str | None, target_level: str) -> bool:
    if mastery_level is None or mastery_level == "not_assessed":
        return True
    current_rank = MASTERY_LEVEL_RANK.get(mastery_level, 0)
    target_rank = MASTERY_LEVEL_RANK.get(target_level, MASTERY_LEVEL_RANK["mastery"])
    return current_rank < target_rank


def build_mastery_matrix_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
) -> dict[str, Any]:
    matrix = get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        load_standards=True,
    )
    evaluations = list_mastery_evaluations(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
    )
    active_evaluations = [row for row in evaluations if row.evaluation_status == "active"]
    draft_evaluations = [row for row in evaluations if row.evaluation_status == "draft"]
    reversed_evaluations = [row for row in evaluations if row.evaluation_status == "reversed"]

    distribution = _level_distribution()
    for row in active_evaluations:
        distribution[row.mastery_level] = distribution.get(row.mastery_level, 0) + 1

    tracked_standard_count = len(matrix.matrix_standards)
    assessed_standard_ids = {row.standard_id for row in active_evaluations}
    unassessed_standard_count = sum(
        1 for row in matrix.matrix_standards if row.standard_id not in assessed_standard_ids
    )

    reteach_candidates = 0
    for row in active_evaluations:
        matrix_standard = next(
            (item for item in matrix.matrix_standards if item.standard_id == row.standard_id),
            None,
        )
        target_level = matrix_standard.target_mastery_level if matrix_standard else "mastery"
        if _needs_reteach(row.mastery_level, target_level):
            reteach_candidates += 1

    student_numbers = sorted({row.student_number for row in evaluations})
    return {
        "mastery_matrix_id": matrix.id,
        "title": matrix.title,
        "status": matrix.status,
        "class_id": matrix.class_id,
        "subject_id": matrix.subject_id,
        "school_year_id": matrix.school_year_id,
        "grading_period_id": matrix.grading_period_id,
        "tracked_standard_count": tracked_standard_count,
        "active_evaluation_count": len(active_evaluations),
        "draft_evaluation_count": len(draft_evaluations),
        "reversed_evaluation_count": len(reversed_evaluations),
        "student_count": len(student_numbers),
        "unassessed_standard_count": unassessed_standard_count,
        "reteach_candidate_count": reteach_candidates,
        "mastery_distribution": distribution,
    }


def build_mastery_matrix_standards_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
) -> dict[str, Any]:
    matrix = get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        load_standards=True,
    )
    evaluations = list_mastery_evaluations(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        evaluation_status="active",
    )
    by_standard: dict[uuid.UUID, list[TeacherAssistMasteryEvaluation]] = {}
    for row in evaluations:
        by_standard.setdefault(row.standard_id, []).append(row)

    standards_summary: list[dict[str, Any]] = []
    for matrix_standard in sorted(matrix.matrix_standards, key=lambda item: item.display_order):
        rows = by_standard.get(matrix_standard.standard_id, [])
        distribution = _level_distribution()
        reteach_count = 0
        for row in rows:
            distribution[row.mastery_level] = distribution.get(row.mastery_level, 0) + 1
            if _needs_reteach(row.mastery_level, matrix_standard.target_mastery_level):
                reteach_count += 1
        standards_summary.append(
            {
                "matrix_standard_id": matrix_standard.id,
                "standard_id": matrix_standard.standard_id,
                "standard_code": matrix_standard.standard.code
                if matrix_standard.standard
                else None,
                "standard_description": matrix_standard.standard.description
                if matrix_standard.standard
                else None,
                "display_order": matrix_standard.display_order,
                "target_mastery_level": matrix_standard.target_mastery_level,
                "assessment_count": matrix_standard.assessment_count,
                "active_evaluation_count": len(rows),
                "reteach_candidate_count": reteach_count,
                "mastery_distribution": distribution,
                "is_unassessed": len(rows) == 0,
            }
        )

    return {
        "mastery_matrix_id": matrix.id,
        "standards": standards_summary,
    }


def build_mastery_matrix_students_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
) -> dict[str, Any]:
    matrix = get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        load_standards=True,
    )
    evaluations = list_mastery_evaluations(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
    )
    standards_by_id = {row.standard_id: row for row in matrix.matrix_standards}
    by_student: dict[int, list[TeacherAssistMasteryEvaluation]] = {}
    for row in evaluations:
        by_student.setdefault(row.student_number, []).append(row)

    students_summary: list[dict[str, Any]] = []
    for student_number in sorted(by_student):
        rows = by_student[student_number]
        active_rows = [row for row in rows if row.evaluation_status == "active"]
        draft_rows = [row for row in rows if row.evaluation_status == "draft"]
        reteach_count = 0
        cells: list[dict[str, Any]] = []
        for row in rows:
            matrix_standard = standards_by_id.get(row.standard_id)
            target_level = matrix_standard.target_mastery_level if matrix_standard else "mastery"
            needs_reteach = row.evaluation_status == "active" and _needs_reteach(
                row.mastery_level, target_level
            )
            if needs_reteach:
                reteach_count += 1
            cells.append(
                {
                    "evaluation_id": row.id,
                    "standard_id": row.standard_id,
                    "evaluation_status": row.evaluation_status,
                    "mastery_level": row.mastery_level,
                    "target_mastery_level": target_level,
                    "needs_reteach": needs_reteach,
                    "confirmed_at": row.confirmed_at,
                }
            )
        students_summary.append(
            {
                "student_number": student_number,
                "active_evaluation_count": len(active_rows),
                "draft_evaluation_count": len(draft_rows),
                "reteach_candidate_count": reteach_count,
                "cells": cells,
            }
        )

    return {
        "mastery_matrix_id": matrix.id,
        "students": students_summary,
    }


def build_mastery_matrix_reteach_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
) -> dict[str, Any]:
    matrix = db.scalars(
        select(TeacherAssistMasteryMatrix)
        .where(
            TeacherAssistMasteryMatrix.id == mastery_matrix_id,
            TeacherAssistMasteryMatrix.tenant_id == tenant_id,
            TeacherAssistMasteryMatrix.owner_user_id == user_id,
        )
        .options(
            selectinload(TeacherAssistMasteryMatrix.matrix_standards).selectinload(
                TeacherAssistMasteryMatrixStandard.standard
            )
        )
    ).one_or_none()
    if matrix is None:
        raise LookupError("Mastery matrix not found")

    evaluations = list_mastery_evaluations(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        evaluation_status="active",
    )
    standards_by_id = {row.standard_id: row for row in matrix.matrix_standards}

    reteach_items: list[dict[str, Any]] = []
    for row in evaluations:
        matrix_standard = standards_by_id.get(row.standard_id)
        target_level = matrix_standard.target_mastery_level if matrix_standard else "mastery"
        if not _needs_reteach(row.mastery_level, target_level):
            continue
        reteach_items.append(
            {
                "evaluation_id": row.id,
                "student_number": row.student_number,
                "standard_id": row.standard_id,
                "standard_code": matrix_standard.standard.code
                if matrix_standard and matrix_standard.standard
                else None,
                "current_mastery_level": row.mastery_level,
                "target_mastery_level": target_level,
                "evidence_source_type": row.evidence_source_type,
                "evidence_source_id": row.evidence_source_id,
                "confirmed_at": row.confirmed_at,
            }
        )

    unassessed_standards = [
        {
            "standard_id": row.standard_id,
            "standard_code": row.standard.code if row.standard else None,
            "target_mastery_level": row.target_mastery_level,
        }
        for row in matrix.matrix_standards
        if row.assessment_count == 0
    ]

    reteach_items.sort(key=lambda item: (item["student_number"], str(item["standard_code"] or "")))
    return {
        "mastery_matrix_id": matrix.id,
        "reteach_candidate_count": len(reteach_items),
        "unassessed_standard_count": len(unassessed_standards),
        "reteach_items": reteach_items,
        "unassessed_standards": unassessed_standards,
    }

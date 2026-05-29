from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_mastery_commit import TeacherAssistMasteryCommit
from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
from oziebot_api.services.teacher_assist.mastery_analytics_helpers import (
    average_mastery_rank,
    commits_by_evaluation_id,
    compute_standard_percentages,
    count_recent_assignment_evaluations,
    count_recent_evaluations,
    last_assessed_timestamp,
    level_distribution,
    operational_status_from_percentages,
    student_trend_from_evaluations,
)
from oziebot_api.services.teacher_assist.mastery_commit_service import list_mastery_evaluations
from oziebot_api.services.teacher_assist.mastery_matrix import get_mastery_matrix_or_404
from oziebot_api.services.teacher_assist.mastery_visualization import _needs_reteach


def _active_evaluations_by_student_standard(
    evaluations: list[TeacherAssistMasteryEvaluation],
) -> dict[tuple[int, uuid.UUID], TeacherAssistMasteryEvaluation]:
    active = [row for row in evaluations if row.evaluation_status == "active"]
    mapping: dict[tuple[int, uuid.UUID], TeacherAssistMasteryEvaluation] = {}
    for row in active:
        mapping[(row.student_number, row.standard_id)] = row
    return mapping


def build_mastery_matrix_heatmap(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    settings: Settings | None = None,
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
    active_by_cell = _active_evaluations_by_student_standard(evaluations)
    student_numbers = sorted({student_number for student_number, _ in active_by_cell})

    standards_payload: list[dict[str, Any]] = []
    for matrix_standard in sorted(matrix.matrix_standards, key=lambda item: item.display_order):
        standard_rows = [row for row in evaluations if row.standard_id == matrix_standard.standard_id]
        percentages = compute_standard_percentages(standard_rows)
        standards_payload.append(
            {
                "matrix_standard_id": matrix_standard.id,
                "standard_id": matrix_standard.standard_id,
                "standard_code": matrix_standard.standard.code if matrix_standard.standard else None,
                "standard_description": matrix_standard.standard.description if matrix_standard.standard else None,
                "display_order": matrix_standard.display_order,
                "target_mastery_level": matrix_standard.target_mastery_level,
                "operational_status": operational_status_from_percentages(
                    float(percentages["mastery_percentage"]),
                    int(percentages["total_committed_evaluations"]),
                    settings=settings,
                ),
                "mastery_distribution": _standard_distribution(standard_rows),
                "evaluation_count": len(standard_rows),
                "last_assessed_at": last_assessed_timestamp(standard_rows),
            }
        )

    rows_payload: list[dict[str, Any]] = []
    for student_number in student_numbers:
        cells: list[dict[str, Any]] = []
        for matrix_standard in sorted(matrix.matrix_standards, key=lambda item: item.display_order):
            active_row = active_by_cell.get((student_number, matrix_standard.standard_id))
            if active_row is None:
                cells.append(
                    {
                        "standard_id": matrix_standard.standard_id,
                        "mastery_level": "not_assessed",
                        "evaluation_id": None,
                        "evaluation_count": 0,
                        "confirmed_at": None,
                        "evidence_source_type": None,
                        "evidence_source_id": None,
                        "needs_reteach": False,
                    }
                )
                continue
            cells.append(
                {
                    "standard_id": matrix_standard.standard_id,
                    "mastery_level": active_row.mastery_level,
                    "evaluation_id": active_row.id,
                    "evaluation_count": 1,
                    "confirmed_at": active_row.confirmed_at,
                    "evidence_source_type": active_row.evidence_source_type,
                    "evidence_source_id": active_row.evidence_source_id,
                    "needs_reteach": _needs_reteach(
                        active_row.mastery_level,
                        matrix_standard.target_mastery_level,
                    ),
                }
            )
        rows_payload.append(
            {
                "student_number": student_number,
                "cells": cells,
            }
        )

    matrix_distribution = level_distribution()
    for row in evaluations:
        matrix_distribution[row.mastery_level] = matrix_distribution.get(row.mastery_level, 0) + 1

    return {
        "mastery_matrix_id": matrix.id,
        "title": matrix.title,
        "class_id": matrix.class_id,
        "subject_id": matrix.subject_id,
        "school_year_id": matrix.school_year_id,
        "grading_period_id": matrix.grading_period_id,
        "standards": standards_payload,
        "student_numbers": student_numbers,
        "rows": rows_payload,
        "mastery_distribution": matrix_distribution,
        "active_evaluation_count": len(evaluations),
        "student_count": len(student_numbers),
    }


def _standard_distribution(active_rows: list[TeacherAssistMasteryEvaluation]) -> dict[str, int]:
    distribution = level_distribution()
    for row in active_rows:
        distribution[row.mastery_level] = distribution.get(row.mastery_level, 0) + 1
    return distribution


def build_student_mastery_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    student_number: int,
    settings: Settings | None = None,
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
        student_number=student_number,
    )
    active_rows = [row for row in evaluations if row.evaluation_status == "active"]
    if not active_rows:
        raise LookupError("No committed mastery records found for this student")

    evaluation_ids = [row.id for row in evaluations]
    commits = db.scalars(
        select(TeacherAssistMasteryCommit).where(
            TeacherAssistMasteryCommit.tenant_id == tenant_id,
            TeacherAssistMasteryCommit.owner_user_id == user_id,
            TeacherAssistMasteryCommit.mastery_evaluation_id.in_(evaluation_ids),
        )
    ).all()
    commits_by_evaluation_id_map = commits_by_evaluation_id(commits)

    standards_by_id = {row.standard_id: row for row in matrix.matrix_standards}
    mastery_states: list[dict[str, Any]] = []
    standards_needing_attention: list[dict[str, Any]] = []
    assignment_evidence: list[dict[str, Any]] = []
    grading_review_references: list[dict[str, Any]] = []
    gradebook_commit_references: list[dict[str, Any]] = []

    for row in sorted(active_rows, key=lambda item: item.standard_id):
        matrix_standard = standards_by_id.get(row.standard_id)
        target_level = matrix_standard.target_mastery_level if matrix_standard else "mastery"
        needs_attention = _needs_reteach(row.mastery_level, target_level)
        state = {
            "evaluation_id": row.id,
            "standard_id": row.standard_id,
            "standard_code": matrix_standard.standard.code if matrix_standard and matrix_standard.standard else None,
            "mastery_level": row.mastery_level,
            "target_mastery_level": target_level,
            "confirmed_at": row.confirmed_at,
            "needs_reteach": needs_attention,
        }
        mastery_states.append(state)
        if needs_attention:
            standards_needing_attention.append(state)

        if row.evidence_source_type == "assignment" and row.evidence_source_id is not None:
            assignment_evidence.append(
                {
                    "evaluation_id": row.id,
                    "standard_id": row.standard_id,
                    "assignment_id": row.evidence_source_id,
                    "confirmed_at": row.confirmed_at,
                }
            )
        elif row.evidence_source_type == "grading_review" and row.evidence_source_id is not None:
            grading_review_references.append(
                {
                    "evaluation_id": row.id,
                    "standard_id": row.standard_id,
                    "grading_review_id": row.evidence_source_id,
                    "confirmed_at": row.confirmed_at,
                }
            )
        elif row.evidence_source_type == "gradebook_commit" and row.evidence_source_id is not None:
            gradebook_commit_references.append(
                {
                    "evaluation_id": row.id,
                    "standard_id": row.standard_id,
                    "gradebook_commit_id": row.evidence_source_id,
                    "confirmed_at": row.confirmed_at,
                }
            )

    return {
        "mastery_matrix_id": matrix.id,
        "student_number": student_number,
        "trend": student_trend_from_evaluations(active_rows, commits_by_evaluation_id_map),
        "active_evaluation_count": len(active_rows),
        "average_mastery_rank": average_mastery_rank(active_rows),
        "recent_assessment_count": count_recent_evaluations(active_rows, settings=settings),
        "recent_assignment_count": count_recent_assignment_evaluations(active_rows, settings=settings),
        "mastery_states": mastery_states,
        "standards_needing_attention": standards_needing_attention,
        "latest_assignment_evidence": assignment_evidence[:5],
        "latest_grading_review_references": grading_review_references[:5],
        "latest_gradebook_commit_references": gradebook_commit_references[:5],
    }

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_assignment_gradebook_commit import (
    TeacherAssistAssignmentGradebookCommit,
)
from oziebot_api.models.teacher_assist_assignment_grading_review import TeacherAssistAssignmentGradingReview
from oziebot_api.models.teacher_assist_assignment_standard import TeacherAssistAssignmentStandard
from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
from oziebot_api.services.teacher_assist.assignments import get_assignment_or_404, list_assignment_standards
from oziebot_api.services.teacher_assist.mastery_analytics_helpers import (
    assignment_effectiveness_status_from_percentages,
    average_mastery_rank,
    compute_standard_percentages,
    level_distribution,
)
from oziebot_api.services.teacher_assist.mastery_matrix import list_mastery_matrices


def _assignment_evidence_evaluation_ids(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
) -> set[uuid.UUID]:
    grading_review_ids = db.scalars(
        select(TeacherAssistAssignmentGradingReview.id).where(
            TeacherAssistAssignmentGradingReview.tenant_id == tenant_id,
            TeacherAssistAssignmentGradingReview.teacher_user_id == user_id,
            TeacherAssistAssignmentGradingReview.assignment_id == assignment_id,
        )
    ).all()
    gradebook_commit_ids = db.scalars(
        select(TeacherAssistAssignmentGradebookCommit.id).where(
            TeacherAssistAssignmentGradebookCommit.tenant_id == tenant_id,
            TeacherAssistAssignmentGradebookCommit.teacher_user_id == user_id,
            TeacherAssistAssignmentGradebookCommit.assignment_id == assignment_id,
        )
    ).all()
    return set(grading_review_ids) | set(gradebook_commit_ids)


def _related_active_evaluations(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    class_id: uuid.UUID,
    standard_ids: list[uuid.UUID],
) -> list[TeacherAssistMasteryEvaluation]:
    if not standard_ids:
        return []
    matrices = list_mastery_matrices(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        status="active",
    )
    matrix_ids = [row.id for row in matrices]
    if not matrix_ids:
        return []

    evidence_ids = _assignment_evidence_evaluation_ids(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=assignment_id,
    )
    rows = db.scalars(
        select(TeacherAssistMasteryEvaluation).where(
            TeacherAssistMasteryEvaluation.tenant_id == tenant_id,
            TeacherAssistMasteryEvaluation.owner_user_id == user_id,
            TeacherAssistMasteryEvaluation.mastery_matrix_id.in_(matrix_ids),
            TeacherAssistMasteryEvaluation.standard_id.in_(standard_ids),
            TeacherAssistMasteryEvaluation.evaluation_status == "active",
        )
    ).all()

    related: list[TeacherAssistMasteryEvaluation] = []
    for row in rows:
        if row.evidence_source_type == "assignment" and row.evidence_source_id == assignment_id:
            related.append(row)
            continue
        if row.evidence_source_id in evidence_ids and row.evidence_source_type in {
            "grading_review",
            "gradebook_commit",
        }:
            related.append(row)
    return related


def build_assignment_effectiveness(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict[str, Any]:
    assignment = get_assignment_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=assignment_id,
    )
    assignment_standards = db.scalars(
        select(TeacherAssistAssignmentStandard)
        .where(TeacherAssistAssignmentStandard.assignment_id == assignment_id)
        .options(selectinload(TeacherAssistAssignmentStandard.standard))
        .order_by(TeacherAssistAssignmentStandard.created_at.asc())
    ).all()
    if not assignment_standards:
        assignment_standards = list_assignment_standards(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            assignment_ids=[assignment_id],
        )
    standard_ids = [row.standard_id for row in assignment_standards]
    active_rows = _related_active_evaluations(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=assignment_id,
        class_id=assignment.class_id,
        standard_ids=standard_ids,
    )

    percentages = compute_standard_percentages(active_rows)
    distribution = level_distribution()
    for row in active_rows:
        distribution[row.mastery_level] = distribution.get(row.mastery_level, 0) + 1

    developing_or_beginning_count = sum(
        1 for row in active_rows if row.mastery_level in {"developing", "beginning"}
    )

    grading_review_count = len(
        db.scalars(
            select(TeacherAssistAssignmentGradingReview.id).where(
                TeacherAssistAssignmentGradingReview.tenant_id == tenant_id,
                TeacherAssistAssignmentGradingReview.teacher_user_id == user_id,
                TeacherAssistAssignmentGradingReview.assignment_id == assignment_id,
            )
        ).all()
    )
    gradebook_commit_count = len(
        db.scalars(
            select(TeacherAssistAssignmentGradebookCommit.id).where(
                TeacherAssistAssignmentGradebookCommit.tenant_id == tenant_id,
                TeacherAssistAssignmentGradebookCommit.teacher_user_id == user_id,
                TeacherAssistAssignmentGradebookCommit.assignment_id == assignment_id,
                TeacherAssistAssignmentGradebookCommit.commit_status == "active",
            )
        ).all()
    )

    effectiveness_status = assignment_effectiveness_status_from_percentages(
        float(percentages["mastery_percentage"]),
        int(percentages["total_committed_evaluations"]),
        settings=settings,
    )

    return {
        "assignment_id": assignment.id,
        "assignment_title": assignment.title,
        "class_id": assignment.class_id,
        "subject_id": assignment.subject_id,
        "school_year_id": assignment.school_year_id,
        "grading_period_id": assignment.grading_period_id,
        "linked_standards": [
            {
                "standard_id": row.standard_id,
                "standard_code": row.standard.code if row.standard else None,
                "standard_description": row.standard.description if row.standard else None,
            }
            for row in assignment_standards
        ],
        "mastery_distribution": distribution,
        "developing_or_beginning_count": developing_or_beginning_count,
        "average_mastery_rank": average_mastery_rank(active_rows),
        "mastery_percentage": percentages["mastery_percentage"],
        "total_committed_evaluations": percentages["total_committed_evaluations"],
        "grading_review_count": grading_review_count,
        "gradebook_commit_count": gradebook_commit_count,
        "effectiveness_status": effectiveness_status,
    }

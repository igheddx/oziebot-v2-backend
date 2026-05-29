from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.mastery_analytics_helpers import level_distribution
from oziebot_api.services.teacher_assist.mastery_commit_service import list_mastery_evaluations
from oziebot_api.services.teacher_assist.mastery_matrix import list_mastery_matrices
from oziebot_api.services.teacher_assist.reteach_insights import build_mastery_matrix_reteach_insights


def build_mastery_dashboard(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    matrices = list_mastery_matrices(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        status="active",
    )

    matrix_snapshots: list[dict[str, Any]] = []
    aggregate_distribution = level_distribution()
    reteach_recommended_standards: list[dict[str, Any]] = []
    standards_needing_attention: list[dict[str, Any]] = []
    low_mastery_alerts: list[dict[str, Any]] = []
    improving_standards: list[dict[str, Any]] = []
    declining_standards: list[dict[str, Any]] = []
    unassessed_standards: list[dict[str, Any]] = []

    total_active_evaluations = 0
    total_students = set()

    for matrix in matrices:
        insights = build_mastery_matrix_reteach_insights(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            mastery_matrix_id=matrix.id,
            settings=settings,
        )
        evaluations = list_mastery_evaluations(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            mastery_matrix_id=matrix.id,
            evaluation_status="active",
        )
        total_active_evaluations += len(evaluations)
        for row in evaluations:
            total_students.add(row.student_number)
            aggregate_distribution[row.mastery_level] = aggregate_distribution.get(row.mastery_level, 0) + 1

        matrix_snapshots.append(
            {
                "mastery_matrix_id": matrix.id,
                "title": matrix.title,
                "class_id": matrix.class_id,
                "subject_id": matrix.subject_id,
                "school_year_id": matrix.school_year_id,
                "grading_period_id": matrix.grading_period_id,
                "status_counts": insights["status_counts"],
                "active_evaluation_count": len(evaluations),
                "student_count": len({row.student_number for row in evaluations}),
            }
        )

        panels = insights["panels"]
        for item in panels["standards_needing_reteach"]:
            reteach_recommended_standards.append(
                {
                    **item,
                    "mastery_matrix_id": matrix.id,
                    "matrix_title": matrix.title,
                    "class_id": matrix.class_id,
                    "subject_id": matrix.subject_id,
                }
            )
        for item in panels["standards_needing_attention"]:
            standards_needing_attention.append(
                {
                    **item,
                    "mastery_matrix_id": matrix.id,
                    "matrix_title": matrix.title,
                    "class_id": matrix.class_id,
                    "subject_id": matrix.subject_id,
                }
            )
        for item in panels["weakest_standards"]:
            if item["operational_status"] == "critical_attention":
                low_mastery_alerts.append(
                    {
                        **item,
                        "mastery_matrix_id": matrix.id,
                        "matrix_title": matrix.title,
                        "class_id": matrix.class_id,
                        "subject_id": matrix.subject_id,
                    }
                )
        for item in panels["improving_standards"]:
            improving_standards.append(
                {
                    **item,
                    "mastery_matrix_id": matrix.id,
                    "matrix_title": matrix.title,
                    "class_id": matrix.class_id,
                    "subject_id": matrix.subject_id,
                }
            )
        for item in panels["declining_standards"]:
            declining_standards.append(
                {
                    **item,
                    "mastery_matrix_id": matrix.id,
                    "matrix_title": matrix.title,
                    "class_id": matrix.class_id,
                    "subject_id": matrix.subject_id,
                }
            )
        for item in panels["unassessed_standards"]:
            unassessed_standards.append(
                {
                    **item,
                    "mastery_matrix_id": matrix.id,
                    "matrix_title": matrix.title,
                    "class_id": matrix.class_id,
                    "subject_id": matrix.subject_id,
                }
            )

    return {
        "filters": {
            "school_year_id": school_year_id,
            "grading_period_id": grading_period_id,
            "class_id": class_id,
            "subject_id": subject_id,
        },
        "matrix_count": len(matrices),
        "active_evaluation_count": total_active_evaluations,
        "student_count": len(total_students),
        "mastery_distribution": aggregate_distribution,
        "matrix_snapshots": matrix_snapshots,
        "standards_needing_attention": standards_needing_attention,
        "reteach_recommended_standards": reteach_recommended_standards,
        "low_mastery_alerts": low_mastery_alerts,
        "improving_standards": improving_standards,
        "declining_standards": declining_standards,
        "unassessed_standards": unassessed_standards,
    }

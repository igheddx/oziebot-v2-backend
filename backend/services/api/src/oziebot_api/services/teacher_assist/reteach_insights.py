from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_mastery_commit import TeacherAssistMasteryCommit
from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
from oziebot_api.services.teacher_assist.mastery_analytics_helpers import (
    commits_by_evaluation_id,
    count_recent_evaluations,
    last_assessed_timestamp,
    operational_status_from_percentages,
    standard_trend_from_commits,
)
from oziebot_api.services.teacher_assist.mastery_commit_service import list_mastery_evaluations
from oziebot_api.services.teacher_assist.mastery_matrix import get_mastery_matrix_or_404


def _build_standard_insight(
    *,
    matrix_standard,
    active_rows: list[TeacherAssistMasteryEvaluation],
    commits: dict[Any, list[TeacherAssistMasteryCommit]],
    settings: Settings | None = None,
) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist.mastery_analytics_helpers import (
        compute_standard_percentages,
        count_recent_assignment_evaluations,
    )

    percentages = compute_standard_percentages(active_rows)
    standard_commits: list[TeacherAssistMasteryCommit] = []
    for row in active_rows:
        standard_commits.extend(commits.get(row.id, []))
    operational_status = operational_status_from_percentages(
        float(percentages["mastery_percentage"]),
        int(percentages["total_committed_evaluations"]),
        settings=settings,
    )
    return {
        "matrix_standard_id": matrix_standard.id,
        "standard_id": matrix_standard.standard_id,
        "standard_code": matrix_standard.standard.code if matrix_standard.standard else None,
        "standard_description": matrix_standard.standard.description if matrix_standard.standard else None,
        "display_order": matrix_standard.display_order,
        "target_mastery_level": matrix_standard.target_mastery_level,
        "mastery_percentage": percentages["mastery_percentage"],
        "developing_percentage": percentages["developing_percentage"],
        "beginning_percentage": percentages["beginning_percentage"],
        "not_assessed_percentage": percentages["not_assessed_percentage"],
        "total_committed_evaluations": percentages["total_committed_evaluations"],
        "recent_assessment_count": count_recent_evaluations(active_rows, settings=settings),
        "recent_assignment_count": count_recent_assignment_evaluations(active_rows, settings=settings),
        "last_assessed_at": last_assessed_timestamp(active_rows),
        "operational_status": operational_status,
        "trend": standard_trend_from_commits(standard_commits, settings=settings),
        "is_unassessed": int(percentages["total_committed_evaluations"]) == 0,
    }


def build_mastery_matrix_reteach_insights(
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
    by_standard: dict[uuid.UUID, list[TeacherAssistMasteryEvaluation]] = {}
    for row in evaluations:
        by_standard.setdefault(row.standard_id, []).append(row)

    evaluation_ids = [row.id for row in evaluations]
    commits = db.scalars(
        select(TeacherAssistMasteryCommit).where(
            TeacherAssistMasteryCommit.tenant_id == tenant_id,
            TeacherAssistMasteryCommit.owner_user_id == user_id,
            TeacherAssistMasteryCommit.mastery_evaluation_id.in_(evaluation_ids),
        )
    ).all() if evaluation_ids else []
    commits_by_eval = commits_by_evaluation_id(commits)

    standard_insights: list[dict[str, Any]] = []
    for matrix_standard in sorted(matrix.matrix_standards, key=lambda item: item.display_order):
        rows = by_standard.get(matrix_standard.standard_id, [])
        standard_insights.append(
            _build_standard_insight(
                matrix_standard=matrix_standard,
                active_rows=rows,
                commits=commits_by_eval,
                settings=settings,
            )
        )

    def _sort_by_mastery(item: dict[str, Any]) -> float:
        return float(item["mastery_percentage"])

    assessed = [item for item in standard_insights if not item["is_unassessed"]]
    unassessed = [item for item in standard_insights if item["is_unassessed"]]
    reteach_recommended = [
        item
        for item in assessed
        if item["operational_status"] in {"reteach_recommended", "critical_attention"}
    ]
    needs_attention = [
        item
        for item in assessed
        if item["operational_status"] in {"monitor", "reteach_recommended", "critical_attention"}
    ]
    strongest = sorted(assessed, key=_sort_by_mastery, reverse=True)[:5]
    weakest = sorted(assessed, key=_sort_by_mastery)[:5]
    improving = [item for item in assessed if item["trend"] == "improving"]
    declining = [item for item in assessed if item["trend"] == "declining"]

    status_counts = {
        "healthy": sum(1 for item in assessed if item["operational_status"] == "healthy"),
        "monitor": sum(1 for item in assessed if item["operational_status"] == "monitor"),
        "reteach_recommended": sum(
            1 for item in assessed if item["operational_status"] == "reteach_recommended"
        ),
        "critical_attention": sum(
            1 for item in assessed if item["operational_status"] == "critical_attention"
        ),
        "unassessed": len(unassessed),
    }

    return {
        "mastery_matrix_id": matrix.id,
        "title": matrix.title,
        "class_id": matrix.class_id,
        "subject_id": matrix.subject_id,
        "school_year_id": matrix.school_year_id,
        "grading_period_id": matrix.grading_period_id,
        "standard_insights": standard_insights,
        "status_counts": status_counts,
        "panels": {
            "standards_needing_reteach": reteach_recommended,
            "standards_needing_attention": needs_attention,
            "strongest_standards": strongest,
            "weakest_standards": weakest,
            "improving_standards": improving,
            "declining_standards": declining,
            "unassessed_standards": unassessed,
        },
    }

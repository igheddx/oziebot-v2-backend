"""Reteach workspace — objectives, groups, and intervention planning."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.services.teacher_assist.mastery_dashboard_v2 import build_mastery_dashboard_v2
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist.reteach_plans import (
    list_reteach_plans,
    serialize_reteach_plan,
)
from oziebot_api.services.teacher_assist.reteach_insights import (
    build_mastery_matrix_reteach_insights,
)
from oziebot_api.services.teacher_assist.mastery_matrix import list_mastery_matrices
from oziebot_api.services.teacher_assist.student_support_groups import (
    list_support_groups,
    serialize_support_group,
)
from oziebot_api.models.teacher_assist_reteach_effectiveness import (
    TeacherAssistReteachEffectivenessRecord,
)
from sqlalchemy import select


def build_reteach_workspace(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    performance = ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
    )
    objectives_requiring_reteach = [
        row
        for row in performance.get("objectives") or []
        if (row.get("mastery_pct") or 0) < 50 and (row.get("students_assessed") or 0) > 0
    ]

    support_groups = list_support_groups(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        instructional_week_id=instructional_week_id,
    )
    reteach_plans = list_reteach_plans(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        status=None,
    )
    if instructional_week_id is not None:
        reteach_plans = [
            row for row in reteach_plans if row.instructional_week_id == instructional_week_id
        ]

    matrices = list_mastery_matrices(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        status="active",
    )
    matrix_insights: list[dict[str, Any]] = []
    for matrix in matrices[:3]:
        matrix_insights.append(
            build_mastery_matrix_reteach_insights(
                db,
                tenant_id=tenant_id,
                user_id=user_id,
                mastery_matrix_id=matrix.id,
            )
        )

    effectiveness = list(
        db.scalars(
            select(TeacherAssistReteachEffectivenessRecord)
            .where(
                TeacherAssistReteachEffectivenessRecord.tenant_id == tenant_id,
                TeacherAssistReteachEffectivenessRecord.owner_user_id == user_id,
            )
            .order_by(TeacherAssistReteachEffectivenessRecord.recorded_at.desc())
            .limit(10)
        ).all()
    )

    dashboard = build_mastery_dashboard_v2(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
    )

    return {
        "objectives_requiring_reteach": objectives_requiring_reteach,
        "students_impacted": performance.get("students_needing_support") or [],
        "suggested_groups": [
            {
                "title": f"{row.get('objective_code') or 'Objective'} Support Group",
                "objective_code": row.get("objective_code"),
                "student_count_hint": row.get("students_assessed"),
                "suggested_activities": [
                    "Small-group review",
                    "Guided practice",
                    "Reassessment checkpoint",
                ],
            }
            for row in objectives_requiring_reteach[:5]
        ],
        "support_groups": [serialize_support_group(row) for row in support_groups],
        "reteach_plans": [serialize_reteach_plan(row) for row in reteach_plans[:20]],
        "prior_reteach_history": [
            {
                "reteach_plan_id": str(row.reteach_plan_id),
                "before_mastery_pct": row.before_mastery_pct,
                "after_mastery_pct": row.after_mastery_pct,
                "improvement_pct": row.improvement_pct,
                "teacher_reflection": row.teacher_reflection,
                "recorded_at": row.recorded_at.isoformat(),
            }
            for row in effectiveness
        ],
        "matrix_insights": matrix_insights,
        "mastery_dashboard_v2": dashboard.get("v2"),
        "teacher_notes": None,
        "read_only": True,
    }

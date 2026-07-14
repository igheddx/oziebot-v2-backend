"""Mastery dashboard v2 — objective health across scope levels."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.mastery_dashboard import build_mastery_dashboard
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService


def build_mastery_dashboard_v2(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    base = build_mastery_dashboard(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        settings=settings,
    )
    performance = ObjectivePerformanceService.calculate_for_scope(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
    )

    objectives = performance.get("objectives") or []
    objective_health = [
        {
            "objective_code": row.get("objective_code"),
            "objective_id": row.get("objective_id"),
            "standard_id": row.get("standard_id"),
            "mastery_pct": row.get("mastery_pct"),
            "developing_pct": row.get("developing_pct"),
            "beginning_pct": row.get("beginning_pct"),
            "trend_direction": row.get("trend_direction"),
            "students_assessed": row.get("students_assessed"),
            "health_status": _health_status(row.get("mastery_pct") or 0),
        }
        for row in objectives
    ]

    return {
        **base,
        "v2": {
            "scope": performance.get("scope"),
            "objective_health": objective_health,
            "objective_coverage": {
                "total_objectives": len(objectives),
                "assessed_objectives": sum(
                    1 for row in objectives if (row.get("students_assessed") or 0) > 0
                ),
            },
            "students_needing_support": performance.get("students_needing_support") or [],
            "students_near_mastery": performance.get("students_near_mastery") or [],
            "strongest_objectives": performance.get("strongest_objectives") or [],
            "weakest_objectives": performance.get("weakest_objectives") or [],
            "upcoming_objectives": [],
            "read_only": True,
        },
        "read_only": True,
    }


def _health_status(mastery_pct: float) -> str:
    if mastery_pct >= 80:
        return "healthy"
    if mastery_pct >= 50:
        return "monitor"
    if mastery_pct > 0:
        return "needs_attention"
    return "not_assessed"

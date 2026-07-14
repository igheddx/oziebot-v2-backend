from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.services.teacher_assist.lesson_effectiveness import list_lesson_effectiveness
from oziebot_api.services.teacher_assist.setup import (
    get_grading_period_or_404,
    get_school_year_or_404,
)


def _aggregate_effectiveness_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "lesson_count": 0,
            "classification_counts": {},
            "average_mastery_percentage": 0.0,
            "total_assignments": 0,
            "total_reteach_plans": 0,
        }
    classification_counts: dict[str, int] = {}
    mastery_values: list[float] = []
    total_assignments = 0
    total_reteach = 0
    for row in rows:
        classification = str(row.get("classification") or "insufficient_data")
        classification_counts[classification] = classification_counts.get(classification, 0) + 1
        mastery_values.append(float(row.get("aggregate_mastery_percentage") or 0.0))
        total_assignments += int(row.get("assignment_count") or 0)
        total_reteach += int(row.get("reteach_plan_count") or 0)
    return {
        "lesson_count": len(rows),
        "classification_counts": classification_counts,
        "average_mastery_percentage": round(sum(mastery_values) / len(mastery_values), 4)
        if mastery_values
        else 0.0,
        "total_assignments": total_assignments,
        "total_reteach_plans": total_reteach,
    }


def _find_prior_grading_period(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    current_period: TeacherAssistGradingPeriod,
) -> TeacherAssistGradingPeriod | None:
    if current_period.sort_order is None:
        return None
    return db.scalars(
        select(TeacherAssistGradingPeriod)
        .where(
            TeacherAssistGradingPeriod.school_year_id == current_period.school_year_id,
            TeacherAssistGradingPeriod.sort_order < current_period.sort_order,
        )
        .order_by(TeacherAssistGradingPeriod.sort_order.desc())
    ).first()


def _find_prior_school_year(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    current_school_year: TeacherAssistSchoolYear,
) -> TeacherAssistSchoolYear | None:
    return db.scalars(
        select(TeacherAssistSchoolYear)
        .where(
            TeacherAssistSchoolYear.tenant_id == tenant_id,
            TeacherAssistSchoolYear.end_date < current_school_year.start_date,
        )
        .order_by(TeacherAssistSchoolYear.end_date.desc())
    ).first()


def build_lesson_effectiveness_historical_comparison(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID,
    grading_period_id: uuid.UUID | None,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict[str, Any]:
    school_year = get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=school_year_id)
    current_period = None
    prior_period = None
    if grading_period_id is not None:
        current_period = get_grading_period_or_404(
            db, tenant_id=tenant_id, grading_period_id=grading_period_id
        )
        prior_period = _find_prior_grading_period(
            db, tenant_id=tenant_id, current_period=current_period
        )

    prior_school_year = _find_prior_school_year(
        db, tenant_id=tenant_id, current_school_year=school_year
    )

    current_rows = list_lesson_effectiveness(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=school_year_id,
        grading_period_id=grading_period_id,
        class_id=class_id,
        subject_id=subject_id,
        settings=settings,
    )
    prior_period_rows: list[dict[str, Any]] = []
    if prior_period is not None:
        prior_period_rows = list_lesson_effectiveness(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            school_year_id=school_year_id,
            grading_period_id=prior_period.id,
            class_id=class_id,
            subject_id=subject_id,
            settings=settings,
        )
    prior_year_rows: list[dict[str, Any]] = []
    if prior_school_year is not None:
        prior_year_rows = list_lesson_effectiveness(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            school_year_id=prior_school_year.id,
            grading_period_id=current_period.id if current_period is not None else None,
            class_id=class_id,
            subject_id=subject_id,
            settings=settings,
        )

    return {
        "class_id": class_id,
        "subject_id": subject_id,
        "current_grading_period": (
            {
                "school_year_id": school_year_id,
                "grading_period_id": grading_period_id,
                "school_year_title": school_year.title,
                "grading_period_title": current_period.title
                if current_period is not None
                else None,
                "summary": _aggregate_effectiveness_summary(current_rows),
                "lessons": current_rows,
            }
        ),
        "prior_grading_period": (
            {
                "school_year_id": school_year_id,
                "grading_period_id": prior_period.id if prior_period is not None else None,
                "school_year_title": school_year.title,
                "grading_period_title": prior_period.title if prior_period is not None else None,
                "summary": _aggregate_effectiveness_summary(prior_period_rows),
                "lessons": prior_period_rows,
            }
            if prior_period is not None
            else None
        ),
        "prior_school_year": (
            {
                "school_year_id": prior_school_year.id,
                "grading_period_id": current_period.id if current_period is not None else None,
                "school_year_title": prior_school_year.title,
                "grading_period_title": current_period.title
                if current_period is not None
                else None,
                "summary": _aggregate_effectiveness_summary(prior_year_rows),
                "lessons": prior_year_rows,
            }
            if prior_school_year is not None
            else None
        ),
        "read_only": True,
    }

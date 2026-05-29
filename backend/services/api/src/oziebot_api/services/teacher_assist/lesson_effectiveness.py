from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.services.teacher_assist.assignment_effectiveness import build_assignment_effectiveness
from oziebot_api.services.teacher_assist.constants import (
    LESSON_EFFECTIVENESS_ADJUSTMENT_THRESHOLD,
    LESSON_EFFECTIVENESS_EFFECTIVE_THRESHOLD,
    LESSON_EFFECTIVENESS_HIGHLY_THRESHOLD,
)
from oziebot_api.services.teacher_assist.workflow_service import get_visible_weekly_plan_or_404


def lesson_effectiveness_classification_from_signals(
    *,
    mastery_percentage: float,
    total_committed_evaluations: int,
    assignment_count: int,
    reteach_plan_count: int,
    gradebook_commit_count: int,
    mixed_or_reteach_assignments: int,
    settings: Settings | None = None,
) -> str:
    if assignment_count == 0 and total_committed_evaluations == 0:
        return "insufficient_data"
    if total_committed_evaluations == 0:
        if gradebook_commit_count == 0:
            return "insufficient_data"

    highly_threshold = (
        settings.teacher_assist_lesson_effectiveness_highly_threshold
        if settings is not None and hasattr(settings, "teacher_assist_lesson_effectiveness_highly_threshold")
        else LESSON_EFFECTIVENESS_HIGHLY_THRESHOLD
    )
    effective_threshold = (
        settings.teacher_assist_lesson_effectiveness_effective_threshold
        if settings is not None and hasattr(settings, "teacher_assist_lesson_effectiveness_effective_threshold")
        else LESSON_EFFECTIVENESS_EFFECTIVE_THRESHOLD
    )
    adjustment_threshold = (
        settings.teacher_assist_lesson_effectiveness_adjustment_threshold
        if settings is not None and hasattr(settings, "teacher_assist_lesson_effectiveness_adjustment_threshold")
        else LESSON_EFFECTIVENESS_ADJUSTMENT_THRESHOLD
    )

    if reteach_plan_count >= 2 or mixed_or_reteach_assignments >= 2:
        if mastery_percentage < adjustment_threshold:
            return "ineffective"
        return "needs_adjustment"

    if (
        mastery_percentage >= highly_threshold
        and reteach_plan_count == 0
        and mixed_or_reteach_assignments == 0
        and total_committed_evaluations > 0
    ):
        return "highly_effective"
    if mastery_percentage >= effective_threshold and reteach_plan_count == 0:
        return "effective"
    if mastery_percentage >= adjustment_threshold:
        return "needs_adjustment"
    return "ineffective"


def _weekly_plan_context(
    weekly_plan: TeacherAssistWeeklyPlan,
) -> dict[str, uuid.UUID | None]:
    draft = weekly_plan.planning_input_draft
    return {
        "school_year_id": draft.school_year_id if draft is not None else None,
        "grading_period_id": draft.grading_period_id if draft is not None else None,
        "class_id": draft.class_id if draft is not None else None,
        "subject_id": draft.subject_id if draft is not None else None,
    }


def _count_reteach_plans(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
    grading_period_id: uuid.UUID | None,
    school_year_id: uuid.UUID | None,
) -> int:
    if class_id is None or subject_id is None:
        return 0
    query = select(TeacherAssistReteachPlan.id).where(
        TeacherAssistReteachPlan.tenant_id == tenant_id,
        TeacherAssistReteachPlan.owner_user_id == user_id,
        TeacherAssistReteachPlan.class_id == class_id,
        TeacherAssistReteachPlan.subject_id == subject_id,
        TeacherAssistReteachPlan.status != "archived",
    )
    if grading_period_id is not None:
        query = query.where(TeacherAssistReteachPlan.grading_period_id == grading_period_id)
    elif school_year_id is not None:
        query = query.where(TeacherAssistReteachPlan.school_year_id == school_year_id)
    return len(db.scalars(query).all())


def build_weekly_plan_lesson_effectiveness(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan: TeacherAssistWeeklyPlan,
    settings: Settings | None = None,
) -> dict[str, Any]:
    context = _weekly_plan_context(weekly_plan)
    assignments = db.scalars(
        select(TeacherAssistAssignment).where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
            TeacherAssistAssignment.source_plan_id == weekly_plan.id,
        )
    ).all()

    assignment_summaries: list[dict[str, Any]] = []
    mastery_percentages: list[float] = []
    total_evaluations = 0
    gradebook_commit_count = 0
    grading_review_count = 0
    mixed_or_reteach_assignments = 0

    for assignment in assignments:
        try:
            effectiveness = build_assignment_effectiveness(
                db,
                tenant_id=tenant_id,
                user_id=user_id,
                assignment_id=assignment.id,
                settings=settings,
            )
        except LookupError:
            continue
        assignment_summaries.append(
            {
                "assignment_id": assignment.id,
                "assignment_title": assignment.title,
                "effectiveness_status": effectiveness["effectiveness_status"],
                "mastery_percentage": effectiveness["mastery_percentage"],
                "grading_review_count": effectiveness["grading_review_count"],
                "gradebook_commit_count": effectiveness["gradebook_commit_count"],
            }
        )
        if effectiveness["total_committed_evaluations"] > 0:
            mastery_percentages.append(float(effectiveness["mastery_percentage"]))
            total_evaluations += int(effectiveness["total_committed_evaluations"])
        gradebook_commit_count += int(effectiveness["gradebook_commit_count"])
        grading_review_count += int(effectiveness["grading_review_count"])
        if effectiveness["effectiveness_status"] in {"mixed_results", "reteach_likely"}:
            mixed_or_reteach_assignments += 1

    aggregate_mastery = (
        round(sum(mastery_percentages) / len(mastery_percentages), 4) if mastery_percentages else 0.0
    )
    reteach_plan_count = _count_reteach_plans(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        class_id=context["class_id"],
        subject_id=context["subject_id"],
        grading_period_id=context["grading_period_id"],
        school_year_id=context["school_year_id"],
    )
    classification = lesson_effectiveness_classification_from_signals(
        mastery_percentage=aggregate_mastery,
        total_committed_evaluations=total_evaluations,
        assignment_count=len(assignments),
        reteach_plan_count=reteach_plan_count,
        gradebook_commit_count=gradebook_commit_count,
        mixed_or_reteach_assignments=mixed_or_reteach_assignments,
        settings=settings,
    )

    return {
        "weekly_plan_id": weekly_plan.id,
        "weekly_plan_title": weekly_plan.title,
        "planning_scope": weekly_plan.planning_scope,
        "school_year_id": context["school_year_id"],
        "grading_period_id": context["grading_period_id"],
        "class_id": context["class_id"],
        "subject_id": context["subject_id"],
        "classification": classification,
        "aggregate_mastery_percentage": aggregate_mastery,
        "total_committed_evaluations": total_evaluations,
        "assignment_count": len(assignments),
        "grading_review_count": grading_review_count,
        "gradebook_commit_count": gradebook_commit_count,
        "reteach_plan_count": reteach_plan_count,
        "mixed_or_reteach_assignments": mixed_or_reteach_assignments,
        "assignment_summaries": assignment_summaries,
        "read_only": True,
    }


def build_weekly_plan_lesson_effectiveness_by_id(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict[str, Any]:
    weekly_plan = get_visible_weekly_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        weekly_plan_id=weekly_plan_id,
    )
    weekly_plan = db.scalars(
        select(TeacherAssistWeeklyPlan)
        .where(TeacherAssistWeeklyPlan.id == weekly_plan.id)
        .options(selectinload(TeacherAssistWeeklyPlan.planning_input_draft))
    ).one()
    return build_weekly_plan_lesson_effectiveness(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        weekly_plan=weekly_plan,
        settings=settings,
    )


def list_lesson_effectiveness(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    classification: str | None = None,
    settings: Settings | None = None,
) -> list[dict[str, Any]]:
    query = (
        select(TeacherAssistWeeklyPlan)
        .where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
            TeacherAssistWeeklyPlan.status == "completed",
        )
        .options(selectinload(TeacherAssistWeeklyPlan.planning_input_draft))
        .order_by(TeacherAssistWeeklyPlan.updated_at.desc(), TeacherAssistWeeklyPlan.created_at.desc())
    )
    rows = db.scalars(query).all()
    results: list[dict[str, Any]] = []
    for row in rows:
        context = _weekly_plan_context(row)
        if school_year_id is not None and context["school_year_id"] != school_year_id:
            continue
        if grading_period_id is not None and context["grading_period_id"] != grading_period_id:
            continue
        if class_id is not None and context["class_id"] != class_id:
            continue
        if subject_id is not None and context["subject_id"] != subject_id:
            continue
        payload = build_weekly_plan_lesson_effectiveness(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            weekly_plan=row,
            settings=settings,
        )
        if classification is not None and payload["classification"] != classification:
            continue
        results.append(payload)
    return results

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_grade_record import (
    TeacherAssistAssignmentGradeRecord,
)
from oziebot_api.models.teacher_assist_assignment_grading_review import (
    TeacherAssistAssignmentGradingReview,
)
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_student_work_submission import (
    TeacherAssistStudentWorkSubmission,
)
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.services.teacher_assist.action_workspace import (
    SEVERITY_SORT_ORDER,
    get_teacher_assist_action_workspace,
)
from oziebot_api.services.teacher_assist.mastery_dashboard import build_mastery_dashboard
from oziebot_api.services.teacher_assist.education_catalog import get_active_teacher_assignment
from oziebot_api.services.teacher_assist.setup import get_teacher_profile
from oziebot_api.services.teacher_assist.workspace_service import get_teacher_assist_workspace

TODAY_CATEGORY_KEYS = (
    "items_needing_review",
    "grading_pending",
    "extraction_pending",
    "gradebook_pending",
    "mastery_alerts",
    "reteach_plans_pending",
)

WORKFLOW_PIPELINE_STEPS = (
    "lesson_plan",
    "assignment",
    "student_work",
    "grading_review",
    "gradebook",
    "mastery",
)

ONBOARDING_CHECKLIST_KEYS = (
    "profile",
    "school_year",
    "classes",
    "standards",
    "first_plan",
    "first_assignment",
    "first_mastery_matrix",
)


def _sort_timestamp(item: dict[str, Any]) -> datetime:
    value = item.get("updated_at") or item.get("created_at")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return datetime.min.replace(tzinfo=UTC)


def _step_status(*, complete: bool, in_progress: bool = False) -> str:
    if complete:
        return "complete"
    if in_progress:
        return "in_progress"
    return "pending"


def _build_workflow_progress_cards(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    limit: int = 8,
) -> list[dict[str, Any]]:
    assignments = db.scalars(
        select(TeacherAssistAssignment)
        .where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
        )
        .order_by(TeacherAssistAssignment.updated_at.desc())
        .limit(limit)
    ).all()
    if not assignments:
        return []

    assignment_ids = [row.id for row in assignments]
    submissions = db.scalars(
        select(TeacherAssistStudentWorkSubmission).where(
            TeacherAssistStudentWorkSubmission.tenant_id == tenant_id,
            TeacherAssistStudentWorkSubmission.teacher_user_id == user_id,
            TeacherAssistStudentWorkSubmission.assignment_id.in_(assignment_ids),
        )
    ).all()
    reviews = db.scalars(
        select(TeacherAssistAssignmentGradingReview).where(
            TeacherAssistAssignmentGradingReview.tenant_id == tenant_id,
            TeacherAssistAssignmentGradingReview.teacher_user_id == user_id,
            TeacherAssistAssignmentGradingReview.assignment_id.in_(assignment_ids),
        )
    ).all()
    grade_records = db.scalars(
        select(TeacherAssistAssignmentGradeRecord).where(
            TeacherAssistAssignmentGradeRecord.tenant_id == tenant_id,
            TeacherAssistAssignmentGradeRecord.teacher_user_id == user_id,
            TeacherAssistAssignmentGradeRecord.assignment_id.in_(assignment_ids),
            TeacherAssistAssignmentGradeRecord.record_status == "active",
        )
    ).all()
    mastery_evaluations = db.scalars(
        select(TeacherAssistMasteryEvaluation).where(
            TeacherAssistMasteryEvaluation.tenant_id == tenant_id,
            TeacherAssistMasteryEvaluation.owner_user_id == user_id,
            TeacherAssistMasteryEvaluation.evidence_source_type == "assignment",
            TeacherAssistMasteryEvaluation.evidence_source_id.in_(assignment_ids),
            TeacherAssistMasteryEvaluation.evaluation_status == "active",
        )
    ).all()

    submissions_by_assignment: dict[uuid.UUID, list] = {}
    for row in submissions:
        submissions_by_assignment.setdefault(row.assignment_id, []).append(row)

    reviews_by_assignment: dict[uuid.UUID, list] = {}
    for row in reviews:
        reviews_by_assignment.setdefault(row.assignment_id, []).append(row)

    grade_records_by_assignment: dict[uuid.UUID, list] = {}
    for row in grade_records:
        grade_records_by_assignment.setdefault(row.assignment_id, []).append(row)

    mastery_by_assignment: dict[uuid.UUID, list] = {}
    for row in mastery_evaluations:
        if row.evidence_source_id is not None:
            mastery_by_assignment.setdefault(row.evidence_source_id, []).append(row)

    plan_ids = {row.source_plan_id for row in assignments if row.source_plan_id is not None}
    plans_by_id: dict[uuid.UUID, TeacherAssistWeeklyPlan] = {}
    if plan_ids:
        plans = db.scalars(
            select(TeacherAssistWeeklyPlan).where(
                TeacherAssistWeeklyPlan.tenant_id == tenant_id,
                TeacherAssistWeeklyPlan.user_id == user_id,
                TeacherAssistWeeklyPlan.id.in_(plan_ids),
            )
        ).all()
        plans_by_id = {row.id: row for row in plans}

    cards: list[dict[str, Any]] = []
    for assignment in assignments:
        assignment_reviews = reviews_by_assignment.get(assignment.id, [])
        confirmed_reviews = [row for row in assignment_reviews if row.status == "teacher_confirmed"]
        open_reviews = [
            row
            for row in assignment_reviews
            if row.status in {"draft", "ai_suggested", "teacher_reviewing", "returned_for_revision"}
        ]
        has_submissions = bool(submissions_by_assignment.get(assignment.id))
        has_gradebook = bool(grade_records_by_assignment.get(assignment.id))
        has_mastery = bool(mastery_by_assignment.get(assignment.id))
        source_plan = (
            plans_by_id.get(assignment.source_plan_id) if assignment.source_plan_id else None
        )

        steps = {
            "lesson_plan": _step_status(
                complete=source_plan is not None and source_plan.status == "completed",
                in_progress=source_plan is not None and source_plan.status != "completed",
            ),
            "assignment": "complete",
            "student_work": _step_status(
                complete=has_submissions,
                in_progress=assignment.status in {"collected", "review_in_progress"}
                and not has_submissions,
            ),
            "grading_review": _step_status(
                complete=bool(confirmed_reviews),
                in_progress=bool(open_reviews),
            ),
            "gradebook": _step_status(
                complete=has_gradebook,
                in_progress=bool(confirmed_reviews) and not has_gradebook,
            ),
            "mastery": _step_status(
                complete=has_mastery,
                in_progress=has_gradebook and not has_mastery,
            ),
        }
        completed_count = sum(1 for key in WORKFLOW_PIPELINE_STEPS if steps[key] == "complete")
        cards.append(
            {
                "assignment_id": assignment.id,
                "assignment_title": assignment.title,
                "class_id": assignment.class_id,
                "source_plan_id": assignment.source_plan_id,
                "source_plan_title": source_plan.title if source_plan else None,
                "steps": steps,
                "completed_step_count": completed_count,
                "total_step_count": len(WORKFLOW_PIPELINE_STEPS),
                "progress_percent": round((completed_count / len(WORKFLOW_PIPELINE_STEPS)) * 100),
                "navigation_href": f"/teacher-assist/assignments?assignment_id={assignment.id}",
            }
        )
    return cards


def _build_onboarding_checklist(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> dict[str, Any]:
    profile = get_teacher_profile(db, user_id=user_id)
    school_assignment = get_active_teacher_assignment(db, user_id=user_id)
    school_year_count = db.scalar(
        select(func.count())
        .select_from(TeacherAssistSchoolYear)
        .where(TeacherAssistSchoolYear.tenant_id == tenant_id)
    )
    class_count = db.scalar(
        select(func.count())
        .select_from(TeacherAssistClass)
        .where(TeacherAssistClass.tenant_id == tenant_id)
    )
    items = [
        {
            "key": "school_placement",
            "title": "School & district",
            "complete": school_assignment is not None
            and profile is not None
            and bool(profile.preferred_grade_level),
            "navigation_href": "/teacher-assist/settings#school-setup",
            "navigation_label": "Set school placement",
        },
        {
            "key": "school_year",
            "title": "School year",
            "complete": int(school_year_count or 0) > 0,
            "navigation_href": "/teacher-assist/settings#school-year",
            "navigation_label": "Configure school year",
        },
        {
            "key": "classroom",
            "title": "My classroom",
            "complete": int(class_count or 0) > 0
            and profile is not None
            and profile.default_student_count is not None
            and profile.default_student_count > 0,
            "navigation_href": "/teacher-assist/settings#my-classroom",
            "navigation_label": "Configure classroom",
        },
    ]
    completed_count = sum(1 for item in items if item["complete"])
    return {
        "items": items,
        "completed_count": completed_count,
        "total_count": len(items),
        "is_complete": completed_count == len(items),
    }


def _review_item_href(entity_type: str, entity_id: uuid.UUID) -> str:
    if entity_type == "extracted_text":
        return f"/teacher-assist/extractions?id={entity_id}"
    if entity_type == "weekly_plan":
        return f"/teacher-assist/weekly-planning/plans?id={entity_id}"
    if entity_type == "grading_review":
        return f"/teacher-assist/assignments?grading_review_id={entity_id}"
    return "/teacher-assist/workspace"


def _categorize_action_items(action_payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = {key: [] for key in TODAY_CATEGORY_KEYS}
    for section in action_payload.get("sections", []):
        section_key = section.get("section_key")
        for item in section.get("items", []):
            action_type = str(item.get("action_type") or "")
            if section_key == "extractions":
                buckets["extraction_pending"].append(item)
            elif section_key == "grading":
                buckets["grading_pending"].append(item)
            elif section_key == "gradebook":
                buckets["gradebook_pending"].append(item)
            elif action_type.startswith("mastery_reteach"):
                buckets["reteach_plans_pending"].append(item)
            elif action_type.startswith("mastery_"):
                buckets["mastery_alerts"].append(item)
            elif action_type in {
                "plan_in_progress",
                "assignment_in_review",
                "grading_review_draft",
                "grading_review_ai_suggested",
                "grading_review_teacher_reviewing",
                "grading_review_returned_for_revision",
            }:
                buckets["items_needing_review"].append(item)
            elif section_key == "planning_assignments":
                buckets["items_needing_review"].append(item)
    return buckets


def _flatten_priority_items(buckets: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    combined: list[dict[str, Any]] = []
    for key in TODAY_CATEGORY_KEYS:
        for item in buckets[key]:
            combined.append({**item, "today_category": key})
    combined.sort(
        key=lambda row: (
            SEVERITY_SORT_ORDER.get(str(row.get("severity", "info")), 99),
            -_sort_timestamp(row).timestamp(),
        )
    )
    return combined


def get_teacher_assist_today_workspace(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> dict[str, Any]:
    action_payload = get_teacher_assist_action_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    workspace_payload = get_teacher_assist_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    mastery_dashboard = build_mastery_dashboard(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=(
            workspace_payload["current_school_year"].id
            if workspace_payload.get("current_school_year") is not None
            else None
        ),
        grading_period_id=(
            workspace_payload["active_grading_period"].id
            if workspace_payload.get("active_grading_period") is not None
            else None
        ),
        settings=settings,
    )

    buckets = _categorize_action_items(action_payload)
    for review_item in workspace_payload.get("review_required_items", [])[:10]:
        entity_type = str(review_item.get("entity_type") or "item")
        entity_id = review_item.get("entity_id")
        href = (
            _review_item_href(entity_type, entity_id)
            if isinstance(entity_id, uuid.UUID)
            else "/teacher-assist/workspace"
        )
        buckets["items_needing_review"].append(
            {
                "action_key": f"review_required:{entity_type}:{entity_id}",
                "action_type": "review_required",
                "severity": "review",
                "title": review_item.get("title") or "Review required",
                "description": review_item.get("review_reason")
                or "This item needs teacher review.",
                "tenant_id": tenant_id,
                "class_id": review_item.get("class_id"),
                "navigation": {
                    "label": "Review item",
                    "href": href,
                },
                "created_at": review_item.get("updated_at"),
                "updated_at": review_item.get("updated_at"),
            }
        )

    priority_items = _flatten_priority_items(buckets)
    category_counts = {key: len(buckets[key]) for key in TODAY_CATEGORY_KEYS}

    return {
        "summary": {
            **dict(action_payload.get("summary") or {}),
            "today_open_count": len(priority_items),
            "items_needing_review_count": category_counts["items_needing_review"],
            "grading_pending_count": category_counts["grading_pending"],
            "extraction_pending_count": category_counts["extraction_pending"],
            "gradebook_pending_count": category_counts["gradebook_pending"],
            "mastery_alert_count": category_counts["mastery_alerts"],
            "reteach_plans_pending_count": category_counts["reteach_plans_pending"],
            "mastery_reteach_standard_count": len(
                mastery_dashboard.get("reteach_recommended_standards", [])
            ),
        },
        "priority_items": priority_items[:20],
        "categories": {key: buckets[key][:12] for key in TODAY_CATEGORY_KEYS},
        "workflow_progress_cards": _build_workflow_progress_cards(
            db, tenant_id=tenant_id, user_id=user_id
        ),
        "onboarding_checklist": _build_onboarding_checklist(
            db, tenant_id=tenant_id, user_id=user_id
        ),
        "recent_activity": action_payload.get("recent_activity", [])[:15],
        "current_school_year": workspace_payload.get("current_school_year"),
        "active_grading_period": workspace_payload.get("active_grading_period"),
        "mastery_insights": workspace_payload.get("mastery_insights"),
    }

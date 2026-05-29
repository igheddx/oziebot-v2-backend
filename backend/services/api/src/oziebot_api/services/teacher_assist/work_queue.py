from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.action_workspace import (
    SEVERITY_SORT_ORDER,
    get_teacher_assist_action_workspace,
)
from oziebot_api.services.teacher_assist.constants import TEACHER_ASSIST_WORK_QUEUE_SECTION_KEYS

PRIORITY_LEVEL_BY_SEVERITY = {
    "critical": "critical",
    "warning": "high",
    "review": "medium",
    "ready": "medium",
    "info": "informational",
}


def _sort_timestamp(item: dict[str, Any]) -> datetime:
    value = item.get("updated_at") or item.get("created_at")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return datetime.min.replace(tzinfo=UTC)


def _queue_section_for_item(item: dict[str, Any]) -> str | None:
    action_type = str(item.get("action_type") or "")
    section_key = str(item.get("section_key") or "")
    if section_key == "grading" or action_type.startswith("grading_"):
        if "commit" in action_type or section_key == "gradebook":
            return "gradebook_commits"
        return "grades_pending"
    if section_key == "gradebook" or "gradebook" in action_type:
        return "gradebook_commits"
    if section_key == "extractions" or action_type.startswith("extraction_"):
        return "reviews_needed"
    if action_type.startswith("mastery_reteach") or "reteach" in action_type:
        return "reteach_actions"
    if action_type.startswith("mastery_"):
        return "mastery_actions"
    if "newsletter" in action_type:
        return "newsletter_actions"
    if section_key == "workflows_exports" or action_type.startswith("workflow_") or action_type.startswith("export_"):
        return "workflow_failures"
    if section_key in {"planning_assignments", "grading"} or action_type in {
        "review_required",
        "grading_review_draft",
        "grading_review_ai_suggested",
        "grading_review_teacher_reviewing",
        "grading_review_returned_for_revision",
        "assignment_in_review",
        "student_work_pending_review",
    }:
        return "reviews_needed"
    return "reviews_needed"


def _public_queue_item(item: dict[str, Any]) -> dict[str, Any]:
    severity = str(item.get("severity") or "review")
    return {
        **item,
        "priority_level": PRIORITY_LEVEL_BY_SEVERITY.get(severity, "medium"),
        "queue_section": _queue_section_for_item(item),
    }


def build_teacher_assist_work_queue(
    db,
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
    actionable: list[dict[str, Any]] = []
    for section in action_payload.get("sections", []):
        for item in section.get("items", []):
            if str(item.get("severity") or "") == "info":
                continue
            queue_item = _public_queue_item(item)
            actionable.append(queue_item)

    actionable.sort(
        key=lambda row: (
            SEVERITY_SORT_ORDER.get(str(row.get("severity", "info")), 99),
            -_sort_timestamp(row).timestamp(),
        )
    )

    sections: list[dict[str, Any]] = []
    for section_key in TEACHER_ASSIST_WORK_QUEUE_SECTION_KEYS:
        section_items = [item for item in actionable if item.get("queue_section") == section_key]
        sections.append(
            {
                "section_key": section_key,
                "title": section_key.replace("_", " ").title(),
                "count": len(section_items),
                "items": section_items,
            }
        )

    return {
        "summary": {
            "total_actionable": len(actionable),
            "critical_count": sum(1 for item in actionable if item.get("priority_level") == "critical"),
            "high_count": sum(1 for item in actionable if item.get("priority_level") == "high"),
            "medium_count": sum(1 for item in actionable if item.get("priority_level") == "medium"),
        },
        "sections": sections,
        "items": actionable,
        "read_only": True,
    }

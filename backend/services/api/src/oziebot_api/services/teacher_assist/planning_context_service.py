from __future__ import annotations

from datetime import date, datetime
from typing import Any
import uuid

from sqlalchemy.orm import Session

from oziebot_api.services.teacher_assist.planning import get_planning_draft_context_preview


def _serialize_scalar(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def _serialize_planning_draft_out(preview) -> dict[str, Any]:
    draft = preview.draft
    return {
        "id": str(draft.id),
        "tenant_id": str(draft.tenant_id),
        "user_id": str(draft.user_id),
        "school_year_id": str(draft.school_year_id) if draft.school_year_id is not None else None,
        "grading_period_id": str(draft.grading_period_id)
        if draft.grading_period_id is not None
        else None,
        "class_id": str(draft.class_id) if draft.class_id is not None else None,
        "subject_id": str(draft.subject_id) if draft.subject_id is not None else None,
        "planning_scope": draft.planning_scope,
        "subject_ids": [str(subject.id) for subject in preview.subjects],
        "pacing_item_ids": [str(item.id) for item in preview.pacing_items],
        "standard_ids": [str(standard.id) for standard in preview.standards],
        "plan_title": draft.title,
        "module_title": draft.module_title,
        "start_date": draft.start_date.isoformat() if draft.start_date is not None else None,
        "end_date": draft.end_date.isoformat() if draft.end_date is not None else None,
        "estimated_weeks": draft.estimated_weeks,
        "instructional_days_count": draft.instructional_days_count,
        "title": draft.title,
        "notes": draft.notes,
        "status": draft.status,
        "resource_ids": [str(resource.id) for resource in preview.resources],
        "created_at": draft.created_at.isoformat(),
        "updated_at": draft.updated_at.isoformat(),
    }


def build_planning_context_snapshot(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
) -> dict[str, Any]:
    preview = get_planning_draft_context_preview(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    draft = preview.draft
    return {
        "draft": _serialize_planning_draft_out(preview),
        "school_year": (
            {
                "id": str(draft.school_year.id),
                "tenant_id": str(draft.school_year.tenant_id),
                "title": draft.school_year.title,
                "start_date": draft.school_year.start_date.isoformat(),
                "end_date": draft.school_year.end_date.isoformat(),
                "is_active": draft.school_year.is_active,
                "created_at": draft.school_year.created_at.isoformat(),
                "updated_at": draft.school_year.updated_at.isoformat(),
            }
            if draft.school_year is not None
            else None
        ),
        "grading_period": (
            {
                "id": str(draft.grading_period.id),
                "school_year_id": str(draft.grading_period.school_year_id),
                "title": draft.grading_period.title,
                "grading_period_type": draft.grading_period.grading_period_type,
                "start_date": draft.grading_period.start_date.isoformat(),
                "end_date": draft.grading_period.end_date.isoformat(),
                "sort_order": draft.grading_period.sort_order,
                "created_at": draft.grading_period.created_at.isoformat(),
                "updated_at": draft.grading_period.updated_at.isoformat(),
            }
            if draft.grading_period is not None
            else None
        ),
        "class": (
            {
                "id": str(draft.teacher_class.id),
                "tenant_id": str(draft.teacher_class.tenant_id),
                "school_year_id": str(draft.teacher_class.school_year_id),
                "name": draft.teacher_class.name,
                "grade_level": draft.teacher_class.grade_level,
                "student_count": draft.teacher_class.student_count,
                "subject_ids": [str(row.subject_id) for row in draft.teacher_class.class_subjects],
                "student_number_range_start": 1,
                "student_number_range_end": draft.teacher_class.student_count,
                "created_at": draft.teacher_class.created_at.isoformat(),
                "updated_at": draft.teacher_class.updated_at.isoformat(),
            }
            if draft.teacher_class is not None
            else None
        ),
        "subjects": [
            {
                "id": str(subject.id),
                "tenant_id": str(subject.tenant_id),
                "code": subject.code,
                "name": subject.name,
                "created_at": subject.created_at.isoformat(),
                "updated_at": subject.updated_at.isoformat(),
            }
            for subject in preview.subjects
        ],
        "pacing_items": [
            {
                "id": str(item.id),
                "pacing_guide_id": str(item.pacing_guide_id),
                "grading_period_id": str(item.grading_period_id)
                if item.grading_period_id is not None
                else None,
                "subject_id": str(item.subject_id) if item.subject_id is not None else None,
                "week_number": item.week_number,
                "day_number": item.day_number,
                "instructional_date": item.instructional_date.isoformat()
                if item.instructional_date is not None
                else None,
                "title": item.title,
                "instructional_focus": item.instructional_focus,
                "objectives": item.objectives,
                "notes": item.notes,
                "sort_order": item.sort_order,
                "standard_ids": [str(link.standard_id) for link in item.standard_links],
                "resource_ids": [
                    str(link.resource_library_item_id) for link in item.resource_links
                ],
                "created_at": item.created_at.isoformat(),
                "updated_at": item.updated_at.isoformat(),
            }
            for item in preview.pacing_items
        ],
        "pacing_groups": [
            {
                "group_key": group.group_key,
                "label": group.label,
                "pacing_items": [
                    {
                        "id": str(item.id),
                        "week_number": item.week_number,
                        "day_number": item.day_number,
                        "instructional_date": item.instructional_date.isoformat()
                        if item.instructional_date is not None
                        else None,
                        "title": item.title,
                        "instructional_focus": item.instructional_focus,
                        "objectives": item.objectives,
                        "notes": item.notes,
                    }
                    for item in group.pacing_items
                ],
            }
            for group in preview.pacing_groups
        ],
        "standards": [
            {
                "id": str(standard.id),
                "tenant_id": str(standard.tenant_id),
                "subject_id": str(standard.subject_id) if standard.subject_id is not None else None,
                "standard_type": standard.standard_type,
                "code": standard.code,
                "description": standard.description,
                "grade_level": standard.grade_level,
                "school_year_id": str(standard.school_year_id)
                if standard.school_year_id is not None
                else None,
                "created_at": standard.created_at.isoformat(),
                "updated_at": standard.updated_at.isoformat(),
            }
            for standard in preview.standards
        ],
        "resources": [
            {
                "id": str(resource.id),
                "tenant_id": str(resource.tenant_id),
                "uploaded_by_user_id": str(resource.uploaded_by_user_id),
                "title": resource.title,
                "description": resource.description,
                "resource_type": resource.resource_type,
                "storage_key": resource.storage_key,
                "original_filename": resource.original_filename,
                "mime_type": resource.mime_type,
                "file_size": resource.file_size,
                "external_url": resource.external_url,
                "uploaded_at": resource.uploaded_at.isoformat(),
                "created_at": resource.created_at.isoformat(),
                "updated_at": resource.updated_at.isoformat(),
            }
            for resource in preview.resources
        ],
        "teacher_notes": preview.draft.notes,
        "duration_summary": {
            "start_date": _serialize_scalar(preview.duration_summary.start_date),
            "end_date": _serialize_scalar(preview.duration_summary.end_date),
            "estimated_weeks": preview.duration_summary.estimated_weeks,
            "instructional_days_count": preview.duration_summary.instructional_days_count,
            "summary": preview.duration_summary.summary,
        },
        "readiness": {
            "is_ready": preview.readiness.is_ready,
            "missing_items": preview.readiness.missing_items,
            "warnings": preview.readiness.warnings,
        },
    }

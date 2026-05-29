from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from copy import deepcopy
from typing import Any
import traceback
import uuid

from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.user import User
from oziebot_api.models.teacher_assist_planning_input_draft import TeacherAssistPlanningInputDraft
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.teacher_assist_weekly_plan_version import TeacherAssistWeeklyPlanVersion
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.models.teacher_assist_workflow_step import TeacherAssistWorkflowStep
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.instructional_plan_validator import (
    validate_instructional_plan_output,
    validate_instructional_plan_section_output,
)
from oziebot_api.services.teacher_assist.instructional_plan_prompt_builder import (
    teacher_review_checklist,
)
from oziebot_api.services.teacher_assist.constants import (
    validate_plan_reuse_status,
    validate_plan_visibility_scope,
    validate_teacher_assist_workflow_status,
    validate_teacher_assist_workflow_step_status,
    validate_teacher_assist_workflow_type,
    validate_weekly_plan_status,
)
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.provider_config import (
    TeacherAssistProviderCircuitBreaker,
    get_teacher_assist_allowed_models,
)
from oziebot_api.services.teacher_assist.planning import (
    get_planning_draft_or_404,
    validate_planning_draft_readiness,
)
from oziebot_api.services.teacher_assist.planning_context_service import (
    build_planning_context_snapshot,
)
from oziebot_api.services.teacher_assist.prompt_contracts import (
    INSTRUCTIONAL_PLAN_GENERATION_FEATURE,
    INSTRUCTIONAL_PLAN_PROMPT_VERSION,
    INSTRUCTIONAL_PLAN_SECTION_REGENERATION_FEATURE,
)
from oziebot_api.services.teacher_assist.setup import (
    get_class_or_404,
    get_grading_period_or_404,
    get_school_year_or_404,
)

WEEKLY_PLAN_WORKFLOW_TYPE = "weekly_plan_generation"
WORKFLOW_OUTPUT_REF_TYPE = "weekly_plan"
WORKFLOW_STEP_SEQUENCE = (
    "load_context_snapshot",
    "generate_instructional_plan",
    "persist_weekly_plan",
    "finalize_workflow",
)
WORKFLOW_LOG_LIMIT = 40
SECTION_REGENERATION_KEYS = {
    "overview",
    "instructional_arc",
    "weekly_segments",
    "daily_breakdown",
    "vocabulary",
    "materials_needed",
    "differentiation",
    "assessment_checkpoints",
    "standards_progression",
    "review_notes",
}


def _parse_snapshot_date(value: Any) -> date | None:
    if not value:
        return None
    return date.fromisoformat(str(value))


def _parse_snapshot_uuid(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    return uuid.UUID(str(value))


def _snapshot_school_year_id(snapshot: dict[str, Any]) -> uuid.UUID | None:
    draft = dict(snapshot.get("draft") or {})
    school_year = dict(snapshot.get("school_year") or {})
    return _parse_snapshot_uuid(school_year.get("id") or draft.get("school_year_id"))


def _snapshot_grading_period_id(snapshot: dict[str, Any]) -> uuid.UUID | None:
    draft = dict(snapshot.get("draft") or {})
    grading_period = dict(snapshot.get("grading_period") or {})
    return _parse_snapshot_uuid(grading_period.get("id") or draft.get("grading_period_id"))


def _snapshot_class_id(snapshot: dict[str, Any]) -> uuid.UUID | None:
    draft = dict(snapshot.get("draft") or {})
    class_context = dict(snapshot.get("class") or {})
    return _parse_snapshot_uuid(class_context.get("id") or draft.get("class_id"))


def _snapshot_subject_id(snapshot: dict[str, Any]) -> uuid.UUID | None:
    draft = dict(snapshot.get("draft") or {})
    if draft.get("subject_id"):
        return _parse_snapshot_uuid(draft.get("subject_id"))
    subjects = list(snapshot.get("subjects") or [])
    for subject in subjects:
        if isinstance(subject, dict) and subject.get("id"):
            return _parse_snapshot_uuid(subject.get("id"))
    return None


def _plan_school_year_id(plan: TeacherAssistWeeklyPlan) -> uuid.UUID | None:
    return _snapshot_school_year_id(dict(plan.source_context_json or {})) or plan.school_year_origin_id


def _plan_grading_period_id(plan: TeacherAssistWeeklyPlan) -> uuid.UUID | None:
    return _snapshot_grading_period_id(dict(plan.source_context_json or {}))


def _plan_class_id(plan: TeacherAssistWeeklyPlan) -> uuid.UUID | None:
    return _snapshot_class_id(dict(plan.source_context_json or {}))


def _plan_subject_id(plan: TeacherAssistWeeklyPlan) -> uuid.UUID | None:
    return _snapshot_subject_id(dict(plan.source_context_json or {}))


def list_teacher_assist_workflows(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID
) -> list[TeacherAssistWorkflow]:
    return db.scalars(
        select(TeacherAssistWorkflow)
        .where(
            TeacherAssistWorkflow.tenant_id == tenant_id,
            TeacherAssistWorkflow.user_id == user_id,
        )
        .order_by(TeacherAssistWorkflow.created_at.desc())
    ).all()


def get_teacher_assist_workflow_or_404(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID, workflow_id: uuid.UUID
) -> TeacherAssistWorkflow:
    row = db.scalars(
        select(TeacherAssistWorkflow).where(
            TeacherAssistWorkflow.id == workflow_id,
            TeacherAssistWorkflow.tenant_id == tenant_id,
            TeacherAssistWorkflow.user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("TeacherAssist workflow not found")
    return row


def list_weekly_plans(db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID) -> list[TeacherAssistWeeklyPlan]:
    return db.scalars(
        select(TeacherAssistWeeklyPlan)
        .where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
        )
        .order_by(TeacherAssistWeeklyPlan.created_at.desc())
    ).all()


def _can_view_plan(plan: TeacherAssistWeeklyPlan, *, user_id: uuid.UUID) -> bool:
    return plan.owner_user_id == user_id or plan.visibility_scope != "private"


def _visible_weekly_plan_query(tenant_id: uuid.UUID):
    return select(TeacherAssistWeeklyPlan).where(TeacherAssistWeeklyPlan.tenant_id == tenant_id)


def get_visible_weekly_plan_or_404(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID, weekly_plan_id: uuid.UUID
) -> TeacherAssistWeeklyPlan:
    row = db.scalars(
        _visible_weekly_plan_query(tenant_id).where(TeacherAssistWeeklyPlan.id == weekly_plan_id)
    ).one_or_none()
    if row is None or not _can_view_plan(row, user_id=user_id):
        raise LookupError("Weekly plan not found")
    return row


def get_weekly_plan_or_404(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID, weekly_plan_id: uuid.UUID
) -> TeacherAssistWeeklyPlan:
    row = db.scalars(
        select(TeacherAssistWeeklyPlan).where(
            TeacherAssistWeeklyPlan.id == weekly_plan_id,
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Weekly plan not found")
    return row


def list_weekly_plan_versions(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID, weekly_plan_id: uuid.UUID
) -> list[TeacherAssistWeeklyPlanVersion]:
    get_visible_weekly_plan_or_404(db, tenant_id=tenant_id, user_id=user_id, weekly_plan_id=weekly_plan_id)
    return db.scalars(
        select(TeacherAssistWeeklyPlanVersion)
        .where(TeacherAssistWeeklyPlanVersion.weekly_plan_id == weekly_plan_id)
        .order_by(TeacherAssistWeeklyPlanVersion.version_number.desc())
    ).all()


def get_weekly_plan_version_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan_id: uuid.UUID,
    version_id: uuid.UUID,
) -> TeacherAssistWeeklyPlanVersion:
    get_visible_weekly_plan_or_404(db, tenant_id=tenant_id, user_id=user_id, weekly_plan_id=weekly_plan_id)
    row = db.scalars(
        select(TeacherAssistWeeklyPlanVersion).where(
            TeacherAssistWeeklyPlanVersion.id == version_id,
            TeacherAssistWeeklyPlanVersion.weekly_plan_id == weekly_plan_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Weekly plan version not found")
    return row


def _next_weekly_plan_version_number(db: Session, *, weekly_plan_id: uuid.UUID) -> int:
    current = db.scalar(
        select(func.max(TeacherAssistWeeklyPlanVersion.version_number)).where(
            TeacherAssistWeeklyPlanVersion.weekly_plan_id == weekly_plan_id
        )
    )
    return int(current or 0) + 1


def _normalized_weekly_plan_content(
    *,
    content_json: dict[str, Any],
    planning_draft_id: uuid.UUID,
    workflow_id: uuid.UUID | None,
    version_number: int,
    generated_at: str,
    provider_name: str | None = None,
    provider_model: str | None = None,
    prompt_version: str | None = None,
    is_mock: bool | None = None,
) -> dict[str, Any]:
    normalized = deepcopy(content_json)
    metadata = dict(normalized.get("metadata") or {})
    resolved_provider_name = provider_name or str(metadata.get("generator") or metadata.get("provider_mode") or "mock")
    resolved_provider_mode = "mock" if resolved_provider_name == "mock" else "real"
    metadata.update(
        {
            "is_mock": is_mock if is_mock is not None else bool(metadata.get("is_mock", resolved_provider_name == "mock")),
            "generator": resolved_provider_name,
            "provider_mode": metadata.get("provider_mode") or resolved_provider_mode,
            "provider_model": provider_model or metadata.get("provider_model"),
            "version": version_number,
            "generated_at": metadata.get("generated_at") or generated_at,
            "planning_draft_id": str(planning_draft_id),
            "workflow_id": str(workflow_id) if workflow_id else None,
        }
    )
    normalized["metadata"] = metadata
    normalized["weekly_objectives"] = list(normalized.get("weekly_objectives") or [])
    normalized["weekly_segments"] = list(normalized.get("weekly_segments") or [])
    normalized["subjects"] = list(normalized.get("subjects") or [])
    normalized["instructional_arc"] = list(normalized.get("instructional_arc") or [])
    normalized["standards_progression"] = list(normalized.get("standards_progression") or [])
    normalized["vocabulary"] = list(normalized.get("vocabulary") or [])
    normalized["materials_needed"] = list(normalized.get("materials_needed") or [])
    normalized["assessment_checkpoints"] = list(normalized.get("assessment_checkpoints") or [])
    normalized["daily_breakdown"] = list(normalized.get("daily_breakdown") or [])
    normalized["duration"] = dict(normalized.get("duration") or {})
    normalized["differentiation"] = dict(normalized.get("differentiation") or {})
    normalized["resources_used"] = list(normalized.get("resources_used") or [])
    normalized["teacher_notes_used"] = normalized.get("teacher_notes_used") or ""
    normalized["review_notes"] = normalized.get("review_notes") or ""
    normalized["planning_scope"] = normalized.get("planning_scope") or "weekly"
    normalized["plan_title"] = normalized.get("plan_title") or normalized.get("title") or "TeacherAssist Instructional Plan"
    normalized["module_title"] = normalized.get("module_title")
    metadata["provider_mode"] = metadata.get("provider_mode") or resolved_provider_mode
    metadata["prompt_version"] = prompt_version or metadata.get("prompt_version") or INSTRUCTIONAL_PLAN_PROMPT_VERSION
    return normalized


def _build_quality_review_metadata(
    *,
    snapshot: dict[str, Any],
    content_json: dict[str, Any],
    provider_name: str,
) -> dict[str, Any]:
    standards = list(snapshot.get("standards", []) or [])
    resources = list(snapshot.get("resources", []) or [])
    pacing_groups = list(snapshot.get("pacing_groups", []) or [])
    duration = dict(content_json.get("duration") or {})
    warnings: list[str] = list((snapshot.get("readiness") or {}).get("warnings") or [])
    quality_flags: list[str] = []

    if provider_name == "mock":
        quality_flags.append("mock-output")
    if not standards:
        quality_flags.append("standards-context-missing")
        warnings.append("No standards were attached to the planning context.")
    if not resources:
        quality_flags.append("resource-context-light")
        warnings.append("No curriculum resources were attached to the planning context.")
    if not pacing_groups:
        quality_flags.append("pacing-context-light")
        warnings.append("Pacing groups were limited or unavailable in the saved planning context.")
    if not duration.get("start_date") or not duration.get("end_date"):
        quality_flags.append("date-range-incomplete")
        warnings.append("Start or end date was not fully captured in the planning context.")

    if standards:
        standards_alignment_summary = (
            f"Aligned to {len(standards)} supplied standard(s) across the {content_json.get('planning_scope', 'weekly')} plan."
        )
    else:
        standards_alignment_summary = "No standards were supplied, so standards alignment still needs manual review."

    content_json["review_required"] = True
    content_json["quality_flags"] = quality_flags
    content_json["missing_context_warnings"] = list(dict.fromkeys(warnings))
    content_json["standards_alignment_summary"] = standards_alignment_summary
    content_json["teacher_review_checklist"] = teacher_review_checklist()
    return content_json


def _create_weekly_plan_version(
    db: Session,
    *,
    weekly_plan: TeacherAssistWeeklyPlan,
    created_by_user_id: uuid.UUID,
    version_number: int,
    change_reason: str | None,
    created_at: datetime,
) -> TeacherAssistWeeklyPlanVersion:
    version = TeacherAssistWeeklyPlanVersion(
        weekly_plan_id=weekly_plan.id,
        version_number=version_number,
        content_json=deepcopy(weekly_plan.content_json),
        source_context_json=deepcopy(weekly_plan.source_context_json),
        created_by_user_id=created_by_user_id,
        created_at=created_at,
        change_reason=change_reason,
    )
    db.add(version)
    db.flush()
    return version


def list_workflow_usage_events(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID, workflow_ids: list[uuid.UUID]
) -> dict[uuid.UUID, list[TeacherAssistAIUsageEvent]]:
    if not workflow_ids:
        return {}
    rows = db.scalars(
        select(TeacherAssistAIUsageEvent)
        .where(
            TeacherAssistAIUsageEvent.tenant_id == tenant_id,
            TeacherAssistAIUsageEvent.user_id == user_id,
            TeacherAssistAIUsageEvent.workflow_id.in_(workflow_ids),
        )
        .order_by(TeacherAssistAIUsageEvent.created_at.desc())
    ).all()
    usage_by_workflow: dict[uuid.UUID, list[TeacherAssistAIUsageEvent]] = {workflow_id: [] for workflow_id in workflow_ids}
    for row in rows:
        if row.workflow_id is not None:
            usage_by_workflow.setdefault(row.workflow_id, []).append(row)
    return usage_by_workflow


def list_teacher_assist_usage_events(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID
) -> list[TeacherAssistAIUsageEvent]:
    return db.scalars(
        select(TeacherAssistAIUsageEvent)
        .where(
            TeacherAssistAIUsageEvent.tenant_id == tenant_id,
            TeacherAssistAIUsageEvent.user_id == user_id,
        )
        .order_by(TeacherAssistAIUsageEvent.created_at.desc())
    ).all()


def update_weekly_plan(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan_id: uuid.UUID,
    title: str | None,
    status: str | None,
    content_json: dict[str, Any] | None,
    change_reason: str | None,
) -> TeacherAssistWeeklyPlan:
    weekly_plan = get_weekly_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        weekly_plan_id=weekly_plan_id,
    )
    now = datetime.now(UTC)
    next_version = _next_weekly_plan_version_number(db, weekly_plan_id=weekly_plan.id)
    previous_status = weekly_plan.status
    weekly_plan.title = title or weekly_plan.title
    if status is not None:
        weekly_plan.status = validate_weekly_plan_status(status)
    weekly_plan.content_json = _normalized_weekly_plan_content(
        content_json=content_json or weekly_plan.content_json,
        planning_draft_id=weekly_plan.planning_input_draft_id,
        workflow_id=weekly_plan.workflow_id,
        version_number=next_version,
        generated_at=str((weekly_plan.content_json.get("metadata") or {}).get("generated_at") or now.isoformat()),
    )
    weekly_plan.updated_at = now
    _create_weekly_plan_version(
        db,
        weekly_plan=weekly_plan,
        created_by_user_id=user_id,
        version_number=next_version,
        change_reason=change_reason,
        created_at=now,
    )
    record_activity_event(
        db,
        tenant_id=weekly_plan.tenant_id,
        user_id=user_id,
        event_type="plan_completed"
        if weekly_plan.status == "completed" and previous_status != "completed"
        else "plan_updated",
        event_category="planning",
        entity_type="weekly_plan",
        entity_id=weekly_plan.id,
        workflow_id=weekly_plan.workflow_id,
        school_year_id=_plan_school_year_id(weekly_plan),
        grading_period_id=_plan_grading_period_id(weekly_plan),
        class_id=_plan_class_id(weekly_plan),
        subject_id=_plan_subject_id(weekly_plan),
        summary_text=(
            f"Marked plan '{weekly_plan.title}' complete."
            if weekly_plan.status == "completed" and previous_status != "completed"
            else f"Updated plan '{weekly_plan.title}'."
        ),
        details_json={
            "previous_status": previous_status,
            "status": weekly_plan.status,
            "version_number": next_version,
            "change_reason": change_reason,
        },
        event_timestamp=now,
    )
    db.flush()
    return weekly_plan


def _settings_with_provider_mode(settings: Settings, provider_mode: str | None) -> Settings:
    normalized_provider_mode = (provider_mode or "").strip().lower()
    if not normalized_provider_mode:
        return settings
    if normalized_provider_mode == "mock":
        return settings.model_copy(update={"teacher_assist_ai_provider": "mock"})
    if normalized_provider_mode == "real":
        return settings.model_copy(update={"teacher_assist_ai_provider": "openai"})
    raise ValueError("provider_mode must be 'mock' or 'real'")


def _section_change_reason(section_key: str, section_path: str | None) -> str:
    if section_path:
        return f"Regenerated {section_key} at {section_path}"
    return f"Regenerated {section_key}"


def _indexed_path(path: str) -> list[str | int]:
    parts: list[str | int] = []
    for segment in path.split("."):
        normalized = segment.strip()
        if not normalized:
            raise ValueError("section_path cannot contain empty path segments")
        if normalized.isdigit():
            parts.append(int(normalized))
        else:
            parts.append(normalized)
    return parts


def _get_path_value(content_json: dict[str, Any], path: str) -> Any:
    current: Any = content_json
    for segment in _indexed_path(path):
        if isinstance(segment, int):
            if not isinstance(current, list) or segment < 0 or segment >= len(current):
                raise ValueError(f"section_path '{path}' does not exist")
            current = current[segment]
            continue
        if not isinstance(current, dict) or segment not in current:
            raise ValueError(f"section_path '{path}' does not exist")
        current = current[segment]
    return deepcopy(current)


def _set_path_value(content_json: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    updated = deepcopy(content_json)
    current: Any = updated
    parts = _indexed_path(path)
    for index, segment in enumerate(parts[:-1]):
        next_segment = parts[index + 1]
        if isinstance(segment, int):
            if not isinstance(current, list) or segment < 0 or segment >= len(current):
                raise ValueError(f"section_path '{path}' does not exist")
            current = current[segment]
        else:
            if not isinstance(current, dict) or segment not in current:
                raise ValueError(f"section_path '{path}' does not exist")
            current = current[segment]
        if isinstance(next_segment, int) and not isinstance(current, list):
            raise ValueError(f"section_path '{path}' does not point to a list item")
    last_segment = parts[-1]
    if isinstance(last_segment, int):
        if not isinstance(current, list) or last_segment < 0 or last_segment >= len(current):
            raise ValueError(f"section_path '{path}' does not exist")
        current[last_segment] = value
    else:
        if not isinstance(current, dict):
            raise ValueError(f"section_path '{path}' does not point to an object field")
        current[last_segment] = value
    return updated


def _resolve_regeneration_target(
    *,
    content_json: dict[str, Any],
    section_key: str,
    section_path: str | None,
) -> tuple[Any, str | None]:
    if section_key not in SECTION_REGENERATION_KEYS:
        raise ValueError(f"Unsupported section_key '{section_key}'")

    if section_key in {
        "overview",
        "instructional_arc",
        "vocabulary",
        "materials_needed",
        "differentiation",
        "assessment_checkpoints",
        "standards_progression",
        "review_notes",
    }:
        if section_path is not None:
            raise ValueError(f"section_key '{section_key}' does not accept section_path")
        return deepcopy(content_json.get(section_key)), section_key

    if section_key == "weekly_segments":
        if section_path is None:
            return deepcopy(content_json.get("weekly_segments")), "weekly_segments"
        if not section_path.startswith("weekly_segments."):
            raise ValueError("Targeted weekly segment regeneration must use a weekly_segments.<index> path")
        return _get_path_value(content_json, section_path), section_path

    if section_key == "daily_breakdown":
        if section_path is None:
            raise ValueError("section_key 'daily_breakdown' requires a section_path")
        return _get_path_value(content_json, section_path), section_path

    raise ValueError(f"Unsupported section_key '{section_key}'")


def regenerate_weekly_plan_section(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan_id: uuid.UUID,
    section_key: str,
    section_path: str | None,
    teacher_instruction: str | None,
    provider_mode: str | None,
    preserve_existing_context: bool,
) -> TeacherAssistWeeklyPlan:
    weekly_plan = get_visible_weekly_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        weekly_plan_id=weekly_plan_id,
    )
    current_section_content, resolved_path = _resolve_regeneration_target(
        content_json=weekly_plan.content_json,
        section_key=section_key,
        section_path=section_path,
    )
    effective_settings = _settings_with_provider_mode(settings, provider_mode)
    provider = get_teacher_assist_ai_provider(effective_settings, workflow_type=WEEKLY_PLAN_WORKFLOW_TYPE)
    _enforce_teacher_assist_model_allowlist(effective_settings, model_name=getattr(provider, "_model_name", "mock"))
    _enforce_teacher_assist_cost_limit(db, effective_settings)
    circuit_state = TeacherAssistProviderCircuitBreaker().state_for_provider(
        effective_settings,
        provider.provider_name,
    )

    provider_result = provider.regenerate_instructional_plan_section(
        context_preview=weekly_plan.source_context_json,
        current_plan_content=weekly_plan.content_json,
        section_key=section_key,
        section_path=resolved_path,
        current_section_content=current_section_content,
        teacher_instruction=teacher_instruction,
        preserve_existing_context=preserve_existing_context,
    )
    regenerated_section = validate_instructional_plan_section_output(
        provider_result.content_json,
        section_key=section_key,
        section_path=resolved_path,
    )
    now = datetime.now(UTC)
    next_version = _next_weekly_plan_version_number(db, weekly_plan_id=weekly_plan.id)
    if resolved_path is None:
        updated_content = deepcopy(weekly_plan.content_json)
        updated_content[section_key] = regenerated_section
    else:
        updated_content = _set_path_value(weekly_plan.content_json, resolved_path, regenerated_section)
    updated_content = _normalized_weekly_plan_content(
        content_json=updated_content,
        planning_draft_id=weekly_plan.planning_input_draft_id,
        workflow_id=weekly_plan.workflow_id,
        version_number=next_version,
        generated_at=now.isoformat(),
        provider_name=provider_result.provider,
        provider_model=provider_result.model,
        prompt_version=str((provider_result.metadata_json or {}).get("prompt_version") or INSTRUCTIONAL_PLAN_PROMPT_VERSION),
        is_mock=bool((provider_result.metadata_json or {}).get("is_mock", provider_result.provider == "mock")),
    )
    updated_content["metadata"]["generated_at"] = now.isoformat()
    updated_content = _build_quality_review_metadata(
        snapshot=weekly_plan.source_context_json,
        content_json=updated_content,
        provider_name=provider_result.provider,
    )
    validate_instructional_plan_output(
        updated_content,
        context_preview=weekly_plan.source_context_json,
    )
    weekly_plan.content_json = updated_content
    weekly_plan.status = validate_weekly_plan_status("in_progress")
    weekly_plan.updated_at = now
    _create_weekly_plan_version(
        db,
        weekly_plan=weekly_plan,
        created_by_user_id=user_id,
        version_number=next_version,
        change_reason=_section_change_reason(section_key, resolved_path),
        created_at=now,
    )
    db.add(
        TeacherAssistAIUsageEvent(
            tenant_id=weekly_plan.tenant_id,
            user_id=user_id,
            workflow_id=weekly_plan.workflow_id,
            provider=provider_result.provider,
            model=provider_result.model,
            feature=INSTRUCTIONAL_PLAN_SECTION_REGENERATION_FEATURE,
            input_tokens=provider_result.input_tokens,
            output_tokens=provider_result.output_tokens,
            estimated_cost_cents=provider_result.estimated_cost_cents,
            metadata_json={
                **dict(provider_result.metadata_json or {}),
                "weekly_plan_id": str(weekly_plan.id),
                "section_key": section_key,
                "section_path": resolved_path,
                "teacher_instruction_supplied": bool(teacher_instruction and teacher_instruction.strip()),
                "preserve_existing_context": preserve_existing_context,
                "circuit_state": circuit_state.state,
            },
            created_at=now,
        )
    )
    record_activity_event(
        db,
        tenant_id=weekly_plan.tenant_id,
        user_id=user_id,
        event_type="section_regenerated",
        event_category="planning",
        entity_type="weekly_plan",
        entity_id=weekly_plan.id,
        workflow_id=weekly_plan.workflow_id,
        school_year_id=_plan_school_year_id(weekly_plan),
        grading_period_id=_plan_grading_period_id(weekly_plan),
        class_id=_plan_class_id(weekly_plan),
        subject_id=_plan_subject_id(weekly_plan),
        summary_text=f"Regenerated {section_key} for '{weekly_plan.title}'.",
        details_json={
            "section_key": section_key,
            "section_path": resolved_path,
            "provider_name": provider_result.provider,
            "provider_model": provider_result.model,
            "prompt_version": str(
                (provider_result.metadata_json or {}).get("prompt_version")
                or INSTRUCTIONAL_PLAN_PROMPT_VERSION
            ),
        },
        event_timestamp=now,
    )
    db.flush()
    return weekly_plan


def update_weekly_plan_sharing(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan_id: uuid.UUID,
    is_template: bool | None,
    visibility_scope: str | None,
    reuse_status: str | None,
) -> TeacherAssistWeeklyPlan:
    weekly_plan = get_visible_weekly_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        weekly_plan_id=weekly_plan_id,
    )
    if weekly_plan.owner_user_id != user_id:
        raise PermissionError("Only the plan owner can update sharing controls")
    if is_template is not None:
        weekly_plan.is_template = is_template
    if visibility_scope is not None:
        weekly_plan.visibility_scope = validate_plan_visibility_scope(visibility_scope)
    if reuse_status is not None:
        weekly_plan.reuse_status = validate_plan_reuse_status(reuse_status)
    weekly_plan.updated_at = datetime.now(UTC)
    db.flush()
    return weekly_plan


def _source_plan_root_id(plan: TeacherAssistWeeklyPlan) -> uuid.UUID:
    return plan.source_plan_id or plan.id


def _plan_source_metadata(plan: TeacherAssistWeeklyPlan) -> dict[str, Any]:
    source_context = plan.source_context_json or {}
    draft = dict(source_context.get("draft") or {})
    school_year = dict(source_context.get("school_year") or {})
    grading_period = dict(source_context.get("grading_period") or {})
    class_context = dict(source_context.get("class") or {})
    subjects = list(source_context.get("subjects") or [])
    return {
        "source_school_year_id": school_year.get("id") or draft.get("school_year_id") or (
            str(plan.school_year_origin_id) if plan.school_year_origin_id else None
        ),
        "source_school_year_title": school_year.get("title"),
        "subject_ids": [subject.get("id") for subject in subjects if subject.get("id")],
        "subject_names": [subject.get("name") for subject in subjects if subject.get("name")],
        "class_id": class_context.get("id") or draft.get("class_id"),
        "class_name": class_context.get("name"),
        "grading_period_id": grading_period.get("id") or draft.get("grading_period_id"),
        "grading_period_title": grading_period.get("title"),
    }


def _plan_matches_library_filters(
    plan: TeacherAssistWeeklyPlan,
    *,
    school_year_id: uuid.UUID | None,
    subject_id: uuid.UUID | None,
    q: str | None,
) -> bool:
    metadata = _plan_source_metadata(plan)
    if school_year_id is not None:
        source_school_year_id = metadata.get("source_school_year_id")
        if source_school_year_id != str(school_year_id) and plan.school_year_origin_id != school_year_id:
            return False
    if subject_id is not None and str(subject_id) not in metadata.get("subject_ids", []):
        return False
    if q:
        haystack = " ".join(
            filter(
                None,
                [
                    plan.title,
                    str(plan.content_json.get("plan_title") or ""),
                    str(plan.module_title or ""),
                    str(metadata.get("source_school_year_title") or ""),
                    str(metadata.get("class_name") or ""),
                    " ".join(metadata.get("subject_names", [])),
                ],
            )
        ).lower()
        if q.lower() not in haystack:
            return False
    return True


def list_instructional_plan_library(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    planning_scope: str | None = None,
    visibility_scope: str | None = None,
    reuse_status: str | None = None,
    is_template: bool | None = None,
    q: str | None = None,
) -> list[TeacherAssistWeeklyPlan]:
    query = _visible_weekly_plan_query(tenant_id)
    if planning_scope is not None:
        query = query.where(TeacherAssistWeeklyPlan.planning_scope == planning_scope)
    if visibility_scope is not None:
        query = query.where(TeacherAssistWeeklyPlan.visibility_scope == visibility_scope)
    if reuse_status is not None:
        query = query.where(TeacherAssistWeeklyPlan.reuse_status == reuse_status)
    if is_template is not None:
        query = query.where(TeacherAssistWeeklyPlan.is_template == is_template)
    rows = db.scalars(query.order_by(TeacherAssistWeeklyPlan.updated_at.desc())).all()
    return [
        row
        for row in rows
        if _can_view_plan(row, user_id=user_id)
        and _plan_matches_library_filters(
            row,
            school_year_id=school_year_id,
            subject_id=subject_id,
            q=q,
        )
    ]


def _patch_copy_source_context(
    *,
    source_context_json: dict[str, Any],
    target_school_year: TeacherAssistSchoolYear | None,
    target_grading_period: TeacherAssistGradingPeriod | None,
    target_class: TeacherAssistClass | None,
) -> dict[str, Any]:
    patched = deepcopy(source_context_json)
    draft = dict(patched.get("draft") or {})
    if target_school_year is not None:
        draft["school_year_id"] = str(target_school_year.id)
        patched["school_year"] = {
            "id": str(target_school_year.id),
            "tenant_id": str(target_school_year.tenant_id),
            "title": target_school_year.title,
            "start_date": target_school_year.start_date.isoformat(),
            "end_date": target_school_year.end_date.isoformat(),
            "is_active": target_school_year.is_active,
            "created_at": target_school_year.created_at.isoformat(),
            "updated_at": target_school_year.updated_at.isoformat(),
        }
    if target_grading_period is not None:
        draft["grading_period_id"] = str(target_grading_period.id)
        patched["grading_period"] = {
            "id": str(target_grading_period.id),
            "school_year_id": str(target_grading_period.school_year_id),
            "title": target_grading_period.title,
            "grading_period_type": target_grading_period.grading_period_type,
            "start_date": target_grading_period.start_date.isoformat(),
            "end_date": target_grading_period.end_date.isoformat(),
            "sort_order": target_grading_period.sort_order,
            "created_at": target_grading_period.created_at.isoformat(),
            "updated_at": target_grading_period.updated_at.isoformat(),
        }
    if target_class is not None:
        draft["class_id"] = str(target_class.id)
        patched["class"] = {
            "id": str(target_class.id),
            "tenant_id": str(target_class.tenant_id),
            "school_year_id": str(target_class.school_year_id),
            "name": target_class.name,
            "grade_level": target_class.grade_level,
            "student_count": target_class.student_count,
            "subject_ids": [str(row.subject_id) for row in target_class.class_subjects],
            "student_number_range_start": 1,
            "student_number_range_end": target_class.student_count,
            "created_at": target_class.created_at.isoformat(),
            "updated_at": target_class.updated_at.isoformat(),
        }
    patched["draft"] = draft
    return patched


def _build_copy_title(
    *,
    source_plan: TeacherAssistWeeklyPlan,
    copy_mode: str,
    title_override: str | None,
    preserve_titles: bool = True,
    title_suffix: str | None = None,
) -> str:
    if title_override:
        return title_override.strip()
    title = source_plan.title if preserve_titles else f"{source_plan.title} (Copy)"
    if copy_mode == "personal_copy" and preserve_titles and title == source_plan.title:
        title = f"{title} (Copy)"
    if title_suffix:
        title = f"{title} - {title_suffix.strip()}"
    return title


def _find_existing_target_copy(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    root_source_plan_id: uuid.UUID,
    target_school_year_id: uuid.UUID | None,
) -> TeacherAssistWeeklyPlan | None:
    query = select(TeacherAssistWeeklyPlan).where(
        TeacherAssistWeeklyPlan.tenant_id == tenant_id,
        TeacherAssistWeeklyPlan.owner_user_id == user_id,
        TeacherAssistWeeklyPlan.source_plan_id == root_source_plan_id,
    )
    if target_school_year_id is not None:
        query = query.where(TeacherAssistWeeklyPlan.school_year_origin_id == target_school_year_id)
    return db.scalars(query.order_by(TeacherAssistWeeklyPlan.updated_at.desc())).first()


def copy_weekly_plan(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    weekly_plan_id: uuid.UUID,
    target_school_year_id: uuid.UUID | None = None,
    target_grading_period_id: uuid.UUID | None = None,
    target_class_id: uuid.UUID | None = None,
    title_override: str | None = None,
    copy_mode: str = "personal_copy",
    preserve_titles: bool = True,
    title_suffix: str | None = None,
) -> TeacherAssistWeeklyPlan:
    source_plan = get_visible_weekly_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        weekly_plan_id=weekly_plan_id,
    )
    now = datetime.now(UTC)
    target_school_year = (
        get_school_year_or_404(db, tenant_id=tenant_id, school_year_id=target_school_year_id)
        if target_school_year_id is not None
        else None
    )
    target_grading_period = (
        get_grading_period_or_404(db, tenant_id=tenant_id, grading_period_id=target_grading_period_id)
        if target_grading_period_id is not None
        else None
    )
    target_class = (
        get_class_or_404(db, tenant_id=tenant_id, class_id=target_class_id)
        if target_class_id is not None
        else None
    )
    if target_grading_period is not None and target_school_year is not None:
        if target_grading_period.school_year_id != target_school_year.id:
            raise ValueError("Target grading period must belong to the target school year")
    if target_class is not None and target_school_year is not None:
        if target_class.school_year_id != target_school_year.id:
            raise ValueError("Target class must belong to the target school year")

    root_source_plan_id = _source_plan_root_id(source_plan)
    existing_target_copy = _find_existing_target_copy(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        root_source_plan_id=root_source_plan_id,
        target_school_year_id=target_school_year_id,
    )
    if copy_mode == "rollover_copy" and existing_target_copy is not None:
        raise ValueError("A rollover copy for this plan already exists in the selected target school year")

    patched_source_context_json = _patch_copy_source_context(
        source_context_json=source_plan.source_context_json,
        target_school_year=target_school_year,
        target_grading_period=target_grading_period,
        target_class=target_class,
    )
    copied_title = _build_copy_title(
        source_plan=source_plan,
        copy_mode=copy_mode,
        title_override=title_override,
        preserve_titles=preserve_titles,
        title_suffix=title_suffix,
    )
    copied_plan = TeacherAssistWeeklyPlan(
        tenant_id=tenant_id,
        user_id=user_id,
        owner_user_id=user_id,
        planning_input_draft_id=source_plan.planning_input_draft_id,
        workflow_id=None,
        planning_scope=source_plan.planning_scope,
        title=copied_title,
        module_title=source_plan.module_title,
        start_date=source_plan.start_date,
        end_date=source_plan.end_date,
        estimated_weeks=source_plan.estimated_weeks,
        instructional_days_count=source_plan.instructional_days_count,
        source_plan_id=root_source_plan_id,
        derived_from_plan_id=source_plan.id,
        is_template=False,
        visibility_scope=validate_plan_visibility_scope("private"),
        reuse_status=validate_plan_reuse_status("active"),
        school_year_origin_id=target_school_year_id or source_plan.school_year_origin_id,
        status=source_plan.status,
        content_json=_normalized_weekly_plan_content(
            content_json=deepcopy(source_plan.content_json),
            planning_draft_id=source_plan.planning_input_draft_id,
            workflow_id=None,
            version_number=1,
            generated_at=str((source_plan.content_json.get("metadata") or {}).get("generated_at") or now.isoformat()),
        ),
        source_context_json=patched_source_context_json,
        created_at=now,
        updated_at=now,
    )
    copied_plan.content_json["plan_title"] = copied_plan.title
    copied_plan.content_json["duration"] = {
        **dict(copied_plan.content_json.get("duration") or {}),
        "summary": str((copied_plan.content_json.get("duration") or {}).get("summary") or ""),
    }
    copied_plan.content_json["metadata"] = {
        **dict(copied_plan.content_json.get("metadata") or {}),
        "copied_from_plan_id": str(source_plan.id),
        "copied_at": now.isoformat(),
        "copy_mode": copy_mode,
        "version": 1,
        "workflow_id": None,
    }
    db.add(copied_plan)
    db.flush()
    _create_weekly_plan_version(
        db,
        weekly_plan=copied_plan,
        created_by_user_id=user_id,
        version_number=1,
        change_reason="Copied from existing instructional plan",
        created_at=now,
    )
    db.flush()
    return copied_plan


def curriculum_rollover_candidates(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    source_school_year_id: uuid.UUID,
    target_school_year_id: uuid.UUID,
    subject_id: uuid.UUID | None = None,
    planning_scope: str | None = None,
    reuse_status: str | None = None,
) -> tuple[list[TeacherAssistWeeklyPlan], dict[uuid.UUID, TeacherAssistWeeklyPlan | None]]:
    candidates = list_instructional_plan_library(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=source_school_year_id,
        subject_id=subject_id,
        planning_scope=planning_scope,
        visibility_scope=None,
        reuse_status=reuse_status,
        is_template=None,
        q=None,
    )
    existing_by_source_id: dict[uuid.UUID, TeacherAssistWeeklyPlan | None] = {}
    for candidate in candidates:
        existing_by_source_id[candidate.id] = _find_existing_target_copy(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            root_source_plan_id=_source_plan_root_id(candidate),
            target_school_year_id=target_school_year_id,
        )
    return candidates, existing_by_source_id


def curriculum_rollover_copy(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    source_school_year_id: uuid.UUID,
    target_school_year_id: uuid.UUID,
    plan_ids: list[uuid.UUID],
    preserve_titles: bool,
    title_suffix: str | None,
    target_grading_period_mapping: dict[str, uuid.UUID] | None,
) -> tuple[list[TeacherAssistWeeklyPlan], list[str]]:
    warnings: list[str] = []
    copied_plans: list[TeacherAssistWeeklyPlan] = []
    candidates, existing_by_source_id = curriculum_rollover_candidates(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        source_school_year_id=source_school_year_id,
        target_school_year_id=target_school_year_id,
        subject_id=None,
        planning_scope=None,
        reuse_status=None,
    )
    candidate_by_id = {candidate.id: candidate for candidate in candidates}
    for plan_id in plan_ids:
        candidate = candidate_by_id.get(plan_id)
        if candidate is None:
            warnings.append(f"Plan {plan_id} is not available for rollover.")
            continue
        existing_target_copy = existing_by_source_id.get(plan_id)
        if existing_target_copy is not None:
            warnings.append(
                f"{candidate.title} already has a rollover copy in the selected target school year."
            )
            continue
        source_metadata = _plan_source_metadata(candidate)
        source_grading_period_id = source_metadata.get("grading_period_id")
        mapped_target_grading_period_id = (
            target_grading_period_mapping.get(source_grading_period_id)
            if target_grading_period_mapping and source_grading_period_id
            else None
        )
        copied_plans.append(
            copy_weekly_plan(
                db,
                tenant_id=tenant_id,
                user_id=user_id,
                weekly_plan_id=candidate.id,
                target_school_year_id=target_school_year_id,
                target_grading_period_id=uuid.UUID(mapped_target_grading_period_id)
                if mapped_target_grading_period_id
                else None,
                target_class_id=None,
                title_override=None,
                copy_mode="rollover_copy",
                preserve_titles=preserve_titles,
                title_suffix=title_suffix,
            )
        )
    return copied_plans, warnings


def _set_workflow_step_status(
    step: TeacherAssistWorkflowStep,
    *,
    status: str,
    metadata_json: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> None:
    now = datetime.now(UTC)
    step.status = validate_teacher_assist_workflow_step_status(status)
    if status == "running":
        step.started_at = now
        step.completed_at = None
    elif status in {"completed", "failed", "skipped"}:
        if step.started_at is None and status != "skipped":
            step.started_at = now
        step.completed_at = now
    step.metadata_json = metadata_json
    step.error_message = error_message


def _clear_workflow_lease(workflow: TeacherAssistWorkflow) -> None:
    workflow.leased_by_worker = None
    workflow.lease_expires_at = None
    workflow.timeout_at = None


def _append_workflow_log(
    workflow: TeacherAssistWorkflow,
    *,
    event: str,
    message: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    entries = list(workflow.execution_log_json or [])
    entries.append(
        {
            "event": event,
            "message": message,
            "metadata": metadata or {},
            "recorded_at": datetime.now(UTC).isoformat(),
        }
    )
    workflow.execution_log_json = entries[-WORKFLOW_LOG_LIMIT:]


def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _touch_workflow_heartbeat(
    workflow: TeacherAssistWorkflow,
    *,
    settings: Settings,
    worker_name: str,
    progress_percent: int | None = None,
) -> None:
    now = datetime.now(UTC)
    workflow.leased_by_worker = worker_name
    workflow.heartbeat_at = now
    workflow.lease_expires_at = now + timedelta(
        seconds=max(1, settings.teacher_assist_worker_lease_seconds)
    )
    if workflow.timeout_at is None:
        workflow.timeout_at = now + timedelta(
            seconds=max(1, settings.teacher_assist_workflow_timeout_seconds)
        )
    workflow.updated_at = now
    if progress_percent is not None:
        workflow.progress_percent = progress_percent


def _set_workflow_status(
    workflow: TeacherAssistWorkflow,
    *,
    status: str,
    progress_percent: int | None = None,
    error_message: str | None = None,
) -> None:
    now = datetime.now(UTC)
    workflow.status = validate_teacher_assist_workflow_status(status)
    workflow.updated_at = now
    if progress_percent is not None:
        workflow.progress_percent = progress_percent
    workflow.error_message = error_message
    if status == "running" and workflow.started_at is None:
        workflow.started_at = now
    if status in {"completed", "failed", "cancelled"}:
        workflow.completed_at = now
        _clear_workflow_lease(workflow)


class TeacherAssistWorkflowCancelledError(RuntimeError):
    pass


def _refresh_workflow_for_execution(
    session: Session, workflow_id: uuid.UUID
) -> TeacherAssistWorkflow:
    workflow = session.scalars(
        select(TeacherAssistWorkflow).where(TeacherAssistWorkflow.id == workflow_id)
    ).one_or_none()
    if workflow is None:
        raise LookupError("TeacherAssist workflow not found")
    return workflow


def _ensure_workflow_still_active(
    session: Session,
    *,
    workflow_id: uuid.UUID,
    settings: Settings,
    worker_name: str,
    progress_percent: int | None = None,
) -> TeacherAssistWorkflow:
    workflow = _refresh_workflow_for_execution(session, workflow_id)
    if workflow.status == "cancelled":
        raise TeacherAssistWorkflowCancelledError("TeacherAssist workflow was cancelled")
    timeout_at = _coerce_utc_datetime(workflow.timeout_at)
    if timeout_at is not None and timeout_at <= datetime.now(UTC):
        raise TimeoutError("TeacherAssist workflow timed out")
    _touch_workflow_heartbeat(
        workflow,
        settings=settings,
        worker_name=worker_name,
        progress_percent=progress_percent,
    )
    session.flush()
    return workflow


def _teacher_assist_daily_cost_cents(db: Session) -> int:
    now = datetime.now(UTC)
    day_start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    total = db.scalar(
        select(func.coalesce(func.sum(TeacherAssistAIUsageEvent.estimated_cost_cents), 0)).where(
            TeacherAssistAIUsageEvent.created_at >= day_start
        )
    )
    return int(total or 0)


def _enforce_teacher_assist_cost_limit(db: Session, settings: Settings) -> None:
    limit = max(0, settings.teacher_assist_ai_daily_cost_limit_cents)
    if limit <= 0:
        return
    current_total = _teacher_assist_daily_cost_cents(db)
    if current_total >= limit:
        raise RuntimeError("TeacherAssist AI daily cost limit reached")


def _enforce_teacher_assist_model_allowlist(
    settings: Settings, *, model_name: str | None
) -> None:
    normalized_model = (model_name or "").strip() or "mock"
    if normalized_model not in get_teacher_assist_allowed_models(settings):
        raise RuntimeError(f"TeacherAssist model '{normalized_model}' is not allowed")


def _mark_running_step_failed(
    workflow: TeacherAssistWorkflow, *, error_message: str, error_code: str
) -> None:
    current_running_step = next((step for step in workflow.steps if step.status == "running"), None)
    if current_running_step is None:
        return
    _set_workflow_step_status(
        current_running_step,
        status="failed",
        error_message=error_message,
        metadata_json={"error_code": error_code, "traceback": traceback.format_exc(limit=5)},
    )


def _mark_workflow_for_retry_or_failure(
    workflow: TeacherAssistWorkflow,
    *,
    exc: Exception,
    error_code: str,
) -> None:
    attempt_number = workflow.retry_count + 1
    workflow.retry_count = attempt_number
    workflow.error_message = str(exc)
    workflow.last_error_code = error_code
    workflow.updated_at = datetime.now(UTC)
    _append_workflow_log(
        workflow,
        event="workflow_failed",
        message=str(exc),
        metadata={
            "error_code": error_code,
            "attempt": attempt_number,
            "max_retries": workflow.max_retries,
        },
    )
    _mark_running_step_failed(workflow, error_message=str(exc), error_code=error_code)
    if attempt_number <= workflow.max_retries:
        workflow.status = validate_teacher_assist_workflow_status("queued")
        workflow.completed_at = None
        _clear_workflow_lease(workflow)
        return
    _skip_remaining_steps(list(workflow.steps))
    _set_workflow_status(
        workflow,
        status="failed",
        progress_percent=min(max(workflow.progress_percent, 5), 95),
        error_message=str(exc),
    )


def reclaim_stale_teacher_assist_workflows(db: Session) -> int:
    now = datetime.now(UTC)
    stale_rows = db.scalars(
        select(TeacherAssistWorkflow).where(
            TeacherAssistWorkflow.workflow_type == WEEKLY_PLAN_WORKFLOW_TYPE,
            TeacherAssistWorkflow.status == "running",
            TeacherAssistWorkflow.lease_expires_at.is_not(None),
            TeacherAssistWorkflow.lease_expires_at < now,
        )
    ).all()
    for workflow in stale_rows:
        _mark_workflow_for_retry_or_failure(
            workflow,
            exc=TimeoutError("TeacherAssist workflow lease expired"),
            error_code="lease_expired",
        )
    db.flush()
    return len(stale_rows)


def claim_next_teacher_assist_workflow(
    db: Session,
    *,
    settings: Settings,
    worker_name: str,
    workflow_id: uuid.UUID | None = None,
) -> TeacherAssistWorkflow | None:
    reclaim_stale_teacher_assist_workflows(db)
    query = (
        select(TeacherAssistWorkflow)
        .where(
            TeacherAssistWorkflow.workflow_type == WEEKLY_PLAN_WORKFLOW_TYPE,
            TeacherAssistWorkflow.status == "queued",
        )
        .order_by(TeacherAssistWorkflow.created_at.asc())
    )
    if workflow_id is not None:
        query = query.where(TeacherAssistWorkflow.id == workflow_id)
    workflow = db.scalars(query).first()
    if workflow is None:
        return None

    provider_name = settings.teacher_assist_ai_provider.strip() or "mock"
    _set_workflow_status(workflow, status="running", progress_percent=max(workflow.progress_percent, 1))
    workflow.max_retries = max(0, settings.teacher_assist_worker_max_retries)
    workflow.provider_name = provider_name
    workflow.provider_model = "mock" if provider_name == "mock" else settings.teacher_assist_real_provider_model
    workflow.prompt_version = INSTRUCTIONAL_PLAN_PROMPT_VERSION
    workflow.last_error_code = None
    workflow.error_message = None
    _touch_workflow_heartbeat(
        workflow,
        settings=settings,
        worker_name=worker_name,
        progress_percent=max(workflow.progress_percent, 5),
    )
    _append_workflow_log(
        workflow,
        event="workflow_claimed",
        message="TeacherAssist workflow claimed by worker",
        metadata={
            "worker_name": worker_name,
            "retry_count": workflow.retry_count,
            "max_retries": workflow.max_retries,
        },
    )
    record_activity_event(
        db,
        tenant_id=workflow.tenant_id,
        user_id=workflow.user_id,
        event_type="workflow_started",
        event_category="workflow",
        entity_type="workflow",
        entity_id=workflow.id,
        workflow_id=workflow.id,
        school_year_id=_snapshot_school_year_id(workflow.input_snapshot_json),
        grading_period_id=_snapshot_grading_period_id(workflow.input_snapshot_json),
        class_id=_snapshot_class_id(workflow.input_snapshot_json),
        subject_id=_snapshot_subject_id(workflow.input_snapshot_json),
        summary_text="Started TeacherAssist instructional-plan workflow.",
        details_json={"retry_count": workflow.retry_count, "workflow_type": workflow.workflow_type},
    )
    db.flush()
    return workflow


def create_weekly_plan_workflow(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
) -> TeacherAssistWorkflow:
    draft = get_planning_draft_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    if draft.status != "ready":
        raise ValueError("Planning draft must be marked ready before starting generation")
    readiness = validate_planning_draft_readiness(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    if not readiness.is_ready:
        raise ValueError("Planning draft is not ready for generation")

    snapshot = build_planning_context_snapshot(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    now = datetime.now(UTC)
    settings = Settings()
    workflow = TeacherAssistWorkflow(
        tenant_id=tenant_id,
        user_id=user_id,
        planning_input_draft_id=planning_draft_id,
        workflow_type=validate_teacher_assist_workflow_type(WEEKLY_PLAN_WORKFLOW_TYPE),
        status=validate_teacher_assist_workflow_status("queued"),
        input_snapshot_json=snapshot,
        output_ref_type=None,
        output_ref_id=None,
        error_message=None,
        progress_percent=0,
        leased_by_worker=None,
        lease_expires_at=None,
        heartbeat_at=None,
        retry_count=0,
        max_retries=max(0, settings.teacher_assist_worker_max_retries),
        timeout_at=None,
        provider_name=settings.teacher_assist_ai_provider.strip() or "mock",
        provider_model="mock" if (settings.teacher_assist_ai_provider.strip() or "mock") == "mock" else settings.teacher_assist_real_provider_model,
        prompt_version=INSTRUCTIONAL_PLAN_PROMPT_VERSION,
        input_tokens_total=0,
        output_tokens_total=0,
        estimated_cost_cents_total=0,
        last_error_code=None,
        execution_log_json=[
            {
                "event": "workflow_queued",
                "message": "TeacherAssist workflow queued for worker execution",
                "metadata": {},
                "recorded_at": now.isoformat(),
            }
        ],
        created_at=now,
        started_at=None,
        completed_at=None,
        updated_at=now,
    )
    db.add(workflow)
    db.flush()

    for step_name in WORKFLOW_STEP_SEQUENCE:
        db.add(
            TeacherAssistWorkflowStep(
                workflow_id=workflow.id,
                step_name=step_name,
                status=validate_teacher_assist_workflow_step_status("queued"),
                metadata_json=None,
                error_message=None,
                started_at=None,
                completed_at=None,
                created_at=now,
            )
        )
    db.flush()
    return workflow


def cancel_teacher_assist_workflow(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    workflow_id: uuid.UUID,
) -> TeacherAssistWorkflow:
    workflow = get_teacher_assist_workflow_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        workflow_id=workflow_id,
    )
    if workflow.status in {"completed", "failed", "cancelled"}:
        raise ValueError("Workflow can no longer be cancelled")
    _set_workflow_status(workflow, status="cancelled", progress_percent=workflow.progress_percent)
    _append_workflow_log(
        workflow,
        event="workflow_cancel_requested",
        message="TeacherAssist workflow cancellation requested",
        metadata={"workflow_id": str(workflow.id)},
    )
    for step in workflow.steps:
        if step.status == "queued":
            _set_workflow_step_status(step, status="skipped")
    record_activity_event(
        db,
        tenant_id=workflow.tenant_id,
        user_id=workflow.user_id,
        event_type="workflow_cancelled",
        event_category="workflow",
        entity_type="workflow",
        entity_id=workflow.id,
        workflow_id=workflow.id,
        school_year_id=_snapshot_school_year_id(workflow.input_snapshot_json),
        grading_period_id=_snapshot_grading_period_id(workflow.input_snapshot_json),
        class_id=_snapshot_class_id(workflow.input_snapshot_json),
        subject_id=_snapshot_subject_id(workflow.input_snapshot_json),
        summary_text="Cancelled TeacherAssist instructional-plan workflow.",
        details_json={"workflow_type": workflow.workflow_type},
    )
    db.flush()
    return workflow


def _skip_remaining_steps(steps: list[TeacherAssistWorkflowStep]) -> None:
    for step in steps:
        if step.status == "queued":
            _set_workflow_step_status(step, status="skipped")


def _persist_teacher_assist_workflow_success(
    session: Session,
    *,
    workflow_id: uuid.UUID,
    settings: Settings,
    worker_name: str,
) -> None:
    workflow = _refresh_workflow_for_execution(session, workflow_id)
    steps = list(workflow.steps)
    step_by_name = {step.step_name: step for step in steps}
    snapshot = workflow.input_snapshot_json

    workflow = _ensure_workflow_still_active(
        session,
        workflow_id=workflow.id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=5,
    )
    load_step = step_by_name["load_context_snapshot"]
    _set_workflow_step_status(
        load_step,
        status="running",
        metadata_json={"draft_id": str(workflow.planning_input_draft_id)},
    )
    session.commit()

    workflow = _ensure_workflow_still_active(
        session,
        workflow_id=workflow.id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=15,
    )
    if not snapshot.get("readiness", {}).get("is_ready", False):
        raise ValueError("Saved planning context is no longer ready for generation")
    _set_workflow_step_status(
        load_step,
        status="completed",
        metadata_json={"draft_id": str(workflow.planning_input_draft_id), "readiness": "ready"},
    )
    _append_workflow_log(
        workflow,
        event="context_loaded",
        message="TeacherAssist workflow loaded saved planning snapshot",
        metadata={"workflow_id": str(workflow.id)},
    )
    session.commit()

    workflow = _ensure_workflow_still_active(
        session,
        workflow_id=workflow.id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=25,
    )
    generate_step = step_by_name["generate_instructional_plan"]
    _set_workflow_step_status(
        generate_step,
        status="running",
        metadata_json={
            "provider": workflow.provider_name,
            "prompt_version": workflow.prompt_version,
            "retry_count": workflow.retry_count,
        },
    )
    _append_workflow_log(
        workflow,
        event="provider_execution_started",
        message="TeacherAssist provider execution started",
        metadata={
            "provider": workflow.provider_name,
            "prompt_version": workflow.prompt_version,
        },
    )
    session.commit()

    workflow = _ensure_workflow_still_active(
        session,
        workflow_id=workflow.id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=35,
    )
    _enforce_teacher_assist_cost_limit(session, settings)
    provider = get_teacher_assist_ai_provider(settings, workflow_type=workflow.workflow_type)
    circuit_state = TeacherAssistProviderCircuitBreaker().state_for_provider(
        settings, provider.provider_name
    )
    provider_result = provider.generate_instructional_plan(snapshot)
    _enforce_teacher_assist_model_allowlist(settings, model_name=provider_result.model)
    validated_content = validate_instructional_plan_output(
        provider_result.content_json,
        context_preview=snapshot,
    )
    content = _normalized_weekly_plan_content(
        content_json=validated_content,
        planning_draft_id=workflow.planning_input_draft_id,
        workflow_id=workflow.id,
        version_number=1,
        generated_at=datetime.now(UTC).isoformat(),
        provider_name=provider_result.provider,
        provider_model=provider_result.model,
        prompt_version=str((provider_result.metadata_json or {}).get("prompt_version") or workflow.prompt_version),
        is_mock=bool((provider_result.metadata_json or {}).get("is_mock", provider_result.provider == "mock")),
    )
    content = _build_quality_review_metadata(
        snapshot=snapshot,
        content_json=content,
        provider_name=provider_result.provider,
    )
    workflow.provider_name = provider_result.provider
    workflow.provider_model = provider_result.model
    workflow.prompt_version = (
        str((provider_result.metadata_json or {}).get("prompt_version") or workflow.prompt_version)
    )
    workflow.input_tokens_total = int(provider_result.input_tokens or 0)
    workflow.output_tokens_total = int(provider_result.output_tokens or 0)
    workflow.estimated_cost_cents_total = int(provider_result.estimated_cost_cents or 0)
    _set_workflow_step_status(
        generate_step,
        status="completed",
        metadata_json={
            "provider": provider_result.provider,
            "model": provider_result.model,
            "prompt_version": workflow.prompt_version,
            "subject_sections": len(content.get("subjects", [])),
            "weekly_segments": len(content.get("weekly_segments", [])),
            "input_tokens": provider_result.input_tokens,
            "output_tokens": provider_result.output_tokens,
            "estimated_cost_cents": provider_result.estimated_cost_cents,
            "circuit_state": circuit_state.state,
        },
    )
    _append_workflow_log(
        workflow,
        event="provider_execution_completed",
        message="TeacherAssist provider execution completed",
        metadata={
            "provider": provider_result.provider,
            "model": provider_result.model,
            "prompt_version": workflow.prompt_version,
            "estimated_cost_cents": provider_result.estimated_cost_cents or 0,
            "circuit_state": circuit_state.state,
        },
    )
    _touch_workflow_heartbeat(
        workflow,
        settings=settings,
        worker_name=worker_name,
        progress_percent=65,
    )
    session.commit()

    workflow = _ensure_workflow_still_active(
        session,
        workflow_id=workflow.id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=75,
    )
    persist_step = step_by_name["persist_weekly_plan"]
    _set_workflow_step_status(persist_step, status="running")
    session.commit()

    workflow = _ensure_workflow_still_active(
        session,
        workflow_id=workflow.id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=80,
    )
    now = datetime.now(UTC)
    draft_context = snapshot.get("draft", {})
    draft_row = session.get(TeacherAssistPlanningInputDraft, workflow.planning_input_draft_id)
    pacing_period_id = draft_row.pacing_guide_period_id if draft_row else None
    pacing_guide_id = None
    if pacing_period_id is not None:
        period_row = session.get(TeacherAssistPacingGuidePeriod, pacing_period_id)
        pacing_guide_id = period_row.pacing_guide_id if period_row else None
    plan_title = (
        draft_context.get("plan_title") or draft_context.get("title") or "TeacherAssist Instructional Plan"
    )
    artifact_title_prefix = "Mock Instructional Plan" if provider_result.provider == "mock" else "Instructional Plan"
    weekly_plan = TeacherAssistWeeklyPlan(
        tenant_id=workflow.tenant_id,
        user_id=workflow.user_id,
        owner_user_id=workflow.user_id,
        planning_input_draft_id=workflow.planning_input_draft_id,
        workflow_id=workflow.id,
        planning_scope=draft_context.get("planning_scope") or content.get("planning_scope") or "weekly",
        title=f"{artifact_title_prefix} - {plan_title}",
        module_title=draft_context.get("module_title"),
        start_date=_parse_snapshot_date(draft_context.get("start_date")),
        end_date=_parse_snapshot_date(draft_context.get("end_date")),
        estimated_weeks=draft_context.get("estimated_weeks"),
        instructional_days_count=draft_context.get("instructional_days_count"),
        source_plan_id=None,
        derived_from_plan_id=None,
        is_template=False,
        visibility_scope=validate_plan_visibility_scope("private"),
        reuse_status=validate_plan_reuse_status("active"),
        school_year_origin_id=_parse_snapshot_uuid(draft_context.get("school_year_id")),
        pacing_guide_period_id=pacing_period_id,
        pacing_guide_id=pacing_guide_id,
        status=validate_weekly_plan_status("in_progress"),
        content_json=content,
        source_context_json=snapshot,
        created_at=now,
        updated_at=now,
    )
    session.add(weekly_plan)
    session.flush()
    if pacing_period_id is not None and pacing_guide_id is not None and draft_row is not None:
        from oziebot_api.services.teacher_assist.generated_artifacts import (
            link_lesson_plan_artifact,
            register_generated_artifact,
        )

        linked = link_lesson_plan_artifact(
            session,
            tenant_id=workflow.tenant_id,
            user_id=workflow.user_id,
            planning_draft_id=draft_row.id,
            instructional_plan_id=weekly_plan.id,
        )
        if linked is None:
            owner = session.get(User, workflow.user_id)
            if owner is not None:
                register_generated_artifact(
                    session,
                    tenant_id=workflow.tenant_id,
                    user=owner,
                    pacing_guide_id=pacing_guide_id,
                    pacing_guide_period_id=pacing_period_id,
                    artifact_type="LESSON_PLAN",
                    title=weekly_plan.title,
                    status="completed",
                    instructional_plan_id=weekly_plan.id,
                    planning_draft_id=draft_row.id,
                    metadata={"week_context": {"pacing_guide_period_id": str(pacing_period_id)}},
                )
    _create_weekly_plan_version(
        session,
        weekly_plan=weekly_plan,
        created_by_user_id=workflow.user_id,
        version_number=1,
        change_reason="Initial worker generation",
        created_at=now,
    )
    session.add(
        TeacherAssistAIUsageEvent(
            tenant_id=workflow.tenant_id,
            user_id=workflow.user_id,
            workflow_id=workflow.id,
            provider=provider_result.provider,
            model=provider_result.model,
            feature=INSTRUCTIONAL_PLAN_GENERATION_FEATURE,
            input_tokens=provider_result.input_tokens,
            output_tokens=provider_result.output_tokens,
            estimated_cost_cents=provider_result.estimated_cost_cents,
            metadata_json={
                **dict(provider_result.metadata_json or {}),
                "prompt_version": workflow.prompt_version,
                "worker_name": worker_name,
                "weekly_plan_id": str(weekly_plan.id),
            },
            created_at=now,
        )
    )
    record_activity_event(
        session,
        tenant_id=weekly_plan.tenant_id,
        user_id=workflow.user_id,
        event_type="plan_created",
        event_category="planning",
        entity_type="weekly_plan",
        entity_id=weekly_plan.id,
        workflow_id=workflow.id,
        school_year_id=_plan_school_year_id(weekly_plan),
        grading_period_id=_plan_grading_period_id(weekly_plan),
        class_id=_plan_class_id(weekly_plan),
        subject_id=_plan_subject_id(weekly_plan),
        summary_text=f"Generated instructional plan '{weekly_plan.title}'.",
        details_json={
            "planning_scope": weekly_plan.planning_scope,
            "provider_name": provider_result.provider,
            "provider_model": provider_result.model,
            "prompt_version": workflow.prompt_version,
        },
        event_timestamp=now,
    )
    _set_workflow_step_status(
        persist_step,
        status="completed",
        metadata_json={"weekly_plan_id": str(weekly_plan.id)},
    )
    workflow.output_ref_type = WORKFLOW_OUTPUT_REF_TYPE
    workflow.output_ref_id = weekly_plan.id
    _append_workflow_log(
        workflow,
        event="artifact_persisted",
        message="TeacherAssist instructional plan artifact persisted",
        metadata={"weekly_plan_id": str(weekly_plan.id)},
    )
    _touch_workflow_heartbeat(
        workflow,
        settings=settings,
        worker_name=worker_name,
        progress_percent=90,
    )
    session.commit()

    workflow = _ensure_workflow_still_active(
        session,
        workflow_id=workflow.id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=95,
    )
    finalize_step = step_by_name["finalize_workflow"]
    _set_workflow_step_status(finalize_step, status="running")
    workflow.progress_percent = 100
    _set_workflow_status(workflow, status="completed", progress_percent=100)
    _set_workflow_step_status(
        finalize_step,
        status="completed",
        metadata_json={"output_ref_type": workflow.output_ref_type, "output_ref_id": str(weekly_plan.id)},
    )
    _append_workflow_log(
        workflow,
        event="workflow_completed",
        message="TeacherAssist workflow completed successfully",
        metadata={"output_ref_id": str(weekly_plan.id)},
    )
    record_activity_event(
        session,
        tenant_id=workflow.tenant_id,
        user_id=workflow.user_id,
        event_type="workflow_completed",
        event_category="workflow",
        entity_type="workflow",
        entity_id=workflow.id,
        workflow_id=workflow.id,
        school_year_id=_snapshot_school_year_id(workflow.input_snapshot_json),
        grading_period_id=_snapshot_grading_period_id(workflow.input_snapshot_json),
        class_id=_snapshot_class_id(workflow.input_snapshot_json),
        subject_id=_snapshot_subject_id(workflow.input_snapshot_json),
        summary_text="Completed TeacherAssist instructional-plan workflow.",
        details_json={"output_ref_id": str(weekly_plan.id), "workflow_type": workflow.workflow_type},
    )
    session.commit()


def _persist_teacher_assist_workflow_failure(
    factory,
    *,
    workflow_id: uuid.UUID,
    exc: Exception,
    error_code: str,
) -> None:
    failure_session = factory()
    try:
        workflow = _refresh_workflow_for_execution(failure_session, workflow_id)
        _mark_workflow_for_retry_or_failure(workflow, exc=exc, error_code=error_code)
        record_activity_event(
            failure_session,
            tenant_id=workflow.tenant_id,
            user_id=workflow.user_id,
            event_type="workflow_failed",
            event_category="workflow",
            entity_type="workflow",
            entity_id=workflow.id,
            workflow_id=workflow.id,
            school_year_id=_snapshot_school_year_id(workflow.input_snapshot_json),
            grading_period_id=_snapshot_grading_period_id(workflow.input_snapshot_json),
            class_id=_snapshot_class_id(workflow.input_snapshot_json),
            subject_id=_snapshot_subject_id(workflow.input_snapshot_json),
            summary_text=(
                "TeacherAssist instructional-plan workflow failed and is queued to retry."
                if workflow.status == "queued"
                else "TeacherAssist instructional-plan workflow failed."
            ),
            details_json={
                "error_code": error_code,
                "retry_count": workflow.retry_count,
                "max_retries": workflow.max_retries,
                "status": workflow.status,
            },
        )
        failure_session.commit()
    finally:
        failure_session.close()


def _persist_teacher_assist_workflow_cancelled(
    factory,
    *,
    workflow_id: uuid.UUID,
) -> None:
    cancel_session = factory()
    try:
        workflow = _refresh_workflow_for_execution(cancel_session, workflow_id)
        _clear_workflow_lease(workflow)
        current_running_step = next((step for step in workflow.steps if step.status == "running"), None)
        if current_running_step is not None:
            _set_workflow_step_status(
                current_running_step,
                status="skipped",
                metadata_json={"cancelled": True},
            )
        _append_workflow_log(
            workflow,
            event="workflow_cancelled",
            message="TeacherAssist worker observed workflow cancellation",
            metadata={"workflow_id": str(workflow.id)},
        )
        cancel_session.commit()
    finally:
        cancel_session.close()


def _process_teacher_assist_workflow_with_factory(
    factory,
    workflow_id: uuid.UUID,
    settings: Settings,
    *,
    worker_name: str,
) -> None:
    session = factory()
    try:
        _persist_teacher_assist_workflow_success(
            session,
            workflow_id=workflow_id,
            settings=settings,
            worker_name=worker_name,
        )
    except TeacherAssistWorkflowCancelledError:
        session.rollback()
        _persist_teacher_assist_workflow_cancelled(factory, workflow_id=workflow_id)
    except TimeoutError as exc:
        session.rollback()
        _persist_teacher_assist_workflow_failure(
            factory,
            workflow_id=workflow_id,
            exc=exc,
            error_code="timeout",
        )
    except Exception as exc:
        session.rollback()
        _persist_teacher_assist_workflow_failure(
            factory,
            workflow_id=workflow_id,
            exc=exc,
            error_code="execution_failed",
        )
    finally:
        session.close()


def process_next_teacher_assist_workflow_with_engine(
    engine: Engine,
    *,
    settings: Settings | None = None,
    worker_name: str = "teacher-assist-worker",
    workflow_id: uuid.UUID | None = None,
) -> uuid.UUID | None:
    settings = settings or Settings()
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    claim_session = factory()
    try:
        workflow = claim_next_teacher_assist_workflow(
            claim_session,
            settings=settings,
            worker_name=worker_name,
            workflow_id=workflow_id,
        )
        if workflow is None:
            claim_session.commit()
            return None
        claimed_id = workflow.id
        claim_session.commit()
    finally:
        claim_session.close()
    _process_teacher_assist_workflow_with_factory(
        factory,
        claimed_id,
        settings,
        worker_name=worker_name,
    )
    return claimed_id


def process_claimed_teacher_assist_workflow_with_engine(
    engine: Engine,
    workflow_id: uuid.UUID,
    *,
    settings: Settings | None = None,
    worker_name: str = "teacher-assist-worker",
) -> None:
    settings = settings or Settings()
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    _process_teacher_assist_workflow_with_factory(
        factory,
        workflow_id,
        settings,
        worker_name=worker_name,
    )


def process_teacher_assist_workflow_with_engine(
    engine: Engine,
    workflow_id: uuid.UUID,
    *,
    worker_name: str = "teacher-assist-worker-inline",
) -> None:
    process_claimed_teacher_assist_workflow_with_engine(
        engine,
        workflow_id,
        settings=Settings(),
        worker_name=worker_name,
    )

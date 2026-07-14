"""Instructional package generation for TeacherAssist v2."""

from __future__ import annotations

import logging
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import delete as _sa_delete, select
from sqlalchemy.orm import Session, sessionmaker

from oziebot_api.config import Settings
from oziebot_api.db.session import make_session_factory
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
    TeacherAssistV2PlanningSupplementalMaterial,
)
from oziebot_api.models.teacher_assist_v2_slide_visual_asset import TeacherAssistV2SlideVisualAsset
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_usage import get_effective_daily_cost_limit_cents
from oziebot_api.services.teacher_assist.ai_mode import is_teacher_assist_real_ai_active
from oziebot_api.services.teacher_assist.constants import validate_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.artifact_persistence import (
    attach_qr_student_packet,
    attach_quality_report,
    persist_package_artifact,
)
from oziebot_api.services.teacher_assist_v2.image_search import (
    create_pending_image_assets,
    fetch_slide_images_for_artifact,
)
from oziebot_api.services.teacher_assist_v2.assignments import maybe_create_assignment_for_artifact
from oziebot_api.services.teacher_assist_v2.deterministic_package_content import (
    build_daily_lesson_plan,
    build_deterministic_fallback,
    build_rubric_for_written_assignment,
    build_rubric_for_writing_response,
    build_student_lesson_deck,
)
from oziebot_api.services.teacher_assist_v2.instructional_artifact_engine import (
    ARTIFACT_ENGINE_VERSION,
    build_vocabulary_registry,
    enforce_instructional_contract,
    enforce_student_content_prohibitions,
    extract_instructional_contract,
    validate_week_alignment,
)
from oziebot_api.services.teacher_assist_v2.instructional_resource_bank import (
    build_resource_alignment_map,
)
from oziebot_api.services.teacher_assist_v2.instructional_delivery_profile import (
    resolve_profile as resolve_delivery_profile,
)
from oziebot_api.services.teacher_assist_v2.instructional_design_plan import (
    generate_instructional_design_plan,
    get_plan_for_day,
)
from oziebot_api.services.teacher_assist_v2.instructional_validation import (
    validate_and_revise_instructional_plan,
)
from oziebot_api.services.teacher_assist_v2.instructional_package_ai import (
    generate_curriculum_sequence_plan,
    generate_v2_instructional_artifact,
)
from oziebot_api.services.teacher_assist_v2.instructional_variety import VarietyTracker
from oziebot_api.services.teacher_assist_v2.instructional_quality_review import (
    review_artifact,
    REVIEWABLE_TYPES as _QUALITY_REVIEWABLE_TYPES,
)
from oziebot_api.services.teacher_assist_v2.package_export import (
    render_artifact_preview_html,
    render_slide_deck_html,
)
from oziebot_api.services.teacher_assist_v2.package_artifact_refresh import SLIDE_DECK_EXPORT_NOTE
from oziebot_api.services.teacher_assist_v2.teacher_coaching import assemble_teaching_brief
from oziebot_api.services.teacher_assist_v2.package_lifecycle import (
    build_package_title,
    resolve_default_plan_dates,
    resolve_effective_package_status,
)
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import (
    build_subject_lesson_block_from_pacing,
    resolve_daily_plan_objective_text,
    resolve_daily_plan_summary,
    resolve_pacing_day_plan,
    resolve_subject_daily_topic,
)
from oziebot_api.services.teacher_assist_v2.planning_constants import (
    ARTIFACT_PROMPT_VERSIONS,
    OPTIONAL_PACKAGE_OUTPUTS,
    REQUIRED_PACKAGE_OUTPUTS,
    WEEKDAY_LABELS,
)
from oziebot_api.services.teacher_assist_v2.generation_cache import (
    build_artifact_provenance,
    build_generation_manifest,
    compute_curriculum_cache_key,
    compute_curriculum_hash,
    compute_delivery_hash,
    compute_planning_cache_key,
    compute_planning_hash,
    lookup_generation_cache,
    store_generation_cache,
)
from oziebot_api.services.teacher_assist_v2.planning_context import (
    build_teacher_planning_generation_context,
)
from oziebot_api.services.teacher_assist_v2.planning_workflow import _assignment_context

_DAILY_FOCUS_ROTATION = [
    "Launch the week's learning target and activate prior knowledge.",
    "Build understanding through guided practice and discussion.",
    "Apply the skill with collaborative and independent tasks.",
    "Use evidence from text or problems to justify thinking.",
    "Review, reflect, and prepare for the weekly assessment.",
]

logger = logging.getLogger(__name__)
PACKAGE_PROCESSING_MESSAGE = (
    "Your instructional package is being processed and will be available soon."
)
PACKAGE_FAILURE_PREFIX = "Package generation failed."


def _now() -> datetime:
    return datetime.now(UTC)


def _build_package_metadata(
    *,
    context: dict[str, Any],
    stored_status: str,
    plan_start_date: date,
    plan_end_date: date,
    generation_state: str,
    status_detail: str | None = None,
    error_message: str | None = None,
    generation_started_at: str | None = None,
    generation_completed_at: str | None = None,
    generation_duration_seconds: float | None = None,
    job_excluded_ids: list[str] | None = None,
) -> dict[str, Any]:
    metadata = {
        "is_mock": False,
        "context_weeks": len(context["weeks"]),
        "effective_status": resolve_effective_package_status(
            stored_status=stored_status,
            plan_start_date=plan_start_date,
            plan_end_date=plan_end_date,
            today=date.today(),
        ),
        "ai_readiness_summary": context.get("ai_readiness_summary"),
        "generation_document_usage": {
            "district": context.get("district_document_context"),
            "teacher": context.get("teacher_document_context"),
            "district_links": context.get("district_link_context"),
            "teacher_links": context.get("teacher_link_context"),
        },
        "generation_state": generation_state,
    }
    if status_detail:
        metadata["status_detail"] = status_detail
    if error_message:
        metadata["generation_error"] = error_message
    if generation_started_at:
        metadata["generation_started_at"] = generation_started_at
    if generation_completed_at:
        metadata["generation_completed_at"] = generation_completed_at
    if generation_duration_seconds is not None:
        metadata["generation_duration_seconds"] = generation_duration_seconds
    if job_excluded_ids is not None:
        metadata["job_excluded_ids"] = job_excluded_ids
    return metadata


def _validate_outputs(selected_outputs: list[str]) -> list[str]:
    normalized = []
    for value in selected_outputs:
        key = value.strip()
        if key not in REQUIRED_PACKAGE_OUTPUTS + OPTIONAL_PACKAGE_OUTPUTS:
            raise ValueError(f"Unsupported output type '{key}'")
        if key not in normalized:
            normalized.append(key)
    for required in REQUIRED_PACKAGE_OUTPUTS:
        if required not in normalized:
            raise ValueError(f"Missing required output '{required}'")
    return normalized


def _resolve_artifact_content(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    context: dict[str, Any],
    artifact_type: str,
    deterministic_content: dict[str, Any],
    week: dict[str, Any],
    subject_meta: dict[str, Any] | None = None,
    week_subject: dict[str, Any] | None = None,
    day_label: str | None = None,
    title_hint: str | None = None,
    introduced_vocabulary: list[str] | None = None,
    instructional_day_number: int | None = None,
    total_planned_days: int | None = None,
) -> dict[str, Any]:
    try:
        ai_content = generate_v2_instructional_artifact(
            db,
            settings=settings,
            user=user,
            tenant_id=package.tenant_id,
            package_id=package.id,
            artifact_type=artifact_type,
            generation_context=context,
            week=week,
            subject_meta=subject_meta,
            week_subject=week_subject,
            day_label=day_label,
            title_hint=title_hint,
            introduced_vocabulary=introduced_vocabulary,
            instructional_day_number=instructional_day_number,
            total_planned_days=total_planned_days,
        )
        return ai_content if ai_content is not None else deterministic_content
    except Exception:
        db.rollback()
        logger.exception(
            "Sequential AI call failed — artifact_type=%s week=%s day=%s; using deterministic fallback",
            artifact_type,
            (week or {}).get("sequence_number"),
            day_label,
        )
        return deterministic_content


def _objective_fields(
    week_subject: dict[str, Any] | None, subject_name: str
) -> tuple[str | None, str, list[str], list[str], list[str]]:
    objective_code = None
    objective_text = f"Students demonstrate understanding in {subject_name}."
    objectives_list: list[str] = []
    objective_ids: list[str] = []
    teks_ids: list[str] = []
    if week_subject and week_subject.get("objectives"):
        first = week_subject["objectives"][0]
        objective_code = first.get("objective_code")
        objective_text = first.get("description") or objective_text
        objectives_list = [
            str(row.get("description") or row.get("objective_code"))
            for row in week_subject["objectives"]
            if row.get("description") or row.get("objective_code")
        ]
        objective_ids = [
            str(row.get("education_objective_id"))
            for row in week_subject["objectives"]
            if row.get("education_objective_id")
        ]
        teks_ids = [
            str(row.get("objective_code"))
            for row in week_subject["objectives"]
            if row.get("objective_code")
        ]
    return objective_code, objective_text, objectives_list, objective_ids, teks_ids


def _real_ai_requested_but_inactive(db: Session, settings: Settings) -> str | None:
    effective = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(effective.teacher_assist_ai_provider)
    real_provider_enabled = bool(
        effective.teacher_assist_real_provider_enabled
        or effective.teacher_assist_ai_enable_real_provider
    )
    if provider_name != "openai" or not real_provider_enabled:
        return None
    if is_teacher_assist_real_ai_active(db, settings):
        return None

    blockers: list[str] = []
    if not (effective.teacher_assist_openai_api_key or "").strip():
        blockers.append("TEACHER_ASSIST_OPENAI_API_KEY is missing on the server")
    if get_effective_daily_cost_limit_cents(db, effective) <= 0:
        blockers.append("the daily AI cost limit is not set")
    circuit = TeacherAssistProviderCircuitBreaker().state_for_provider(effective, provider_name)
    if circuit.state != "closed" and circuit.reason:
        blockers.append(circuit.reason)
    if not blockers:
        blockers.append("real OpenAI mode is not currently executable")
    return "; ".join(blockers)


def _material_excerpt(row: dict[str, Any]) -> str | None:
    extraction = row.get("extraction") or {}
    excerpt = (
        extraction.get("effective_text_excerpt")
        or extraction.get("teacher_edited_text")
        or extraction.get("extracted_text_preview")
        or extraction.get("extracted_text")
    )
    if not excerpt:
        return None
    title = row.get("title") or row.get("display_name") or row.get("original_filename") or "Source"
    normalized = " ".join(str(excerpt).split())
    return f"{title}: {normalized[:700]}"


def _link_context_excerpts(context: dict[str, Any]) -> list[str]:
    excerpts: list[str] = []
    for key in ("district_link_context", "teacher_link_context"):
        link_context = context.get(key) or {}
        for row in link_context.get("used_links") or []:
            title = row.get("title") or row.get("external_url") or "Reference link"
            excerpt = row.get("excerpt")
            if excerpt:
                excerpts.append(f"{title}: {' '.join(str(excerpt).split())[:700]}")
    return excerpts


def _grounding_fields(
    week_subject: dict[str, Any] | None,
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    pacing_context = (week_subject or {}).get("pacing_context") or {}
    days = pacing_context.get("days") or []
    source_materials: list[str] = []
    source_excerpts: list[str] = []
    daily_topics: list[str] = []
    daily_objectives: list[str] = []
    assessment_checks: list[str] = []
    for day in days:
        if day.get("materials_needed"):
            source_materials.append(str(day["materials_needed"]))
        if day.get("daily_topic"):
            daily_topics.append(str(day["daily_topic"]))
        if day.get("objective_focus"):
            daily_objectives.append(str(day["objective_focus"]))
        if day.get("assessment_check"):
            assessment_checks.append(str(day["assessment_check"]))
        for bucket in ("attached_files", "reference_links", "notes"):
            for row in day.get(bucket) or []:
                title = row.get("title") or row.get("display_name") or row.get("original_filename")
                if title:
                    source_materials.append(str(title))
                excerpt = _material_excerpt(row)
                if excerpt:
                    source_excerpts.append(excerpt)
    for bucket in (
        "guide_level_materials",
        "week_level_materials",
        "day_level_materials",
        "objective_level_materials",
        "catalog_resources",
    ):
        for row in pacing_context.get(bucket) or []:
            title = row.get("title") or row.get("display_name") or row.get("original_filename")
            if title:
                source_materials.append(str(title))
            excerpt = _material_excerpt(row)
            if excerpt:
                source_excerpts.append(excerpt)
    return source_materials, source_excerpts, daily_topics, daily_objectives, assessment_checks


def _link_assessment_rubric(
    assessment_artifact: TeacherAssistV2InstructionalPackageArtifact,
    rubric_artifact: TeacherAssistV2InstructionalPackageArtifact,
    *,
    assessment_content: dict[str, Any],
) -> None:
    assessment_content["rubric_reference"] = rubric_artifact.title
    assessment_content["linked_rubric_artifact_id"] = str(rubric_artifact.id)
    assessment_artifact.content_json = assessment_content
    assessment_artifact.title = str(
        assessment_content.get("title") or assessment_artifact.title or "Assessment"
    )
    assessment_metadata = dict(assessment_artifact.metadata_json or {})
    assessment_metadata["linked_rubric_artifact_id"] = str(rubric_artifact.id)
    assessment_artifact.metadata_json = assessment_metadata
    rubric_metadata = dict(rubric_artifact.metadata_json or {})
    rubric_metadata["linked_assessment_artifact_id"] = str(assessment_artifact.id)
    if assessment_artifact.artifact_type == "writing_response":
        rubric_metadata["linked_writing_response_artifact_id"] = str(assessment_artifact.id)
    rubric_artifact.metadata_json = rubric_metadata


def _link_writing_response_rubric(
    writing_artifact: TeacherAssistV2InstructionalPackageArtifact,
    rubric_artifact: TeacherAssistV2InstructionalPackageArtifact,
    *,
    writing_content: dict[str, Any],
) -> None:
    _link_assessment_rubric(writing_artifact, rubric_artifact, assessment_content=writing_content)


def _persist_linked_assessment_rubric(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    assessment_artifact: TeacherAssistV2InstructionalPackageArtifact,
    assessment_content: dict[str, Any],
    context: dict[str, Any],
    week: dict[str, Any],
    week_subject: dict[str, Any] | None,
    subject_meta: dict[str, Any],
    provider_name: str,
    sequence: int,
    now: datetime,
    rubric_deterministic: dict[str, Any],
    preview_artifact_type: str,
) -> tuple[int, TeacherAssistV2InstructionalPackageArtifact]:
    rubric_content = _resolve_artifact_content(
        db,
        settings=settings,
        user=user,
        package=package,
        context=context,
        artifact_type="rubric",
        deterministic_content=rubric_deterministic,
        week=week,
        subject_meta=subject_meta,
        week_subject=week_subject,
        title_hint=rubric_deterministic["title"],
    )
    sequence += 1
    rubric_artifact = persist_package_artifact(
        db,
        settings=settings,
        package=package,
        artifact_type="rubric",
        content=rubric_content,
        provider_name=provider_name,
        sequence_number=sequence,
        created_at=now,
        subject_id=assessment_artifact.subject_id,
        period_id=assessment_artifact.period_id,
    )
    _link_assessment_rubric(
        assessment_artifact, rubric_artifact, assessment_content=assessment_content
    )
    assessment_artifact.preview_html = render_artifact_preview_html(
        artifact_type=preview_artifact_type,
        content=assessment_content,
    )
    assessment_artifact.updated_at = now
    return sequence, rubric_artifact


def _persist_linked_writing_rubric(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    writing_artifact: TeacherAssistV2InstructionalPackageArtifact,
    writing_content: dict[str, Any],
    context: dict[str, Any],
    week: dict[str, Any],
    week_subject: dict[str, Any] | None,
    subject_meta: dict[str, Any],
    provider_name: str,
    sequence: int,
    now: datetime,
) -> tuple[int, TeacherAssistV2InstructionalPackageArtifact]:
    objective_code, objective_text, _, objective_ids, teks_ids = _objective_fields(
        week_subject, subject_meta["subject_name"]
    )
    rubric_deterministic = build_rubric_for_writing_response(
        writing_content=writing_content,
        subject_name=subject_meta["subject_name"],
        package_title=package.title,
        objective_code=objective_code,
        objective_text=objective_text,
    )
    rubric_deterministic["objective_ids"] = objective_ids
    rubric_deterministic["teks_ids"] = teks_ids
    rubric_deterministic["alignment_summary"] = (
        f"Aligned to {objective_code or 'selected objective'}: {objective_text}"
    )
    if isinstance(rubric_deterministic.get("objective_mapping"), dict):
        rubric_deterministic["objective_mapping"]["objective_ids"] = objective_ids
        rubric_deterministic["objective_mapping"]["teks_ids"] = teks_ids
        rubric_deterministic["objective_mapping"]["alignment_summary"] = rubric_deterministic[
            "alignment_summary"
        ]
    return _persist_linked_assessment_rubric(
        db,
        settings=settings,
        user=user,
        package=package,
        assessment_artifact=writing_artifact,
        assessment_content=writing_content,
        context=context,
        week=week,
        week_subject=week_subject,
        subject_meta=subject_meta,
        provider_name=provider_name,
        sequence=sequence,
        now=now,
        rubric_deterministic=rubric_deterministic,
        preview_artifact_type="writing_response",
    )


def _persist_linked_assignment_rubric(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    assignment_artifact: TeacherAssistV2InstructionalPackageArtifact,
    assignment_content: dict[str, Any],
    context: dict[str, Any],
    week: dict[str, Any],
    week_subject: dict[str, Any] | None,
    subject_meta: dict[str, Any],
    provider_name: str,
    sequence: int,
    now: datetime,
) -> tuple[int, TeacherAssistV2InstructionalPackageArtifact]:
    objective_code, objective_text, _, objective_ids, teks_ids = _objective_fields(
        week_subject, subject_meta["subject_name"]
    )
    rubric_deterministic = build_rubric_for_written_assignment(
        assignment_content=assignment_content,
        subject_name=subject_meta["subject_name"],
        package_title=package.title,
        objective_code=objective_code,
        objective_text=objective_text,
    )
    rubric_deterministic["objective_ids"] = objective_ids
    rubric_deterministic["teks_ids"] = teks_ids
    rubric_deterministic["alignment_summary"] = (
        f"Aligned to {objective_code or 'selected objective'}: {objective_text}"
    )
    if isinstance(rubric_deterministic.get("objective_mapping"), dict):
        rubric_deterministic["objective_mapping"]["objective_ids"] = objective_ids
        rubric_deterministic["objective_mapping"]["teks_ids"] = teks_ids
        rubric_deterministic["objective_mapping"]["alignment_summary"] = rubric_deterministic[
            "alignment_summary"
        ]
    return _persist_linked_assessment_rubric(
        db,
        settings=settings,
        user=user,
        package=package,
        assessment_artifact=assignment_artifact,
        assessment_content=assignment_content,
        context=context,
        week=week,
        week_subject=week_subject,
        subject_meta=subject_meta,
        provider_name=provider_name,
        sequence=sequence,
        now=now,
        rubric_deterministic=rubric_deterministic,
        preview_artifact_type="assignment",
    )


def prepare_instructional_package_generation(
    db: Session,
    *,
    settings: Settings,
    user: User,
    week_start: int,
    week_end: int,
    teaching_order: list[uuid.UUID],
    selected_outputs: list[str],
    plan_start_date: date | None = None,
    plan_end_date: date | None = None,
    excluded_pacing_material_ids: list[uuid.UUID] | None = None,
    instructional_delivery_profile: dict | None = None,
    lost_instructional_days: int = 0,
    quality_review_enabled: bool = True,
) -> tuple[
    TeacherAssistV2InstructionalPackage,
    dict[str, Any],
    list[str],
    set[str],
    str,
]:
    outputs = _validate_outputs(selected_outputs)
    if plan_start_date is None or plan_end_date is None:
        default_start, default_end = resolve_default_plan_dates(
            db, user=user, week_start=week_start, week_end=week_end
        )
        plan_start_date = plan_start_date or default_start
        plan_end_date = plan_end_date or default_end
    if plan_end_date < plan_start_date:
        raise ValueError({"plan_end_date": "End date must be on or after start date."})

    total_weeks = week_end - week_start + 1
    total_curriculum_days = total_weeks * 5
    lost_days = max(0, min(int(lost_instructional_days), total_curriculum_days - 1))
    total_planned_days = total_curriculum_days - lost_days

    context = build_teacher_planning_generation_context(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        teaching_order=teaching_order,
        selected_outputs=outputs,
        settings=settings,
        excluded_pacing_material_ids=excluded_pacing_material_ids,
    )
    excluded_ids = {str(value) for value in (excluded_pacing_material_ids or [])}
    base = _assignment_context(db, user=user)
    onboarding = base["onboarding"]
    inactive_real_ai_reason = _real_ai_requested_but_inactive(db, settings)
    if inactive_real_ai_reason:
        raise ValueError(
            "TeacherAssist is configured for real OpenAI generation, but it cannot run: "
            f"{inactive_real_ai_reason}. Package generation was stopped to avoid producing "
            "generic deterministic content."
        )
    real_ai = is_teacher_assist_real_ai_active(db, settings)
    provider_name = "openai" if real_ai else "deterministic"
    subject_names = [row["subject_name"] for row in context["subjects"]]
    primary_guide_id = (
        uuid.UUID(context["pacing_guide_ids"][0]) if context["pacing_guide_ids"] else None
    )
    today = date.today()
    final_status = "active" if plan_start_date <= today <= plan_end_date else "generated"

    now = _now()
    resolved_delivery_profile = resolve_delivery_profile(instructional_delivery_profile)
    package = TeacherAssistV2InstructionalPackage(
        id=uuid.uuid4(),
        tenant_id=base["ctx"].tenant_id,
        teacher_user_id=user.id,
        platform_school_year_id=base["platform_year"].id,
        catalog_state_id=onboarding.state_id,
        catalog_district_id=onboarding.district_id,
        catalog_school_id=onboarding.school_id,
        catalog_grade_id=onboarding.grade_id,
        subject_ids_json=[row["subject_id"] for row in context["subjects"]],
        pacing_guide_ids_json=context["pacing_guide_ids"],
        primary_pacing_guide_id=primary_guide_id,
        title=build_package_title(
            week_start=week_start, week_end=week_end, subject_names=subject_names
        ),
        week_start=week_start,
        week_end=week_end,
        plan_start_date=plan_start_date,
        plan_end_date=plan_end_date,
        teaching_order_json=[str(value) for value in teaching_order],
        selected_outputs_json=outputs,
        status="processing",
        provider_name=provider_name,
        instructional_delivery_profile=resolved_delivery_profile,
        metadata_json={
            **_build_package_metadata(
                context=context,
                stored_status="processing",
                plan_start_date=plan_start_date,
                plan_end_date=plan_end_date,
                generation_state="queued",
                status_detail=PACKAGE_PROCESSING_MESSAGE,
                job_excluded_ids=sorted(excluded_ids),
            ),
            "total_curriculum_days": total_curriculum_days,
            "lost_instructional_days": lost_days,
            "total_planned_days": total_planned_days,
            "quality_review_enabled": quality_review_enabled,
        },
        created_at=now,
        updated_at=now,
    )
    db.add(package)
    db.flush()

    supplemental_rows = db.scalars(
        select(TeacherAssistV2PlanningSupplementalMaterial).where(
            TeacherAssistV2PlanningSupplementalMaterial.teacher_user_id == user.id,
            TeacherAssistV2PlanningSupplementalMaterial.week_start == week_start,
            TeacherAssistV2PlanningSupplementalMaterial.week_end == week_end,
            TeacherAssistV2PlanningSupplementalMaterial.package_id.is_(None),
            TeacherAssistV2PlanningSupplementalMaterial.active.is_(True),
        )
    ).all()
    for row in supplemental_rows:
        row.package_id = package.id
        row.updated_at = now

    return package, context, outputs, excluded_ids, final_status


_PARALLEL_MAX_WORKERS = 10
_RATE_LIMIT_MAX_RETRIES = 3
_RATE_LIMIT_BASE_DELAY = 6.0  # seconds
_RATE_LIMIT_MAX_DELAY = 45.0  # cap: never wait more than 45s regardless of what the API suggests


def _call_ai_for_job(
    *,
    session_factory: Any,
    settings: Settings,
    user_id: uuid.UUID,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    artifact_type: str,
    generation_context: dict[str, Any],
    week: dict[str, Any],
    subject_meta: dict[str, Any] | None,
    week_subject: dict[str, Any] | None,
    day_label: str | None,
    title_hint: str | None,
    deterministic_content: dict[str, Any],
    introduced_vocabulary: list[str] | None = None,
    instructional_day_number: int | None = None,
    total_planned_days: int | None = None,
) -> dict[str, Any]:
    """Run one AI artifact call in its own DB session thread. Retries on 429; falls back to deterministic on persistent error."""
    db = session_factory()
    try:
        user = db.get(User, user_id)
        if user is None:
            return deterministic_content

        last_exc: Exception | None = None
        for attempt in range(_RATE_LIMIT_MAX_RETRIES):
            try:
                result = generate_v2_instructional_artifact(
                    db,
                    settings=settings,
                    user=user,
                    tenant_id=tenant_id,
                    package_id=package_id,
                    artifact_type=artifact_type,
                    generation_context=generation_context,
                    week=week,
                    subject_meta=subject_meta,
                    week_subject=week_subject,
                    day_label=day_label,
                    title_hint=title_hint,
                    introduced_vocabulary=introduced_vocabulary,
                    instructional_day_number=instructional_day_number,
                    total_planned_days=total_planned_days,
                )
                db.commit()
                return result if result is not None else deterministic_content
            except Exception as exc:
                db.rollback()
                msg = str(exc)
                _msg_lower = msg.lower()
                _is_retryable_429 = (
                    "429" in msg
                    and "credits are depleted" not in _msg_lower
                    and "prepayment credits" not in _msg_lower
                    and attempt < _RATE_LIMIT_MAX_RETRIES - 1
                )
                if _is_retryable_429:
                    # Parse the suggested retry delay from the API error message, but cap it.
                    match = re.search(r"try again in ([\d.]+)s", msg)
                    raw_delay = (
                        float(match.group(1)) + 1.0
                        if match
                        else _RATE_LIMIT_BASE_DELAY * (attempt + 1)
                    )
                    delay = min(raw_delay, _RATE_LIMIT_MAX_DELAY)
                    logger.warning(
                        "Rate limit on attempt %d for artifact_type=%s week=%s day=%s — retrying in %.1fs",
                        attempt + 1,
                        artifact_type,
                        (week or {}).get("sequence_number"),
                        day_label,
                        delay,
                    )
                    time.sleep(delay)
                    last_exc = exc
                else:
                    last_exc = exc
                    break

        logger.exception(
            "Parallel AI call failed after %d attempts — artifact_type=%s week=%s day=%s; using deterministic fallback",
            _RATE_LIMIT_MAX_RETRIES,
            artifact_type,
            (week or {}).get("sequence_number"),
            day_label,
            exc_info=last_exc,
        )
        return deterministic_content
    except Exception:
        db.rollback()
        logger.exception(
            "Unexpected error in parallel AI call — artifact_type=%s week=%s day=%s; using deterministic fallback",
            artifact_type,
            (week or {}).get("sequence_number"),
            day_label,
        )
        return deterministic_content
    finally:
        db.close()


def _populate_instructional_package(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    context: dict[str, Any],
    teaching_order: list[str],
    outputs: list[str],
    excluded_ids: set[str],
    final_status: str,
    session_factory: Any = None,
    quality_review_enabled: bool = True,
) -> TeacherAssistV2InstructionalPackage:
    import time as _time_module

    _generation_start_time = _time_module.monotonic()
    _generation_started_at = _now().isoformat()

    # ── Incremental generation setup ──────────────────────────────────────────
    _pkg_regen_options: dict[str, Any] = (package.metadata_json or {}).get("regen_options") or {}
    _regen_scope: str = _pkg_regen_options.get("regen_scope", "full")
    _regen_artifact_types: set[str] = set(_pkg_regen_options.get("regen_artifact_types") or [])
    # Hashes computed once and stored in the manifest; used for dirty-checking on future regens.
    _curriculum_hash = compute_curriculum_hash(context)
    _delivery_hash = compute_delivery_hash(package.instructional_delivery_profile)
    # _planning_hash computed after Phase 0 stages complete (depends on their output)
    _planning_hash: str = ""
    # Track generation_source counts per artifact_type for the manifest
    _artifact_source_counts: dict[str, dict[str, int]] = {}
    # Track which Layer 1/2 cache entries were hit this run
    _cache_layer_hits: dict[str, bool] = {"curriculum": False, "planning": False}

    def _record_source(artifact_type: str, source: str) -> None:
        counts = _artifact_source_counts.setdefault(
            artifact_type, {"ai": 0, "deterministic": 0, "cached": 0}
        )
        counts[source] = counts.get(source, 0) + 1

    provider_name = package.provider_name or "deterministic"

    # Guard against silent fallback: if this package was queued for real AI but the
    # current process (e.g. the background worker) cannot reach OpenAI, fail loudly
    # rather than producing deterministic content mislabeled as AI-generated.
    if provider_name == "openai" and not is_teacher_assist_real_ai_active(db, settings):
        raise RuntimeError(
            "Package was queued with provider_name='openai' but real AI is not active in this "
            "process. Ensure TEACHER_ASSIST_OPENAI_API_KEY, TEACHER_ASSIST_REAL_PROVIDER_ENABLED, "
            "and TEACHER_ASSIST_AI_DAILY_COST_LIMIT_CENTS are set in the worker environment."
        )

    # Use parallel AI calls when a session factory is available (background worker path).
    # Parallel execution fires all AI calls for a week concurrently; each thread uses its
    # own DB session so SQLAlchemy state is never shared across threads.
    _real_ai_providers = frozenset({"openai", "gemini"})
    use_parallel = session_factory is not None and provider_name in _real_ai_providers

    # Resolve the teacher's actual grade code (K, 1–12) for image search and AI prompts.
    grade_code: str | None = None
    _grade_display_name: str | None = None
    if package.catalog_grade_id:
        from oziebot_api.models.education_catalog import EducationGrade

        _grade_row = db.get(EducationGrade, package.catalog_grade_id)
        if _grade_row:
            grade_code = _grade_row.grade_code
            _grade_display_name = _grade_row.display_name
    # Inject into context so every AI prompt knows the exact grade.
    if grade_code:
        context = {**context, "grade_code": grade_code, "grade_display_name": _grade_display_name}

    now = _now()
    link_excerpts = _link_context_excerpts(context)

    sequence = 0
    subject_lookup = {row["subject_id"]: row for row in context["subjects"]}
    teaching_order_keys = [str(value) for value in teaching_order]
    assignment_artifact = None
    assignment_content: dict[str, Any] | None = None

    total_weeks = len(context["weeks"])

    # Instructional day tracking — read from package metadata set during prepare phase.
    _pkg_meta = package.metadata_json or {}
    total_curriculum_days: int = _pkg_meta.get("total_curriculum_days") or total_weeks * 5
    total_planned_days: int = _pkg_meta.get("total_planned_days") or total_curriculum_days
    lost_instructional_days: int = _pkg_meta.get("lost_instructional_days") or 0
    # Counter incremented for each daily lesson plan generated; stops at total_planned_days.
    instructional_day_counter: int = 0

    # ── Layer 1 cache: curriculum (check before Phase 0a) ─────────────────────────────
    _curriculum_cache_key = compute_curriculum_cache_key(
        context,
        curriculum_prompt_version=ARTIFACT_PROMPT_VERSIONS.get("curriculum_sequence_plan", "v1"),
    )
    _curriculum_cache_result: dict[str, Any] | None = (
        lookup_generation_cache(
            db,
            tenant_id=package.tenant_id,
            cache_layer="curriculum",
            cache_key=_curriculum_cache_key,
        )
        if provider_name in _real_ai_providers
        else None
    )

    # ── Phase 0a: Curriculum Sequence Plan + Instructional Resource Bank ─────────────────
    # One AI call that reads the full curriculum and returns:
    #   - curriculum_sequence_plan: fixed week-by-week teaching plan (themes, TEKS, mentor texts)
    #   - instructional_resource_bank: structured entries built via regex scan of full curriculum
    #     text plus the AI-confirmed mentor_texts per week. No new OCR or AI calls.
    # Both ground all downstream artifact prompts.
    curriculum_sequence_plan: dict[int, dict[str, Any]] = {}
    instructional_resource_bank: dict[str, Any] = {}
    if _curriculum_cache_result is not None:
        curriculum_sequence_plan = _curriculum_cache_result.get("curriculum_sequence_plan") or {}
        instructional_resource_bank = (
            _curriculum_cache_result.get("instructional_resource_bank") or {}
        )
        _cache_layer_hits["curriculum"] = True
        if instructional_resource_bank:
            package.instructional_resource_bank_json = instructional_resource_bank
            package.updated_at = _now()
            db.flush()
        logger.info(
            "package=%s: Layer 1 curriculum cache HIT — skipped Phase 0a (%d weeks)",
            package.id,
            len(curriculum_sequence_plan),
        )
    elif provider_name in _real_ai_providers:
        try:
            curriculum_sequence_plan, instructional_resource_bank = (
                generate_curriculum_sequence_plan(
                    db,
                    settings=settings,
                    user=user,
                    tenant_id=uuid.UUID(str(package.tenant_id)),
                    package_id=package.id,
                    generation_context=context,
                )
            )
            package.instructional_resource_bank_json = instructional_resource_bank
            package.updated_at = _now()
            db.flush()
            logger.info(
                "package=%s: curriculum sequence plan generated for %d weeks; "
                "resource bank contains %d entries (structured=%d, ai=%d, regex=%d)",
                package.id,
                len(curriculum_sequence_plan),
                (instructional_resource_bank.get("total_entries") or 0),
                (instructional_resource_bank.get("extraction_summary") or {}).get("structured", 0),
                (instructional_resource_bank.get("extraction_summary") or {}).get(
                    "ai_sequence_plan", 0
                ),
                (instructional_resource_bank.get("extraction_summary") or {}).get(
                    "regex_document", 0
                ),
            )
            store_generation_cache(
                db,
                tenant_id=package.tenant_id,
                cache_layer="curriculum",
                cache_key=_curriculum_cache_key,
                prompt_version=ARTIFACT_PROMPT_VERSIONS.get("curriculum_sequence_plan", "v1"),
                model=None,
                content={
                    "curriculum_sequence_plan": curriculum_sequence_plan,
                    "instructional_resource_bank": instructional_resource_bank,
                },
            )
        except Exception:
            logger.exception(
                "package=%s: curriculum sequence plan failed — proceeding without", package.id
            )

    # ── Layer 2 cache: planning (check before Phase 0c) ───────────────────────────────
    _planning_cache_key = compute_planning_cache_key(
        _curriculum_cache_key,
        context,
        package.instructional_delivery_profile,
        lost_instructional_days,
        planning_prompt_version=ARTIFACT_PROMPT_VERSIONS.get("instructional_design_plan", "v1"),
    )
    _planning_cache_result: dict[str, Any] | None = (
        lookup_generation_cache(
            db,
            tenant_id=package.tenant_id,
            cache_layer="planning",
            cache_key=_planning_cache_key,
        )
        if provider_name in _real_ai_providers
        else None
    )

    # ── Phase 0b + 0c: Instructional Design Plan ──────────────────────────────────────
    # Backward-designed instructional plan covering all weeks × subjects. Generated once
    # before any artifact generation begins. Encodes:
    #   - unit_mastery_arc: terminal mastery + per-week mastery gates (backward design spine)
    #   - knowledge_dependency_graph: per-objective pre-instructional dependency analysis
    #   - district_anchors: district-defined data extracted from pacing guide (not AI-generated)
    #   - instructional_design: AI-generated HOW for each week × subject × day
    #
    # Every artifact generator receives a slice of this plan. No artifact independently
    # invents its instructional sequence — all are grounded in this single design.
    #
    # Phase 0b (district_anchors) runs inside generate_instructional_design_plan as
    # a pure-Python pre-step before any AI call. Phase 0c is the AI call itself.
    instructional_design_plan: dict[str, Any] = {}
    validation_report: dict | None = None
    strand_learning_journeys: dict[str, Any] = {}

    if _planning_cache_result is not None:
        # Layer 2 cache hit — load design plan + strand journeys, skip Phases 0c/0d/0f
        instructional_design_plan = _planning_cache_result.get("instructional_design_plan") or {}
        strand_learning_journeys = _planning_cache_result.get("strand_learning_journeys") or {}
        _cache_layer_hits["planning"] = True
        if instructional_design_plan:
            package.instructional_design_plan_json = instructional_design_plan
            package.updated_at = _now()
        if strand_learning_journeys:
            package.metadata_json = {
                **(package.metadata_json or {}),
                "strand_learning_journeys": strand_learning_journeys,
            }
            package.updated_at = _now()
        if instructional_design_plan or strand_learning_journeys:
            db.flush()
        logger.info(
            "package=%s: Layer 2 planning cache HIT — skipped Phases 0c/0d/0f",
            package.id,
        )
    else:
        # Layer 2 cache miss — run Phases 0c/0d/0f and store result
        if provider_name in _real_ai_providers:
            try:
                instructional_design_plan = generate_instructional_design_plan(
                    db,
                    settings=settings,
                    user=user,
                    tenant_id=uuid.UUID(str(package.tenant_id)),
                    package_id=package.id,
                    generation_context=context,
                    curriculum_sequence_plan=curriculum_sequence_plan,
                    excluded_ids=excluded_ids or None,
                    delivery_profile=package.instructional_delivery_profile,
                )
                if instructional_design_plan:
                    package.instructional_design_plan_json = instructional_design_plan
                    package.updated_at = _now()
                    db.flush()
                    logger.info(
                        "package=%s: instructional design plan stored (%d weeks, schema_version=%s)",
                        package.id,
                        len(instructional_design_plan.get("weeks") or []),
                        (instructional_design_plan.get("plan_meta") or {}).get(
                            "schema_version", "?"
                        ),
                    )
            except Exception:
                logger.exception(
                    "package=%s: instructional design plan failed — proceeding without; "
                    "artifacts will fall back to curriculum_sequence_plan and pacing guide data",
                    package.id,
                )

        # ── Phase 0d: Curriculum Validation & Instructional Quality Engine ────────────────
        # Validation is the explicit owner of the plan until it returns the (possibly revised)
        # final plan and the validation report. The plan lock is set only after this phase
        # completes so the locked timestamp always reflects the post-validation, post-revision plan.
        #
        # The engine runs two tiers:
        #   Tier 1 — Deterministic rules (R1-R19): structure, district fidelity, assessment
        #             alignment, cognitive load heuristics. Pure Python. < 1s.
        #   Tier 2 — AI pedagogical review: one call per subject evaluating learning
        #             progression, backward design integrity, mastery realism, and engagement.
        #
        # Issues are separated into educational_issues (pedagogically unsound) and
        # generation_issues (AI generation artifacts). Every issue carries an
        # improvement_source tag (Prompt / Rule / Architecture / Teacher Input) so
        # TeacherAssist can improve over time.
        #
        # On any failure, returns the original plan unchanged and proceeds with generation.
        # Never blocks artifact generation.
        if instructional_design_plan and provider_name in _real_ai_providers:
            try:
                validated_plan, validation_report = validate_and_revise_instructional_plan(
                    db,
                    settings=settings,
                    user=user,
                    tenant_id=uuid.UUID(str(package.tenant_id)),
                    package_id=package.id,
                    plan=instructional_design_plan,
                    generation_context=context,
                )
                # If revision changed the plan, persist the updated version before locking
                if validated_plan is not instructional_design_plan and validated_plan:
                    instructional_design_plan = validated_plan
                    package.instructional_design_plan_json = instructional_design_plan
                package.instructional_validation_report_json = validation_report
                package.updated_at = _now()
                db.flush()
                logger.info(
                    "package=%s: validation complete — confidence=%s score=%.1f",
                    package.id,
                    validation_report.get("confidence_label", "?"),
                    float(validation_report.get("overall_score") or 0),
                )
            except Exception:
                logger.exception(
                    "package=%s: Phase 0d validation failed — proceeding with unvalidated plan",
                    package.id,
                )

        # ── Phase 0f: Strand Learning Journeys ──────────────────────────────────────────────
        # Reads the validated design plan and stitches per-week entries into cross-week arcs,
        # one per instructional strand. Resources are bound to objectives (not days).
        # Cross-strand connections are only recorded when instructionally genuine.
        # Stored in metadata_json["strand_learning_journeys"]; injected into every week_context
        # so artifact generators know the journey position for each strand.
        if instructional_design_plan and provider_name in _real_ai_providers:
            try:
                from oziebot_api.services.teacher_assist_v2.strand_learning_journeys import (
                    generate_strand_learning_journeys,
                )

                strand_learning_journeys = generate_strand_learning_journeys(
                    db,
                    settings=settings,
                    user=user,
                    tenant_id=uuid.UUID(str(package.tenant_id)),
                    package_id=package.id,
                    generation_context=context,
                    instructional_design_plan=instructional_design_plan,
                    curriculum_sequence_plan=curriculum_sequence_plan,
                    total_planned_days=total_planned_days,
                )
                if strand_learning_journeys:
                    package.metadata_json = {
                        **(package.metadata_json or {}),
                        "strand_learning_journeys": strand_learning_journeys,
                    }
                    package.updated_at = _now()
                    db.flush()
                    logger.info(
                        "package=%s: strand learning journeys generated for %d strand(s)",
                        package.id,
                        len((strand_learning_journeys.get("strand_journeys") or {})),
                    )
            except Exception:
                logger.exception(
                    "package=%s: strand learning journeys (Phase 0f) failed — proceeding without",
                    package.id,
                )

        # Store Layer 2 planning cache after Phase 0f (only if we produced a design plan)
        if instructional_design_plan and provider_name in _real_ai_providers:
            store_generation_cache(
                db,
                tenant_id=package.tenant_id,
                cache_layer="planning",
                cache_key=_planning_cache_key,
                prompt_version=ARTIFACT_PROMPT_VERSIONS.get("instructional_design_plan", "v1"),
                model=None,
                content={
                    "instructional_design_plan": instructional_design_plan,
                    "strand_learning_journeys": strand_learning_journeys,
                },
            )

    # ── Phase 0e: Vocabulary Registry ────────────────────────────────────────────────
    # Build the per-(week, subject) vocabulary registry from the KDG and objective
    # descriptions. Used in Phase 1 to attach introduced_vocabulary to each assessment
    # artifact's instructional_contract, and in Phase 3b for vocabulary sequence checks.
    # No AI call. Falls back to empty dict on any failure. Always run — not cached.
    vocabulary_registry: dict[tuple[int, str], list[str]] = {}
    if instructional_design_plan:
        try:
            vocabulary_registry = build_vocabulary_registry(instructional_design_plan, context)
            logger.info(
                "package=%s: vocabulary registry built — %d week×subject entries",
                package.id,
                len(vocabulary_registry),
            )
        except Exception:
            logger.exception(
                "package=%s: vocabulary registry build failed — proceeding without",
                package.id,
            )

    # Compute planning hash now that all Tier-1 stages are done.
    # This hash captures the full planning context that Tier-2 artifacts depend on.
    _planning_hash = compute_planning_hash(
        curriculum_sequence_plan=curriculum_sequence_plan,
        instructional_design_plan=instructional_design_plan,
        strand_learning_journeys=strand_learning_journeys,
    )

    # Build version bundle for per-artifact traceability. Stored in every artifact's
    # metadata_json["version"] so generation provenance is always inspectable.
    _plan_meta = (instructional_design_plan or {}).get("plan_meta") or {}
    _report_meta = (validation_report or {}).get("report_meta") or {}
    _version_bundle: dict[str, str | None] = {
        "instructional_plan_version": _plan_meta.get("schema_version"),
        "validation_version": _report_meta.get("schema_version"),
        "artifact_generation_version": ARTIFACT_ENGINE_VERSION,
    }

    # Inject the (validated, possibly revised) plan, resource bank, delivery profile, and
    # version bundle into context so every artifact call can read them without direct args.
    # Load the bank from the package record (in case Phase 0a stored a richer version than
    # what was built in-memory, or this is a regeneration run where in-memory bank is empty).
    _final_resource_bank = (
        (package.instructional_resource_bank_json or {})
        if (
            package.instructional_resource_bank_json
            and (package.instructional_resource_bank_json.get("total_entries") or 0)
            >= (instructional_resource_bank.get("total_entries") or 0)
        )
        else instructional_resource_bank
    )
    # Build resource alignment map — pre-commits one resource per (week, day, strand) so
    # all artifacts on the same day always reference the same curriculum resource.
    _resource_alignment_map: dict[str, Any] = {}
    try:
        _resource_alignment_map = build_resource_alignment_map(_final_resource_bank, context)
        logger.info(
            "package=%s: resource alignment map built for %d week(s)",
            package.id,
            _resource_alignment_map.get("total_weeks", 0),
        )
    except Exception:
        logger.exception(
            "package=%s: resource alignment map build failed — proceeding without", package.id
        )

    context = {
        **context,
        "instructional_design_plan": instructional_design_plan,
        "instructional_resource_bank": _final_resource_bank,
        "resource_alignment_map": _resource_alignment_map,
        "instructional_delivery_profile": package.instructional_delivery_profile,
        "version_bundle": _version_bundle,
        "total_curriculum_days": total_curriculum_days,
        "lost_instructional_days": lost_instructional_days,
        "total_planned_days": total_planned_days,
    }

    # Lock the plan: validation, revision, and vocabulary registry are complete.
    # Artifact generation begins now. Any subsequent re-generation of individual
    # artifacts on this package uses the locked, validated plan.
    package.instructional_design_plan_locked_at = _now()
    package.updated_at = _now()
    db.flush()

    # Tracks instructional strategy + image usage across the package to prevent repetition.
    # Updated after each artifact in the sequential path; carries cross-week history.
    variety_tracker = VarietyTracker()

    for week in context["weeks"]:
        week_label = week["title"]
        week_num = week["sequence_number"]
        week_subjects = {row["subject_id"]: row for row in week["subjects"]}

        # Inject this week's curriculum plan so every artifact knows what to emphasize
        # and how this week connects to the weeks before and after it.
        # variety_context reflects strategies already used in ALL prior weeks — gives the
        # AI cross-week variety awareness. Updated per-artifact in the sequential path below.
        week_context = {
            **context,
            "week_curriculum_plan": curriculum_sequence_plan.get(week_num),
            "variety_context": variety_tracker.build_variety_context(),
            "strand_learning_journeys": strand_learning_journeys or None,
            "total_curriculum_days": total_curriculum_days,
            "total_planned_days": total_planned_days,
            "lost_instructional_days": lost_instructional_days,
        }

        # ── PHASE 1: collect all primary artifact jobs for this week ──────────────
        # Each job carries: deterministic fallback content, AI call params, persist
        # kwargs, and a post_type tag for side effects. AI calls fire in Phase 2;
        # DB writes happen in Phase 3 with a commit after each artifact so the
        # teacher sees progressive results and container restarts are recoverable.
        week_jobs: list[dict[str, Any]] = []
        artifacts_this_week: list[Any] = []  # collected in Phase 3 for Phase 3b

        # Track per-week instructional day assignments so student decks use the same numbering.
        _week_day_assignments: list[
            tuple[str, int]
        ] = []  # (weekday_label, instructional_day_number)

        if "daily_lesson_plan" in outputs:
            for day_index, day_label in enumerate(WEEKDAY_LABELS):
                instructional_day_counter += 1
                if instructional_day_counter > total_planned_days:
                    break
                current_instructional_day = instructional_day_counter
                _week_day_assignments.append((day_label, current_instructional_day))
                day_focus = _DAILY_FOCUS_ROTATION[day_index % len(_DAILY_FOCUS_ROTATION)]
                subject_blocks = []
                objective_code = None
                objective_text = "Students demonstrate understanding across scheduled subjects."
                plan_summary = day_focus
                for subject_id in teaching_order_keys:
                    week_subject = week_subjects.get(subject_id)
                    subject_meta = subject_lookup[subject_id]
                    subj_code, subj_text, _, _, _ = _objective_fields(
                        week_subject, subject_meta["subject_name"]
                    )
                    if objective_code is None:
                        objective_code = subj_code
                    block = build_subject_lesson_block_from_pacing(
                        subject_name=subject_meta["subject_name"],
                        week_subject=week_subject,
                        day_label=day_label,
                        fallback_objective_text=subj_text,
                        excluded_material_ids=excluded_ids or None,
                    )
                    subject_blocks.append(block)

                primary_week_subject = (
                    week_subjects.get(teaching_order_keys[0]) if teaching_order_keys else None
                )
                if primary_week_subject:
                    _, week_objective_text, _, _, _ = _objective_fields(
                        primary_week_subject,
                        subject_lookup[teaching_order_keys[0]]["subject_name"]
                        if teaching_order_keys
                        else "",
                    )
                else:
                    week_objective_text = objective_text
                plan_summary = resolve_daily_plan_summary(
                    primary_week_subject,
                    day_label,
                    fallback=day_focus,
                )
                daily_topic = resolve_subject_daily_topic(primary_week_subject, day_label=day_label)
                objective_text = resolve_daily_plan_objective_text(
                    primary_week_subject,
                    day_label,
                    fallback=week_objective_text,
                )
                _, _, _, objective_ids, teks_ids = _objective_fields(
                    primary_week_subject,
                    subject_lookup[teaching_order_keys[0]]["subject_name"]
                    if primary_week_subject and teaching_order_keys
                    else "",
                )
                deterministic = build_daily_lesson_plan(
                    day_label=day_label,
                    week_label=week_label,
                    package_title=package.title,
                    subject_blocks=subject_blocks,
                    objective_code=objective_code,
                    objective_text=objective_text,
                    objective_ids=objective_ids,
                    teks_ids=teks_ids,
                    summary=plan_summary,
                    daily_topic=daily_topic,
                )
                _period_id = None
                if teaching_order_keys and teaching_order_keys[0] in week_subjects:
                    _period_id = uuid.UUID(week_subjects[teaching_order_keys[0]]["period_id"])
                stored_day_label = f"ID-{current_instructional_day}"
                _dlp_primary_name = (
                    subject_lookup[teaching_order_keys[0]]["subject_name"]
                    if teaching_order_keys
                    else ""
                )
                _dlp_contract = extract_instructional_contract(
                    artifact_type="daily_lesson_plan",
                    plan=instructional_design_plan or {},
                    week_num=week_num,
                    subject_name=_dlp_primary_name,
                    day_label=day_label,
                    vocabulary_registry=vocabulary_registry,
                )
                week_jobs.append(
                    {
                        "ai_params": {
                            "artifact_type": "daily_lesson_plan",
                            "week": week,
                            "subject_meta": None,
                            "week_subject": primary_week_subject,
                            "day_label": day_label,
                            "title_hint": deterministic["title"],
                            "introduced_vocabulary": None,
                            "instructional_day_number": current_instructional_day,
                            "total_planned_days": total_planned_days,
                        },
                        "deterministic": deterministic,
                        "persist_kwargs": {
                            "artifact_type": "daily_lesson_plan",
                            "provider_name": provider_name,
                            "created_at": now,
                            "period_id": _period_id,
                            "day_label": stored_day_label,
                            "metadata_override": {
                                "instructional_day_number": current_instructional_day,
                                "total_planned_days": total_planned_days,
                                "week_reference": f"W{week_num}-{day_label}",
                            },
                        },
                        "post_type": None,
                        "instructional_contract": _dlp_contract,
                    }
                )

        if "subject_slide_deck" in outputs:
            for subject_id in teaching_order_keys:
                week_subject = week_subjects.get(subject_id)
                subject_meta = subject_lookup[subject_id]
                objective_code, objective_text, objectives_list, objective_ids, teks_ids = (
                    _objective_fields(week_subject, subject_meta["subject_name"])
                )
                (
                    source_materials,
                    source_excerpts,
                    daily_topics,
                    daily_objectives,
                    assessment_checks,
                ) = _grounding_fields(week_subject)
                source_excerpts = source_excerpts + link_excerpts
                deterministic = build_deterministic_fallback(
                    "subject_slide_deck",
                    subject_name=subject_meta["subject_name"],
                    week_label=week_label,
                    package_title=package.title,
                    objective_code=objective_code,
                    objective_text=objective_text,
                    objectives_list=objectives_list,
                    objective_ids=objective_ids,
                    teks_ids=teks_ids,
                    source_materials=source_materials,
                    source_excerpts=source_excerpts,
                    daily_topics=daily_topics,
                    daily_objectives=daily_objectives,
                    assessment_checks=assessment_checks,
                )
                _ssd_contract = extract_instructional_contract(
                    artifact_type="subject_slide_deck",
                    plan=instructional_design_plan or {},
                    week_num=week_num,
                    subject_name=subject_meta["subject_name"],
                    day_label=None,
                    vocabulary_registry=vocabulary_registry,
                )
                week_jobs.append(
                    {
                        "ai_params": {
                            "artifact_type": "subject_slide_deck",
                            "week": week,
                            "subject_meta": subject_meta,
                            "week_subject": week_subject,
                            "day_label": None,
                            "title_hint": deterministic["title"],
                            "introduced_vocabulary": None,
                        },
                        "deterministic": deterministic,
                        "persist_kwargs": {
                            "artifact_type": "subject_slide_deck",
                            "provider_name": provider_name,
                            "created_at": now,
                            "subject_id": uuid.UUID(subject_id),
                            "period_id": uuid.UUID(week_subject["period_id"])
                            if week_subject
                            else None,
                            "export_note": SLIDE_DECK_EXPORT_NOTE,
                        },
                        "post_type": None,
                        "instructional_contract": _ssd_contract,
                    }
                )

        # Student lesson decks — daily (one per weekday, primary subject, student-facing).
        # Mirrors the days actually generated for daily lesson plans; uses same ID numbering.
        _daily_deck_day_iter: list[tuple[str, int | None]] = (
            _week_day_assignments
            if _week_day_assignments
            else [(label, None) for label in WEEKDAY_LABELS]
        )
        for day_index, (day_label, _deck_id_num) in enumerate(_daily_deck_day_iter):
            primary_week_subject = (
                week_subjects.get(teaching_order_keys[0]) if teaching_order_keys else None
            )
            primary_subject_meta = (
                subject_lookup[teaching_order_keys[0]] if teaching_order_keys else None
            )
            if not primary_subject_meta:
                continue
            objective_code, objective_text, objectives_list, objective_ids, teks_ids = (
                _objective_fields(primary_week_subject, primary_subject_meta["subject_name"])
            )
            (
                source_materials,
                source_excerpts,
                daily_topics,
                daily_objectives,
                assessment_checks,
            ) = _grounding_fields(primary_week_subject)
            _day_plan = resolve_pacing_day_plan(primary_week_subject, day_label)
            _day_topic = (_day_plan or {}).get("daily_topic") if _day_plan else None
            _idp_day = get_plan_for_day(
                instructional_design_plan,
                week_num,
                primary_subject_meta["subject_name"],
                day_label,
            )
            _student_goal = (_idp_day or {}).get("student_goal") or None
            deterministic = build_student_lesson_deck(
                subject_name=primary_subject_meta["subject_name"],
                week_label=week_label,
                package_title=package.title,
                objective_code=objective_code,
                objective_text=objective_text,
                objectives_list=objectives_list,
                day_label=day_label,
                day_topic=_day_topic,
                student_goal=_student_goal,
                objective_ids=objective_ids,
                teks_ids=teks_ids,
                source_materials=source_materials,
                daily_topics=daily_topics,
            )
            _period_id = None
            if teaching_order_keys and teaching_order_keys[0] in week_subjects:
                _period_id = uuid.UUID(week_subjects[teaching_order_keys[0]]["period_id"])
            stored_daily_deck_label = (
                f"ID-{_deck_id_num}"
                if _deck_id_num
                else (f"W{week_num}-{day_label}" if total_weeks > 1 else day_label)
            )
            _daily_deck_contract = extract_instructional_contract(
                artifact_type="student_lesson_deck",
                plan=instructional_design_plan or {},
                week_num=week_num,
                subject_name=(primary_subject_meta or {}).get("subject_name") or "",
                day_label=day_label,
                vocabulary_registry=vocabulary_registry,
            )
            week_jobs.append(
                {
                    "ai_params": {
                        "artifact_type": "student_lesson_deck",
                        "week": week,
                        "subject_meta": None,
                        "week_subject": primary_week_subject,
                        "day_label": day_label,
                        "title_hint": deterministic["title"],
                        "introduced_vocabulary": None,
                        "instructional_day_number": _deck_id_num,
                        "total_planned_days": total_planned_days,
                    },
                    "deterministic": deterministic,
                    "persist_kwargs": {
                        "artifact_type": "student_lesson_deck",
                        "provider_name": provider_name,
                        "created_at": now,
                        "period_id": _period_id,
                        "day_label": stored_daily_deck_label,
                        **(
                            {
                                "metadata_override": {
                                    "instructional_day_number": _deck_id_num,
                                    "total_planned_days": total_planned_days,
                                }
                            }
                            if _deck_id_num
                            else {}
                        ),
                    },
                    "post_type": "image_deck",
                    "instructional_contract": _daily_deck_contract,
                }
            )

        # Student lesson decks — per subject (one per subject for the week, student-facing)
        for subject_id in teaching_order_keys:
            week_subject = week_subjects.get(subject_id)
            subject_meta = subject_lookup[subject_id]
            objective_code, objective_text, objectives_list, objective_ids, teks_ids = (
                _objective_fields(week_subject, subject_meta["subject_name"])
            )
            (
                source_materials,
                source_excerpts,
                daily_topics,
                daily_objectives,
                assessment_checks,
            ) = _grounding_fields(week_subject)
            deterministic = build_student_lesson_deck(
                subject_name=subject_meta["subject_name"],
                week_label=week_label,
                package_title=package.title,
                objective_code=objective_code,
                objective_text=objective_text,
                objectives_list=objectives_list,
                objective_ids=objective_ids,
                teks_ids=teks_ids,
                source_materials=source_materials,
                daily_topics=daily_topics,
            )
            _subj_deck_contract = extract_instructional_contract(
                artifact_type="student_lesson_deck",
                plan=instructional_design_plan or {},
                week_num=week_num,
                subject_name=subject_meta["subject_name"],
                day_label=None,
                vocabulary_registry=vocabulary_registry,
            )
            week_jobs.append(
                {
                    "ai_params": {
                        "artifact_type": "student_lesson_deck",
                        "week": week,
                        "subject_meta": subject_meta,
                        "week_subject": week_subject,
                        "day_label": None,
                        "title_hint": deterministic["title"],
                        "introduced_vocabulary": None,
                    },
                    "deterministic": deterministic,
                    "persist_kwargs": {
                        "artifact_type": "student_lesson_deck",
                        "provider_name": provider_name,
                        "created_at": now,
                        "subject_id": uuid.UUID(subject_id),
                        "period_id": uuid.UUID(week_subject["period_id"]) if week_subject else None,
                    },
                    "post_type": "image_deck",
                    "instructional_contract": _subj_deck_contract,
                }
            )

        # Other outputs (quiz, assignment, writing_response, rubric, etc.)
        for output_type in outputs:
            if output_type in REQUIRED_PACKAGE_OUTPUTS:
                continue
            if output_type == "rubric" and (
                "writing_response" in outputs or "assignment" in outputs
            ):
                continue
            for subject_id in teaching_order_keys:
                week_subject = week_subjects.get(subject_id)
                subject_meta = subject_lookup[subject_id]
                objective_code, objective_text, objectives_list, objective_ids, teks_ids = (
                    _objective_fields(week_subject, subject_meta["subject_name"])
                )
                (
                    source_materials,
                    source_excerpts,
                    daily_topics,
                    daily_objectives,
                    assessment_checks,
                ) = _grounding_fields(week_subject)
                source_excerpts = source_excerpts + link_excerpts
                deterministic = build_deterministic_fallback(
                    output_type,
                    subject_name=subject_meta["subject_name"],
                    week_label=week_label,
                    package_title=package.title,
                    objective_code=objective_code,
                    objective_text=objective_text,
                    daily_topic=resolve_subject_daily_topic(week_subject, day_label="Monday"),
                    objectives_list=objectives_list,
                    objective_ids=objective_ids,
                    teks_ids=teks_ids,
                    source_materials=source_materials,
                    source_excerpts=source_excerpts,
                    daily_topics=daily_topics,
                    daily_objectives=daily_objectives,
                    assessment_checks=assessment_checks,
                )
                _other_contract = extract_instructional_contract(
                    artifact_type=output_type,
                    plan=instructional_design_plan or {},
                    week_num=week_num,
                    subject_name=subject_meta["subject_name"],
                    day_label=None,
                    vocabulary_registry=vocabulary_registry,
                )
                _other_vocab = (
                    _other_contract.get("introduced_vocabulary")
                    if output_type in {"quiz", "assignment", "writing_response"}
                    else None
                )
                week_jobs.append(
                    {
                        "ai_params": {
                            "artifact_type": output_type,
                            "week": week,
                            "subject_meta": subject_meta,
                            "week_subject": week_subject,
                            "day_label": None,
                            "title_hint": deterministic["title"],
                            "introduced_vocabulary": _other_vocab,
                        },
                        "deterministic": deterministic,
                        "persist_kwargs": {
                            "artifact_type": output_type,
                            "provider_name": provider_name,
                            "created_at": now,
                            "subject_id": uuid.UUID(subject_id),
                            "period_id": uuid.UUID(week_subject["period_id"])
                            if week_subject and week_subject.get("period_id")
                            else None,
                        },
                        "post_type": output_type,
                        "post_week_subject": week_subject,
                        "post_subject_meta": subject_meta,
                        "instructional_contract": _other_contract,
                    }
                )

        # ── PHASE 2: resolve AI content (parallel if enabled, sequential fallback) ─
        if use_parallel and week_jobs:
            futures: dict[Any, int] = {}
            with ThreadPoolExecutor(max_workers=_PARALLEL_MAX_WORKERS) as pool:
                for job_idx, job in enumerate(week_jobs):
                    ai_p = job["ai_params"]
                    fut = pool.submit(
                        _call_ai_for_job,
                        session_factory=session_factory,
                        settings=settings,
                        user_id=user.id,
                        tenant_id=package.tenant_id,
                        package_id=package.id,
                        artifact_type=ai_p["artifact_type"],
                        generation_context=week_context,
                        week=ai_p["week"],
                        subject_meta=ai_p["subject_meta"],
                        week_subject=ai_p["week_subject"],
                        day_label=ai_p["day_label"],
                        title_hint=ai_p["title_hint"],
                        deterministic_content=job["deterministic"],
                        introduced_vocabulary=ai_p.get("introduced_vocabulary"),
                        instructional_day_number=ai_p.get("instructional_day_number"),
                        total_planned_days=ai_p.get("total_planned_days"),
                    )
                    futures[fut] = job_idx
            job_results: list[dict[str, Any]] = [job["deterministic"] for job in week_jobs]
            for fut, job_idx in futures.items():
                try:
                    job_results[job_idx] = fut.result()
                except Exception:
                    logger.exception(
                        "Unexpected future error for job %d — using deterministic fallback",
                        job_idx,
                    )
            # Update tracker with all parallel results for cross-week variety awareness.
            for job, result in zip(week_jobs, job_results):
                variety_tracker.record_from_artifact(result, job["ai_params"]["artifact_type"])
        else:
            job_results = []
            # Sequential path: update variety_context after each artifact so the NEXT
            # job sees what strategies were just used — within-week variety awareness.
            _seq_context = dict(week_context)
            for job in week_jobs:
                ai_p = job["ai_params"]
                result = _resolve_artifact_content(
                    db,
                    settings=settings,
                    user=user,
                    package=package,
                    context=_seq_context,
                    artifact_type=ai_p["artifact_type"],
                    deterministic_content=job["deterministic"],
                    week=ai_p["week"],
                    subject_meta=ai_p.get("subject_meta"),
                    week_subject=ai_p.get("week_subject"),
                    day_label=ai_p.get("day_label"),
                    title_hint=ai_p.get("title_hint"),
                    introduced_vocabulary=ai_p.get("introduced_vocabulary"),
                    instructional_day_number=ai_p.get("instructional_day_number"),
                    total_planned_days=ai_p.get("total_planned_days"),
                )
                job_results.append(result)
                # Update tracker after each result so next job gets fresh variety context.
                variety_tracker.record_from_artifact(result, ai_p["artifact_type"])
                _seq_context = {
                    **_seq_context,
                    "variety_context": variety_tracker.build_variety_context(),
                }

        # ── PHASE 3: persist each artifact and run post-persist side effects ───────
        # Contract enforcement and student content prohibition scan run before each
        # persist. Commit after each artifact so the teacher sees results progressively
        # and container restarts don't lose completed work.
        _version_bundle = context.get("version_bundle")
        for job, content in zip(week_jobs, job_results):
            # Determine generation_source: "ai" if the content differs from the
            # deterministic fallback; "deterministic" if AI was skipped/fell back.
            _artifact_type_probe = job["persist_kwargs"]["artifact_type"]
            _is_ai_content = (
                provider_name in _real_ai_providers
                and content is not job["deterministic"]
                and content != job["deterministic"]
            )
            _gen_source = "ai" if _is_ai_content else "deterministic"
            # Enforce instructional contracts (exit ticket stem, quiz objectives, etc.)
            # and strip internal metadata from student-facing content before persisting.
            _contract = job.get("instructional_contract") or {}
            _artifact_type_for_enforce = job["persist_kwargs"]["artifact_type"]
            try:
                content, _alignment = enforce_instructional_contract(
                    artifact_type=_artifact_type_for_enforce,
                    content=content,
                    contract=_contract,
                )
                content = enforce_student_content_prohibitions(
                    artifact_type=_artifact_type_for_enforce,
                    content=content,
                    alignment_result=_alignment,
                )
            except Exception:
                logger.exception(
                    "package=%s: contract enforcement failed for artifact_type=%s — persisting unmodified",
                    package.id,
                    _artifact_type_for_enforce,
                )
                _alignment = None

            sequence += 1
            _provenance = build_artifact_provenance(
                prompt_version=ARTIFACT_PROMPT_VERSIONS.get(_artifact_type_probe, "unknown"),
                model=None,  # resolved at the AI call level; unknown here
                generation_source=_gen_source,
                dependency_hash=_planning_hash,
            )
            _record_source(_artifact_type_probe, _gen_source)
            artifact = persist_package_artifact(
                db,
                settings=settings,
                package=package,
                content=content,
                sequence_number=sequence,
                alignment_metadata=_alignment,
                version_metadata=_version_bundle,
                generation_provenance=_provenance,
                **job["persist_kwargs"],
            )
            db.commit()
            artifacts_this_week.append(artifact)

            # ── PHASE 3b: Instructional Quality Review ────────────────────────────
            # AI instructional coach reviews the generated artifact across 8 quality
            # dimensions and applies targeted auto-revisions to safe-to-change fields.
            # Runs after persist so the teacher sees progressive results even if review fails.
            if quality_review_enabled and artifact.artifact_type in _QUALITY_REVIEWABLE_TYPES:
                try:
                    _quality_report, _revised_content, _corrections = review_artifact(
                        db,
                        settings=settings,
                        user=user,
                        tenant_id=package.tenant_id,
                        package_id=package.id,
                        artifact_type=artifact.artifact_type,
                        content=dict(artifact.content_json or {}),
                        generation_context=week_context,
                    )
                    if _quality_report:
                        attach_quality_report(
                            db,
                            artifact=artifact,
                            quality_report=_quality_report,
                            review_status=_quality_report.get("review_status", "ready"),
                            revised_content=_revised_content,
                            settings=settings,
                        )
                        db.commit()
                        logger.info(
                            "package=%s: quality review attached to artifact %s (score=%s, status=%s, corrections=%d)",
                            package.id,
                            artifact.id,
                            _quality_report.get("overall_score"),
                            _quality_report.get("review_status"),
                            len(_corrections),
                        )
                except Exception:
                    logger.exception(
                        "package=%s: quality review phase failed for artifact %s — continuing without review",
                        package.id,
                        artifact.id,
                    )

            post_type = job.get("post_type")

            if post_type == "image_deck":
                try:
                    create_pending_image_assets(db, artifact=artifact)
                except Exception:
                    logger.exception("Pending asset creation failed for artifact %s", artifact.id)
                if pixabay_key := settings.teacher_assist_pixabay_api_key:
                    try:
                        fetch_slide_images_for_artifact(
                            db,
                            artifact=artifact,
                            settings=settings,
                            api_key=pixabay_key,
                            grade_code=grade_code,
                            variety_tracker=variety_tracker,
                        )
                        # Regenerate preview_html to embed the fetched Pixabay images.
                        db.flush()
                        _fetched = (
                            db.query(TeacherAssistV2SlideVisualAsset)
                            .filter_by(artifact_id=artifact.id)
                            .filter(
                                TeacherAssistV2SlideVisualAsset.visual_generation_status
                                == "fetched"
                            )
                            .all()
                        )
                        _image_map = {
                            row.slide_id: row.source_url for row in _fetched if row.source_url
                        }
                        if _image_map:
                            artifact.preview_html = render_slide_deck_html(
                                artifact.content_json or {},
                                image_map=_image_map,
                            )
                    except Exception:
                        logger.exception("Pixabay image fetch failed for artifact %s", artifact.id)

            elif post_type is not None:
                output_type = post_type
                week_subject = job.get("post_week_subject")
                subject_meta = job.get("post_subject_meta")

                db.flush()
                obj_ids = [
                    uuid.UUID(str(row["education_objective_id"]))
                    for row in (week_subject or {}).get("objectives", [])
                    if row.get("education_objective_id")
                ]
                pacing_guide_id = (
                    uuid.UUID(week_subject["pacing_guide_id"])
                    if week_subject and week_subject.get("pacing_guide_id")
                    else None
                )
                maybe_create_assignment_for_artifact(
                    db,
                    user=user,
                    package=package,
                    artifact=artifact,
                    week_number=int(week["sequence_number"]),
                    pacing_guide_id=pacing_guide_id,
                    education_objective_ids=obj_ids,
                )
                if output_type in {"quiz", "assignment"}:
                    from oziebot_api.services.teacher_assist_v2.assessment_student_exports import (
                        refresh_assessment_student_exports,
                    )

                    refresh_assessment_student_exports(
                        db,
                        settings=settings,
                        package=package,
                        artifact=artifact,
                    )
                if output_type == "assignment":
                    from oziebot_api.services.teacher_assist_v2.assessment_student_exports import (
                        refresh_assessment_student_exports,
                    )

                    sequence, _linked_rubric = _persist_linked_assignment_rubric(
                        db,
                        settings=settings,
                        user=user,
                        package=package,
                        assignment_artifact=artifact,
                        assignment_content=dict(content),
                        context=context,
                        week=week,
                        week_subject=week_subject,
                        subject_meta=subject_meta,
                        provider_name=provider_name,
                        sequence=sequence,
                        now=now,
                    )
                    db.commit()
                    if _linked_rubric is not None:
                        artifacts_this_week.append(_linked_rubric)
                    refresh_assessment_student_exports(
                        db,
                        settings=settings,
                        package=package,
                        artifact=artifact,
                    )
                if output_type == "writing_response":
                    from oziebot_api.services.teacher_assist_v2.assessment_student_exports import (
                        refresh_assessment_student_exports,
                    )

                    sequence, _linked_rubric = _persist_linked_writing_rubric(
                        db,
                        settings=settings,
                        user=user,
                        package=package,
                        writing_artifact=artifact,
                        writing_content=dict(content),
                        context=context,
                        week=week,
                        week_subject=week_subject,
                        subject_meta=subject_meta,
                        provider_name=provider_name,
                        sequence=sequence,
                        now=now,
                    )
                    db.commit()
                    if _linked_rubric is not None:
                        artifacts_this_week.append(_linked_rubric)
                    refresh_assessment_student_exports(
                        db,
                        settings=settings,
                        package=package,
                        artifact=artifact,
                    )
                if output_type == "assignment":
                    assignment_artifact = artifact
                    assignment_content = content

        # ── PHASE 3b: cross-artifact alignment validation (per week) ─────────────
        # Runs after all artifacts for this week are committed. Validates coherence
        # across artifact pairs (exit ticket ↔ daily plan, quiz ↔ objectives, etc.).
        # Accumulates into package.instructional_alignment_report_json. No AI calls.
        if instructional_design_plan and artifacts_this_week:
            try:
                week_alignment = validate_week_alignment(
                    package_id=package.id,
                    week_num=week_num,
                    artifacts=artifacts_this_week,
                    plan=instructional_design_plan,
                    vocabulary_registry=vocabulary_registry,
                )
                _align_report = dict(package.instructional_alignment_report_json or {})
                _align_weeks = list(_align_report.get("weeks") or [])
                _align_weeks.append(week_alignment)
                _align_report["weeks"] = _align_weeks
                package.instructional_alignment_report_json = _align_report
                package.updated_at = _now()
                db.flush()
                logger.info(
                    "package=%s week=%d: cross-artifact alignment — %s",
                    package.id,
                    week_num,
                    week_alignment.get("week_alignment_confidence", "?"),
                )
            except Exception:
                logger.exception(
                    "package=%s week=%d: cross-artifact alignment failed — continuing",
                    package.id,
                    week_num,
                )

    if assignment_artifact and assignment_content:
        attach_qr_student_packet(
            db,
            settings=settings,
            assignment_artifact=assignment_artifact,
            assignment_content=assignment_content,
        )

    _generation_completed_at = _now().isoformat()
    _generation_duration = round(_time_module.monotonic() - _generation_start_time, 1)

    # Build generation manifest: cost by feature + artifact source counts.
    from oziebot_api.services.teacher_assist.ai_usage import get_package_ai_cost_by_feature

    _cost_by_feature = get_package_ai_cost_by_feature(db, package_id=package.id)
    _generation_manifest = build_generation_manifest(
        generation_mode=_regen_scope,
        curriculum_hash=_curriculum_hash,
        planning_hash=_planning_hash,
        delivery_hash=_delivery_hash,
        prompt_versions=ARTIFACT_PROMPT_VERSIONS,
        cost_cents_by_feature=_cost_by_feature,
        artifact_source_counts=_artifact_source_counts,
        cache_layer_hits=_cache_layer_hits,
    )

    package.status = final_status
    # Preserve fields written by Phase 0f (strand_learning_journeys) plus new manifest.
    _existing_meta = package.metadata_json or {}
    package.metadata_json = {
        **_build_package_metadata(
            context=context,
            stored_status=final_status,
            plan_start_date=package.plan_start_date,
            plan_end_date=package.plan_end_date,
            generation_state="completed",
            generation_started_at=_generation_started_at,
            generation_completed_at=_generation_completed_at,
            generation_duration_seconds=_generation_duration,
        ),
        # Preserve Phase 0f output written before this call
        **(
            {"strand_learning_journeys": _existing_meta["strand_learning_journeys"]}
            if "strand_learning_journeys" in _existing_meta
            else {}
        ),
        "generation_manifest": _generation_manifest,
    }
    package.updated_at = _now()
    db.flush()

    # Phase 5: assemble Today's Teaching Brief from plan + artifacts (zero AI calls).
    # Non-fatal: a failure here must never block the package from being marked ready.
    try:
        package.teacher_coaching_summary_json = assemble_teaching_brief(package)
        db.flush()
        logger.info(
            "package=%s: teaching brief assembled — %d day(s)",
            package.id,
            len((package.teacher_coaching_summary_json or {}).get("days") or []),
        )
    except Exception:
        logger.exception("package=%s: teaching brief assembly failed — non-fatal", package.id)

    # Phase 6: back-fill pending slide images in a background thread so generation
    # time is not extended. Pixabay 429s during parallel artifact generation leave
    # some assets in "pending"; this pass retries them after the package is active.
    if pixabay_key := settings.teacher_assist_pixabay_api_key:
        _backfill_package_id = package.id
        _backfill_grade_code = grade_code

        def _backfill_images() -> None:
            _time_module.sleep(5)  # brief pause so Pixabay rate window resets
            try:
                from sqlalchemy.orm import Session as _Session
                with _Session(db.get_bind()) as _bg_db:
                    _pending_ids = [
                        row[0]
                        for row in _bg_db.query(TeacherAssistV2SlideVisualAsset.artifact_id)
                        .join(
                            TeacherAssistV2InstructionalPackageArtifact,
                            TeacherAssistV2InstructionalPackageArtifact.id
                            == TeacherAssistV2SlideVisualAsset.artifact_id,
                        )
                        .filter(
                            TeacherAssistV2InstructionalPackageArtifact.package_id
                            == _backfill_package_id,
                            TeacherAssistV2SlideVisualAsset.visual_generation_status == "pending",
                        )
                        .distinct()
                        .all()
                    ]
                    if not _pending_ids:
                        return
                    logger.info(
                        "package=%s: Phase 6 back-fill (bg) — %d artifact(s) with pending images",
                        _backfill_package_id,
                        len(_pending_ids),
                    )
                    for _idx, _art_id in enumerate(_pending_ids):
                        _art = _bg_db.get(TeacherAssistV2InstructionalPackageArtifact, _art_id)
                        if _art is None:
                            continue
                        try:
                            fetch_slide_images_for_artifact(
                                _bg_db,
                                artifact=_art,
                                settings=settings,
                                api_key=pixabay_key,
                                grade_code=_backfill_grade_code,
                            )
                            _bg_db.flush()
                            _bg_db.commit()
                        except Exception:
                            logger.exception(
                                "package=%s: Phase 6 back-fill failed for artifact %s",
                                _backfill_package_id,
                                _art_id,
                            )
                        if _idx < len(_pending_ids) - 1:
                            _time_module.sleep(2)
                    logger.info(
                        "package=%s: Phase 6 back-fill complete", _backfill_package_id
                    )
            except Exception:
                logger.exception(
                    "package=%s: Phase 6 back-fill thread error", _backfill_package_id
                )

        import threading as _threading
        _threading.Thread(target=_backfill_images, daemon=True, name="img-backfill").start()

    return package


def mark_instructional_package_generation_failed(
    db: Session,
    *,
    package: TeacherAssistV2InstructionalPackage,
    context: dict[str, Any],
    error_message: str,
) -> None:
    package.status = "failed"
    package.metadata_json = _build_package_metadata(
        context=context,
        stored_status="failed",
        plan_start_date=package.plan_start_date,
        plan_end_date=package.plan_end_date,
        generation_state="failed",
        status_detail=f"{PACKAGE_FAILURE_PREFIX} {error_message}",
        error_message=error_message,
    )
    package.updated_at = _now()
    db.flush()


def run_instructional_package_generation_job(
    *,
    settings: Settings,
    user_id: uuid.UUID,
    package_id: uuid.UUID,
    context: dict[str, Any],
    teaching_order: list[str],
    outputs: list[str],
    excluded_ids: list[str],
    final_status: str,
) -> None:
    session_factory = make_session_factory(settings)
    if session_factory is None:
        return
    db = session_factory()
    try:
        user = db.get(User, user_id)
        package = db.get(TeacherAssistV2InstructionalPackage, package_id)
        if user is None or package is None:
            return
        _populate_instructional_package(
            db,
            settings=settings,
            user=user,
            package=package,
            context=context,
            teaching_order=teaching_order,
            outputs=outputs,
            excluded_ids=set(excluded_ids),
            final_status=final_status,
            session_factory=session_factory,
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        try:
            package = db.get(TeacherAssistV2InstructionalPackage, package_id)
            if package is not None:
                mark_instructional_package_generation_failed(
                    db,
                    package=package,
                    context=context,
                    error_message=str(exc),
                )
                db.commit()
        except Exception:
            db.rollback()
            logger.exception(
                "Failed to mark instructional package generation as failed",
                extra={"package_id": str(package_id)},
            )
        logger.exception(
            "Instructional package generation failed", extra={"package_id": str(package_id)}
        )
    finally:
        db.close()


def claim_and_run_next_queued_v2_package(engine: Any, *, settings: "Settings") -> uuid.UUID | None:
    """Claim one queued v2 package and run its generation. Returns the package ID or None."""
    from sqlalchemy import text as _text

    session_factory = sessionmaker(
        bind=engine, autoflush=False, autocommit=False, expire_on_commit=False
    )
    db = session_factory()
    package_id: uuid.UUID | None = None
    context: dict[str, Any] = {}
    try:
        # Claim one queued package atomically (skip locked = don't wait for other workers)
        row = db.execute(
            _text(
                "UPDATE teacher_assist_v2_instructional_packages "
                "SET metadata_json = jsonb_set(metadata_json::jsonb, '{generation_state}', '\"running\"')::json, "
                "    updated_at = NOW() "
                "WHERE id = ("
                "  SELECT id FROM teacher_assist_v2_instructional_packages "
                "  WHERE status = 'processing' "
                "    AND metadata_json->>'generation_state' = 'queued' "
                "  ORDER BY created_at ASC "
                "  LIMIT 1 "
                "  FOR UPDATE SKIP LOCKED "
                ") "
                "RETURNING id"
            )
        ).fetchone()
        if row is None:
            db.rollback()
            return None
        package_id = row[0]
        db.commit()
    except Exception:
        db.rollback()
        db.close()
        logger.exception("v2-worker: failed to claim queued package")
        return None

    # Re-open a fresh session to run the job
    db.close()
    db = session_factory()
    try:
        package = db.get(TeacherAssistV2InstructionalPackage, package_id)
        if package is None:
            return None
        user = db.get(User, package.teacher_user_id)
        if user is None:
            return None

        pkg_meta = package.metadata_json or {}
        excluded_ids = set(pkg_meta.get("job_excluded_ids") or [])
        final_status = (
            "active"
            if package.plan_start_date <= date.today() <= package.plan_end_date
            else "generated"
        )

        context = build_teacher_planning_generation_context(
            db,
            user=user,
            week_start=package.week_start,
            week_end=package.week_end,
            teaching_order=[uuid.UUID(x) for x in (package.teaching_order_json or [])],
            selected_outputs=list(package.selected_outputs_json or []),
            settings=settings,
        )

        # Dispatch: full generation vs. partial regen
        _regen_opts = pkg_meta.get("regen_options") or {}
        _regen_scope = _regen_opts.get("regen_scope", "full")
        _quality_review_enabled: bool = bool(pkg_meta.get("quality_review_enabled", True))

        if _regen_scope == "quality_review":
            _run_quality_review_regen(
                db,
                settings=settings,
                user=user,
                package=package,
                context=context,
                artifact_types=_regen_opts.get("regen_artifact_types") or None,
            )
        elif _regen_scope == "artifact_types":
            _run_artifact_type_regen(
                db,
                settings=settings,
                user=user,
                package=package,
                context=context,
                regen_artifact_types=set(_regen_opts.get("regen_artifact_types") or []),
                session_factory=session_factory,
            )
        else:
            # "full" or "dirty" (dirty falls back to full for now)
            _populate_instructional_package(
                db,
                settings=settings,
                user=user,
                package=package,
                context=context,
                teaching_order=[str(x) for x in (package.teaching_order_json or [])],
                outputs=list(package.selected_outputs_json or []),
                excluded_ids=excluded_ids,
                final_status=final_status,
                session_factory=session_factory,
                quality_review_enabled=_quality_review_enabled,
            )
        db.commit()
        logger.info("v2-worker: package=%s %s complete", package_id, _regen_scope)
    except Exception as exc:
        db.rollback()
        try:
            package = db.get(TeacherAssistV2InstructionalPackage, package_id)
            if package is not None:
                mark_instructional_package_generation_failed(
                    db,
                    package=package,
                    context=context,
                    error_message=str(exc),
                )
                db.commit()
        except Exception:
            db.rollback()
            logger.exception("v2-worker: failed to mark package=%s as failed", package_id)
        logger.exception("v2-worker: package=%s generation failed", package_id)
    finally:
        db.close()

    return package_id


def recover_stale_v2_running_packages(db: Any) -> None:
    """Mark any packages stuck in generation_state='running' as failed.

    Called at worker startup: a 'running' package at startup has no live worker behind it.
    """
    from sqlalchemy import text as _text

    try:
        result = db.execute(
            _text(
                "UPDATE teacher_assist_v2_instructional_packages "
                "SET status = 'failed', updated_at = NOW(), "
                "    metadata_json = jsonb_set(metadata_json::jsonb, '{generation_state}', '\"abandoned\"')::json "
                "WHERE status = 'processing' "
                "  AND metadata_json->>'generation_state' = 'running' "
                "RETURNING id"
            )
        )
        recovered = result.fetchall()
        db.commit()
        if recovered:
            ids = [str(r[0]) for r in recovered]
            logger.warning(
                "v2-worker startup: marked %d abandoned package(s) as failed: %s",
                len(ids),
                ", ".join(ids),
            )
    except Exception:
        db.rollback()
        logger.exception("v2-worker startup: failed to recover abandoned packages")


def queue_package_partial_regen(
    db: Session,
    *,
    package: TeacherAssistV2InstructionalPackage,
    regen_scope: str,
    regen_artifact_types: list[str] | None = None,
) -> None:
    """Mark an existing completed package for partial regeneration.

    Sets generation_state='queued' so the worker picks it up.  The regen_options
    dict instructs the worker which artifacts to skip vs. regenerate.
    """
    from oziebot_api.services.teacher_assist_v2.planning_constants import REGEN_SCOPES

    if regen_scope not in REGEN_SCOPES:
        raise ValueError(f"Invalid regen_scope '{regen_scope}'. Valid: {sorted(REGEN_SCOPES)}")

    # Preserve existing metadata; update only generation control fields.
    existing_meta = dict(package.metadata_json or {})
    existing_meta["generation_state"] = "queued"
    existing_meta["regen_options"] = {
        "regen_scope": regen_scope,
        "regen_artifact_types": list(regen_artifact_types or []),
    }
    existing_meta["status_detail"] = PACKAGE_PROCESSING_MESSAGE
    # Clear any previous error
    existing_meta.pop("generation_error", None)

    package.status = "processing"
    package.metadata_json = existing_meta
    package.updated_at = _now()
    db.flush()
    logger.info(
        "package=%s: queued for partial regen — scope=%s artifact_types=%s",
        package.id,
        regen_scope,
        regen_artifact_types,
    )


def _run_quality_review_regen(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    context: dict[str, Any],
    artifact_types: list[str] | None = None,
) -> None:
    """Re-run quality review for all (or specified) artifacts in the package without
    regenerating the artifact content itself."""
    artifacts = db.scalars(
        select(TeacherAssistV2InstructionalPackageArtifact).where(
            TeacherAssistV2InstructionalPackageArtifact.package_id == package.id
        )
    ).all()
    week_context = {**context, "strand_learning_journeys": None}
    reviewed = 0
    for artifact in artifacts:
        if artifact_types and artifact.artifact_type not in artifact_types:
            continue
        if artifact.artifact_type not in _QUALITY_REVIEWABLE_TYPES:
            continue
        try:
            _quality_report, _revised_content, _ = review_artifact(
                db,
                settings=settings,
                user=user,
                tenant_id=package.tenant_id,
                package_id=package.id,
                artifact_type=artifact.artifact_type,
                content=dict(artifact.content_json or {}),
                generation_context=week_context,
            )
            if _quality_report:
                attach_quality_report(
                    db,
                    artifact=artifact,
                    quality_report=_quality_report,
                    review_status=_quality_report.get("review_status", "ready"),
                    revised_content=_revised_content,
                    settings=settings,
                )
                db.commit()
                reviewed += 1
        except Exception:
            logger.exception(
                "package=%s: quality-review regen failed for artifact %s", package.id, artifact.id
            )

    final_status = (
        "active"
        if package.plan_start_date <= date.today() <= package.plan_end_date
        else "generated"
    )
    from oziebot_api.services.teacher_assist.ai_usage import get_package_ai_cost_by_feature

    _cost_by_feature = get_package_ai_cost_by_feature(db, package_id=package.id)
    existing_meta = dict(package.metadata_json or {})
    existing_manifest = existing_meta.get("generation_manifest") or {}
    existing_manifest["cost_cents_by_feature"] = _cost_by_feature
    existing_manifest["total_generation_cost_cents"] = sum(_cost_by_feature.values())
    existing_meta["generation_manifest"] = existing_manifest
    existing_meta["generation_state"] = "completed"
    existing_meta.pop("regen_options", None)
    package.status = final_status
    package.metadata_json = existing_meta
    package.updated_at = _now()
    db.flush()
    logger.info(
        "package=%s: quality-review regen complete — reviewed %d artifact(s)", package.id, reviewed
    )


def _run_artifact_type_regen(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    context: dict[str, Any],
    regen_artifact_types: set[str],
    session_factory: Any = None,
) -> None:
    """Delete existing artifacts of the specified types, then regenerate only those.

    Phase 0 (design plan, resource bank, etc.) is skipped: the existing plan stored
    in the package record is loaded directly into context.  Only the specified
    Tier-2 artifact slots are re-generated; everything else is untouched.
    """
    if not regen_artifact_types:
        return

    # Load existing plan from the package record to skip Phase 0 AI calls.
    _existing_idp = package.instructional_design_plan_json or {}
    _existing_irb = package.instructional_resource_bank_json or {}
    _existing_meta = package.metadata_json or {}
    _existing_slj = _existing_meta.get("strand_learning_journeys") or {}

    # Load locked artifact types upfront. Any type with at least one locked artifact
    # is entirely excluded from deletion and regeneration — the developer must unlock
    # individual artifacts before their type can be regenerated.
    from sqlalchemy import text as _text

    _locked_type_rows = db.execute(
        _text(
            "SELECT DISTINCT artifact_type FROM teacher_assist_v2_instructional_package_artifacts "
            "WHERE package_id = :pkg_id "
            "AND COALESCE((metadata_json->>'dev_locked')::boolean, false) = true"
        ),
        {"pkg_id": str(package.id)},
    ).fetchall()
    _locked_types: set[str] = {str(row[0]) for row in _locked_type_rows}
    _effective_regen_types = regen_artifact_types - _locked_types

    if _locked_types & regen_artifact_types:
        logger.info(
            "package=%s: skipping locked artifact types from regen — locked=%s",
            package.id,
            sorted(_locked_types & regen_artifact_types),
        )

    # Delete unlocked artifacts of the effective (non-locked) types so they get fresh records.
    for _atype in _effective_regen_types:
        db.execute(
            _sa_delete(TeacherAssistV2InstructionalPackageArtifact).where(
                TeacherAssistV2InstructionalPackageArtifact.package_id == package.id,
                TeacherAssistV2InstructionalPackageArtifact.artifact_type == _atype,
            )
        )
    db.flush()
    logger.info(
        "package=%s: deleted existing artifacts for regen — types=%s",
        package.id,
        sorted(_effective_regen_types),
    )

    # Inject pre-computed plan into context so artifact generators use the same plan.
    context = {
        **context,
        "instructional_design_plan": _existing_idp,
        "instructional_resource_bank": _existing_irb,
    }

    # Restrict selected_outputs to the effective (non-locked) types being regenerated.
    outputs_for_regen = [
        o for o in (package.selected_outputs_json or []) if o in _effective_regen_types
    ]
    if not outputs_for_regen:
        logger.warning(
            "package=%s: no matching outputs for regen_artifact_types=%s",
            package.id,
            regen_artifact_types,
        )
        return

    excluded_ids = set(_existing_meta.get("job_excluded_ids") or [])
    final_status = (
        "active"
        if package.plan_start_date <= date.today() <= package.plan_end_date
        else "generated"
    )

    # Override regen_options in package metadata so _populate_instructional_package
    # records the correct generation_mode in the manifest.
    _regen_meta = dict(_existing_meta)
    _regen_meta["regen_options"] = {
        "regen_scope": "artifact_types",
        "regen_artifact_types": sorted(regen_artifact_types),
    }
    package.metadata_json = _regen_meta

    _populate_instructional_package(
        db,
        settings=settings,
        user=user,
        package=package,
        context=context,
        teaching_order=[str(x) for x in (package.teaching_order_json or [])],
        outputs=outputs_for_regen,
        excluded_ids=excluded_ids,
        final_status=final_status,
        session_factory=session_factory,
    )
    logger.info(
        "package=%s: artifact_type regen complete — types=%s",
        package.id,
        sorted(regen_artifact_types),
    )


def generate_instructional_package(
    db: Session,
    *,
    settings: Settings,
    user: User,
    week_start: int,
    week_end: int,
    teaching_order: list[uuid.UUID],
    selected_outputs: list[str],
    plan_start_date: date | None = None,
    plan_end_date: date | None = None,
    excluded_pacing_material_ids: list[uuid.UUID] | None = None,
    instructional_delivery_profile: dict | None = None,
    lost_instructional_days: int = 0,
) -> TeacherAssistV2InstructionalPackage:
    package, context, outputs, excluded_ids, final_status = (
        prepare_instructional_package_generation(
            db,
            settings=settings,
            user=user,
            week_start=week_start,
            week_end=week_end,
            teaching_order=teaching_order,
            selected_outputs=selected_outputs,
            plan_start_date=plan_start_date,
            plan_end_date=plan_end_date,
            excluded_pacing_material_ids=excluded_pacing_material_ids,
            instructional_delivery_profile=instructional_delivery_profile,
            lost_instructional_days=lost_instructional_days,
        )
    )
    return _populate_instructional_package(
        db,
        settings=settings,
        user=user,
        package=package,
        context=context,
        teaching_order=[str(value) for value in teaching_order],
        outputs=outputs,
        excluded_ids=excluded_ids,
        final_status=final_status,
    )

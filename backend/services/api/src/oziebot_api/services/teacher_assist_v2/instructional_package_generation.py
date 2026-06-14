"""Instructional package generation for TeacherAssist v2."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.db.session import make_session_factory
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
    TeacherAssistV2PlanningSupplementalMaterial,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_usage import get_effective_daily_cost_limit_cents
from oziebot_api.services.teacher_assist.ai_mode import is_teacher_assist_real_ai_active
from oziebot_api.services.teacher_assist.constants import validate_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.artifact_persistence import (
    attach_qr_student_packet,
    persist_package_artifact,
)
from oziebot_api.services.teacher_assist_v2.assignments import maybe_create_assignment_for_artifact
from oziebot_api.services.teacher_assist_v2.deterministic_package_content import (
    build_daily_lesson_plan,
    build_deterministic_fallback,
    build_rubric_for_written_assignment,
    build_rubric_for_writing_response,
)
from oziebot_api.services.teacher_assist_v2.instructional_package_ai import generate_v2_instructional_artifact
from oziebot_api.services.teacher_assist_v2.package_export import render_artifact_preview_html
from oziebot_api.services.teacher_assist_v2.package_artifact_refresh import SLIDE_DECK_EXPORT_NOTE
from oziebot_api.services.teacher_assist_v2.package_lifecycle import (
    build_package_title,
    resolve_default_plan_dates,
    resolve_effective_package_status,
)
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import (
    build_subject_lesson_block_from_pacing,
    resolve_daily_plan_objective_text,
    resolve_daily_plan_summary,
    resolve_subject_daily_topic,
)
from oziebot_api.services.teacher_assist_v2.planning_constants import (
    OPTIONAL_PACKAGE_OUTPUTS,
    REQUIRED_PACKAGE_OUTPUTS,
    WEEKDAY_LABELS,
)
from oziebot_api.services.teacher_assist_v2.planning_context import build_teacher_planning_generation_context
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
) -> dict[str, Any]:
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
    )
    return ai_content if ai_content is not None else deterministic_content


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
            str(row.get("objective_code") or row.get("description"))
            for row in week_subject["objectives"]
            if row.get("objective_code") or row.get("description")
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
    assessment_artifact.title = str(assessment_content["title"])
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
    _link_assessment_rubric(assessment_artifact, rubric_artifact, assessment_content=assessment_content)
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
        rubric_deterministic["objective_mapping"]["alignment_summary"] = rubric_deterministic["alignment_summary"]
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
        rubric_deterministic["objective_mapping"]["alignment_summary"] = rubric_deterministic["alignment_summary"]
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
        title=build_package_title(week_start=week_start, week_end=week_end, subject_names=subject_names),
        week_start=week_start,
        week_end=week_end,
        plan_start_date=plan_start_date,
        plan_end_date=plan_end_date,
        teaching_order_json=[str(value) for value in teaching_order],
        selected_outputs_json=outputs,
        status="processing",
        provider_name=provider_name,
        metadata_json=_build_package_metadata(
            context=context,
            stored_status="processing",
            plan_start_date=plan_start_date,
            plan_end_date=plan_end_date,
            generation_state="processing",
            status_detail=PACKAGE_PROCESSING_MESSAGE,
        ),
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
) -> TeacherAssistV2InstructionalPackage:
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

    now = _now()
    link_excerpts = _link_context_excerpts(context)

    sequence = 0
    subject_lookup = {row["subject_id"]: row for row in context["subjects"]}
    teaching_order_keys = [str(value) for value in teaching_order]
    assignment_artifact = None
    assignment_content: dict[str, Any] | None = None

    for week in context["weeks"]:
        week_label = week["title"]
        week_subjects = {row["subject_id"]: row for row in week["subjects"]}

        if "daily_lesson_plan" in outputs:
            for day_index, day_label in enumerate(WEEKDAY_LABELS):
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
                        subject_lookup[teaching_order_keys[0]]["subject_name"] if teaching_order_keys else "",
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
                    subject_lookup[teaching_order_keys[0]]["subject_name"] if primary_week_subject and teaching_order_keys else "",
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
                content = _resolve_artifact_content(
                    db,
                    settings=settings,
                    user=user,
                    package=package,
                    context=context,
                    artifact_type="daily_lesson_plan",
                    deterministic_content=deterministic,
                    week=week,
                    week_subject=primary_week_subject,
                    day_label=day_label,
                    title_hint=deterministic["title"],
                )
                sequence += 1
                period_id = None
                if teaching_order_keys and teaching_order_keys[0] in week_subjects:
                    period_id = uuid.UUID(week_subjects[teaching_order_keys[0]]["period_id"])
                persist_package_artifact(
                    db,
                    settings=settings,
                    package=package,
                    artifact_type="daily_lesson_plan",
                    content=content,
                    provider_name=provider_name,
                    sequence_number=sequence,
                    created_at=now,
                    period_id=period_id,
                    day_label=day_label,
                )

        if "subject_slide_deck" in outputs:
            for subject_id in teaching_order_keys:
                week_subject = week_subjects.get(subject_id)
                subject_meta = subject_lookup[subject_id]
                objective_code, objective_text, objectives_list, objective_ids, teks_ids = _objective_fields(
                    week_subject, subject_meta["subject_name"]
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
                content = _resolve_artifact_content(
                    db,
                    settings=settings,
                    user=user,
                    package=package,
                    context=context,
                    artifact_type="subject_slide_deck",
                    deterministic_content=deterministic,
                    week=week,
                    subject_meta=subject_meta,
                    week_subject=week_subject,
                    title_hint=deterministic["title"],
                )
                sequence += 1
                artifact = persist_package_artifact(
                    db,
                    settings=settings,
                    package=package,
                    artifact_type="subject_slide_deck",
                    content=content,
                    provider_name=provider_name,
                    sequence_number=sequence,
                    created_at=now,
                    subject_id=uuid.UUID(subject_id),
                    period_id=uuid.UUID(week_subject["period_id"]) if week_subject else None,
                    export_note=SLIDE_DECK_EXPORT_NOTE,
                )

        for output_type in outputs:
            if output_type in REQUIRED_PACKAGE_OUTPUTS:
                continue
            if output_type == "rubric" and ("writing_response" in outputs or "assignment" in outputs):
                continue
            for subject_id in teaching_order_keys:
                week_subject = week_subjects.get(subject_id)
                subject_meta = subject_lookup[subject_id]
                objective_code, objective_text, objectives_list, objective_ids, teks_ids = _objective_fields(
                    week_subject, subject_meta["subject_name"]
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
                content = _resolve_artifact_content(
                    db,
                    settings=settings,
                    user=user,
                    package=package,
                    context=context,
                    artifact_type=output_type,
                    deterministic_content=deterministic,
                    week=week,
                    subject_meta=subject_meta,
                    week_subject=week_subject,
                    title_hint=deterministic["title"],
                )
                sequence += 1
                artifact = persist_package_artifact(
                    db,
                    settings=settings,
                    package=package,
                    artifact_type=output_type,
                    content=content,
                    provider_name=provider_name,
                    sequence_number=sequence,
                    created_at=now,
                    subject_id=uuid.UUID(subject_id),
                    period_id=uuid.UUID(week_subject["period_id"])
                    if week_subject and week_subject.get("period_id")
                    else None,
                )
                db.flush()
                objective_ids = [
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
                    education_objective_ids=objective_ids,
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
                    sequence, _ = _persist_linked_assignment_rubric(
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

                    sequence, _ = _persist_linked_writing_rubric(
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
                    refresh_assessment_student_exports(
                        db,
                        settings=settings,
                        package=package,
                        artifact=artifact,
                    )
                if output_type == "assignment":
                    assignment_artifact = artifact
                    assignment_content = content

    if assignment_artifact and assignment_content:
        attach_qr_student_packet(
            db,
            settings=settings,
            assignment_artifact=assignment_artifact,
            assignment_content=assignment_content,
        )

    package.status = final_status
    package.metadata_json = _build_package_metadata(
        context=context,
        stored_status=final_status,
        plan_start_date=package.plan_start_date,
        plan_end_date=package.plan_end_date,
        generation_state="completed",
    )
    package.updated_at = _now()
    db.flush()
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
            logger.exception("Failed to mark instructional package generation as failed", extra={"package_id": str(package_id)})
        logger.exception("Instructional package generation failed", extra={"package_id": str(package_id)})
    finally:
        db.close()


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
) -> TeacherAssistV2InstructionalPackage:
    package, context, outputs, excluded_ids, final_status = prepare_instructional_package_generation(
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

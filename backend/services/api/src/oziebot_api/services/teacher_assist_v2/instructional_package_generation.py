"""Mock-safe instructional package generation for TeacherAssist v2."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
    TeacherAssistV2PlanningSupplementalMaterial,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist_v2.package_export import render_artifact_preview_html, save_artifact_export
from oziebot_api.services.teacher_assist_v2.planning_constants import (
    OPTIONAL_PACKAGE_OUTPUTS,
    REQUIRED_PACKAGE_OUTPUTS,
    WEEKDAY_LABELS,
)
from oziebot_api.services.teacher_assist_v2.assignments import maybe_create_assignment_for_artifact
from oziebot_api.services.teacher_assist_v2.package_lifecycle import (
    build_package_title,
    resolve_default_plan_dates,
    resolve_effective_package_status,
)
from oziebot_api.services.teacher_assist_v2.planning_context import build_teacher_planning_generation_context
from oziebot_api.services.teacher_assist_v2.planning_workflow import _assignment_context


def _now() -> datetime:
    return datetime.now(UTC)


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


def _mock_subject_block(*, subject_name: str, objective_code: str | None, daily_topic: str | None) -> dict[str, Any]:
    objective = objective_code or f"{subject_name} weekly objective"
    topic = daily_topic or f"{subject_name} focus"
    return {
        "subject_name": subject_name,
        "objective": f"[MOCK OUTPUT] {objective}",
        "mini_lesson": f"[MOCK OUTPUT] Model {topic} with a short teacher-led example.",
        "teacher_actions": [
            f"Review the objective {objective}.",
            f"Model the daily focus: {topic}.",
        ],
        "student_activity": [
            f"Complete a guided practice task for {subject_name}.",
            "Discuss evidence with a partner.",
        ],
        "materials": [
            f"{subject_name} district curriculum file",
            "Teacher supplemental notes",
        ],
        "assessment": "Quick check for understanding exit prompt.",
        "notes": "Generated with mock provider for local testing.",
    }


def _mock_slides(*, subject_name: str, week_label: str, objectives: list[str]) -> list[dict[str, Any]]:
    objective_text = objectives[0] if objectives else f"{subject_name} weekly objective"
    return [
        {"title": f"{subject_name} {week_label}", "bullets": ["Weekly instructional slides", "[MOCK OUTPUT]"]},
        {"title": "Objectives", "bullets": [f"[MOCK OUTPUT] {item}" for item in objectives] or [objective_text]},
        {"title": "Vocabulary", "bullets": [f"[MOCK OUTPUT] {subject_name} term 1", f"[MOCK OUTPUT] {subject_name} term 2"]},
        {"title": "Mini lesson", "bullets": ["Model the target skill.", "Guide student practice."]},
        {"title": "Practice / activity", "bullets": ["Collaborative task.", "Independent application."]},
        {"title": "Exit ticket", "bullets": ["One question aligned to today's objective."]},
    ]


def _generic_sections(*, title: str, subject_name: str, week_label: str) -> dict[str, Any]:
    return {
        "title": title,
        "summary": f"[MOCK OUTPUT] {subject_name} resource for {week_label}.",
        "sections": [
            {"heading": "Purpose", "body": f"Support {subject_name} instruction during {week_label}."},
            {
                "heading": "Key points",
                "bullets": [
                    "Aligned to pacing guide objectives.",
                    "Uses district and teacher supplemental context.",
                ],
            },
        ],
    }


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
) -> TeacherAssistV2InstructionalPackage:
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
    )
    base = _assignment_context(db, user=user)
    onboarding = base["onboarding"]
    provider = get_teacher_assist_ai_provider(settings)
    provider_name = provider.provider_name
    subject_names = [row["subject_name"] for row in context["subjects"]]
    primary_guide_id = (
        uuid.UUID(context["pacing_guide_ids"][0]) if context["pacing_guide_ids"] else None
    )
    today = date.today()
    stored_status = "active" if plan_start_date <= today <= plan_end_date else "generated"

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
        status=stored_status,
        provider_name=provider_name,
        metadata_json={
            "is_mock": provider_name == "mock",
            "context_weeks": len(context["weeks"]),
            "effective_status": resolve_effective_package_status(
                stored_status=stored_status,
                plan_start_date=plan_start_date,
                plan_end_date=plan_end_date,
                today=today,
            ),
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
            TeacherAssistV2PlanningSupplementalMaterial.active.is_(True),
        )
    ).all()
    for row in supplemental_rows:
        row.package_id = package.id
        row.updated_at = now

    sequence = 0
    subject_lookup = {row["subject_id"]: row for row in context["subjects"]}
    teaching_order_keys = [str(value) for value in teaching_order]

    for week in context["weeks"]:
        week_label = week["title"]
        week_subjects = {row["subject_id"]: row for row in week["subjects"]}

        if "daily_lesson_plan" in outputs:
            for day_index, day_label in enumerate(WEEKDAY_LABELS):
                subject_blocks = []
                for subject_id in teaching_order_keys:
                    week_subject = week_subjects.get(subject_id)
                    subject_meta = subject_lookup[subject_id]
                    objective_code = None
                    if week_subject and week_subject["objectives"]:
                        objective_code = week_subject["objectives"][0].get("objective_code")
                    subject_blocks.append(
                        _mock_subject_block(
                            subject_name=subject_meta["subject_name"],
                            objective_code=objective_code,
                            daily_topic=week_subject.get("daily_topic") if week_subject else None,
                        )
                    )
                content = {
                    "title": f"{day_label} Daily Plan — {week_label}",
                    "summary": f"[MOCK OUTPUT] Daily plan covering {', '.join(row['subject_name'] for row in context['subjects'])}.",
                    "subjects": subject_blocks,
                }
                sequence += 1
                artifact = TeacherAssistV2InstructionalPackageArtifact(
                    id=uuid.uuid4(),
                    tenant_id=package.tenant_id,
                    package_id=package.id,
                    artifact_type="daily_lesson_plan",
                    subject_id=None,
                    period_id=uuid.UUID(week_subjects[teaching_order_keys[0]]["period_id"]) if teaching_order_keys and teaching_order_keys[0] in week_subjects else None,
                    day_label=day_label,
                    sequence_number=sequence,
                    title=content["title"],
                    status="ready",
                    content_json=content,
                    preview_html=render_artifact_preview_html(artifact_type="daily_lesson_plan", content=content),
                    metadata_json={"provider": provider_name},
                    created_at=now,
                    updated_at=now,
                )
                storage_key, export_format = save_artifact_export(
                    settings=settings,
                    tenant_id=package.tenant_id,
                    artifact_id=artifact.id,
                    artifact_type="daily_lesson_plan",
                    content=content,
                )
                artifact.storage_key = storage_key
                artifact.export_format = export_format
                db.add(artifact)

        if "subject_slide_deck" in outputs:
            for subject_id in teaching_order_keys:
                week_subject = week_subjects.get(subject_id)
                subject_meta = subject_lookup[subject_id]
                objectives = [
                    row.get("objective_code") or row.get("description")
                    for row in (week_subject or {}).get("objectives", [])
                ]
                content = {
                    "title": f"{subject_meta['subject_name']} {week_label} Slides",
                    "slides": _mock_slides(
                        subject_name=subject_meta["subject_name"],
                        week_label=week_label,
                        objectives=[str(item) for item in objectives if item],
                    ),
                }
                sequence += 1
                artifact = TeacherAssistV2InstructionalPackageArtifact(
                    id=uuid.uuid4(),
                    tenant_id=package.tenant_id,
                    package_id=package.id,
                    artifact_type="subject_slide_deck",
                    subject_id=uuid.UUID(subject_id),
                    period_id=uuid.UUID(week_subject["period_id"]) if week_subject else None,
                    day_label=None,
                    sequence_number=sequence,
                    title=content["title"],
                    status="ready",
                    content_json=content,
                    preview_html=render_artifact_preview_html(artifact_type="subject_slide_deck", content=content),
                    metadata_json={"provider": provider_name, "export_note": "PPTX export coming soon; HTML preview available."},
                    created_at=now,
                    updated_at=now,
                )
                storage_key, export_format = save_artifact_export(
                    settings=settings,
                    tenant_id=package.tenant_id,
                    artifact_id=artifact.id,
                    artifact_type="subject_slide_deck",
                    content=content,
                )
                artifact.storage_key = storage_key
                artifact.export_format = export_format
                db.add(artifact)

        optional_map = {
            "assignment": "Assignment",
            "quiz": "Quiz",
            "rubric": "Rubric",
            "exit_ticket": "Exit Ticket",
            "bell_ringer": "Bell Ringer",
            "vocabulary_list": "Vocabulary List",
            "study_guide": "Study Guide",
            "parent_newsletter_summary": "Parent Newsletter Summary",
        }
        for output_type in outputs:
            if output_type in REQUIRED_PACKAGE_OUTPUTS:
                continue
            for subject_id in teaching_order_keys:
                week_subject = week_subjects.get(subject_id)
                subject_meta = subject_lookup[subject_id]
                label = optional_map[output_type]
                content = _generic_sections(
                    title=f"{subject_meta['subject_name']} {week_label} {label}",
                    subject_name=subject_meta["subject_name"],
                    week_label=week_label,
                )
                sequence += 1
                artifact = TeacherAssistV2InstructionalPackageArtifact(
                    id=uuid.uuid4(),
                    tenant_id=package.tenant_id,
                    package_id=package.id,
                    artifact_type=output_type,
                    subject_id=uuid.UUID(subject_id),
                    period_id=uuid.UUID(week_subject["period_id"]) if week_subject and week_subject.get("period_id") else None,
                    day_label=None,
                    sequence_number=sequence,
                    title=content["title"],
                    status="ready",
                    content_json=content,
                    preview_html=render_artifact_preview_html(artifact_type=output_type, content=content),
                    metadata_json={"provider": provider_name},
                    created_at=now,
                    updated_at=now,
                )
                storage_key, export_format = save_artifact_export(
                    settings=settings,
                    tenant_id=package.tenant_id,
                    artifact_id=artifact.id,
                    artifact_type=output_type,
                    content=content,
                )
                artifact.storage_key = storage_key
                artifact.export_format = export_format
                db.add(artifact)
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

    db.flush()
    return package

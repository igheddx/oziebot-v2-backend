"""Unified teacher planning generation context."""

from __future__ import annotations

import uuid

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.document_extraction import (
    build_ai_readiness_summary,
    build_document_prompt_context,
)
from oziebot_api.services.teacher_assist_v2.link_context import build_link_prompt_context
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import flatten_pacing_materials
from oziebot_api.services.teacher_assist_v2.planning_workflow import (
    _assignment_context,
    _teacher_assignments,
    build_planning_review_context,
    list_planning_supplemental_materials,
)


def build_teacher_planning_generation_context(
    db: Session,
    *,
    user: User,
    week_start: int,
    week_end: int,
    teaching_order: list[uuid.UUID],
    selected_outputs: list[str],
    settings: Settings | None = None,
    excluded_pacing_material_ids: list[uuid.UUID] | None = None,
) -> dict:
    base = _assignment_context(db, user=user)
    onboarding = base["onboarding"]
    review = build_planning_review_context(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        settings=settings,
        excluded_pacing_material_ids=excluded_pacing_material_ids,
    )
    supplemental = list_planning_supplemental_materials(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        settings=settings,
        unlinked_only=True,
    )

    allowed_subject_ids = {row["subject_id"] for row in base["subjects"]}
    ordered_subjects = []
    for subject_id in teaching_order:
        subject_key = str(subject_id)
        if subject_key not in allowed_subject_ids:
            raise ValueError(f"Subject {subject_key} is not part of your assignment")
        match = next(row for row in base["subjects"] if row["subject_id"] == subject_key)
        ordered_subjects.append(match)

    assignments = _teacher_assignments(db, user=user)
    pacing_guide_ids = [str(row.pacing_guide_id) for row in assignments]

    excluded_ids = {str(value) for value in (excluded_pacing_material_ids or [])}
    pacing_materials = [
        material
        for week in review["weeks"]
        for subject in week["subjects"]
        for material in flatten_pacing_materials(
            subject.get("pacing_context"),
            excluded_material_ids=excluded_ids or None,
        )
    ]
    district_files = [row for row in pacing_materials if row.get("material_kind") == "file"]
    teacher_files = [row for row in supplemental if row.get("material_kind") == "file"]
    district_links = [row for row in pacing_materials if row.get("material_kind") == "link"]
    teacher_links = [row for row in supplemental if row.get("material_kind") == "link"]
    district_document_context = build_document_prompt_context(
        district_files,
        source_label="district_curriculum",
    )
    teacher_document_context = build_document_prompt_context(
        teacher_files,
        source_label="teacher_supplemental",
    )
    district_link_context = build_link_prompt_context(
        district_links,
        source_label="district_reference_link",
    )
    teacher_link_context = build_link_prompt_context(
        teacher_links,
        source_label="teacher_reference_link",
    )
    readiness_summary = build_ai_readiness_summary(
        district_materials=pacing_materials,
        teacher_materials=supplemental,
    )

    return {
        "teacher": {"id": str(user.id), "email": user.email, "full_name": user.full_name},
        "school_year": {
            "id": str(base["platform_year"].id),
            "title": base["platform_year"].title,
        },
        "state_id": str(onboarding.state_id),
        "district_id": str(onboarding.district_id),
        "school_id": str(onboarding.school_id) if onboarding.school_id else None,
        "grade_id": str(onboarding.grade_id),
        "subjects": ordered_subjects,
        "pacing_guide_ids": pacing_guide_ids,
        "week_start": week_start,
        "week_end": week_end,
        "weeks": review["weeks"],
        "pacing_materials": pacing_materials,
        "district_materials_summary": pacing_materials,
        "teacher_supplemental_files": [row for row in supplemental if row["material_kind"] == "file"],
        "teacher_supplemental_links": [row for row in supplemental if row["material_kind"] == "link"],
        "teacher_supplemental_notes": [row for row in supplemental if row["material_kind"] == "note"],
        "district_document_context": district_document_context,
        "teacher_document_context": teacher_document_context,
        "district_link_context": district_link_context,
        "teacher_link_context": teacher_link_context,
        "ai_readiness_summary": readiness_summary,
        "selected_output_types": selected_outputs,
        "teaching_order": [row["subject_id"] for row in ordered_subjects],
    }


def get_pacing_guide_planning_context_for_teacher(
    db: Session,
    *,
    user: User,
    week_start: int,
    week_end: int,
    settings: Settings | None = None,
) -> dict:
    return build_planning_review_context(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        settings=settings,
    )

from __future__ import annotations

from datetime import date
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.assignments import create_assignment
from oziebot_api.services.teacher_assist.generated_artifacts import register_generated_artifact, serialize_generated_artifact
from oziebot_api.services.teacher_assist.newsletters import create_newsletter
from oziebot_api.services.teacher_assist.planning import attach_planning_draft_resource, create_planning_draft
from oziebot_api.services.teacher_assist.instructional_weeks import link_entities_to_instructional_week
from oziebot_api.services.teacher_assist.teacher_classroom import _resolve_homeroom_class
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create
from oziebot_api.services.teacher_assist.week_context_service import WeekContextService


def _resolve_standard_ids(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    objective_codes: list[str],
    school_year_id: uuid.UUID | None = None,
) -> list[uuid.UUID]:
    standard_ids: list[uuid.UUID] = []
    for code in objective_codes:
        base_query = select(TeacherAssistStandard).where(
            TeacherAssistStandard.tenant_id == tenant_id,
            TeacherAssistStandard.code == code,
        )
        standard = None
        if school_year_id is not None:
            standard = db.scalars(
                base_query.where(TeacherAssistStandard.school_year_id == school_year_id)
            ).first()
        if standard is None:
            standard = db.scalars(
                base_query.where(TeacherAssistStandard.school_year_id.is_(None))
            ).first()
        if standard is None:
            standard = db.scalars(base_query).first()
        if standard is not None:
            standard_ids.append(standard.id)
    return standard_ids


def _resolve_resource_ids(week_context: dict[str, Any]) -> list[uuid.UUID]:
    resource_ids: list[uuid.UUID] = []
    for row in week_context.get("resources", []):
        library_id = row.get("resource_library_item_id")
        if library_id:
            resource_ids.append(uuid.UUID(library_id))
    return resource_ids


def _default_class_id(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
    grade_level: str | None = None,
) -> uuid.UUID:
    prefs = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user_id)
    if prefs.last_class_id is not None:
        preferred = db.scalars(
            select(TeacherAssistClass).where(
                TeacherAssistClass.id == prefs.last_class_id,
                TeacherAssistClass.tenant_id == tenant_id,
            )
        ).first()
        if preferred is not None and (
            school_year_id is None or preferred.school_year_id == school_year_id
        ):
            return preferred.id

    if school_year_id is not None and grade_level:
        homeroom = _resolve_homeroom_class(
            db,
            tenant_id=tenant_id,
            school_year_id=school_year_id,
            grade_level=grade_level,
        )
        if homeroom is not None:
            return homeroom.id

    query = select(TeacherAssistClass).where(TeacherAssistClass.tenant_id == tenant_id)
    if school_year_id is not None:
        query = query.where(TeacherAssistClass.school_year_id == school_year_id)
    first_class = db.scalars(query.order_by(TeacherAssistClass.created_at.desc())).first()
    if first_class is None:
        if school_year_id is not None:
            raise ValueError(
                "Add at least one class for the pacing guide school year before generating week artifacts."
            )
        raise ValueError("Add at least one class before generating week artifacts.")
    return first_class.id


def _build_rubric_json(week_context: dict[str, Any]) -> dict[str, Any]:
    criteria = []
    for objective in week_context.get("objectives", []):
        code = objective.get("objective_code") or "Objective"
        criteria.append(
            {
                "criterion": code,
                "description": objective.get("description"),
                "levels": [
                    {"label": "Exceeds", "points": 4, "description": f"Demonstrates mastery of {code}."},
                    {"label": "Meets", "points": 3, "description": f"Meets expectations for {code}."},
                    {"label": "Approaching", "points": 2, "description": f"Partial understanding of {code}."},
                    {"label": "Beginning", "points": 1, "description": f"Limited evidence for {code}."},
                ],
            }
        )
    return {
        "title": f"Rubric — {week_context.get('period_title')}",
        "subject": week_context.get("subject_name"),
        "grade_level": week_context.get("grade_level"),
        "criteria": criteria,
        "week_context": week_context.get("traceability"),
    }


def _build_quiz_metadata(week_context: dict[str, Any]) -> dict[str, Any]:
    questions = []
    for objective in week_context.get("objectives", []):
        code = objective.get("objective_code") or "Objective"
        questions.append(
            {
                "question_type": "multiple_choice",
                "prompt": f"Which statement best reflects {code}?",
                "objective_code": code,
                "choices": [
                    "Correct understanding",
                    "Partial understanding",
                    "Misconception A",
                    "Misconception B",
                ],
                "answer_key": "Correct understanding",
            }
        )
    return {
        "question_bank": questions,
        "week_context": week_context.get("traceability"),
        "generation_mode": "foundation",
    }


def generate_week_artifact(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    period_id: uuid.UUID,
    artifact_type: str,
    class_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    normalized_type = artifact_type.strip().upper()
    if normalized_type in {"INSTRUCTIONAL_PLAN", "LESSON_PLAN"}:
        normalized_type = "LESSON_PLAN"

    dto = WeekContextService.build(db, tenant_id=tenant_id, user=user, period_id=period_id)
    week_context = WeekContextService.serialize(dto)
    objective_codes = [row.get("objective_code") for row in week_context.get("objectives", []) if row.get("objective_code")]
    standard_ids = _resolve_standard_ids(
        db,
        tenant_id=tenant_id,
        objective_codes=objective_codes,
        school_year_id=dto.school_year_id,
    )
    resource_ids = _resolve_resource_ids(week_context)
    traceability = week_context.get("traceability") or {}
    resource_links = week_context.get("resources") or []

    if normalized_type == "LESSON_PLAN":
        draft = create_planning_draft(
            db,
            tenant_id=tenant_id,
            user=user,
            planning_scope="weekly",
            school_year_id=dto.school_year_id,
            grading_period_id=dto.grading_period_id,
            class_id=class_id
            or _default_class_id(
                db,
                tenant_id=tenant_id,
                user_id=user.id,
                school_year_id=dto.school_year_id,
                grade_level=dto.grade_level,
            ),
            subject_id=dto.subject_id,
            subject_ids=[dto.subject_id] if dto.subject_id else [],
            pacing_item_ids=[],
            standard_ids=standard_ids,
            pacing_guide_period_id=period_id,
            title=dto.period_title,
            module_title=dto.period_title,
            start_date=date.fromisoformat(dto.start_date) if dto.start_date else None,
            end_date=date.fromisoformat(dto.end_date) if dto.end_date else None,
            estimated_weeks=1,
            instructional_days_count=5,
            notes=dto.notes,
            status="draft",
        )
        for resource_id in resource_ids:
            attach_planning_draft_resource(
                db,
                tenant_id=tenant_id,
                user_id=user.id,
                planning_draft_id=draft.id,
                resource_library_item_id=resource_id,
            )
        artifact = register_generated_artifact(
            db,
            tenant_id=tenant_id,
            user=user,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
            artifact_type="LESSON_PLAN",
            title=dto.period_title,
            status="draft",
            planning_draft_id=draft.id,
            resource_links=resource_links,
            metadata={"week_context": traceability, "objective_codes": objective_codes},
        )
        link_entities_to_instructional_week(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            pacing_guide_period_id=period_id,
        )
        payload = serialize_generated_artifact(db, artifact)
        payload["navigation_href"] = f"/teacher-assist/weekly-planning?draft_id={draft.id}"
        return payload

    resolved_class_id = class_id or _default_class_id(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        school_year_id=dto.school_year_id,
        grade_level=dto.grade_level,
    )
    if dto.subject_id is None:
        raise ValueError("Week context is missing a subject. Complete setup or select a subject-aligned pacing guide.")
    due_date = None

    if normalized_type == "ASSIGNMENT":
        assignment = create_assignment(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            school_year_id=dto.school_year_id,
            grading_period_id=dto.grading_period_id,
            class_id=resolved_class_id,
            subject_id=dto.subject_id,
            title=f"{dto.period_title} Assignment",
            description=dto.notes,
            assignment_type="other",
            due_date=due_date,
            status="draft",
            instructions=dto.notes,
            rubric_json=None,
            source_plan_id=None,
            source_context_json={"week_context": traceability},
            standard_ids=standard_ids,
            resource_ids=resource_ids,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
        )
        artifact = register_generated_artifact(
            db,
            tenant_id=tenant_id,
            user=user,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
            artifact_type="ASSIGNMENT",
            title=assignment.title,
            status=assignment.status,
            assignment_id=assignment.id,
            resource_links=resource_links,
            metadata={"week_context": traceability, "objective_codes": objective_codes},
        )
        link_entities_to_instructional_week(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            pacing_guide_period_id=period_id,
            assignment_id=assignment.id,
        )
        payload = serialize_generated_artifact(db, artifact)
        payload["navigation_href"] = f"/teacher-assist/assignments?assignment_id={assignment.id}"
        return payload

    if normalized_type == "QUIZ":
        assignment = create_assignment(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            school_year_id=dto.school_year_id,
            grading_period_id=dto.grading_period_id,
            class_id=resolved_class_id,
            subject_id=dto.subject_id,
            title=f"{dto.period_title} Quiz",
            description="Quiz generated from pacing week objectives.",
            assignment_type="quiz",
            due_date=due_date,
            status="draft",
            instructions=dto.notes,
            rubric_json=None,
            source_plan_id=None,
            source_context_json={"week_context": traceability, "quiz": _build_quiz_metadata(week_context)},
            standard_ids=standard_ids,
            resource_ids=resource_ids,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
        )
        artifact = register_generated_artifact(
            db,
            tenant_id=tenant_id,
            user=user,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
            artifact_type="QUIZ",
            title=assignment.title,
            status=assignment.status,
            assignment_id=assignment.id,
            resource_links=resource_links,
            metadata={"week_context": traceability, "quiz": _build_quiz_metadata(week_context)},
        )
        link_entities_to_instructional_week(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            pacing_guide_period_id=period_id,
            assignment_id=assignment.id,
        )
        payload = serialize_generated_artifact(db, artifact)
        payload["navigation_href"] = f"/teacher-assist/assignments?assignment_id={assignment.id}"
        return payload

    if normalized_type == "RUBRIC":
        rubric_json = _build_rubric_json(week_context)
        assignment = create_assignment(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            school_year_id=dto.school_year_id,
            grading_period_id=dto.grading_period_id,
            class_id=resolved_class_id,
            subject_id=dto.subject_id,
            title=f"{dto.period_title} Rubric",
            description="Rubric scaffold generated from week objectives.",
            assignment_type="other",
            due_date=None,
            status="draft",
            instructions="Review and attach this rubric to the appropriate assignment.",
            rubric_json=rubric_json,
            source_plan_id=None,
            source_context_json={"week_context": traceability, "rubric": rubric_json},
            standard_ids=standard_ids,
            resource_ids=resource_ids,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
        )
        artifact = register_generated_artifact(
            db,
            tenant_id=tenant_id,
            user=user,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
            artifact_type="RUBRIC",
            title=assignment.title,
            status=assignment.status,
            assignment_id=assignment.id,
            resource_links=resource_links,
            metadata={"week_context": traceability, "rubric": rubric_json},
        )
        link_entities_to_instructional_week(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            pacing_guide_period_id=period_id,
            assignment_id=assignment.id,
        )
        payload = serialize_generated_artifact(db, artifact)
        payload["navigation_href"] = f"/teacher-assist/assignments?assignment_id={assignment.id}"
        return payload

    if normalized_type == "NEWSLETTER":
        objective_summary = ", ".join(objective_codes)
        notes_parts = [part for part in [dto.notes, f"Objectives: {objective_summary}" if objective_summary else ""] if part]
        newsletter = create_newsletter(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            school_year_id=dto.school_year_id,
            grading_period_id=dto.grading_period_id,
            class_id=resolved_class_id,
            subject_id=dto.subject_id,
            title=f"Weekly Newsletter — {dto.period_title}",
            teacher_notes="\n\n".join(notes_parts),
            week_start_date=date.fromisoformat(dto.start_date) if dto.start_date else None,
            week_end_date=date.fromisoformat(dto.end_date) if dto.end_date else None,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
        )
        artifact = register_generated_artifact(
            db,
            tenant_id=tenant_id,
            user=user,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
            artifact_type="NEWSLETTER",
            title=newsletter.title,
            status=newsletter.status,
            newsletter_id=newsletter.id,
            resource_links=resource_links,
            metadata={
                "week_context": traceability,
                "objectives_taught": objective_codes,
                "resources_used": [row.get("title") for row in resource_links if row.get("title")],
                "upcoming_topics": week_context.get("upcoming_topics") or [],
            },
        )
        link_entities_to_instructional_week(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            pacing_guide_period_id=period_id,
            newsletter_id=newsletter.id,
        )
        payload = serialize_generated_artifact(db, artifact)
        payload["navigation_href"] = f"/teacher-assist/newsletters?id={newsletter.id}"
        return payload

    if normalized_type == "PARENT_COMMUNICATION":
        artifact = register_generated_artifact(
            db,
            tenant_id=tenant_id,
            user=user,
            pacing_guide_id=dto.pacing_guide_id,
            pacing_guide_period_id=period_id,
            artifact_type="PARENT_COMMUNICATION",
            title=f"Parent Update — {dto.period_title}",
            status="draft",
            resource_links=resource_links,
            metadata={
                "week_context": traceability,
                "draft_body": (
                    f"This week in {dto.subject_name or 'class'} we focused on {dto.period_title}. "
                    f"Key objectives: {', '.join(objective_codes) if objective_codes else 'see pacing guide'}."
                ),
                "objectives_taught": objective_codes,
                "resources_used": [row.get("title") for row in resource_links if row.get("title")],
                "upcoming_topics": week_context.get("upcoming_topics") or [],
                "outbound_send_enabled": False,
            },
        )
        payload = serialize_generated_artifact(db, artifact)
        payload["navigation_href"] = f"/teacher-assist/planning/weeks?period_id={period_id}&artifact_id={artifact.id}"
        return payload

    raise ValueError("Unsupported artifact type for week generation")

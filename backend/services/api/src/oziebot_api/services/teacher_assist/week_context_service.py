from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.education_catalog import EducationCurriculumResource, EducationGrade, EducationSubject
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_objective import TeacherAssistPacingGuideObjective
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_period_note import TeacherAssistPacingGuidePeriodNote
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.current_week_resolver import CurrentWeekResolver
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create


@dataclass
class WeekContextDTO:
    pacing_guide_id: uuid.UUID
    pacing_guide_period_id: uuid.UUID
    pacing_guide_title: str
    period_title: str
    period_type: str
    school_year_id: uuid.UUID | None
    school_year_title: str | None
    grading_period_id: uuid.UUID | None
    subject_id: uuid.UUID | None
    subject_name: str | None
    grade_level: str | None
    grade_display_name: str | None
    start_date: str | None
    end_date: str | None
    notes: str | None
    teacher_user_id: uuid.UUID
    teacher_name: str | None
    objectives: list[dict[str, Any]]
    resources: list[dict[str, Any]]
    curriculum_references: list[dict[str, Any]]
    textbook_references: list[dict[str, Any]]
    teacher_resources: list[dict[str, Any]]
    external_links: list[dict[str, Any]]
    upcoming_topics: list[str]
    traceability: dict[str, Any]


class WeekContextService:
    @staticmethod
    def build(
        db: Session,
        *,
        tenant_id: uuid.UUID,
        user: User,
        period_id: uuid.UUID,
    ) -> WeekContextDTO:
        period = db.scalars(
            select(TeacherAssistPacingGuidePeriod)
            .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
            .where(
                TeacherAssistPacingGuidePeriod.id == period_id,
                TeacherAssistPacingGuide.tenant_id == tenant_id,
            )
            .options(
                selectinload(TeacherAssistPacingGuidePeriod.objectives).selectinload(
                    TeacherAssistPacingGuideObjective.objective
                ),
                selectinload(TeacherAssistPacingGuidePeriod.resources),
            )
        ).one_or_none()
        if period is None:
            raise LookupError("Pacing guide period not found")
        guide = db.get(TeacherAssistPacingGuide, period.pacing_guide_id)
        if guide is None:
            raise LookupError("Pacing guide not found")

        context = CurrentWeekResolver.resolve(db, tenant_id=tenant_id, user_id=user.id, guide_id=guide.id)
        school_year = db.get(TeacherAssistSchoolYear, guide.school_year_id)
        subject_id = None
        subject_name = None
        grade_level = None
        grade_display_name = None
        if guide.catalog_subject_id is not None:
            catalog_subject = db.get(EducationSubject, guide.catalog_subject_id)
            if catalog_subject is not None:
                subject_name = catalog_subject.display_name
                grade_level = catalog_subject.subject_code
                grade = db.get(EducationGrade, catalog_subject.grade_id)
                if grade is not None:
                    grade_level = grade.grade_code
                    grade_display_name = grade.display_name
                subject = db.scalars(
                    select(TeacherAssistSubject).where(
                        TeacherAssistSubject.tenant_id == tenant_id,
                        TeacherAssistSubject.name.ilike(f"%{catalog_subject.display_name}%"),
                    )
                ).first()
                subject_id = subject.id if subject else None
        if subject_id is None:
            prefs = get_user_preferences_or_create(db, tenant_id=tenant_id, user_id=user.id)
            if prefs.last_subject_id is not None:
                subject_id = prefs.last_subject_id
                subject = db.get(TeacherAssistSubject, subject_id)
                subject_name = subject.name if subject else None

        note = db.scalars(
            select(TeacherAssistPacingGuidePeriodNote).where(
                TeacherAssistPacingGuidePeriodNote.tenant_id == tenant_id,
                TeacherAssistPacingGuidePeriodNote.user_id == user.id,
                TeacherAssistPacingGuidePeriodNote.period_id == period_id,
            )
        ).one_or_none()
        notes_parts = [part for part in [period.description, note.notes if note else None] if part]
        combined_notes = "\n\n".join(notes_parts) if notes_parts else None

        objectives: list[dict[str, Any]] = []
        for mapping in period.objectives:
            objective = mapping.objective or None
            objectives.append(
                {
                    "objective_id": str(mapping.objective_id),
                    "objective_code": getattr(objective, "objective_id", None),
                    "description": getattr(objective, "description", None),
                    "is_required": mapping.is_required,
                    "notes": mapping.notes,
                }
            )

        resources: list[dict[str, Any]] = []
        curriculum_references: list[dict[str, Any]] = []
        textbook_references: list[dict[str, Any]] = []
        teacher_resources: list[dict[str, Any]] = []
        external_links: list[dict[str, Any]] = []
        for mapping in period.resources:
            catalog_resource = None
            if mapping.catalog_resource_id is not None:
                catalog_resource = db.get(EducationCurriculumResource, mapping.catalog_resource_id)
            payload = {
                "catalog_resource_id": str(mapping.catalog_resource_id) if mapping.catalog_resource_id else None,
                "resource_library_item_id": str(mapping.resource_library_item_id)
                if mapping.resource_library_item_id
                else None,
                "title": getattr(catalog_resource, "title", None),
                "resource_type": getattr(catalog_resource, "resource_type", None),
                "notes": mapping.notes,
                "is_primary": mapping.is_primary,
            }
            resources.append(payload)
            resource_type = (getattr(catalog_resource, "resource_type", None) or "").lower()
            if resource_type == "textbook":
                textbook_references.append(payload)
            elif resource_type in {"curriculum", "reference"}:
                curriculum_references.append(payload)
            elif mapping.resource_library_item_id is not None:
                teacher_resources.append(payload)
            elif resource_type == "link":
                external_links.append(payload)
            else:
                curriculum_references.append(payload)

        upcoming_topics: list[str] = []
        if context and context.upcoming_week is not None:
            upcoming_topics.append(context.upcoming_week.title)
            if context.upcoming_week.description:
                upcoming_topics.append(context.upcoming_week.description)

        return WeekContextDTO(
            pacing_guide_id=guide.id,
            pacing_guide_period_id=period.id,
            pacing_guide_title=guide.title,
            period_title=period.title,
            period_type=period.period_type,
            school_year_id=guide.school_year_id,
            school_year_title=school_year.title if school_year else None,
            grading_period_id=context.grading_period.id if context and context.grading_period else None,
            subject_id=subject_id,
            subject_name=subject_name,
            grade_level=grade_level,
            grade_display_name=grade_display_name,
            start_date=period.start_date.isoformat() if period.start_date else None,
            end_date=period.end_date.isoformat() if period.end_date else None,
            notes=combined_notes,
            teacher_user_id=user.id,
            teacher_name=user.full_name,
            objectives=objectives,
            resources=resources,
            curriculum_references=curriculum_references,
            textbook_references=textbook_references,
            teacher_resources=teacher_resources,
            external_links=external_links,
            upcoming_topics=upcoming_topics,
            traceability={
                "pacing_guide_id": str(guide.id),
                "pacing_guide_period_id": str(period.id),
                "school_year_id": str(guide.school_year_id),
                "objective_ids": [row["objective_id"] for row in objectives],
                "resource_links": resources,
            },
        )

    @staticmethod
    def serialize(dto: WeekContextDTO) -> dict[str, Any]:
        return {
            "pacing_guide_id": str(dto.pacing_guide_id),
            "pacing_guide_period_id": str(dto.pacing_guide_period_id),
            "pacing_guide_title": dto.pacing_guide_title,
            "period_title": dto.period_title,
            "period_type": dto.period_type,
            "school_year_id": str(dto.school_year_id) if dto.school_year_id else None,
            "school_year_title": dto.school_year_title,
            "grading_period_id": str(dto.grading_period_id) if dto.grading_period_id else None,
            "subject_id": str(dto.subject_id) if dto.subject_id else None,
            "subject_name": dto.subject_name,
            "grade_level": dto.grade_level,
            "grade_display_name": dto.grade_display_name,
            "start_date": dto.start_date,
            "end_date": dto.end_date,
            "notes": dto.notes,
            "teacher": {
                "user_id": str(dto.teacher_user_id),
                "full_name": dto.teacher_name,
            },
            "objectives": dto.objectives,
            "resources": dto.resources,
            "curriculum_references": dto.curriculum_references,
            "textbook_references": dto.textbook_references,
            "teacher_resources": dto.teacher_resources,
            "external_links": dto.external_links,
            "upcoming_topics": dto.upcoming_topics,
            "traceability": dto.traceability,
        }


def week_context_as_of(db: Session, *, tenant_id: uuid.UUID, user: User, period_id: uuid.UUID) -> dict[str, Any]:
    dto = WeekContextService.build(db, tenant_id=tenant_id, user=user, period_id=period_id)
    return WeekContextService.serialize(dto)

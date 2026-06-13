"""Build grading context for TeacherAssist v2 AI draft generation."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationGrade, EducationObjective, EducationSchoolYear, EducationSubject
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
    TeacherAssistV2PlanningSupplementalMaterial,
)
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.services.teacher_assist_v2.document_extraction import (
    load_submission_extractions,
    serialize_document_extraction,
)
from oziebot_api.services.teacher_assist_v2.grading_rubric import (
    grading_template_from_package_rubric,
    resolve_linked_rubric_content,
)


def _artifact_content_summary(content: dict[str, Any] | None) -> str | None:
    if not isinstance(content, dict):
        return None
    parts: list[str] = []
    if content.get("summary"):
        parts.append(str(content["summary"]))
    if content.get("title"):
        parts.append(str(content["title"]))
    sections = content.get("sections")
    if isinstance(sections, list):
        for section in sections[:4]:
            if not isinstance(section, dict):
                continue
            heading = section.get("heading")
            body = section.get("body")
            if heading:
                parts.append(str(heading))
            if body:
                parts.append(str(body))
            bullets = section.get("bullets")
            if isinstance(bullets, list):
                parts.extend(str(item) for item in bullets[:3])
    criteria = content.get("criteria")
    if isinstance(criteria, list):
        for row in criteria[:6]:
            if isinstance(row, dict) and row.get("name"):
                parts.append(f"{row['name']} ({row.get('points', 0)} pts)")
    return "\n".join(part for part in parts if part).strip() or None


def build_grading_context(
    db: Session,
    *,
    assignment: TeacherAssistV2Assignment,
    submission: TeacherAssistV2StudentSubmission,
) -> dict[str, Any]:
    package = db.get(TeacherAssistV2InstructionalPackage, assignment.instructional_package_id)
    if package is None:
        raise ValueError("Assignment is missing its instructional plan.")

    grade = db.get(EducationGrade, assignment.catalog_grade_id)
    subject = db.get(EducationSubject, assignment.catalog_subject_id)
    school_year = db.get(EducationSchoolYear, assignment.platform_school_year_id)

    objective_ids = [uuid.UUID(str(value)) for value in assignment.education_objective_ids_json]
    objectives = db.scalars(
        select(EducationObjective).where(EducationObjective.id.in_(objective_ids))
    ).all() if objective_ids else []

    artifacts = db.scalars(
        select(TeacherAssistV2InstructionalPackageArtifact).where(
            TeacherAssistV2InstructionalPackageArtifact.package_id == package.id
        )
    ).all()

    assignment_instructions = assignment.description
    rubric_content: dict[str, Any] | None = resolve_linked_rubric_content(artifacts, assignment=assignment)
    for artifact in artifacts:
        content = artifact.content_json if isinstance(artifact.content_json, dict) else None
        if artifact.assignment_id == assignment.id and artifact.artifact_type == "writing_response":
            prompt = content.get("prompt") if content else None
            instructions = content.get("instructions") if content else None
            parts = [str(prompt).strip()] if prompt else []
            if isinstance(instructions, list):
                parts.extend(str(item).strip() for item in instructions if str(item).strip())
            if parts:
                assignment_instructions = "\n".join(parts)
        if artifact.assignment_id == assignment.id and artifact.artifact_type == "assignment":
            summary = _artifact_content_summary(content)
            if summary:
                assignment_instructions = summary

    rubric_template = grading_template_from_package_rubric(rubric_content)
    rubric_summary = _artifact_content_summary(rubric_content)

    teacher_notes = db.scalars(
        select(TeacherAssistV2PlanningSupplementalMaterial).where(
            TeacherAssistV2PlanningSupplementalMaterial.teacher_user_id == assignment.teacher_user_id,
            TeacherAssistV2PlanningSupplementalMaterial.week_start <= assignment.week_number,
            TeacherAssistV2PlanningSupplementalMaterial.week_end >= assignment.week_number,
            TeacherAssistV2PlanningSupplementalMaterial.material_kind == "note",
        )
    ).all()
    teacher_note_bodies = [
        row.note_body.strip()
        for row in teacher_notes
        if row.note_body and row.note_body.strip()
    ]
    if package.close_out_notes:
        teacher_note_bodies.append(package.close_out_notes.strip())

    extraction = serialize_document_extraction(
        load_submission_extractions(db, submission_ids=[submission.id]).get(submission.id),
        include_full_text=True,
    )
    student_response_text = (extraction or {}).get("effective_text")

    return {
        "assignment": {
            "id": str(assignment.id),
            "title": assignment.title,
            "description": assignment.description,
            "assignment_type": assignment.assignment_type,
            "week_number": assignment.week_number,
            "instructions": assignment_instructions,
        },
        "grade_level": grade.display_name if grade else None,
        "subject": subject.display_name if subject else None,
        "school_year": school_year.title if school_year else None,
        "objectives": [
            {
                "objective_id": objective.objective_id,
                "description": objective.description,
                "objective_type": objective.objective_type,
            }
            for objective in objectives
        ],
        "rubric": rubric_content or {},
        "rubric_template": rubric_template,
        "rubric_summary": rubric_summary,
        "teacher_notes": teacher_note_bodies,
        "student_submission": {
            "id": str(submission.id),
            "student_number": submission.student_number,
            "original_filename": submission.original_filename,
            "mime_type": submission.mime_type,
            "match_method": submission.match_method,
            "extraction": extraction,
            "response_text": student_response_text,
            "response_text_available": bool(student_response_text),
        },
    }

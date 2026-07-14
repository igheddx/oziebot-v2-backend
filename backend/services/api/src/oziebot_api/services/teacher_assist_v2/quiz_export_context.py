"""Build export context and refresh quiz artifact downloads."""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import (
    EducationGrade,
    EducationSchoolYear,
    EducationSubject,
)
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.services.teacher_assist_v2.assignment_print_packets import resolve_student_count


def build_quiz_export_context(
    db: Session,
    *,
    package: TeacherAssistV2InstructionalPackage,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    assignment: TeacherAssistV2Assignment | None = None,
) -> dict[str, Any]:
    assignment_row = assignment
    if assignment_row is None and artifact.assignment_id:
        assignment_row = db.get(TeacherAssistV2Assignment, artifact.assignment_id)

    subject_name = None
    if artifact.subject_id:
        subject = db.get(EducationSubject, artifact.subject_id)
        subject_name = subject.display_name if subject else None

    grade_name = None
    grade_id = package.catalog_grade_id
    if grade_id:
        grade = db.get(EducationGrade, grade_id)
        grade_name = grade.display_name if grade else None

    school_year_label = None
    school_year_id = package.platform_school_year_id
    if school_year_id:
        year = db.get(EducationSchoolYear, school_year_id)
        school_year_label = year.title if year else None

    content = artifact.content_json if isinstance(artifact.content_json, dict) else {}
    student_count = resolve_student_count(db, teacher_user_id=package.teacher_user_id)
    return {
        "assignment_id": str(assignment_row.id) if assignment_row else None,
        "package_id": str(package.id),
        "teacher_user_id": str(package.teacher_user_id),
        "student_count": student_count,
        "school_year_id": str(school_year_id) if school_year_id else None,
        "school_year_label": school_year_label,
        "district_id": str(package.catalog_district_id),
        "school_id": str(package.catalog_school_id) if package.catalog_school_id else None,
        "grade_id": str(grade_id) if grade_id else None,
        "grade_name": grade_name,
        "subject_id": str(artifact.subject_id) if artifact.subject_id else None,
        "subject_name": subject_name,
        "objective_ids": list(assignment_row.education_objective_ids_json)
        if assignment_row
        else [],
        "objective_mapping": content.get("objective_mapping"),
    }


def refresh_quiz_artifact_exports(
    db: Session,
    *,
    settings: Settings,
    package: TeacherAssistV2InstructionalPackage,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    assignment: TeacherAssistV2Assignment | None = None,
) -> None:
    if artifact.artifact_type != "quiz":
        return
    from oziebot_api.services.teacher_assist_v2.assessment_student_exports import (
        refresh_assessment_student_exports,
    )

    refresh_assessment_student_exports(
        db, settings=settings, package=package, artifact=artifact, assignment=assignment
    )

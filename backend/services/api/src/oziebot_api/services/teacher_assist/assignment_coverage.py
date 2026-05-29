"""Assignment coverage analysis — objectives assessed per assignment."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_grade_record import TeacherAssistAssignmentGradeRecord
from oziebot_api.models.teacher_assist_assignment_standard import TeacherAssistAssignmentStandard
from oziebot_api.models.teacher_assist_instructional_evidence import TeacherAssistInstructionalEvidence
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.services.teacher_assist.objective_performance import ObjectivePerformanceService


def build_assignment_coverage_view(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    query = select(TeacherAssistAssignment).where(
        TeacherAssistAssignment.tenant_id == tenant_id,
        TeacherAssistAssignment.teacher_user_id == user_id,
    )
    if assignment_id is not None:
        query = query.where(TeacherAssistAssignment.id == assignment_id)
    if instructional_week_id is not None:
        query = query.where(TeacherAssistAssignment.instructional_week_id == instructional_week_id)
    if class_id is not None:
        query = query.where(TeacherAssistAssignment.class_id == class_id)
    assignments = list(db.scalars(query.options(selectinload(TeacherAssistAssignment.standard_links))).all())

    assignment_rows: list[dict[str, Any]] = []
    for assignment in assignments:
        standard_ids = [row.standard_id for row in assignment.standard_links]
        standards = list(
            db.scalars(select(TeacherAssistStandard).where(TeacherAssistStandard.id.in_(standard_ids))).all()
        ) if standard_ids else []

        grade_records = list(
            db.scalars(
                select(TeacherAssistAssignmentGradeRecord).where(
                    TeacherAssistAssignmentGradeRecord.tenant_id == tenant_id,
                    TeacherAssistAssignmentGradeRecord.assignment_id == assignment.id,
                    TeacherAssistAssignmentGradeRecord.record_status == "active",
                )
            ).all()
        )
        evidence_rows = list(
            db.scalars(
                select(TeacherAssistInstructionalEvidence).where(
                    TeacherAssistInstructionalEvidence.tenant_id == tenant_id,
                    TeacherAssistInstructionalEvidence.owner_user_id == user_id,
                    TeacherAssistInstructionalEvidence.source_type == "ASSIGNMENT",
                    TeacherAssistInstructionalEvidence.source_id == assignment.id,
                )
            ).all()
        )
        students_assessed = {str(row.student_number) for row in grade_records}
        students_assessed.update(row.student_identifier for row in evidence_rows)

        objective_performance: list[dict[str, Any]] = []
        for standard in standards:
            objective_performance.append(
                ObjectivePerformanceService.calculate_for_objective(
                    db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    standard_id=standard.id,
                    class_id=assignment.class_id,
                    instructional_week_id=assignment.instructional_week_id,
                )
            )

        mastery_values = [row.get("mastery_pct") or 0 for row in objective_performance]
        mastery_pct = round(sum(mastery_values) / len(mastery_values), 1) if mastery_values else 0.0
        coverage_pct = 100.0 if students_assessed else 0.0

        assignment_rows.append(
            {
                "assignment_id": str(assignment.id),
                "title": assignment.title,
                "status": assignment.status,
                "instructional_week_id": str(assignment.instructional_week_id)
                if assignment.instructional_week_id
                else None,
                "associated_objectives": [
                    {"standard_id": str(standard.id), "code": standard.code, "title": standard.title}
                    for standard in standards
                ],
                "students_assessed": len(students_assessed),
                "coverage_pct": coverage_pct,
                "mastery_pct": mastery_pct,
                "objective_performance": objective_performance,
                "teacher_notes": assignment.description,
                "navigation_href": f"/teacher-assist/assignments?assignment_id={assignment.id}",
            }
        )

    return {
        "assignments": assignment_rows,
        "summary": {
            "assignment_count": len(assignment_rows),
            "average_coverage_pct": round(
                sum(row["coverage_pct"] for row in assignment_rows) / len(assignment_rows), 1
            )
            if assignment_rows
            else 0.0,
            "average_mastery_pct": round(
                sum(row["mastery_pct"] for row in assignment_rows) / len(assignment_rows), 1
            )
            if assignment_rows
            else 0.0,
        },
        "read_only": True,
    }

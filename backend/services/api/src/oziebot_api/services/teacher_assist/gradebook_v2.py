"""Gradebook v2 — objective alignment and mastery impact on grade records."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_grade_record import (
    TeacherAssistAssignmentGradeRecord,
)
from oziebot_api.models.teacher_assist_instructional_evidence import (
    TeacherAssistInstructionalEvidence,
)
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.services.teacher_assist.instructional_evidence import (
    serialize_instructional_evidence,
)


def build_gradebook_v2_view(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID | None = None,
    assignment_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    query = select(TeacherAssistAssignmentGradeRecord).where(
        TeacherAssistAssignmentGradeRecord.tenant_id == tenant_id,
        TeacherAssistAssignmentGradeRecord.teacher_user_id == user_id,
        TeacherAssistAssignmentGradeRecord.record_status == "active",
    )
    if class_id is not None:
        query = query.where(TeacherAssistAssignmentGradeRecord.class_id == class_id)
    if assignment_id is not None:
        query = query.where(TeacherAssistAssignmentGradeRecord.assignment_id == assignment_id)
    records = list(
        db.scalars(query.order_by(TeacherAssistAssignmentGradeRecord.updated_at.desc())).all()
    )

    rows: list[dict[str, Any]] = []
    for record in records:
        assignment = db.scalars(
            select(TeacherAssistAssignment)
            .where(TeacherAssistAssignment.id == record.assignment_id)
            .options(selectinload(TeacherAssistAssignment.standard_links))
        ).one_or_none()
        standards = []
        if assignment is not None and assignment.standard_links:
            standard_rows = list(
                db.scalars(
                    select(TeacherAssistStandard).where(
                        TeacherAssistStandard.id.in_(
                            [row.standard_id for row in assignment.standard_links]
                        )
                    )
                ).all()
            )
            standards = [
                {"id": str(row.id), "code": row.code, "title": row.title} for row in standard_rows
            ]

        evidence_rows = list(
            db.scalars(
                select(TeacherAssistInstructionalEvidence).where(
                    TeacherAssistInstructionalEvidence.tenant_id == tenant_id,
                    TeacherAssistInstructionalEvidence.owner_user_id == user_id,
                    TeacherAssistInstructionalEvidence.source_type == "ASSIGNMENT",
                    TeacherAssistInstructionalEvidence.source_id == record.assignment_id,
                    TeacherAssistInstructionalEvidence.student_identifier
                    == str(record.student_number),
                )
            ).all()
        )

        rows.append(
            {
                "grade_record_id": str(record.id),
                "assignment_id": str(record.assignment_id),
                "assignment_title": assignment.title if assignment else None,
                "student_number": record.student_number,
                "score": record.committed_score,
                "objective_alignment": standards,
                "mastery_impact": [serialize_instructional_evidence(row) for row in evidence_rows],
                "teacher_confirmation_required": not any(
                    row.teacher_confirmed for row in evidence_rows
                ),
                "navigation_href": f"/teacher-assist/gradebook?assignment_id={record.assignment_id}",
            }
        )

    return {
        "records": rows,
        "summary": {
            "record_count": len(rows),
            "pending_mastery_confirmation": sum(
                1 for row in rows if row["teacher_confirmation_required"]
            ),
        },
        "read_only": True,
    }

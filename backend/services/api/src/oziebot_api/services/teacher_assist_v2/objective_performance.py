"""Read model for objective performance derived from confirmed mastery evidence."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_mastery_evidence import TeacherAssistV2MasteryEvidence
from oziebot_api.models.user import User


class ObjectivePerformanceService:
    @staticmethod
    def summarize_objective(
        db: Session,
        *,
        user: User,
        objective_id: uuid.UUID,
        assignment_id: uuid.UUID | None = None,
    ) -> dict[str, Any]:
        objective = db.get(EducationObjective, objective_id)
        if objective is None:
            raise LookupError("Objective not found")

        stmt = select(TeacherAssistV2MasteryEvidence).where(
            TeacherAssistV2MasteryEvidence.teacher_user_id == user.id,
            TeacherAssistV2MasteryEvidence.education_objective_id == objective_id,
            TeacherAssistV2MasteryEvidence.is_current.is_(True),
            TeacherAssistV2MasteryEvidence.teacher_confirmed.is_(True),
        )
        if assignment_id is not None:
            stmt = stmt.where(TeacherAssistV2MasteryEvidence.assignment_id == assignment_id)

        rows = db.scalars(stmt).all()
        if not rows:
            return {
                "objective_id": str(objective.id),
                "objective_code": objective.objective_id,
                "description": objective.description,
                "students_assessed": 0,
                "average_score": 0.0,
                "average_percentage": 0.0,
                "mastery_count": 0,
                "developing_count": 0,
                "beginning_count": 0,
                "mastery_percentage": 0.0,
                "latest_assessment_at": None,
            }

        student_numbers = {row.student_number for row in rows}
        mastery_count = sum(1 for row in rows if row.mastery_level == "mastery")
        developing_count = sum(1 for row in rows if row.mastery_level == "developing")
        beginning_count = sum(1 for row in rows if row.mastery_level == "beginning")
        average_score = round(sum(row.score for row in rows) / len(rows), 2)
        average_percentage = round(sum(row.percentage for row in rows) / len(rows), 2)
        latest: datetime | None = max(row.created_at for row in rows)

        return {
            "objective_id": str(objective.id),
            "objective_code": objective.objective_id,
            "description": objective.description,
            "students_assessed": len(student_numbers),
            "average_score": average_score,
            "average_percentage": average_percentage,
            "mastery_count": mastery_count,
            "developing_count": developing_count,
            "beginning_count": beginning_count,
            "mastery_percentage": round((mastery_count / len(rows)) * 100, 1) if rows else 0.0,
            "latest_assessment_at": latest.isoformat() if latest else None,
        }

    @staticmethod
    def summarize_assignment_objectives(
        db: Session,
        *,
        user: User,
        assignment_id: uuid.UUID,
    ) -> list[dict[str, Any]]:
        assignment = db.scalars(
            select(TeacherAssistV2Assignment).where(
                TeacherAssistV2Assignment.id == assignment_id,
                TeacherAssistV2Assignment.teacher_user_id == user.id,
            )
        ).first()
        if assignment is None:
            raise LookupError("Assignment not found")

        summaries: list[dict[str, Any]] = []
        for raw in assignment.education_objective_ids_json or []:
            try:
                objective_id = uuid.UUID(str(raw))
            except ValueError:
                continue
            summaries.append(
                ObjectivePerformanceService.summarize_objective(
                    db,
                    user=user,
                    objective_id=objective_id,
                    assignment_id=assignment_id,
                )
            )
        return summaries


def count_objectives_assessed(db: Session, *, user: User) -> int:
    value = db.scalar(
        select(
            func.count(func.distinct(TeacherAssistV2MasteryEvidence.education_objective_id))
        ).where(
            TeacherAssistV2MasteryEvidence.teacher_user_id == user.id,
            TeacherAssistV2MasteryEvidence.is_current.is_(True),
            TeacherAssistV2MasteryEvidence.teacher_confirmed.is_(True),
        )
    )
    return int(value or 0)


def count_mastery_alerts(db: Session, *, user: User) -> int:
    value = db.scalar(
        select(func.count(TeacherAssistV2MasteryEvidence.id)).where(
            TeacherAssistV2MasteryEvidence.teacher_user_id == user.id,
            TeacherAssistV2MasteryEvidence.is_current.is_(True),
            TeacherAssistV2MasteryEvidence.teacher_confirmed.is_(True),
            TeacherAssistV2MasteryEvidence.mastery_level == "beginning",
        )
    )
    return int(value or 0)

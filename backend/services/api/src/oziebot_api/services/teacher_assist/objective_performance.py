"""Objective performance calculations — transparent, teacher-confirmed evidence only."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_instructional_evidence import TeacherAssistInstructionalEvidence
from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.services.teacher_assist.constants import MASTERY_LEVELS, MASTERY_LEVEL_RANK


def _now() -> datetime:
    return datetime.now(UTC)


def _pct(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((count / total) * 100, 1)


def _level_distribution(rows: list[str]) -> dict[str, int]:
    distribution = {level: 0 for level in MASTERY_LEVELS}
    for level in rows:
        distribution[level] = distribution.get(level, 0) + 1
    return distribution


def _trend_direction(recent_levels: list[str], prior_levels: list[str]) -> str:
    if not recent_levels:
        return "stable"
    recent_rank = sum(MASTERY_LEVEL_RANK.get(level, 0) for level in recent_levels) / len(recent_levels)
    if not prior_levels:
        return "stable"
    prior_rank = sum(MASTERY_LEVEL_RANK.get(level, 0) for level in prior_levels) / len(prior_levels)
    delta = recent_rank - prior_rank
    if delta >= 0.35:
        return "improving"
    if delta <= -0.35:
        return "declining"
    return "stable"


class ObjectivePerformanceService:
    @staticmethod
    def calculate_for_objective(
        db: Session,
        *,
        tenant_id: uuid.UUID,
        user_id: uuid.UUID,
        objective_id: uuid.UUID | None = None,
        standard_id: uuid.UUID | None = None,
        class_id: uuid.UUID | None = None,
        instructional_week_id: uuid.UUID | None = None,
    ) -> dict[str, Any]:
        evidence_query = select(TeacherAssistInstructionalEvidence).where(
            TeacherAssistInstructionalEvidence.tenant_id == tenant_id,
            TeacherAssistInstructionalEvidence.owner_user_id == user_id,
        )
        if objective_id is not None:
            evidence_query = evidence_query.where(TeacherAssistInstructionalEvidence.objective_id == objective_id)
        if standard_id is not None:
            evidence_query = evidence_query.where(TeacherAssistInstructionalEvidence.standard_id == standard_id)
        if class_id is not None:
            evidence_query = evidence_query.where(TeacherAssistInstructionalEvidence.class_id == class_id)
        if instructional_week_id is not None:
            evidence_query = evidence_query.where(
                TeacherAssistInstructionalEvidence.instructional_week_id == instructional_week_id
            )
        evidence_rows = list(db.scalars(evidence_query.order_by(TeacherAssistInstructionalEvidence.created_at.desc())).all())

        mastery_query = select(TeacherAssistMasteryEvaluation).where(
            TeacherAssistMasteryEvaluation.tenant_id == tenant_id,
            TeacherAssistMasteryEvaluation.owner_user_id == user_id,
            TeacherAssistMasteryEvaluation.evaluation_status == "active",
        )
        if standard_id is not None:
            mastery_query = mastery_query.where(TeacherAssistMasteryEvaluation.standard_id == standard_id)
        if class_id is not None:
            mastery_query = mastery_query.where(TeacherAssistMasteryEvaluation.mastery_matrix_id.is_not(None))
        mastery_rows = list(db.scalars(mastery_query).all())

        objective = db.get(EducationObjective, objective_id) if objective_id else None
        standard = db.get(TeacherAssistStandard, standard_id) if standard_id else None
        objective_code = getattr(objective, "objective_id", None) if objective else getattr(standard, "code", None)

        students_assessed = {row.student_identifier for row in evidence_rows}
        students_assessed.update(str(row.student_number) for row in mastery_rows)

        confirmed_evidence = [row for row in evidence_rows if row.teacher_confirmed]
        levels = [
            row.mastery_level
            for row in confirmed_evidence + mastery_rows
            if row.mastery_level
        ]
        distribution = _level_distribution(levels)
        total = len(levels) or 1

        scores = [row.score for row in confirmed_evidence if row.score is not None]
        average_score = round(sum(scores) / len(scores), 1) if scores else None

        recent = levels[: max(1, len(levels) // 2)]
        prior = levels[max(1, len(levels) // 2) :]

        last_assessment_date = None
        if evidence_rows:
            last_assessment_date = evidence_rows[0].created_at.isoformat()
        elif mastery_rows:
            last_assessment_date = max(row.updated_at for row in mastery_rows).isoformat()

        return {
            "objective_id": str(objective_id) if objective_id else None,
            "standard_id": str(standard_id) if standard_id else None,
            "objective_code": objective_code,
            "students_assessed": len(students_assessed),
            "average_score": average_score,
            "mastery_pct": _pct(distribution.get("mastery", 0) + distribution.get("advanced", 0), total),
            "developing_pct": _pct(distribution.get("developing", 0), total),
            "beginning_pct": _pct(distribution.get("beginning", 0), total),
            "trend_direction": _trend_direction(recent, prior),
            "last_assessment_date": last_assessment_date,
            "level_distribution": distribution,
            "confirmed_evidence_count": len(confirmed_evidence),
            "read_only": True,
        }

    @staticmethod
    def calculate_for_scope(
        db: Session,
        *,
        tenant_id: uuid.UUID,
        user_id: uuid.UUID,
        class_id: uuid.UUID | None = None,
        subject_id: uuid.UUID | None = None,
        instructional_week_id: uuid.UUID | None = None,
        school_year_id: uuid.UUID | None = None,
        grading_period_id: uuid.UUID | None = None,
    ) -> dict[str, Any]:
        evidence_query = select(TeacherAssistInstructionalEvidence).where(
            TeacherAssistInstructionalEvidence.tenant_id == tenant_id,
            TeacherAssistInstructionalEvidence.owner_user_id == user_id,
        )
        if class_id is not None:
            evidence_query = evidence_query.where(TeacherAssistInstructionalEvidence.class_id == class_id)
        if subject_id is not None:
            evidence_query = evidence_query.where(TeacherAssistInstructionalEvidence.subject_id == subject_id)
        if instructional_week_id is not None:
            evidence_query = evidence_query.where(
                TeacherAssistInstructionalEvidence.instructional_week_id == instructional_week_id
            )
        evidence_rows = list(db.scalars(evidence_query).all())

        objective_ids = {row.objective_id for row in evidence_rows if row.objective_id is not None}
        standard_ids = {row.standard_id for row in evidence_rows if row.standard_id is not None}

        objectives: list[dict[str, Any]] = []
        for objective_id in objective_ids:
            objectives.append(
                ObjectivePerformanceService.calculate_for_objective(
                    db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    objective_id=objective_id,
                    class_id=class_id,
                    instructional_week_id=instructional_week_id,
                )
            )
        for standard_id in standard_ids:
            if any(row.get("standard_id") == str(standard_id) for row in objectives):
                continue
            objectives.append(
                ObjectivePerformanceService.calculate_for_objective(
                    db,
                    tenant_id=tenant_id,
                    user_id=user_id,
                    standard_id=standard_id,
                    class_id=class_id,
                    instructional_week_id=instructional_week_id,
                )
            )

        objectives.sort(key=lambda row: row.get("mastery_pct") or 0)
        return {
            "scope": {
                "class_id": str(class_id) if class_id else None,
                "subject_id": str(subject_id) if subject_id else None,
                "instructional_week_id": str(instructional_week_id) if instructional_week_id else None,
                "school_year_id": str(school_year_id) if school_year_id else None,
                "grading_period_id": str(grading_period_id) if grading_period_id else None,
            },
            "objectives": objectives,
            "strongest_objectives": list(reversed(objectives[-3:])),
            "weakest_objectives": objectives[:3],
            "students_needing_support": _students_needing_support(objectives, evidence_rows),
            "students_near_mastery": _students_near_mastery(evidence_rows),
            "read_only": True,
        }


def _students_needing_support(
    objectives: list[dict[str, Any]],
    evidence_rows: list[TeacherAssistInstructionalEvidence],
) -> list[dict[str, Any]]:
    grouped: dict[str, set[str]] = {}
    for row in evidence_rows:
        if not row.teacher_confirmed:
            continue
        if row.mastery_level not in {"beginning", "developing", "not_assessed"}:
            continue
        key = row.student_identifier
        grouped.setdefault(key, set()).add(row.student_identifier)
    return [{"student_identifier": student, "support_signal_count": 1} for student in grouped][:10]


def _students_near_mastery(evidence_rows: list[TeacherAssistInstructionalEvidence]) -> list[dict[str, Any]]:
    near: dict[str, int] = {}
    for row in evidence_rows:
        if row.teacher_confirmed and row.mastery_level == "developing":
            near[row.student_identifier] = near.get(row.student_identifier, 0) + 1
    return [{"student_identifier": student, "developing_count": count} for student, count in near.items()][:10]

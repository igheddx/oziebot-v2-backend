"""Deterministic class-level grading insight — zero AI calls."""

from __future__ import annotations

import uuid
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_mastery_evidence import TeacherAssistV2MasteryEvidence
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.grade_review_constants import OFFICIAL_ASSIGNMENT_GRADE_STATUSES
from oziebot_api.services.teacher_assist_v2.mastery_constants import (
    MASTERY_THRESHOLD_MASTERY,
    MASTERY_THRESHOLD_DEVELOPING,
    serialize_mastery_level_fields,
)
from oziebot_api.services.teacher_assist_v2.submission_intake import _get_assignment_or_404

_MIN_GRADES_REQUIRED = 3


def _mastery_label(level: str) -> str:
    return {
        "mastery": "Mastery",
        "developing": "Developing",
        "beginning": "Beginning",
        "missing": "Missing",
    }.get(level, level.title())


def _reteach_recommendation(
    *,
    confirmed_count: int,
    mastery_count: int,
    developing_count: int,
    beginning_count: int,
    top_misconception: str | None,
) -> dict[str, str]:
    struggling = developing_count + beginning_count
    if confirmed_count == 0:
        return {"type": "insufficient_data", "explanation": "No confirmed grades yet."}
    ratio = struggling / confirmed_count
    if ratio >= 0.60 and top_misconception:
        reteach_type = "whole_class"
        explanation = (
            f"{struggling} of {confirmed_count} students ({round(ratio * 100)}%) show similar gaps. "
            f"Consider a whole-class reteach targeting: {top_misconception}."
        )
    elif ratio >= 0.20 and top_misconception:
        reteach_type = "small_group"
        explanation = (
            f"{struggling} of {confirmed_count} students ({round(ratio * 100)}%) may benefit from support. "
            f"A small-group session targeting '{top_misconception}' is recommended."
        )
    else:
        reteach_type = "individual_follow_up"
        explanation = (
            f"{struggling} of {confirmed_count} students ({round(ratio * 100)}%) are below mastery. "
            "Individual follow-up or brief conferences are recommended."
        )
    return {"type": reteach_type, "explanation": explanation}


def _teacher_action_prompt(reteach_type: str, struggling_count: int, misconception: str | None) -> str:
    if reteach_type == "whole_class":
        anchor = f" Anchor on: {misconception}." if misconception else ""
        return (
            f"Consider a whole-class reteach session.{anchor} "
            "A brief think-aloud before the next independent practice is a low-prep starting point."
        )
    elif reteach_type == "small_group":
        target = f" Target: {misconception}." if misconception else ""
        return (
            f"Pull {struggling_count} students for a small-group session during independent work.{target}"
        )
    else:
        return (
            f"Schedule brief 1:1 check-ins with {struggling_count} student(s) "
            "who are below mastery. A 5-minute conference per student is effective."
        )


def _objective_breakdown(
    db: Session,
    grades: list[TeacherAssistV2AssignmentGrade],
    assignment: TeacherAssistV2Assignment,
) -> list[dict[str, Any]]:
    """Per-objective mastery summary for this assignment, drawn from MasteryEvidence."""
    objective_ids: list[uuid.UUID] = []
    for raw in (assignment.education_objective_ids_json or []):
        try:
            objective_ids.append(uuid.UUID(str(raw)))
        except (ValueError, TypeError):
            continue
    if not objective_ids:
        return []

    grade_ids = [g.id for g in grades]
    evidence_rows = db.scalars(
        select(TeacherAssistV2MasteryEvidence).where(
            TeacherAssistV2MasteryEvidence.assignment_grade_id.in_(grade_ids),
            TeacherAssistV2MasteryEvidence.education_objective_id.in_(objective_ids),
            TeacherAssistV2MasteryEvidence.teacher_confirmed.is_(True),
        )
    ).all()

    objectives_map = {
        obj.id: obj
        for obj in db.scalars(
            select(EducationObjective).where(EducationObjective.id.in_(objective_ids))
        ).all()
    }

    breakdown: list[dict[str, Any]] = []
    for obj_id in objective_ids:
        obj = objectives_map.get(obj_id)
        rows = [r for r in evidence_rows if r.education_objective_id == obj_id]
        if not rows:
            continue
        student_numbers = {r.student_number for r in rows}
        mastery_count = sum(1 for r in rows if r.mastery_level == "mastery")
        developing_count = sum(1 for r in rows if r.mastery_level == "developing")
        beginning_count = sum(1 for r in rows if r.mastery_level == "beginning")
        avg_pct = round(sum(r.percentage for r in rows) / len(rows), 1)
        mastery_pct = round((mastery_count / len(rows)) * 100, 1)
        breakdown.append({
            "objective_id": str(obj_id),
            "objective_code": obj.objective_id if obj else None,
            "description": obj.description if obj else None,
            "students_assessed": len(student_numbers),
            "mastery_count": mastery_count,
            "developing_count": developing_count,
            "beginning_count": beginning_count,
            "average_percentage": avg_pct,
            "mastery_percentage": mastery_pct,
        })
    return breakdown


def _criterion_averages(grades: list[TeacherAssistV2AssignmentGrade]) -> list[dict[str, Any]]:
    criterion_scores: dict[str, list[float]] = {}
    criterion_maxes: dict[str, float] = {}
    for grade in grades:
        rubric = grade.rubric_json if isinstance(grade.rubric_json, dict) else {}
        sections = rubric.get("sections") or []
        for section in sections:
            if not isinstance(section, dict):
                continue
            name = section.get("name")
            score = section.get("score")
            max_score = section.get("max_score")
            if not name or score is None or not max_score:
                continue
            criterion_scores.setdefault(name, []).append(float(score))
            criterion_maxes[name] = float(max_score)

    averages: list[dict[str, Any]] = []
    for name, scores in criterion_scores.items():
        max_score = criterion_maxes.get(name, 0)
        avg = sum(scores) / len(scores)
        averages.append({
            "criterion": name,
            "average_score": round(avg, 2),
            "max_score": max_score,
            "average_percentage": round((avg / max_score) * 100, 1) if max_score else 0,
            "sample_count": len(scores),
        })
    averages.sort(key=lambda r: r["average_percentage"])
    return averages


def build_assignment_class_insight(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> dict[str, Any]:
    assignment = _get_assignment_or_404(db, user=user, assignment_id=assignment_id)

    confirmed_grades = db.scalars(
        select(TeacherAssistV2AssignmentGrade)
        .where(
            TeacherAssistV2AssignmentGrade.assignment_id == assignment.id,
            TeacherAssistV2AssignmentGrade.teacher_user_id == user.id,
            TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES),
        )
        .order_by(TeacherAssistV2AssignmentGrade.student_number.asc().nulls_last())
    ).all()

    unconfirmed_count = db.scalar(
        select(
            __import__("sqlalchemy", fromlist=["func"]).func.count(
                TeacherAssistV2StudentSubmission.id
            )
        ).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment.id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status.notin_({"CONFIRMED", "ARCHIVED", "INCOMPLETE"}),
        )
    ) or 0

    confirmed_count = len(confirmed_grades)
    if confirmed_count < _MIN_GRADES_REQUIRED:
        return {
            "assignment_id": str(assignment_id),
            "available": False,
            "reason": (
                f"Class insight is available after {_MIN_GRADES_REQUIRED} confirmed grades. "
                f"Currently {confirmed_count} confirmed."
            ),
            "confirmed_grades_count": confirmed_count,
            "unconfirmed_count": int(unconfirmed_count),
            "generated_at": datetime.now(UTC).isoformat(),
        }

    # Mastery distribution
    mastery_counts: Counter[str] = Counter()
    total_percentage = 0.0
    total_score = 0.0
    total_max_score = 0.0
    reteach_students: list[dict[str, Any]] = []
    extension_students: list[dict[str, Any]] = []

    for grade in confirmed_grades:
        fields = serialize_mastery_level_fields(percentage=grade.percentage, mastery_level=grade.mastery_level)
        level = fields["mastery_level"]
        mastery_counts[level] += 1
        total_percentage += grade.percentage or 0
        total_score += grade.score or 0
        total_max_score += grade.max_score or 0
        if level in {"developing", "beginning"}:
            grade_rubric = grade.rubric_json if isinstance(grade.rubric_json, dict) else {}
            reteach_students.append({
                "student_number": grade.student_number,
                "percentage": grade.percentage,
                "mastery_level": level,
                "mastery_level_label": fields["mastery_level_label"],
                "assignment_grade_id": str(grade.id),
                "suspected_misconception": grade_rubric.get("suspected_misconception"),
            })
        if level == "mastery" and (grade.percentage or 0) >= 92:
            extension_students.append({
                "student_number": grade.student_number,
                "percentage": grade.percentage,
                "mastery_level": level,
            })

    mastery_distribution: dict[str, dict[str, Any]] = {}
    for level in ["mastery", "developing", "beginning"]:
        count = mastery_counts.get(level, 0)
        mastery_distribution[level] = {
            "label": _mastery_label(level),
            "count": count,
            "percent": round((count / confirmed_count) * 100, 1),
        }

    # Most common misconception
    misconception_strings: list[str] = []
    for grade in confirmed_grades:
        rubric = grade.rubric_json if isinstance(grade.rubric_json, dict) else {}
        m = rubric.get("suspected_misconception")
        if m and isinstance(m, str) and m.strip():
            misconception_strings.append(m.strip())
    misconception_counter: Counter[str] = Counter(misconception_strings)
    top_misconception: str | None = None
    top_misconception_count: int = 0
    if misconception_counter:
        top_misconception, top_misconception_count = misconception_counter.most_common(1)[0]

    # Criterion averages — strongest and weakest
    criterion_avgs = _criterion_averages(confirmed_grades)
    weakest_criterion = criterion_avgs[0] if criterion_avgs else None
    strongest_criterion = criterion_avgs[-1] if criterion_avgs else None

    # Objective frequency (most and least addressed)
    objective_scores: dict[str, list[float]] = {}
    for grade in confirmed_grades:
        rubric = grade.rubric_json if isinstance(grade.rubric_json, dict) else {}
        for ev in (rubric.get("objective_evidence") or []):
            if not isinstance(ev, dict):
                continue
            obj_id = ev.get("objective_id")
            if obj_id:
                objective_scores.setdefault(str(obj_id), []).append(grade.percentage or 0)

    # Reteach recommendation
    reteach_rec = _reteach_recommendation(
        confirmed_count=confirmed_count,
        mastery_count=mastery_counts.get("mastery", 0),
        developing_count=mastery_counts.get("developing", 0),
        beginning_count=mastery_counts.get("beginning", 0),
        top_misconception=top_misconception,
    )

    struggling_count = mastery_counts.get("developing", 0) + mastery_counts.get("beginning", 0)
    teacher_action_prompt = _teacher_action_prompt(
        reteach_rec["type"],
        struggling_count,
        top_misconception,
    )

    # Objective breakdown (requires MasteryEvidence + EducationObjective join)
    obj_breakdown = _objective_breakdown(db, confirmed_grades, assignment)

    return {
        "assignment_id": str(assignment_id),
        "available": True,
        "confirmed_grades_count": confirmed_count,
        "unconfirmed_count": int(unconfirmed_count),
        "class_average_percentage": round(total_percentage / confirmed_count, 1),
        "class_average_score": round(total_score / confirmed_count, 2),
        "max_score": round(total_max_score / confirmed_count, 2) if confirmed_count else 0,
        "mastery_distribution": mastery_distribution,
        "most_common_misconception": (
            {
                "text": top_misconception,
                "frequency": top_misconception_count,
                "percent_of_class": round((top_misconception_count / confirmed_count) * 100, 1),
            }
            if top_misconception
            else None
        ),
        "criterion_averages": criterion_avgs,
        "strongest_criterion": strongest_criterion,
        "weakest_criterion": weakest_criterion,
        "students_needing_support": sorted(
            reteach_students,
            key=lambda r: (r["percentage"] or 0),
        ),
        "students_ready_for_extension": extension_students,
        "reteach_recommendation": reteach_rec,
        "teacher_action_prompt": teacher_action_prompt,
        "objective_breakdown": obj_breakdown,
        "generated_at": datetime.now(UTC).isoformat(),
    }

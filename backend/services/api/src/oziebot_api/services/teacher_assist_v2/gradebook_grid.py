"""Subject-scoped 9-week gradebook grid (students × TEKS/assignment columns)."""

from __future__ import annotations

import csv
import io
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_gradebook_record import TeacherAssistV2GradebookRecord
from oziebot_api.models.teacher_assist_v2_onboarding import TeacherAssistV2PacingGuideAssignment
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user
from oziebot_api.services.teacher_assist_v2.assignment_print_packets import resolve_student_count
from oziebot_api.services.teacher_assist_v2.mastery_constants import (
    MASTERY_LEVEL_SHORT_LABELS,
    format_mastery_level_label,
    resolve_mastery_level,
    serialize_mastery_level_fields,
)
from oziebot_api.services.teacher_assist_v2.planning_workflow import (
    _assignment_context,
    _guide_for_assignment,
    _require_planning_ready,
    _teacher_assignments,
    _week_periods,
)
from oziebot_api.services.teacher_assist_v2.platform_context import (
    resolve_instructional_catalog_tenant_id,
)
from oziebot_api.services.teacher_assist_v2.student_packet_docx import student_number_label


def _field_errors(**errors: str) -> ValueError:
    return ValueError({key: value for key, value in errors.items() if value})


def _week_periods_sorted(guide) -> list:
    return _week_periods(guide)


def _grading_period_week_ranges(guide) -> list[dict[str, Any]]:
    periods = sorted(guide.periods, key=lambda row: row.sequence_number)
    week_periods = _week_periods_sorted(guide)
    week_number_by_id = {period.id: index for index, period in enumerate(week_periods, start=1)}

    ranges: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for period in periods:
        if period.period_type == "GRADING_PERIOD":
            if current is not None:
                ranges.append(current)
            current = {
                "grading_period_id": period.id,
                "title": period.title,
                "description": period.description,
                "week_numbers": [],
            }
        elif period.period_type == "WEEK" and current is not None:
            week_number = week_number_by_id.get(period.id)
            if week_number is not None:
                current["week_numbers"].append(week_number)

    if current is not None:
        ranges.append(current)

    if not ranges and week_periods:
        ranges.append(
            {
                "grading_period_id": None,
                "title": "Full pacing plan",
                "description": None,
                "week_numbers": list(range(1, len(week_periods) + 1)),
            }
        )
    return ranges


def _guide_for_subject(
    db: Session,
    *,
    user: User,
    subject_id: uuid.UUID,
) -> tuple[TeacherAssistV2PacingGuideAssignment, TeacherAssistPacingGuide]:
    subject_key = str(subject_id)
    assignment = next(
        (row for row in _teacher_assignments(db, user=user) if str(row.subject_id) == subject_key),
        None,
    )
    if assignment is None:
        raise _field_errors(subject_id="Selected subject is not linked to a pacing guide.")

    ctx = teacher_assist_context_for_user(db, user)
    platform_tenant_id = resolve_instructional_catalog_tenant_id(db)
    guide = _guide_for_assignment(
        db,
        tenant_id=ctx.tenant_id,
        platform_tenant_id=platform_tenant_id,
        assignment=assignment,
    )
    return assignment, guide


def build_gradebook_grid_form(db: Session, *, user: User) -> dict[str, Any]:
    _require_planning_ready(db, user=user)
    base = _assignment_context(db, user=user)
    subjects: list[dict[str, Any]] = []
    for row in base["subjects"]:
        subject_id = uuid.UUID(row["subject_id"])
        try:
            _, guide = _guide_for_subject(db, user=user, subject_id=subject_id)
        except ValueError:
            continue
        grading_periods = [
            {
                "grading_period_id": str(item["grading_period_id"])
                if item["grading_period_id"]
                else None,
                "title": item["title"],
                "week_numbers": item["week_numbers"],
            }
            for item in _grading_period_week_ranges(guide)
        ]
        subjects.append(
            {
                "subject_id": row["subject_id"],
                "subject_name": row["subject_name"],
                "pacing_guide_id": str(guide.id),
                "pacing_guide_title": guide.title,
                "grading_periods": grading_periods,
            }
        )
    return {
        "student_count": resolve_student_count(db, teacher_user_id=user.id),
        "subjects": subjects,
    }


def _resolve_week_numbers(
    guide,
    *,
    grading_period_id: uuid.UUID | None,
) -> tuple[str, list[int]]:
    ranges = _grading_period_week_ranges(guide)
    if grading_period_id is None:
        selected = ranges[0] if ranges else {"title": "Full pacing plan", "week_numbers": []}
        return selected["title"], selected["week_numbers"]

    for item in ranges:
        if item["grading_period_id"] == grading_period_id:
            return item["title"], item["week_numbers"]
    raise _field_errors(grading_period_id="Selected grading period was not found for this subject.")


def _primary_objective_id(assignment: TeacherAssistV2Assignment) -> uuid.UUID | None:
    for raw in assignment.education_objective_ids_json or []:
        try:
            return uuid.UUID(str(raw))
        except ValueError:
            continue
    return None


def _assignment_cell(*, record: TeacherAssistV2GradebookRecord | None) -> dict[str, Any]:
    if record is None:
        return {
            "has_grade": False,
            "percentage": None,
            "score": None,
            "max_score": None,
            "mastery_level": "missing",
            "mastery_level_label": "Missing",
            "mastery_level_short": MASTERY_LEVEL_SHORT_LABELS["missing"],
        }
    fields = serialize_mastery_level_fields(
        percentage=record.percentage, mastery_level=record.mastery_level
    )
    level = fields["mastery_level"]
    return {
        "has_grade": True,
        "percentage": record.percentage,
        "score": record.score,
        "max_score": record.max_score,
        "gradebook_record_id": str(record.id),
        "mastery_level": level,
        "mastery_level_label": fields["mastery_level_label"],
        "mastery_level_short": MASTERY_LEVEL_SHORT_LABELS.get(level, level[:1].upper()),
    }


def _teks_summary_cell(*, assignment_cells: list[dict[str, Any]]) -> dict[str, Any]:
    if any(cell["mastery_level"] == "missing" for cell in assignment_cells):
        return {
            "has_grade": False,
            "percentage": None,
            "mastery_level": "missing",
            "mastery_level_label": "Missing",
            "mastery_level_short": MASTERY_LEVEL_SHORT_LABELS["missing"],
        }
    if not assignment_cells:
        return {
            "has_grade": False,
            "percentage": None,
            "mastery_level": "missing",
            "mastery_level_label": "Missing",
            "mastery_level_short": MASTERY_LEVEL_SHORT_LABELS["missing"],
        }
    average = round(sum(cell["percentage"] for cell in assignment_cells) / len(assignment_cells), 1)
    level = resolve_mastery_level(average)
    return {
        "has_grade": True,
        "percentage": average,
        "mastery_level": level,
        "mastery_level_label": format_mastery_level_label(level),
        "mastery_level_short": MASTERY_LEVEL_SHORT_LABELS[level],
    }


def _format_cell_export(cell: dict[str, Any]) -> str:
    if cell.get("mastery_level") == "missing":
        return "Missing"
    percentage = cell.get("percentage")
    short = cell.get("mastery_level_short") or ""
    if percentage is None:
        return short or "Missing"
    if float(percentage).is_integer():
        return f"{int(percentage)}|{short}"
    return f"{percentage}|{short}"


def build_subject_gradebook_grid(
    db: Session,
    *,
    user: User,
    subject_id: uuid.UUID,
    grading_period_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    _require_planning_ready(db, user=user)
    _, guide = _guide_for_subject(db, user=user, subject_id=subject_id)
    period_title, week_numbers = _resolve_week_numbers(guide, grading_period_id=grading_period_id)
    if not week_numbers:
        week_numbers = [index for index, _period in enumerate(_week_periods_sorted(guide), start=1)]

    assignments = db.scalars(
        select(TeacherAssistV2Assignment)
        .where(
            TeacherAssistV2Assignment.teacher_user_id == user.id,
            TeacherAssistV2Assignment.catalog_subject_id == subject_id,
            TeacherAssistV2Assignment.status != "ARCHIVED",
            TeacherAssistV2Assignment.week_number.in_(week_numbers),
        )
        .order_by(
            TeacherAssistV2Assignment.week_number.asc(),
            TeacherAssistV2Assignment.title.asc(),
        )
    ).all()
    assignments = [row for row in assignments if _primary_objective_id(row) is not None]
    assignment_ids = [row.id for row in assignments]

    records_by_key: dict[tuple[uuid.UUID, int], TeacherAssistV2GradebookRecord] = {}
    if assignment_ids:
        records = db.scalars(
            select(TeacherAssistV2GradebookRecord).where(
                TeacherAssistV2GradebookRecord.teacher_user_id == user.id,
                TeacherAssistV2GradebookRecord.assignment_id.in_(assignment_ids),
            )
        ).all()
        for record in records:
            records_by_key[(record.assignment_id, record.student_number)] = record

    objective_ids = {_primary_objective_id(row) for row in assignments}
    objective_ids.discard(None)
    objectives = (
        {
            row.id: row
            for row in db.scalars(
                select(EducationObjective).where(EducationObjective.id.in_(objective_ids))
            ).all()
        }
        if objective_ids
        else {}
    )

    grouped: dict[uuid.UUID, dict[str, Any]] = {}
    for assignment in assignments:
        objective_id = _primary_objective_id(assignment)
        if objective_id is None:
            continue
        objective = objectives.get(objective_id)
        group = grouped.setdefault(
            objective_id,
            {
                "objective_id": str(objective_id),
                "objective_code": objective.objective_id if objective else str(objective_id),
                "objective_description": objective.description if objective else None,
                "assignments": [],
            },
        )
        group["assignments"].append(
            {
                "assignment_id": str(assignment.id),
                "title": assignment.title,
                "week_number": assignment.week_number,
                "assignment_type": assignment.assignment_type,
                "creation_origin": assignment.creation_origin,
                "column_key": f"assignment:{assignment.id}",
            }
        )

    teks_groups = sorted(
        grouped.values(),
        key=lambda row: (row["objective_code"] or "", row["objective_id"]),
    )
    for group in teks_groups:
        group["assignments"].sort(key=lambda row: (row["week_number"], row["title"].lower()))

    columns: list[dict[str, Any]] = []
    for group in teks_groups:
        for assignment in group["assignments"]:
            columns.append(
                {
                    "column_key": assignment["column_key"],
                    "column_kind": "assignment",
                    "objective_id": group["objective_id"],
                    "objective_code": group["objective_code"],
                    "assignment_id": assignment["assignment_id"],
                    "title": assignment["title"],
                    "week_number": assignment["week_number"],
                }
            )
        columns.append(
            {
                "column_key": f"teks:{group['objective_id']}",
                "column_kind": "teks_summary",
                "objective_id": group["objective_id"],
                "objective_code": group["objective_code"],
                "title": "TEKS mastery",
            }
        )

    student_count = resolve_student_count(db, teacher_user_id=user.id)
    rows: list[dict[str, Any]] = []
    for student_number in range(1, student_count + 1):
        cells: dict[str, dict[str, Any]] = {}
        for group in teks_groups:
            assignment_cells: list[dict[str, Any]] = []
            for assignment in group["assignments"]:
                assignment_id = uuid.UUID(assignment["assignment_id"])
                cell = _assignment_cell(
                    record=records_by_key.get((assignment_id, student_number)),
                )
                cells[assignment["column_key"]] = cell
                assignment_cells.append(cell)
            cells[f"teks:{group['objective_id']}"] = _teks_summary_cell(
                assignment_cells=assignment_cells
            )

        rows.append(
            {
                "student_number": student_number,
                "student_label": student_number_label(student_number),
                "cells": cells,
            }
        )

    return {
        "subject_id": str(subject_id),
        "pacing_guide_id": str(guide.id),
        "pacing_guide_title": guide.title,
        "grading_period_id": str(grading_period_id) if grading_period_id else None,
        "grading_period_title": period_title,
        "week_numbers": week_numbers,
        "student_count": student_count,
        "teks_groups": teks_groups,
        "columns": columns,
        "rows": rows,
    }


def export_subject_gradebook_grid_csv(
    db: Session,
    *,
    user: User,
    subject_id: uuid.UUID,
    grading_period_id: uuid.UUID | None = None,
) -> tuple[str, bytes]:
    grid = build_subject_gradebook_grid(
        db,
        user=user,
        subject_id=subject_id,
        grading_period_id=grading_period_id,
    )
    buffer = io.StringIO()
    writer = csv.writer(buffer)

    header_row_1 = ["Student"]
    header_row_2 = [""]
    for group in grid["teks_groups"]:
        for index, assignment in enumerate(group["assignments"]):
            header_row_1.append(group["objective_code"] if index == 0 else "")
            header_row_2.append(assignment["title"])
        header_row_1.append("")
        header_row_2.append(f"{group['objective_code']} mastery")

    writer.writerow(header_row_1)
    writer.writerow(header_row_2)

    for row in grid["rows"]:
        csv_row = [row["student_label"]]
        for group in grid["teks_groups"]:
            for assignment in group["assignments"]:
                csv_row.append(_format_cell_export(row["cells"][assignment["column_key"]]))
            csv_row.append(_format_cell_export(row["cells"][f"teks:{group['objective_id']}"]))
        writer.writerow(csv_row)

    safe_title = grid["grading_period_title"].replace("/", "-").replace(" ", "_")
    filename = f"gradebook_{safe_title}.csv"
    return filename, buffer.getvalue().encode("utf-8")

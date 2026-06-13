"""Pacing guide week day records for daily plans and day-level resources."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_pacing_guide_period_day import TeacherAssistPacingGuidePeriodDay
from oziebot_api.schemas.pacing_guide import CatalogPacingGuideDailyPlanOut


def _now() -> datetime:
    return datetime.now(UTC)


def replace_period_days(
    db: Session,
    *,
    period: TeacherAssistPacingGuidePeriod,
    daily_plans: list[dict[str, Any]],
) -> list[TeacherAssistPacingGuidePeriodDay]:
    db.execute(
        delete(TeacherAssistPacingGuidePeriodDay).where(
            TeacherAssistPacingGuidePeriodDay.period_id == period.id
        )
    )
    db.flush()
    rows: list[TeacherAssistPacingGuidePeriodDay] = []
    now = _now()
    for index, item in enumerate(daily_plans, start=1):
        day_label = str(item.get("day_label") or "").strip()
        daily_topic = str(item.get("daily_topic") or "").strip()
        if not day_label or not daily_topic:
            continue
        row = TeacherAssistPacingGuidePeriodDay(
            period_id=period.id,
            day_label=day_label,
            sequence_number=int(item.get("sequence_number") or index),
            daily_topic=daily_topic,
            objective_focus=(str(item.get("objective_focus")).strip() if item.get("objective_focus") else None),
            teacher_notes=(str(item.get("teacher_notes")).strip() if item.get("teacher_notes") else None),
            materials_needed=(str(item.get("materials_needed")).strip() if item.get("materials_needed") else None),
            assessment_check=(str(item.get("assessment_check")).strip() if item.get("assessment_check") else None),
            created_at=now,
            updated_at=now,
        )
        db.add(row)
        rows.append(row)
    db.flush()
    return rows


def daily_plans_metadata_snapshot(days: list[TeacherAssistPacingGuidePeriodDay]) -> list[dict[str, Any]]:
    return [
        {
            "id": str(day.id),
            "day_label": day.day_label,
            "sequence_number": day.sequence_number,
            "daily_topic": day.daily_topic,
            "objective_focus": day.objective_focus,
            "teacher_notes": day.teacher_notes,
            "materials_needed": day.materials_needed,
            "assessment_check": day.assessment_check,
        }
        for day in sorted(days, key=lambda row: row.sequence_number)
    ]


def load_period_days(db: Session, *, period_id: uuid.UUID) -> list[TeacherAssistPacingGuidePeriodDay]:
    return db.scalars(
        select(TeacherAssistPacingGuidePeriodDay)
        .where(TeacherAssistPacingGuidePeriodDay.period_id == period_id)
        .order_by(TeacherAssistPacingGuidePeriodDay.sequence_number.asc())
    ).all()


def serialize_period_daily_plans(
    db: Session,
    *,
    period: TeacherAssistPacingGuidePeriod,
) -> list[CatalogPacingGuideDailyPlanOut]:
    days = load_period_days(db, period_id=period.id)
    if days:
        return [
            CatalogPacingGuideDailyPlanOut(
                id=day.id,
                day_label=day.day_label,
                sequence_number=day.sequence_number,
                daily_topic=day.daily_topic,
                objective_focus=day.objective_focus,
                teacher_notes=day.teacher_notes,
                materials_needed=day.materials_needed,
                assessment_check=day.assessment_check,
            )
            for day in days
        ]
    metadata = period.metadata_json if isinstance(getattr(period, "metadata_json", None), dict) else {}
    raw_plans = metadata.get("daily_plans") or []
    return [
        CatalogPacingGuideDailyPlanOut(
            id=uuid.UUID(str(item["id"])) if item.get("id") else None,
            day_label=str(item.get("day_label") or ""),
            sequence_number=item.get("sequence_number"),
            daily_topic=item.get("daily_topic"),
            objective_focus=item.get("objective_focus"),
            teacher_notes=item.get("teacher_notes"),
            materials_needed=item.get("materials_needed"),
            assessment_check=item.get("assessment_check"),
        )
        for item in raw_plans
        if isinstance(item, dict) and item.get("day_label")
    ]


def copy_period_days(
    db: Session,
    *,
    source_period: TeacherAssistPacingGuidePeriod,
    target_period: TeacherAssistPacingGuidePeriod,
) -> dict[str, uuid.UUID]:
    day_id_map: dict[str, uuid.UUID] = {}
    now = _now()
    source_days = sorted(getattr(source_period, "days", []) or [], key=lambda row: row.sequence_number)
    if not source_days:
        metadata = (
            source_period.metadata_json if isinstance(getattr(source_period, "metadata_json", None), dict) else {}
        )
        for index, item in enumerate(metadata.get("daily_plans") or [], start=1):
            if not isinstance(item, dict):
                continue
            day_label = str(item.get("day_label") or "").strip()
            daily_topic = str(item.get("daily_topic") or "").strip()
            if not day_label or not daily_topic:
                continue
            source_days.append(
                TeacherAssistPacingGuidePeriodDay(
                    id=uuid.uuid4(),
                    period_id=source_period.id,
                    day_label=day_label,
                    sequence_number=index,
                    daily_topic=daily_topic,
                    objective_focus=item.get("objective_focus"),
                    teacher_notes=item.get("teacher_notes"),
                    materials_needed=item.get("materials_needed"),
                    assessment_check=item.get("assessment_check"),
                    created_at=now,
                    updated_at=now,
                )
            )
    for day in source_days:
        new_day = TeacherAssistPacingGuidePeriodDay(
            period_id=target_period.id,
            day_label=day.day_label,
            sequence_number=day.sequence_number,
            daily_topic=day.daily_topic,
            objective_focus=day.objective_focus,
            teacher_notes=day.teacher_notes,
            materials_needed=day.materials_needed,
            assessment_check=day.assessment_check,
            created_at=now,
            updated_at=now,
        )
        db.add(new_day)
        db.flush()
        day_id_map[str(day.id)] = new_day.id
    return day_id_map

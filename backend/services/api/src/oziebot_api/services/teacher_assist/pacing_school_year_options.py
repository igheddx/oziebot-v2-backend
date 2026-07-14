"""Rolling school year options for district pacing guide creation."""

from __future__ import annotations

from datetime import date
import uuid
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.services.teacher_assist.setup import create_school_year

PacingSchoolYearRole = Literal["current", "next", "above_next"]


def current_school_year_start_year(*, today: date | None = None) -> int:
    """Return the starting calendar year for the active US K-12 school year."""
    reference = today or date.today()
    if reference.month >= 8:
        return reference.year
    return reference.year - 1


def _school_year_title(start_year: int) -> str:
    return f"{start_year}-{start_year + 1}"


def _school_year_dates(start_year: int) -> tuple[date, date]:
    return date(start_year, 8, 1), date(start_year + 1, 5, 31)


def build_pacing_school_year_specs(*, today: date | None = None) -> list[dict[str, Any]]:
    reference = today or date.today()
    current_start = current_school_year_start_year(today=reference)
    next_start = current_start + 1
    above_next_start = next_start + 1

    specs: list[dict[str, Any]] = [
        {
            "role": "current",
            "start_year": current_start,
            "title": _school_year_title(current_start),
            "start_date": _school_year_dates(current_start)[0],
            "end_date": _school_year_dates(current_start)[1],
        },
        {
            "role": "next",
            "start_year": next_start,
            "title": _school_year_title(next_start),
            "start_date": _school_year_dates(next_start)[0],
            "end_date": _school_year_dates(next_start)[1],
        },
    ]

    if reference >= date(next_start, 1, 1):
        specs.append(
            {
                "role": "above_next",
                "start_year": above_next_start,
                "title": _school_year_title(above_next_start),
                "start_date": _school_year_dates(above_next_start)[0],
                "end_date": _school_year_dates(above_next_start)[1],
            }
        )

    default_role: PacingSchoolYearRole = (
        "next" if reference >= date(next_start, 5, 1) else "current"
    )
    for spec in specs:
        spec["is_default"] = spec["role"] == default_role
    return specs


def _ensure_school_year_for_spec(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    spec: dict[str, Any],
) -> TeacherAssistSchoolYear:
    existing = db.scalars(
        select(TeacherAssistSchoolYear).where(
            TeacherAssistSchoolYear.tenant_id == tenant_id,
            TeacherAssistSchoolYear.title == spec["title"],
        )
    ).first()
    if existing is not None:
        return existing

    has_active = db.scalars(
        select(TeacherAssistSchoolYear).where(
            TeacherAssistSchoolYear.tenant_id == tenant_id,
            TeacherAssistSchoolYear.is_active.is_(True),
        )
    ).first()
    return create_school_year(
        db,
        tenant_id=tenant_id,
        title=spec["title"],
        start_date=spec["start_date"],
        end_date=spec["end_date"],
        is_active=spec["role"] == "current" and has_active is None,
    )


def build_pacing_school_year_options(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    today: date | None = None,
) -> dict[str, Any]:
    specs = build_pacing_school_year_specs(today=today)
    options: list[dict[str, Any]] = []
    default_school_year_id: uuid.UUID | None = None

    for spec in specs:
        row = _ensure_school_year_for_spec(db, tenant_id=tenant_id, spec=spec)
        option = {
            "id": str(row.id),
            "title": row.title,
            "role": spec["role"],
            "start_date": row.start_date.isoformat(),
            "end_date": row.end_date.isoformat(),
            "is_default": bool(spec["is_default"]),
            "is_active": row.is_active,
        }
        options.append(option)
        if spec["is_default"]:
            default_school_year_id = row.id

    if default_school_year_id is None and options:
        default_school_year_id = uuid.UUID(options[0]["id"])

    return {
        "options": options,
        "default_school_year_id": str(default_school_year_id) if default_school_year_id else None,
    }

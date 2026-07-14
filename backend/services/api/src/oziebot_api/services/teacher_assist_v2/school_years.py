from __future__ import annotations

import uuid
from datetime import date, datetime, UTC

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationSchoolYear
from oziebot_api.services.teacher_assist.education_catalog import (
    get_district_or_404,
    get_school_or_404,
    get_state_or_404,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _validate_date_window(start_date: date, end_date: date) -> None:
    if start_date > end_date:
        raise ValueError("School year start date must be on or before end date")


def _enforce_single_active_school_year(db: Session, *, active_id: uuid.UUID) -> None:
    for row in db.scalars(select(EducationSchoolYear)).all():
        row.active = row.id == active_id
        row.updated_at = _now()


def list_platform_school_years(
    db: Session,
    *,
    state_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[EducationSchoolYear]:
    stmt = select(EducationSchoolYear).order_by(EducationSchoolYear.start_date.desc())
    if state_id is not None:
        stmt = stmt.where(EducationSchoolYear.state_id == state_id)
    if active_only:
        stmt = stmt.where(EducationSchoolYear.active.is_(True))
    return db.scalars(stmt).all()


def get_platform_school_year_or_404(
    db: Session, *, school_year_id: uuid.UUID
) -> EducationSchoolYear:
    row = db.get(EducationSchoolYear, school_year_id)
    if row is None:
        raise LookupError("School year not found")
    return row


def create_platform_school_year(
    db: Session,
    *,
    state_id: uuid.UUID,
    district_id: uuid.UUID | None,
    school_id: uuid.UUID | None,
    title: str,
    start_date: date,
    end_date: date,
    active: bool,
) -> EducationSchoolYear:
    _validate_date_window(start_date, end_date)
    get_state_or_404(db, state_id)
    if district_id is not None:
        district = get_district_or_404(db, district_id)
        if district.state_id != state_id:
            raise ValueError("District does not belong to the selected state")
    if school_id is not None:
        school = get_school_or_404(db, school_id)
        if district_id is not None and school.district_id != district_id:
            raise ValueError("School does not belong to the selected district")
    now = _now()
    row = EducationSchoolYear(
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
        title=title.strip(),
        start_date=start_date,
        end_date=end_date,
        active=active,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    if active:
        _enforce_single_active_school_year(db, active_id=row.id)
    return row


def update_platform_school_year(
    db: Session,
    *,
    school_year_id: uuid.UUID,
    state_id: uuid.UUID,
    district_id: uuid.UUID | None,
    school_id: uuid.UUID | None,
    title: str,
    start_date: date,
    end_date: date,
    active: bool,
) -> EducationSchoolYear:
    row = get_platform_school_year_or_404(db, school_year_id=school_year_id)
    _validate_date_window(start_date, end_date)
    get_state_or_404(db, state_id)
    if district_id is not None:
        district = get_district_or_404(db, district_id)
        if district.state_id != state_id:
            raise ValueError("District does not belong to the selected state")
    if school_id is not None:
        school = get_school_or_404(db, school_id)
        if district_id is not None and school.district_id != district_id:
            raise ValueError("School does not belong to the selected district")
    row.state_id = state_id
    row.district_id = district_id
    row.school_id = school_id
    row.title = title.strip()
    row.start_date = start_date
    row.end_date = end_date
    row.active = active
    row.updated_at = _now()
    db.flush()
    if active:
        _enforce_single_active_school_year(db, active_id=row.id)
    elif not db.scalars(
        select(EducationSchoolYear).where(EducationSchoolYear.active.is_(True))
    ).first():
        pass
    return row


def archive_platform_school_year(db: Session, *, school_year_id: uuid.UUID) -> EducationSchoolYear:
    row = get_platform_school_year_or_404(db, school_year_id=school_year_id)
    row.active = False
    row.updated_at = _now()
    db.flush()
    return row

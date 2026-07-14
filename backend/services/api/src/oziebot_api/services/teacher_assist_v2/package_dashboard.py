"""Instructional package dashboard, listing, and close-out."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationSubject
from oziebot_api.models.teacher_assist_v2_instructional_package import TeacherAssistV2InstructionalPackage
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.package_lifecycle import (
    can_close_out_package,
    is_upcoming_package,
    package_status_message,
    resolve_default_plan_dates,
    resolve_effective_package_status,
)
from oziebot_api.services.teacher_assist_v2.planning_workflow import (
    _require_planning_ready,
    build_planning_review_context,
    get_instructional_package_detail,
    list_planning_supplemental_materials,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _field_errors(**errors: str) -> ValueError:
    return ValueError({key: value for key, value in errors.items() if value})


def _subject_names_for_package(db: Session, row: TeacherAssistV2InstructionalPackage) -> list[str]:
    subject_ids = [uuid.UUID(str(value)) for value in row.subject_ids_json]
    if not subject_ids:
        return []
    subjects = db.scalars(select(EducationSubject).where(EducationSubject.id.in_(subject_ids))).all()
    by_id = {item.id: item.display_name for item in subjects}
    return [by_id[subject_id] for subject_id in subject_ids if subject_id in by_id]


def serialize_package_summary(
    row: TeacherAssistV2InstructionalPackage,
    *,
    subject_names: list[str] | None = None,
    settings: Settings | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    names = subject_names if subject_names is not None else []
    effective = resolve_effective_package_status(
        stored_status=row.status,
        plan_start_date=row.plan_start_date,
        plan_end_date=row.plan_end_date,
        today=today,
    )
    download_url = None
    if settings is not None and row.artifacts:
        from oziebot_api.services.teacher_assist_v2.package_export import artifact_download_url

        for artifact in sorted(row.artifacts, key=lambda item: item.sequence_number):
            download_url = artifact_download_url(artifact, settings=settings)
            if download_url:
                break

    return {
        "id": str(row.id),
        "title": row.title,
        "subject_names": names,
        "week_start": row.week_start,
        "week_end": row.week_end,
        "plan_start_date": row.plan_start_date.isoformat(),
        "plan_end_date": row.plan_end_date.isoformat(),
        "stored_status": row.status,
        "status": effective,
        "status_message": package_status_message(effective, metadata=row.metadata_json),
        "can_close_out": can_close_out_package(stored_status=row.status, effective_status=effective),
        "download_url": download_url,
        "created_at": row.created_at.isoformat(),
        "closed_at": row.closed_at.isoformat() if row.closed_at else None,
    }


def list_teacher_instructional_packages(
    db: Session,
    *,
    user: User,
    settings: Settings | None = None,
    status: str | None = None,
    school_year_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[dict[str, Any]]:
    is_root_admin = bool(getattr(user, "is_root_admin", False))
    if not is_root_admin:
        _require_planning_ready(db, user=user)
    base_filter = [TeacherAssistV2InstructionalPackage.status != "archived"]
    if not is_root_admin:
        base_filter.append(TeacherAssistV2InstructionalPackage.teacher_user_id == user.id)
    stmt = (
        select(TeacherAssistV2InstructionalPackage)
        .where(*base_filter)
        .options(selectinload(TeacherAssistV2InstructionalPackage.artifacts))
        .order_by(TeacherAssistV2InstructionalPackage.created_at.desc())
    )
    if school_year_id is not None:
        stmt = stmt.where(TeacherAssistV2InstructionalPackage.platform_school_year_id == school_year_id)
    if date_from is not None:
        stmt = stmt.where(TeacherAssistV2InstructionalPackage.plan_end_date >= date_from)
    if date_to is not None:
        stmt = stmt.where(TeacherAssistV2InstructionalPackage.plan_start_date <= date_to)

    rows = db.scalars(stmt).all()
    today = date.today()
    summaries: list[dict[str, Any]] = []
    for row in rows:
        names = _subject_names_for_package(db, row)
        if subject_id is not None and str(subject_id) not in {str(value) for value in row.subject_ids_json}:
            continue
        summary = serialize_package_summary(row, subject_names=names, settings=settings, today=today)
        if status is not None and summary["status"] != status:
            continue
        summaries.append(summary)
    return summaries


def build_package_dashboard(
    db: Session,
    *,
    user: User,
    settings: Settings | None = None,
) -> dict[str, Any]:
    _require_planning_ready(db, user=user)
    rows = db.scalars(
        select(TeacherAssistV2InstructionalPackage)
        .where(
            TeacherAssistV2InstructionalPackage.teacher_user_id == user.id,
            TeacherAssistV2InstructionalPackage.status != "archived",
        )
        .options(selectinload(TeacherAssistV2InstructionalPackage.artifacts))
        .order_by(TeacherAssistV2InstructionalPackage.created_at.desc())
    ).all()

    today = date.today()
    summaries = [
        serialize_package_summary(
            row,
            subject_names=_subject_names_for_package(db, row),
            settings=settings,
            today=today,
        )
        for row in rows
    ]

    current = next((row for row in summaries if row["status"] == "active"), None)
    upcoming = [row for row in summaries if is_upcoming_package(
        effective_status=row["status"],
        plan_start_date=date.fromisoformat(row["plan_start_date"]),
        today=today,
    )]
    ending_soon = [row for row in summaries if row["status"] == "ending_soon"]
    expired = [row for row in summaries if row["status"] == "expired"]
    recently_generated = summaries[:8]

    return {
        "current_package": current,
        "upcoming_packages": upcoming[:6],
        "ending_soon_packages": ending_soon[:6],
        "expired_packages": expired[:6],
        "recently_generated_packages": recently_generated,
    }


def close_out_instructional_package(
    db: Session,
    *,
    user: User,
    package_id: uuid.UUID,
    close_out_notes: str | None = None,
    completed_date: date | None = None,
) -> TeacherAssistV2InstructionalPackage:
    row = db.scalars(
        select(TeacherAssistV2InstructionalPackage).where(
            TeacherAssistV2InstructionalPackage.id == package_id,
            TeacherAssistV2InstructionalPackage.teacher_user_id == user.id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Instructional package not found")

    effective = resolve_effective_package_status(
        stored_status=row.status,
        plan_start_date=row.plan_start_date,
        plan_end_date=row.plan_end_date,
    )
    if not can_close_out_package(stored_status=row.status, effective_status=effective):
        raise ValueError("This package is already closed out.")

    completion_day = completed_date or date.today()
    now = _now()
    row.status = "completed"
    row.closed_at = datetime.combine(completion_day, datetime.min.time(), tzinfo=UTC)
    row.closed_by_user_id = user.id
    row.close_out_notes = close_out_notes.strip() if close_out_notes else None
    row.updated_at = now
    db.flush()
    return row


def get_instructional_package_detail_enriched(
    db: Session,
    *,
    user: User,
    package_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict[str, Any]:
    _is_root_admin = bool(getattr(user, "is_root_admin", False))
    _pkg_filter = [TeacherAssistV2InstructionalPackage.id == package_id]
    if not _is_root_admin:
        _pkg_filter.append(TeacherAssistV2InstructionalPackage.teacher_user_id == user.id)
    row = db.scalars(
        select(TeacherAssistV2InstructionalPackage).where(*_pkg_filter)
    ).one_or_none()
    if row is None:
        raise LookupError("Instructional package not found")

    detail = get_instructional_package_detail(db, user=user, package_id=package_id, settings=settings)
    effective = resolve_effective_package_status(
        stored_status=row.status,
        plan_start_date=row.plan_start_date,
        plan_end_date=row.plan_end_date,
    )

    if _is_root_admin:
        review = {"weeks": []}
        supplemental: list = []
        plan_start = row.plan_start_date
        plan_end = row.plan_end_date
    else:
        review = build_planning_review_context(
            db,
            user=user,
            week_start=row.week_start,
            week_end=row.week_end,
            settings=settings,
        )
        supplemental = list_planning_supplemental_materials(
            db,
            user=user,
            week_start=row.week_start,
            week_end=row.week_end,
            settings=settings,
            package_id=row.id,
        )
        plan_start, plan_end = resolve_default_plan_dates(
            db, user=user, week_start=row.week_start, week_end=row.week_end
        )

    _seen_material_ids: set[str] = set()
    district_materials = []
    for week in review["weeks"]:
        for subject in week["subjects"]:
            for material in subject["district_materials"]:
                mid = str(material.get("id") or "")
                if mid and mid in _seen_material_ids:
                    continue
                if mid:
                    _seen_material_ids.add(mid)
                district_materials.append(material)

    detail.update(
        {
            "title": row.title,
            "stored_status": row.status,
            "status": effective,
            "status_message": package_status_message(effective, metadata=row.metadata_json),
            "plan_start_date": row.plan_start_date.isoformat(),
            "plan_end_date": row.plan_end_date.isoformat(),
            "primary_pacing_guide_id": str(row.primary_pacing_guide_id) if row.primary_pacing_guide_id else None,
            "subject_names": _subject_names_for_package(db, row),
            "can_close_out": can_close_out_package(stored_status=row.status, effective_status=effective),
            "close_out_notes": row.close_out_notes,
            "closed_at": row.closed_at.isoformat() if row.closed_at else None,
            "district_materials": district_materials,
            "teacher_supplemental_materials": supplemental,
            "suggested_plan_start_date": plan_start.isoformat(),
            "suggested_plan_end_date": plan_end.isoformat(),
            "qr_student_packet": _extract_qr_student_packet(detail.get("artifacts") or []),
        }
    )
    return detail


def _extract_qr_student_packet(artifacts: list[dict[str, Any]]) -> dict[str, Any] | None:
    for artifact in artifacts:
        if artifact.get("artifact_type") != "assignment":
            continue
        qr = artifact.get("qr_student_packet")
        if isinstance(qr, dict):
            return qr
    return None

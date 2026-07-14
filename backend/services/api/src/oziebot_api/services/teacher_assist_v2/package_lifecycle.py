from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy.orm import Session

from oziebot_api.models.user import User

STORED_PACKAGE_STATUSES = (
    "draft",
    "processing",
    "generated",
    "active",
    "completed",
    "archived",
    "failed",
)

DISPLAY_PACKAGE_STATUSES = STORED_PACKAGE_STATUSES + ("ending_soon", "expired")

ENDING_SOON_DAYS = 3


def resolve_effective_package_status(
    *,
    stored_status: str,
    plan_start_date: date,
    plan_end_date: date,
    today: date | None = None,
) -> str:
    reference = today or date.today()
    normalized = stored_status if stored_status in STORED_PACKAGE_STATUSES else "generated"

    if normalized in {"completed", "archived", "draft", "processing", "failed"}:
        return normalized

    if reference > plan_end_date:
        return "expired"

    days_until_end = (plan_end_date - reference).days
    if 0 <= days_until_end <= ENDING_SOON_DAYS:
        return "ending_soon"

    if plan_start_date <= reference <= plan_end_date:
        return "active"

    if reference < plan_start_date:
        return "generated"

    return normalized


def package_status_message(effective_status: str, *, metadata: dict | None = None) -> str | None:
    if metadata:
        status_detail = metadata.get("status_detail")
        if isinstance(status_detail, str) and status_detail.strip():
            return status_detail.strip()
    if effective_status == "ending_soon":
        return "This plan is ending soon. Review and close it out when teaching is complete."
    if effective_status == "expired":
        return "This plan has passed its end date. Review it and mark it done when complete."
    if effective_status == "failed":
        return "Package generation was interrupted (likely a server restart). Partially generated artifacts are shown below. You can generate a new package to replace this one."
    return None


def can_close_out_package(*, stored_status: str, effective_status: str) -> bool:
    if stored_status in {"completed", "archived", "processing", "failed"}:
        return False
    return effective_status in {"active", "ending_soon", "expired", "generated"}


def is_upcoming_package(
    *, effective_status: str, plan_start_date: date, today: date | None = None
) -> bool:
    reference = today or date.today()
    return effective_status == "generated" and plan_start_date > reference


def default_plan_dates_for_week_range(
    *,
    week_start: int,
    week_end: int,
    period_start_dates: list[date],
    period_end_dates: list[date],
) -> tuple[date, date]:
    if period_start_dates and period_end_dates:
        return min(period_start_dates), max(period_end_dates)
    anchor = date.today()
    span_weeks = max(1, week_end - week_start + 1)
    return anchor, anchor + timedelta(days=7 * span_weeks - 1)


def build_package_title(*, week_start: int, week_end: int, subject_names: list[str]) -> str:
    week_label = (
        f"Week {week_start}" if week_start == week_end else f"Weeks {week_start}–{week_end}"
    )
    subjects = ", ".join(subject_names[:4])
    if len(subject_names) > 4:
        subjects += ", …"
    return f"{week_label} — {subjects}" if subjects else f"{week_label} Instructional Package"


def resolve_default_plan_dates(
    db: Session,
    *,
    user: User,
    week_start: int,
    week_end: int,
) -> tuple[date, date]:
    from oziebot_api.services.teacher_assist_v2.planning_workflow import (
        build_planning_review_context,
    )

    review = build_planning_review_context(
        db,
        user=user,
        week_start=week_start,
        week_end=week_end,
        settings=None,
    )
    starts: list[date] = []
    ends: list[date] = []
    for week in review["weeks"]:
        if week.get("start_date"):
            starts.append(date.fromisoformat(str(week["start_date"])))
        if week.get("end_date"):
            ends.append(date.fromisoformat(str(week["end_date"])))
    return default_plan_dates_for_week_range(
        week_start=week_start,
        week_end=week_end,
        period_start_dates=starts,
        period_end_dates=ends,
    )

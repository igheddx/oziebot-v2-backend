from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_mastery_commit import TeacherAssistMasteryCommit
from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
from oziebot_api.services.teacher_assist.constants import (
    ASSIGNMENT_EFFECTIVENESS_MIXED_THRESHOLD,
    ASSIGNMENT_EFFECTIVENESS_STATUSES,
    MASTERY_ANALYTICS_RECENT_DAYS,
    MASTERY_LEVEL_RANK,
    MASTERY_LEVELS,
    MASTERY_MASTERED_LEVELS,
    MASTERY_TREND_RANK_DELTA,
    RETEACH_MASTERY_HEALTHY_THRESHOLD,
    RETEACH_MASTERY_MONITOR_THRESHOLD,
    RETEACH_MASTERY_RECOMMENDED_THRESHOLD,
    RETEACH_OPERATIONAL_STATUSES,
    STANDARD_MASTERY_TRENDS,
    STUDENT_MASTERY_TRENDS,
)


def _as_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def level_distribution() -> dict[str, int]:
    return {level: 0 for level in MASTERY_LEVELS}


def compute_standard_percentages(
    active_rows: list[TeacherAssistMasteryEvaluation],
) -> dict[str, float | int]:
    total = len(active_rows)
    if total == 0:
        return {
            "mastery_percentage": 0.0,
            "developing_percentage": 0.0,
            "beginning_percentage": 0.0,
            "not_assessed_percentage": 0.0,
            "total_committed_evaluations": 0,
        }
    mastered = sum(1 for row in active_rows if row.mastery_level in MASTERY_MASTERED_LEVELS)
    developing = sum(1 for row in active_rows if row.mastery_level == "developing")
    beginning = sum(1 for row in active_rows if row.mastery_level == "beginning")
    not_assessed = sum(1 for row in active_rows if row.mastery_level == "not_assessed")
    return {
        "mastery_percentage": round(mastered / total, 4),
        "developing_percentage": round(developing / total, 4),
        "beginning_percentage": round(beginning / total, 4),
        "not_assessed_percentage": round(not_assessed / total, 4),
        "total_committed_evaluations": total,
    }


def operational_status_from_percentages(
    mastery_percentage: float,
    total_committed_evaluations: int,
    *,
    settings: Settings | None = None,
) -> str:
    if total_committed_evaluations == 0:
        return "unassessed"
    healthy_threshold = (
        settings.teacher_assist_reteach_mastery_healthy_threshold
        if settings is not None
        else RETEACH_MASTERY_HEALTHY_THRESHOLD
    )
    monitor_threshold = (
        settings.teacher_assist_reteach_mastery_monitor_threshold
        if settings is not None
        else RETEACH_MASTERY_MONITOR_THRESHOLD
    )
    recommended_threshold = (
        settings.teacher_assist_reteach_mastery_recommended_threshold
        if settings is not None
        else RETEACH_MASTERY_RECOMMENDED_THRESHOLD
    )
    if mastery_percentage >= healthy_threshold:
        return "healthy"
    if mastery_percentage >= monitor_threshold:
        return "monitor"
    if mastery_percentage >= recommended_threshold:
        return "reteach_recommended"
    return "critical_attention"


def assignment_effectiveness_status_from_percentages(
    mastery_percentage: float,
    total_committed_evaluations: int,
    *,
    settings: Settings | None = None,
) -> str:
    if total_committed_evaluations == 0:
        return "insufficient_data"
    healthy_threshold = (
        settings.teacher_assist_assignment_effectiveness_healthy_threshold
        if settings is not None
        else 0.70
    )
    mixed_threshold = (
        settings.teacher_assist_assignment_effectiveness_mixed_threshold
        if settings is not None
        else ASSIGNMENT_EFFECTIVENESS_MIXED_THRESHOLD
    )
    if mastery_percentage >= healthy_threshold:
        return "effective"
    if mastery_percentage >= mixed_threshold:
        return "mixed_results"
    return "reteach_likely"


def average_mastery_rank(active_rows: list[TeacherAssistMasteryEvaluation]) -> float | None:
    if not active_rows:
        return None
    return round(
        sum(MASTERY_LEVEL_RANK.get(row.mastery_level, 0) for row in active_rows) / len(active_rows),
        4,
    )


def recent_cutoff(
    *,
    now: datetime | None = None,
    settings: Settings | None = None,
) -> datetime:
    current = _as_utc_datetime(now) or datetime.now(tz=UTC)
    recent_days = (
        settings.teacher_assist_mastery_analytics_recent_days
        if settings is not None
        else MASTERY_ANALYTICS_RECENT_DAYS
    )
    return current - timedelta(days=recent_days)


def count_recent_evaluations(
    active_rows: list[TeacherAssistMasteryEvaluation],
    *,
    now: datetime | None = None,
    settings: Settings | None = None,
) -> int:
    cutoff = recent_cutoff(now=now, settings=settings)
    return sum(
        1
        for row in active_rows
        if (_as_utc_datetime(row.confirmed_at) or datetime.min.replace(tzinfo=UTC)) >= cutoff
    )


def count_recent_assignment_evaluations(
    active_rows: list[TeacherAssistMasteryEvaluation],
    *,
    now: datetime | None = None,
    settings: Settings | None = None,
) -> int:
    cutoff = recent_cutoff(now=now, settings=settings)
    return sum(
        1
        for row in active_rows
        if row.evidence_source_type == "assignment"
        and (_as_utc_datetime(row.confirmed_at) or datetime.min.replace(tzinfo=UTC)) >= cutoff
    )


def last_assessed_timestamp(active_rows: list[TeacherAssistMasteryEvaluation]) -> datetime | None:
    timestamps = [_as_utc_datetime(row.confirmed_at) for row in active_rows if row.confirmed_at is not None]
    return max(timestamps) if timestamps else None


def trend_from_rank_samples(
    rank_samples: list[tuple[datetime, float]],
    *,
    trend_delta: float = MASTERY_TREND_RANK_DELTA,
) -> str:
    if len(rank_samples) < 2:
        return "insufficient_data"
    ordered = sorted(rank_samples, key=lambda item: item[0])
    midpoint = max(1, len(ordered) // 2)
    earlier = ordered[:midpoint]
    later = ordered[midpoint:]
    if not earlier or not later:
        return "stable"
    earlier_avg = sum(item[1] for item in earlier) / len(earlier)
    later_avg = sum(item[1] for item in later) / len(later)
    delta = later_avg - earlier_avg
    if delta >= trend_delta:
        return "improving"
    if delta <= -trend_delta:
        return "declining"
    return "stable"


def standard_trend_from_commits(
    commits: list[TeacherAssistMasteryCommit],
    *,
    now: datetime | None = None,
    settings: Settings | None = None,
) -> str:
    cutoff = recent_cutoff(now=now, settings=settings)
    rank_samples: list[tuple[datetime, float]] = []
    for commit in commits:
        created_at = _as_utc_datetime(commit.created_at)
        if created_at is None:
            continue
        if commit.commit_status not in {"active", "superseded"}:
            continue
        rank_samples.append((created_at, float(MASTERY_LEVEL_RANK.get(commit.new_mastery_level, 0))))
    if len(rank_samples) < 2:
        return "insufficient_data"
    recent = [item for item in rank_samples if item[0] >= cutoff]
    older = [item for item in rank_samples if item[0] < cutoff]
    if recent and older:
        recent_avg = sum(item[1] for item in recent) / len(recent)
        older_avg = sum(item[1] for item in older) / len(older)
        delta = recent_avg - older_avg
        if delta >= MASTERY_TREND_RANK_DELTA:
            return "improving"
        if delta <= -MASTERY_TREND_RANK_DELTA:
            return "declining"
        return "stable"
    return trend_from_rank_samples(rank_samples)


def student_trend_from_evaluations(
    active_rows: list[TeacherAssistMasteryEvaluation],
    commits_by_evaluation_id: dict[Any, list[TeacherAssistMasteryCommit]],
) -> str:
    rank_samples: list[tuple[datetime, float]] = []
    for row in active_rows:
        for commit in commits_by_evaluation_id.get(row.id, []):
            created_at = _as_utc_datetime(commit.created_at)
            if created_at is None:
                continue
            if commit.commit_status not in {"active", "superseded"}:
                continue
            rank_samples.append((created_at, float(MASTERY_LEVEL_RANK.get(commit.new_mastery_level, 0))))
    if len(rank_samples) < 2:
        if active_rows:
            return "stable"
        return "insufficient_data"
    return trend_from_rank_samples(rank_samples)


def validate_operational_status(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in RETEACH_OPERATIONAL_STATUSES:
        raise ValueError(f"Unsupported reteach operational status '{value}'")
    return normalized


def validate_assignment_effectiveness_status(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ASSIGNMENT_EFFECTIVENESS_STATUSES:
        raise ValueError(f"Unsupported assignment effectiveness status '{value}'")
    return normalized


def validate_student_mastery_trend(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in STUDENT_MASTERY_TRENDS:
        raise ValueError(f"Unsupported student mastery trend '{value}'")
    return normalized


def validate_standard_mastery_trend(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in STANDARD_MASTERY_TRENDS:
        raise ValueError(f"Unsupported standard mastery trend '{value}'")
    return normalized


def commits_by_evaluation_id(
    commits: list[TeacherAssistMasteryCommit],
) -> dict[Any, list[TeacherAssistMasteryCommit]]:
    grouped: dict[Any, list[TeacherAssistMasteryCommit]] = {}
    for commit in commits:
        grouped.setdefault(commit.mastery_evaluation_id, []).append(commit)
    return grouped

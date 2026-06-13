"""Resolve pacing guide day plans and materials for instructional package generation."""

from __future__ import annotations

from typing import Any

from oziebot_api.services.teacher_assist_v2.planning_constants import WEEKDAY_LABELS


def resolve_pacing_day_plan(week_subject: dict[str, Any] | None, day_label: str) -> dict[str, Any] | None:
    pacing_context = (week_subject or {}).get("pacing_context") or {}
    days = pacing_context.get("days") or []
    if not days:
        return None

    normalized = day_label.strip().lower()
    for day in days:
        if str(day.get("day_label", "")).strip().lower() == normalized:
            return day

    if day_label in WEEKDAY_LABELS:
        sequence_number = WEEKDAY_LABELS.index(day_label) + 1
        for day in days:
            if int(day.get("sequence_number") or 0) == sequence_number:
                return day
        day_index = WEEKDAY_LABELS.index(day_label)
        if day_index < len(days):
            return days[day_index]
    return None


def resolve_week_daily_topic(week_subject: dict[str, Any] | None) -> str | None:
    pacing_context = (week_subject or {}).get("pacing_context") or {}
    days = pacing_context.get("days") or []
    for day in days:
        topic = day.get("daily_topic")
        if topic:
            return str(topic)
    week_description = pacing_context.get("week_description")
    if week_description:
        return str(week_description)
    legacy = (week_subject or {}).get("daily_topic")
    return str(legacy) if legacy else None


def resolve_subject_daily_topic(week_subject: dict[str, Any] | None, *, day_label: str | None = None) -> str | None:
    if day_label:
        day_plan = resolve_pacing_day_plan(week_subject, day_label)
        if day_plan and day_plan.get("daily_topic"):
            return str(day_plan["daily_topic"])
    return resolve_week_daily_topic(week_subject)


def _material_row_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("id") or row.get("material_id") or ""),
        str(row.get("title") or row.get("display_name") or row.get("original_filename") or ""),
        str(row.get("material_kind") or row.get("resource_type") or ""),
    )


def _material_label(row: dict[str, Any]) -> str | None:
    title = row.get("title") or row.get("display_name") or row.get("original_filename")
    if title:
        url = row.get("url") or row.get("external_url")
        if url:
            return f"{title} ({url})"
        return str(title)
    if row.get("url"):
        return str(row["url"])
    if row.get("notes"):
        return str(row["notes"])
    return None


def filter_excluded_pacing_materials(
    materials: list[dict[str, Any]] | None,
    excluded_material_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    if not materials:
        return []
    if not excluded_material_ids:
        return list(materials)
    filtered: list[dict[str, Any]] = []
    for row in materials:
        material_id = str(row.get("id") or row.get("material_id") or "")
        if material_id and material_id in excluded_material_ids:
            continue
        filtered.append(row)
    return filtered


def flatten_pacing_materials(
    pacing_context: dict[str, Any] | None,
    *,
    excluded_material_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    if not pacing_context:
        return []

    seen: set[tuple[str, str, str]] = set()
    flattened: list[dict[str, Any]] = []

    def add_row(row: dict[str, Any] | None) -> None:
        if not isinstance(row, dict):
            return
        material_id = str(row.get("id") or row.get("material_id") or "")
        if material_id and excluded_material_ids and material_id in excluded_material_ids:
            return
        key = _material_row_key(row)
        if key in seen:
            return
        seen.add(key)
        flattened.append(row)

    for bucket in (
        "guide_level_materials",
        "week_level_materials",
        "day_level_materials",
        "objective_level_materials",
        "catalog_resources",
    ):
        for row in pacing_context.get(bucket) or []:
            add_row(row)

    for day in pacing_context.get("days") or []:
        for bucket in ("attached_files", "reference_links", "notes"):
            for row in day.get(bucket) or []:
                add_row(row)

    for objective in pacing_context.get("objectives") or []:
        for bucket in ("curriculum_files", "reference_links", "notes", "supporting_documents"):
            for row in objective.get(bucket) or []:
                add_row(row)

    return flattened


def material_labels_from_pacing(
    pacing_context: dict[str, Any] | None,
    *,
    day_plan: dict[str, Any] | None = None,
    include_week_level: bool = True,
    excluded_material_ids: set[str] | None = None,
) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()

    def add_label(value: str | None) -> None:
        if not value:
            return
        normalized = value.strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        labels.append(normalized)

    if day_plan and day_plan.get("materials_needed"):
        add_label(str(day_plan["materials_needed"]))

    if day_plan:
        for bucket in ("attached_files", "reference_links", "notes"):
            for row in filter_excluded_pacing_materials(day_plan.get(bucket) or [], excluded_material_ids):
                add_label(_material_label(row))

    if include_week_level and pacing_context:
        for row in flatten_pacing_materials(pacing_context, excluded_material_ids=excluded_material_ids):
            add_label(_material_label(row))

    return labels


def build_subject_lesson_block_from_pacing(
    *,
    subject_name: str,
    week_subject: dict[str, Any] | None,
    day_label: str,
    fallback_objective_text: str,
    excluded_material_ids: set[str] | None = None,
) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist_v2.deterministic_package_content import _subject_lesson_block

    day_plan = resolve_pacing_day_plan(week_subject, day_label)
    pacing_context = (week_subject or {}).get("pacing_context") or {}

    daily_topic = (
        (day_plan or {}).get("daily_topic")
        or resolve_week_daily_topic(week_subject)
        or f"{subject_name} instructional focus"
    )
    objective_text = (day_plan or {}).get("objective_focus") or fallback_objective_text
    day_focus = (day_plan or {}).get("teacher_notes") or str(daily_topic)
    materials = material_labels_from_pacing(
        pacing_context,
        day_plan=day_plan,
        include_week_level=True,
        excluded_material_ids=excluded_material_ids,
    )

    block = _subject_lesson_block(
        subject_name=subject_name,
        objective_text=objective_text,
        daily_topic=str(daily_topic),
        day_focus=str(day_focus),
        materials=materials or None,
    )
    block["daily_topic"] = str(daily_topic)
    if day_plan and day_plan.get("assessment_check"):
        block["assessment"] = str(day_plan["assessment_check"])
        block["check_for_understanding"] = str(day_plan["assessment_check"])
    if day_plan and day_plan.get("teacher_notes"):
        block["notes"] = str(day_plan["teacher_notes"])
    return block


def resolve_daily_plan_objective_text(
    week_subject: dict[str, Any] | None,
    day_label: str,
    *,
    fallback: str,
) -> str:
    day_plan = resolve_pacing_day_plan(week_subject, day_label)
    if day_plan and day_plan.get("objective_focus"):
        return str(day_plan["objective_focus"])
    return fallback


def resolve_daily_plan_summary(week_subject: dict[str, Any] | None, day_label: str, *, fallback: str) -> str:
    day_plan = resolve_pacing_day_plan(week_subject, day_label)
    if day_plan:
        for key in ("daily_topic", "teacher_notes", "objective_focus"):
            value = day_plan.get(key)
            if value:
                return str(value)
    return fallback

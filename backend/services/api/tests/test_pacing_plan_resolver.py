from __future__ import annotations

from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import (
    build_subject_lesson_block_from_pacing,
    filter_excluded_pacing_materials,
    flatten_pacing_materials,
    material_labels_from_pacing,
    resolve_daily_plan_objective_text,
    resolve_pacing_day_plan,
    resolve_week_daily_topic,
)
from oziebot_api.services.teacher_assist_v2.deterministic_package_content import (
    build_daily_lesson_plan,
)


def test_resolve_pacing_day_plan_by_label() -> None:
    week_subject = {
        "pacing_context": {
            "days": [
                {"day_label": "Monday", "sequence_number": 1, "daily_topic": "Intro to poetry"},
                {"day_label": "Tuesday", "sequence_number": 2, "daily_topic": "Drama structure"},
            ]
        }
    }
    assert resolve_pacing_day_plan(week_subject, "Tuesday")["daily_topic"] == "Drama structure"


def test_flatten_pacing_materials_includes_all_levels() -> None:
    pacing_context = {
        "week_level_materials": [{"id": "1", "title": "Week reader", "material_kind": "file"}],
        "catalog_resources": [
            {"id": "2", "title": "Charlotte's Web", "material_kind": "catalog_resource"}
        ],
        "days": [
            {
                "attached_files": [
                    {"id": "3", "title": "Tuesday worksheet", "material_kind": "file"}
                ],
                "reference_links": [],
                "notes": [],
            }
        ],
        "objectives": [],
    }
    flattened = flatten_pacing_materials(pacing_context)
    titles = {row["title"] for row in flattened}
    assert titles == {"Week reader", "Charlotte's Web", "Tuesday worksheet"}


def test_build_subject_lesson_block_uses_day_plan() -> None:
    week_subject = {
        "pacing_context": {
            "days": [
                {
                    "day_label": "Wednesday",
                    "sequence_number": 3,
                    "daily_topic": "Compare texts",
                    "objective_focus": "Students compare ideas across texts.",
                    "materials_needed": "Two short passages and a Venn diagram",
                    "teacher_notes": "Use partner talk before writing.",
                    "assessment_check": "Exit ticket with evidence sentence.",
                    "attached_files": [],
                    "reference_links": [
                        {"title": "Roblox vs Candyland", "url": "https://example.com"}
                    ],
                    "notes": [],
                }
            ]
        }
    }
    block = build_subject_lesson_block_from_pacing(
        subject_name="ELA",
        week_subject=week_subject,
        day_label="Wednesday",
        fallback_objective_text="Fallback objective",
    )
    assert block["objective"] == "Students compare ideas across texts."
    assert "Compare texts" in block["mini_lesson"]
    assert "Roblox vs Candyland" in " ".join(block["materials"])
    assert block["assessment"] == "Exit ticket with evidence sentence."


def test_resolve_week_daily_topic_prefers_day_rows() -> None:
    week_subject = {
        "daily_topic": "Legacy week blob",
        "pacing_context": {
            "week_description": "Week summary",
            "days": [{"day_label": "Monday", "daily_topic": "Poetry inference"}],
        },
    }
    assert resolve_week_daily_topic(week_subject) == "Poetry inference"


def test_material_labels_include_day_materials_needed() -> None:
    day_plan = {
        "materials_needed": "Anchor chart and mentor text",
        "attached_files": [],
        "reference_links": [],
        "notes": [],
    }
    labels = material_labels_from_pacing({}, day_plan=day_plan, include_week_level=False)
    assert labels == ["Anchor chart and mentor text"]


def test_filter_excluded_pacing_materials() -> None:
    materials = [
        {"id": "keep-me", "title": "Reader"},
        {"id": "drop-me", "title": "Old worksheet"},
    ]
    filtered = filter_excluded_pacing_materials(materials, {"drop-me"})
    assert [row["id"] for row in filtered] == ["keep-me"]


def test_resolve_daily_plan_objective_text_prefers_day_focus() -> None:
    week_subject = {
        "objectives": [{"objective_code": "5.6A", "description": "Weekly TEKS objective"}],
        "pacing_context": {
            "days": [
                {
                    "day_label": "Monday",
                    "daily_topic": "Main idea Monday",
                    "objective_focus": "Students identify the main idea in a short passage.",
                }
            ]
        },
    }
    assert (
        resolve_daily_plan_objective_text(
            week_subject,
            "Monday",
            fallback="Weekly TEKS objective",
        )
        == "Students identify the main idea in a short passage."
    )


def test_build_daily_lesson_plan_includes_daily_topic() -> None:
    content = build_daily_lesson_plan(
        day_label="Monday",
        week_label="Week 1",
        package_title="ELA Week 1",
        subject_blocks=[],
        objective_code="5.6A",
        objective_text="Students identify the main idea in a short passage.",
        summary="Main idea Monday",
        daily_topic="Main idea Monday",
    )
    assert content["daily_topic"] == "Main idea Monday"
    assert content["objective_mapping"]["daily_topic"] == "Main idea Monday"
    assert (
        content["objective_mapping"]["objective_text"]
        == "Students identify the main idea in a short passage."
    )

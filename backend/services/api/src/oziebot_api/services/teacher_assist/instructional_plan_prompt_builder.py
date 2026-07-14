from __future__ import annotations

from typing import Any
from copy import deepcopy

from oziebot_api.services.teacher_assist.prompt_contracts import (
    INSTRUCTIONAL_PLAN_PROMPT_VERSION,
    INSTRUCTIONAL_PLAN_SECTION_REGEN_PROMPT_VERSION,
)


def instructional_plan_output_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "planning_scope",
            "plan_title",
            "module_title",
            "duration",
            "overview",
            "instructional_arc",
            "weekly_segments",
            "standards_progression",
            "vocabulary",
            "materials_needed",
            "differentiation",
            "assessment_checkpoints",
            "resources_used",
            "teacher_notes_used",
            "review_notes",
        ],
        "properties": {
            "planning_scope": {"type": "string"},
            "plan_title": {"type": "string"},
            "module_title": {"type": ["string", "null"]},
            "duration": {"type": "object"},
            "overview": {"type": "string"},
            "instructional_arc": {"type": "array"},
            "weekly_segments": {"type": "array"},
            "standards_progression": {"type": "array"},
            "vocabulary": {"type": "array"},
            "materials_needed": {"type": "array"},
            "differentiation": {"type": "object"},
            "assessment_checkpoints": {"type": "array"},
            "resources_used": {"type": "array"},
            "teacher_notes_used": {"type": ["string", "null"]},
            "review_notes": {"type": "string"},
        },
    }


def instructional_plan_section_output_schema(
    section_key: str, *, section_path: str | None = None
) -> dict[str, Any]:
    top_level_array_sections = {
        "instructional_arc",
        "weekly_segments",
        "vocabulary",
        "materials_needed",
        "assessment_checkpoints",
        "standards_progression",
    }
    if section_key in {"overview", "review_notes"}:
        section_schema: dict[str, Any] = {"type": "string"}
    elif section_key == "differentiation":
        section_schema = {"type": "object"}
    elif section_key == "daily_breakdown":
        section_schema = {"type": "object"}
    elif section_key == "weekly_segments" and section_path:
        section_schema = {"type": "object"}
    elif section_key in top_level_array_sections:
        section_schema = {"type": "array"}
    else:
        section_schema = {"type": ["string", "array", "object"]}
    return {
        "type": "object",
        "required": ["section_content"],
        "properties": {
            "section_content": section_schema,
        },
    }


def teacher_review_checklist() -> list[str]:
    return [
        "Verify standards alignment.",
        "Verify pacing and dates.",
        "Verify classroom appropriateness.",
        "Verify differentiation.",
        "Verify materials and resources.",
        "Edit before use.",
    ]


def build_instructional_plan_prompt(context_preview: dict[str, Any]) -> dict[str, Any]:
    draft = context_preview.get("draft", {})
    duration_summary = context_preview.get("duration_summary", {})
    standards = context_preview.get("standards", [])
    resources = context_preview.get("resources", [])
    readiness = deepcopy(context_preview.get("readiness", {}))
    readiness.setdefault("teacher_review_checklist", teacher_review_checklist())
    return {
        "prompt_version": INSTRUCTIONAL_PLAN_PROMPT_VERSION,
        "task": (
            "Generate a practical teacher-ready instructional plan artifact from saved curriculum context. "
            "Return structured JSON only and avoid generic educational filler."
        ),
        "planning_scope": draft.get("planning_scope", "weekly"),
        "plan_title": draft.get("plan_title") or draft.get("title"),
        "module_title": draft.get("module_title"),
        "duration": {
            "start_date": draft.get("start_date"),
            "end_date": draft.get("end_date"),
            "estimated_weeks": draft.get("estimated_weeks")
            or duration_summary.get("estimated_weeks"),
            "instructional_days_count": draft.get("instructional_days_count")
            or duration_summary.get("instructional_days_count"),
            "summary": duration_summary.get("summary"),
        },
        "pacing_groups": context_preview.get("pacing_groups", []),
        "subjects": context_preview.get("subjects", []),
        "standards": standards,
        "resources": resources,
        "teacher_notes": context_preview.get("teacher_notes"),
        "readiness": readiness,
        "output_contract": {
            "format": "JSON object only",
            "schema": instructional_plan_output_schema(),
            "quality_review_metadata_note": (
                "The platform will add review-required metadata and teacher checklist fields after validation. "
                "Still provide review_notes and practical content ready for teacher review."
            ),
        },
        "instructions": [
            "Teacher review is required before classroom use.",
            "Do not include personally identifying student information.",
            "Use anonymous STUDENT # references where student examples are needed.",
            "Do not commit grades or grading judgments.",
            "Do not generate parent communication or newsletters.",
            "Anchor the plan to the supplied pacing items, standards/TEKS, resources, and teacher notes.",
            "Prefer concrete teacher actions, realistic classroom sequencing, and concise instructional language.",
            "If standards are supplied, include a meaningful standards_progression.",
            "If planning scope is multi-week, module, unit, or grading_period, include multiple weekly segments.",
        ],
        "quality_focus": {
            "teacher_ready": True,
            "practical_over_generic": True,
            "respect_missing_context": True,
            "standards_count": len(standards),
            "resource_count": len(resources),
        },
    }


def build_instructional_plan_section_regeneration_prompt(
    *,
    context_preview: dict[str, Any],
    current_plan_content: dict[str, Any],
    section_key: str,
    section_path: str | None,
    current_section_content: Any,
    teacher_instruction: str | None,
    preserve_existing_context: bool,
) -> dict[str, Any]:
    draft = context_preview.get("draft", {})
    duration_summary = context_preview.get("duration_summary", {})
    standards = context_preview.get("standards", [])
    resources = context_preview.get("resources", [])
    return {
        "prompt_version": INSTRUCTIONAL_PLAN_SECTION_REGEN_PROMPT_VERSION,
        "task": (
            "Regenerate only the requested instructional-plan section. "
            "Return JSON only in the required wrapper and keep the output teacher-ready."
        ),
        "section_key": section_key,
        "section_path": section_path,
        "teacher_instruction": teacher_instruction,
        "preserve_existing_context": preserve_existing_context,
        "planning_scope": current_plan_content.get("planning_scope")
        or draft.get("planning_scope", "weekly"),
        "plan_title": current_plan_content.get("plan_title")
        or draft.get("plan_title")
        or draft.get("title"),
        "module_title": current_plan_content.get("module_title") or draft.get("module_title"),
        "duration": {
            "start_date": current_plan_content.get("duration", {}).get("start_date")
            or draft.get("start_date"),
            "end_date": current_plan_content.get("duration", {}).get("end_date")
            or draft.get("end_date"),
            "estimated_weeks": current_plan_content.get("duration", {}).get("estimated_weeks")
            or draft.get("estimated_weeks")
            or duration_summary.get("estimated_weeks"),
            "instructional_days_count": current_plan_content.get("duration", {}).get(
                "instructional_days_count"
            )
            or draft.get("instructional_days_count")
            or duration_summary.get("instructional_days_count"),
            "summary": current_plan_content.get("duration", {}).get("summary")
            or duration_summary.get("summary"),
        },
        "standards": standards,
        "resources": resources,
        "teacher_notes": context_preview.get("teacher_notes"),
        "current_plan_excerpt": {
            "overview": current_plan_content.get("overview"),
            "instructional_arc": current_plan_content.get("instructional_arc"),
            "weekly_segments": current_plan_content.get("weekly_segments"),
            "vocabulary": current_plan_content.get("vocabulary"),
            "materials_needed": current_plan_content.get("materials_needed"),
            "differentiation": current_plan_content.get("differentiation"),
            "assessment_checkpoints": current_plan_content.get("assessment_checkpoints"),
            "standards_progression": current_plan_content.get("standards_progression"),
            "review_notes": current_plan_content.get("review_notes"),
        },
        "current_section_content": current_section_content if preserve_existing_context else None,
        "output_contract": {
            "format": "JSON object only",
            "schema": instructional_plan_section_output_schema(
                section_key, section_path=section_path
            ),
            "required_wrapper": {"section_content": "..."},
        },
        "instructions": [
            "Regenerate only the requested section_content and do not rewrite unrelated sections.",
            "Teacher review is required before classroom use.",
            "Do not include personally identifying student information.",
            "Use anonymous STUDENT # references where student examples are needed.",
            "Do not commit grades or grading judgments.",
            "Do not generate parent communication or newsletters.",
            "Prefer practical teacher-facing classroom language over generic filler.",
        ],
    }

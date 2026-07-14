from __future__ import annotations

import re
from typing import Any

from oziebot_api.services.teacher_assist.constants import PLANNING_SCOPES

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?){2}\d{4}\b")
SUSPICIOUS_PII_KEYS = {
    "student_name",
    "student_names",
    "student_id",
    "student_ids",
    "student_identifier",
    "student_email",
    "student_phone",
    "student_address",
    "real_student_id",
    "real_student_ids",
    "parent_email",
    "parent_phone",
}
MULTI_SEGMENT_SCOPES = {"multi_week", "module", "unit", "grading_period"}


def _contains_pii(value: Any) -> bool:
    if isinstance(value, str):
        return bool(EMAIL_RE.search(value) or PHONE_RE.search(value))
    if isinstance(value, list):
        return any(_contains_pii(item) for item in value)
    if isinstance(value, dict):
        for key, item in value.items():
            normalized_key = str(key).strip().lower()
            if normalized_key in SUSPICIOUS_PII_KEYS:
                return True
            if _contains_pii(item):
                return True
    return False


def contains_pii_like_content(value: Any) -> bool:
    return _contains_pii(value)


def _require_non_empty_list(content_json: dict[str, Any], field_name: str) -> list[Any]:
    value = content_json.get(field_name)
    if not isinstance(value, list) or not value:
        raise ValueError(f"Instructional plan output field '{field_name}' must be a non-empty list")
    return value


def validate_instructional_plan_output(
    content_json: dict[str, Any],
    *,
    context_preview: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(content_json, dict):
        raise ValueError("Instructional plan output must be a JSON object")

    required_top_level = (
        "planning_scope",
        "plan_title",
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
    )
    missing = [key for key in required_top_level if key not in content_json]
    if missing:
        raise ValueError(
            "Instructional plan output is missing required fields: " + ", ".join(missing)
        )

    planning_scope = str(content_json.get("planning_scope") or "").strip()
    if planning_scope not in PLANNING_SCOPES:
        raise ValueError("Instructional plan output has unsupported planning_scope")

    if not isinstance(content_json.get("duration"), dict):
        raise ValueError("Instructional plan output duration must be an object")
    if not isinstance(content_json.get("differentiation"), dict):
        raise ValueError("Instructional plan output differentiation must be an object")
    if not isinstance(content_json.get("resources_used"), list):
        raise ValueError("Instructional plan output resources_used must be a list")
    if not isinstance(content_json.get("assessment_checkpoints"), list):
        raise ValueError("Instructional plan output assessment_checkpoints must be a list")
    if not isinstance(content_json.get("standards_progression"), list):
        raise ValueError("Instructional plan output standards_progression must be a list")

    instructional_arc = _require_non_empty_list(content_json, "instructional_arc")
    if not any(str(item).strip() for item in instructional_arc):
        raise ValueError("Instructional plan output instructional_arc cannot be empty")

    weekly_segments = _require_non_empty_list(content_json, "weekly_segments")
    if planning_scope in MULTI_SEGMENT_SCOPES and len(weekly_segments) < 2:
        raise ValueError(
            "Instructional plan output must include multiple weekly segments for non-weekly scope"
        )

    if context_preview:
        provided_standards = context_preview.get("standards", [])
        if provided_standards and not content_json.get("standards_progression"):
            raise ValueError(
                "Instructional plan output must include standards_progression when standards are provided"
            )

    if contains_pii_like_content(content_json):
        raise ValueError("Instructional plan output contains disallowed PII-like content")

    return content_json


def validate_instructional_plan_section_output(
    payload: dict[str, Any],
    *,
    section_key: str,
    section_path: str | None = None,
) -> Any:
    if not isinstance(payload, dict):
        raise ValueError("Section regeneration output must be a JSON object")
    if "section_content" not in payload:
        raise ValueError("Section regeneration output is missing required field 'section_content'")

    section_content = payload["section_content"]
    if contains_pii_like_content(section_content):
        raise ValueError("Section regeneration output contains disallowed PII-like content")

    if section_key in {"overview", "review_notes"}:
        if not isinstance(section_content, str) or not section_content.strip():
            raise ValueError(f"Regenerated section '{section_key}' must be a non-empty string")
        return section_content.strip()

    if section_key == "differentiation":
        if not isinstance(section_content, dict) or not section_content:
            raise ValueError("Regenerated section 'differentiation' must be a non-empty object")
        return section_content

    if section_key == "daily_breakdown":
        if not isinstance(section_content, dict) or not section_content:
            raise ValueError("Regenerated section 'daily_breakdown' must be a non-empty object")
        return section_content

    if section_key == "weekly_segments" and section_path is not None:
        if not isinstance(section_content, dict) or not section_content:
            raise ValueError(
                "A targeted weekly segment regeneration must return one non-empty object"
            )
        return section_content

    if section_key in {
        "instructional_arc",
        "weekly_segments",
        "vocabulary",
        "materials_needed",
        "assessment_checkpoints",
        "standards_progression",
    }:
        if not isinstance(section_content, list) or not section_content:
            raise ValueError(f"Regenerated section '{section_key}' must be a non-empty list")
        return section_content

    raise ValueError(f"Unsupported section regeneration key '{section_key}'")

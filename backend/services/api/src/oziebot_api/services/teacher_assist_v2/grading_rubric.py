"""Map package rubrics to grading drafts, score sync, and scorecard exports."""

from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import TeacherAssistV2InstructionalPackageArtifact
from oziebot_api.services.teacher_assist_v2.grading_constants import DEFAULT_RUBRIC_SECTIONS, GRADING_DRAFT_MAX_SCORE

RUBRIC_LINKED_ASSESSMENT_TYPES = frozenset({"writing_response", "assignment"})


def _default_grading_sections(*, section_max: float | None = None) -> list[dict[str, Any]]:
    max_value = section_max if section_max is not None else GRADING_DRAFT_MAX_SCORE / len(DEFAULT_RUBRIC_SECTIONS)
    return [
        {
            "name": name,
            "score": 0.0,
            "max_score": max_value,
            "feedback": "",
        }
        for name in DEFAULT_RUBRIC_SECTIONS
    ]


def grading_template_from_package_rubric(rubric_content: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(rubric_content, dict):
        return {
            "title": "Assignment Rubric",
            "total_points": GRADING_DRAFT_MAX_SCORE,
            "criteria": [],
            "sections": _default_grading_sections(),
        }

    criteria = rubric_content.get("criteria") or []
    if not criteria:
        return {
            "title": rubric_content.get("title") or "Assignment Rubric",
            "total_points": float(rubric_content.get("total_points") or GRADING_DRAFT_MAX_SCORE),
            "criteria": [],
            "sections": _default_grading_sections(),
        }

    sections = [
        {
            "name": str(row.get("name") or "Criterion"),
            "score": 0.0,
            "max_score": float(row.get("points") or 0),
            "feedback": "",
        }
        for row in criteria
        if isinstance(row, dict)
    ]
    total_points = float(rubric_content.get("total_points") or sum(row["max_score"] for row in sections))
    return {
        "title": rubric_content.get("title") or "Assignment Rubric",
        "total_points": total_points,
        "criteria": criteria,
        "sections": sections,
    }


def totals_from_grading_rubric(rubric_json: dict[str, Any] | None) -> tuple[float, float]:
    sections = rubric_json.get("sections") if isinstance(rubric_json, dict) else None
    if not isinstance(sections, list) or not sections:
        return 0.0, 0.0
    score = round(sum(float(row.get("score") or 0) for row in sections if isinstance(row, dict)), 2)
    max_score = round(sum(float(row.get("max_score") or 0) for row in sections if isinstance(row, dict)), 2)
    return score, max_score


def mock_sections_from_template(
    template: dict[str, Any],
    *,
    score_ratio: float,
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    for index, row in enumerate(template.get("sections") or []):
        max_score = float(row.get("max_score") or 0)
        section_score = round(max_score * score_ratio * (0.9 + (index * 0.02)), 1)
        sections.append(
            {
                "name": row.get("name"),
                "score": min(max_score, section_score),
                "max_score": max_score,
                "feedback": f"Draft feedback for {str(row.get('name') or 'criterion').lower()}.",
            }
        )
    return sections


def resolve_final_grade_values(
    *,
    draft_score: float,
    draft_max_score: float,
    rubric_json: dict[str, Any],
    score: float | None = None,
    max_score: float | None = None,
) -> tuple[float, float, dict[str, Any]]:
    normalized_rubric = dict(rubric_json or {})
    rubric_score, rubric_max = totals_from_grading_rubric(normalized_rubric)
    if rubric_max > 0:
        final_score = rubric_score if score is None else float(score)
        final_max = rubric_max if max_score is None else float(max_score)
    else:
        final_score = float(draft_score if score is None else score)
        final_max = float(draft_max_score if max_score is None else max_score)
    return final_score, final_max, normalized_rubric


def resolve_linked_rubric_content(
    artifacts: list[TeacherAssistV2InstructionalPackageArtifact],
    *,
    assignment: TeacherAssistV2Assignment,
) -> dict[str, Any] | None:
    assessment_artifact = next(
        (
            row
            for row in artifacts
            if row.assignment_id == assignment.id and row.artifact_type in RUBRIC_LINKED_ASSESSMENT_TYPES
        ),
        None,
    )
    if assessment_artifact is not None:
        assessment_content = (
            assessment_artifact.content_json if isinstance(assessment_artifact.content_json, dict) else {}
        )
        assessment_metadata = (
            assessment_artifact.metadata_json if isinstance(assessment_artifact.metadata_json, dict) else {}
        )
        linked_id = assessment_content.get("linked_rubric_artifact_id") or assessment_metadata.get(
            "linked_rubric_artifact_id"
        )
        if linked_id:
            rubric_artifact = next((row for row in artifacts if str(row.id) == str(linked_id)), None)
            if rubric_artifact is not None and isinstance(rubric_artifact.content_json, dict):
                return rubric_artifact.content_json

    for artifact in artifacts:
        if artifact.artifact_type != "rubric" or artifact.subject_id != assignment.catalog_subject_id:
            continue
        metadata = artifact.metadata_json if isinstance(artifact.metadata_json, dict) else {}
        linked_assessment_id = metadata.get("linked_assessment_artifact_id") or metadata.get(
            "linked_writing_response_artifact_id"
        )
        if not linked_assessment_id:
            continue
        linked_assessment = next((row for row in artifacts if str(row.id) == str(linked_assessment_id)), None)
        if linked_assessment is not None and linked_assessment.assignment_id == assignment.id:
            content = artifact.content_json if isinstance(artifact.content_json, dict) else None
            if content:
                return content
    return None


def resolve_assignment_rubric_content(
    db: Session,
    *,
    assignment: TeacherAssistV2Assignment,
) -> dict[str, Any] | None:
    if assignment.instructional_package_id is None:
        return None
    artifacts = db.scalars(
        select(TeacherAssistV2InstructionalPackageArtifact).where(
            TeacherAssistV2InstructionalPackageArtifact.package_id == assignment.instructional_package_id
        )
    ).all()
    return resolve_linked_rubric_content(artifacts, assignment=assignment)


def assignment_supports_rubric_scorecards(*, assignment_type: str) -> bool:
    return assignment_type in {"WRITING", "WRITTEN_ASSIGNMENT"}


def linked_rubric_artifact_id_from_content(content: dict[str, Any] | None) -> str | None:
    if not isinstance(content, dict):
        return None
    linked = content.get("linked_rubric_artifact_id")
    return str(linked) if linked else None

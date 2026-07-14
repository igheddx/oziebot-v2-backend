from __future__ import annotations

import io
import zipfile
from types import SimpleNamespace
from unittest.mock import Mock

from oziebot_api.services.teacher_assist_v2.deterministic_package_content import (
    build_rubric_for_written_assignment,
    build_rubric_for_writing_response,
)
from oziebot_api.services.teacher_assist_v2.grading_rubric import (
    grading_template_from_package_rubric,
    mock_sections_from_template,
    resolve_final_grade_values,
    totals_from_grading_rubric,
)
from oziebot_api.services.teacher_assist_v2.rubric_score_exports import (
    render_class_rubric_score_report_docx,
)
from oziebot_api.services.teacher_assist_v2.submission_workflow import (
    pending_submission_student_numbers,
)


def test_build_rubric_for_written_assignment_uses_success_criteria() -> None:
    assignment_content = {
        "title": "Week 1 — ELA Written Assignment",
        "passage_title": "The River",
        "success_criteria": ["Clear target", "Two details", "Explanation", "Organization"],
    }
    rubric = build_rubric_for_written_assignment(
        assignment_content=assignment_content,
        subject_name="ELA",
        package_title="Week 1 Instructional Package",
        objective_code="5.6A",
        objective_text="Students analyze text evidence.",
    )
    assert rubric["total_points"] == sum(row["points"] for row in rubric["criteria"])
    assert rubric["criteria"][0]["levels"][0] == "Clear target"


def test_grading_template_and_totals_from_package_rubric() -> None:
    rubric = build_rubric_for_writing_response(
        writing_content={
            "title": "Writing",
            "prompt": "Explain the main idea.",
            "instructions": [],
        },
        subject_name="ELA",
        package_title="Week 1",
        objective_code="5.6A",
        objective_text="Main idea",
    )
    template = grading_template_from_package_rubric(rubric)
    assert len(template["sections"]) == len(rubric["criteria"])
    assert template["total_points"] == rubric["total_points"]

    sections = mock_sections_from_template(template, score_ratio=0.8)
    score, max_score = totals_from_grading_rubric({"sections": sections})
    assert max_score == rubric["total_points"]
    assert 0 < score <= max_score


def test_resolve_final_grade_values_prefers_rubric_totals() -> None:
    rubric_json = {
        "sections": [
            {"name": "A", "score": 4, "max_score": 5, "feedback": "Good"},
            {"name": "B", "score": 3, "max_score": 5, "feedback": "OK"},
        ]
    }
    score, max_score, normalized = resolve_final_grade_values(
        draft_score=50,
        draft_max_score=100,
        rubric_json=rubric_json,
        score=None,
        max_score=None,
    )
    assert score == 7
    assert max_score == 10
    assert normalized == rubric_json


def test_render_class_rubric_score_report_docx_has_student_pages() -> None:
    payload = render_class_rubric_score_report_docx(
        assignment=SimpleNamespace(title="Week 1 Writing Response"),
        rubric_content={"title": "Constructed Response Rubric"},
        rows=[
            {
                "student_number": 3,
                "score": 8,
                "max_score": 10,
                "percentage": 80,
                "mastery_level_label": "Developing",
                "teacher_comment": "Strong evidence.",
                "sections": [
                    {"name": "Claim", "score": 4, "max_score": 5, "feedback": "Clear claim"},
                    {
                        "name": "Evidence",
                        "score": 4,
                        "max_score": 5,
                        "feedback": "Add one more quote",
                    },
                ],
            },
            {
                "student_number": 4,
                "score": 10,
                "max_score": 10,
                "percentage": 100,
                "mastery_level_label": "Mastery",
                "teacher_comment": "Excellent.",
                "sections": [
                    {"name": "Claim", "score": 5, "max_score": 5, "feedback": "Excellent"},
                ],
            },
        ],
    )

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        document_xml = archive.read("word/document.xml").decode("utf-8")

    assert "Student #: 3" in document_xml
    assert "Student #: 4" in document_xml
    assert "Constructed Response Rubric" in document_xml
    assert "Claim: 4 / 5" in document_xml
    assert "<w:br w:type='page'/>" in document_xml


def test_pending_submission_student_numbers_ignores_stale_status_with_official_grade() -> None:
    db = Mock()
    scalars_result = Mock()
    scalars_result.all.return_value = [
        SimpleNamespace(id="submission-1", student_number=1, status="READY_FOR_REVIEW"),
        SimpleNamespace(id="submission-2", student_number=2, status="READY_FOR_REVIEW"),
    ]
    db.scalars.return_value = scalars_result
    db.scalar.side_effect = ["grade-1", None]

    pending = pending_submission_student_numbers(
        db,
        user=SimpleNamespace(id="teacher-1"),
        assignment=SimpleNamespace(id="assignment-1"),
    )

    assert pending == [2]

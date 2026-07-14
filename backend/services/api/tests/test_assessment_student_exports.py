from __future__ import annotations

import io
import zipfile

from oziebot_api.services.teacher_assist_v2.package_export import build_answer_key_json_payload
from oziebot_api.services.teacher_assist_v2.quiz_exports import build_google_forms_readonly_json
from oziebot_api.services.teacher_assist_v2.student_packet_content import (
    compute_pages_per_student,
    split_assessment_content_pages,
)
from oziebot_api.services.teacher_assist_v2.student_packet_docx import (
    build_student_packet_docx_bytes,
    render_qr_png_bytes,
    student_number_label,
)


def test_split_quiz_pages_and_student_count() -> None:
    content = {
        "title": "Week 1 Quiz",
        "description": "Instructions here",
        "questions": [
            {
                "number": index,
                "type": "multiple_choice",
                "prompt": f"Q{index}",
                "choices": ["A"],
                "points": 1,
            }
            for index in range(1, 8)
        ],
        "questions_per_page": 3,
    }
    pages = split_assessment_content_pages(artifact_type="quiz", content=content)
    assert len(pages) == 4
    assert compute_pages_per_student(artifact_type="quiz", content=content) == 4


def test_build_student_packet_docx_contains_page_breaks_and_images() -> None:
    payload = {"student_number": 1, "page_number": 1, "assignment_id": "abc"}
    docx = build_student_packet_docx_bytes(
        title="Sample Quiz",
        pages=[
            {
                "qr_png": render_qr_png_bytes(payload),
                "student_label": student_number_label(1),
                "paragraphs": [("Question 1", True), ("Prompt", False)],
                "page_break_after": True,
            },
            {
                "qr_png": render_qr_png_bytes({**payload, "page_number": 2}),
                "student_label": student_number_label(1),
                "paragraphs": [("Question 2", True)],
                "page_break_after": False,
            },
        ],
    )
    with zipfile.ZipFile(io.BytesIO(docx)) as archive:
        names = archive.namelist()
        assert "word/document.xml" in names
        assert "word/media/image1.png" in names
        assert "word/media/image2.png" in names
        document = archive.read("word/document.xml").decode("utf-8")
        assert "w:br w:type='page'" in document
        assert "Student #001" in document


def test_google_forms_json_uses_student_dropdown() -> None:
    payload = build_google_forms_readonly_json(
        {"title": "Quiz", "student_number_field": True, "questions": []},
        export_context={"student_count": 3},
    )
    question = payload["studentNumberQuestion"]
    assert question is not None
    assert question["type"] == "multiple_choice"
    assert question["choices"] == ["Student #001", "Student #002", "Student #003"]


def test_google_forms_json_accepts_list_objective_mapping() -> None:
    payload = build_google_forms_readonly_json(
        {
            "title": "Quiz",
            "questions": [],
            "objective_mapping": [
                {
                    "objective_code": "5.6E",
                    "objective_text": "Identify the main idea and supporting details.",
                }
            ],
        }
    )
    assert payload["objectiveMappings"]["objective_code"] == "5.6E"
    assert (
        payload["objectiveMappings"]["objective_text"]
        == "Identify the main idea and supporting details."
    )


def test_answer_key_payload_accepts_list_objective_mapping() -> None:
    payload = build_answer_key_json_payload(
        {
            "title": "Quiz",
            "questions": [{"number": 1, "answer": "A", "points": 1}],
            "objective_mapping": [
                {
                    "objective_code": "5.6E",
                    "objective_text": "Identify the main idea and supporting details.",
                }
            ],
        }
    )
    assert payload["objective_mapping"]["objective_code"] == "5.6E"
    assert payload["answers"][0]["objective_mapping"]["objective_text"] == (
        "Identify the main idea and supporting details."
    )

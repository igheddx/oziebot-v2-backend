"""Offline quiz export helpers (DOCX + read-only Google Forms JSON)."""

from __future__ import annotations

import html
import io
import zipfile
from typing import Any


def safe_export_filename(title: str, suffix: str, extension: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in title).strip("_")
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    base = cleaned or "Quiz"
    return f"{base}_{suffix}.{extension}"


def _docx_paragraph(text: str, *, bold: bool = False) -> str:
    escaped = html.escape(text)
    if bold:
        return f"<w:p><w:r><w:rPr><w:b/></w:rPr><w:t xml:space='preserve'>{escaped}</w:t></w:r></w:p>"
    return f"<w:p><w:r><w:t xml:space='preserve'>{escaped}</w:t></w:r></w:p>"


def _docx_blank_lines(count: int = 3) -> str:
    return "".join(_docx_paragraph("") for _ in range(count))


def build_docx_bytes(paragraphs: list[tuple[str, bool]]) -> bytes:
    document_xml_parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">',
        "<w:body>",
    ]
    for text, bold in paragraphs:
        document_xml_parts.append(_docx_paragraph(text, bold=bold))
    document_xml_parts.extend(["</w:body>", "</w:document>"])
    document_xml = "".join(document_xml_parts).encode("utf-8")

    content_types = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>"""
    rels = b"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>"""

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", rels)
        archive.writestr("word/document.xml", document_xml)
    return buffer.getvalue()


def _meta_lines(export_context: dict[str, Any] | None) -> list[tuple[str, bool]]:
    if not export_context:
        return []
    lines: list[tuple[str, bool]] = []
    for label, key in (
        ("Subject", "subject_name"),
        ("Grade", "grade_name"),
        ("School Year", "school_year_label"),
    ):
        value = export_context.get(key)
        if value:
            lines.append((f"{label}: {value}", False))
    mapping = export_context.get("objective_mapping") or {}
    objective_text = mapping.get("objective_text")
    objective_code = mapping.get("objective_code")
    if objective_code or objective_text:
        lines.append((f"Learning Objective: {objective_code or ''} {objective_text or ''}".strip(), False))
    return lines


def render_quiz_docx_bytes(
    content: dict[str, Any],
    *,
    export_context: dict[str, Any] | None = None,
) -> bytes:
    title = str(content.get("title") or "Quiz")
    paragraphs: list[tuple[str, bool]] = [(title, True), ("", False)]
    paragraphs.extend(_meta_lines(export_context))
    if paragraphs[-1] != ("", False):
        paragraphs.append(("", False))

    paragraphs.append(("Student Number: ________________________________", False))
    paragraphs.append(("", False))

    instructions = content.get("instructions") or content.get("summary") or content.get("description")
    if instructions:
        paragraphs.append(("Instructions", True))
        paragraphs.append((str(instructions), False))
        paragraphs.append(("", False))

    for question in content.get("questions") or []:
        number = question.get("number")
        points = question.get("points", 1)
        paragraphs.append((f"Question {number} ({points} point{'s' if int(points or 1) != 1 else ''})", True))
        paragraphs.append((str(question.get("prompt") or ""), False))
        q_type = str(question.get("type") or "multiple_choice")
        if q_type == "multiple_choice":
            for index, choice in enumerate(question.get("choices") or [], start=1):
                letter = chr(64 + index) if index <= 26 else str(index)
                paragraphs.append((f"{letter}. {choice}", False))
        else:
            paragraphs.append(("Response:", False))
            for _ in range(4):
                paragraphs.append(("", False))
        paragraphs.append(("", False))

    return build_docx_bytes(paragraphs)


def render_quiz_answer_key_docx_bytes(
    content: dict[str, Any],
    *,
    export_context: dict[str, Any] | None = None,
) -> bytes:
    title = f"{content.get('title') or 'Quiz'} — Answer Key"
    paragraphs: list[tuple[str, bool]] = [(title, True), ("Teacher use only.", False), ("", False)]
    paragraphs.extend(_meta_lines(export_context))
    mapping = content.get("objective_mapping") or (export_context or {}).get("objective_mapping") or {}
    if mapping.get("objective_text") or mapping.get("objective_code"):
        paragraphs.append(
            (
                f"Objective mapping: {mapping.get('objective_code') or ''} — {mapping.get('objective_text') or ''}".strip(
                    " —"
                ),
                False,
            )
        )
    paragraphs.append(("", False))

    entries = content.get("answer_key") or []
    if not entries:
        entries = [
            {
                "number": question.get("number"),
                "answer": question.get("answer"),
                "explanation": question.get("explanation"),
                "points": question.get("points", 1),
            }
            for question in content.get("questions") or []
        ]

    for item in entries:
        paragraphs.append((f"Question {item.get('number')}", True))
        paragraphs.append((f"Correct answer: {item.get('answer')}", False))
        if item.get("explanation"):
            paragraphs.append((f"Explanation: {item.get('explanation')}", False))
        if item.get("points") is not None:
            paragraphs.append((f"Points: {item.get('points')}", False))
        paragraphs.append(("", False))

    return build_docx_bytes(paragraphs)


GOOGLE_FORMS_JSON_HELPER = (
    "Google Forms API integration is not enabled yet. Use this JSON to manually create a Google Form: "
    "add the student-number dropdown first, then add quiz questions, turn on quiz mode, and assign the form link in Google Classroom."
)


def _student_number_question(content: dict[str, Any], ctx: dict[str, Any]) -> dict[str, Any] | None:
    if not content.get("student_number_field"):
        return None
    student_count = int(ctx.get("student_count") or 0)
    if student_count < 1:
        return None
    from oziebot_api.services.teacher_assist_v2.student_packet_docx import student_number_label

    return {
        "prompt": "Student Number",
        "type": "multiple_choice",
        "required": True,
        "choices": [student_number_label(number) for number in range(1, student_count + 1)],
    }


def build_google_forms_readonly_json(
    content: dict[str, Any],
    *,
    export_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ctx = export_context or {}
    mapping = content.get("objective_mapping") or ctx.get("objective_mapping") or {}
    questions = []
    for question in content.get("questions") or []:
        questions.append(
            {
                "number": question.get("number"),
                "type": question.get("type"),
                "prompt": question.get("prompt"),
                "choices": question.get("choices"),
                "correctAnswer": question.get("answer"),
                "pointValue": question.get("points", 1),
            }
        )
    return {
        "packageType": "google_forms_readonly",
        "formTitle": content.get("title"),
        "formDescription": content.get("description") or content.get("summary"),
        "studentNumberQuestion": _student_number_question(content, ctx),
        "studentCount": int(ctx.get("student_count") or 0),
        "questions": questions,
        "objectiveMappings": mapping,
        "teacherAssistAssignmentId": ctx.get("assignment_id"),
        "packageId": ctx.get("package_id"),
        "teacherId": ctx.get("teacher_user_id"),
        "schoolYear": ctx.get("school_year_label"),
        "schoolYearId": ctx.get("school_year_id"),
        "districtId": ctx.get("district_id"),
        "schoolId": ctx.get("school_id"),
        "grade": ctx.get("grade_name"),
        "gradeId": ctx.get("grade_id"),
        "subject": ctx.get("subject_name"),
        "subjectId": ctx.get("subject_id"),
        "objectiveIds": ctx.get("objective_ids") or [],
        "helperText": GOOGLE_FORMS_JSON_HELPER,
    }

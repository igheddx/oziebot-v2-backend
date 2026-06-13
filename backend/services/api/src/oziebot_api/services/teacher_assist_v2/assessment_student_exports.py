"""Per-student assessment exports (DOCX with QR on every page)."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_print_page import TeacherAssistV2AssignmentPrintPage
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.services.teacher_assist_v2.assignment_print_packets import generate_assignment_print_packet
from oziebot_api.services.teacher_assist_v2.package_export import (
    build_google_forms_package_payload,
    render_answer_key_html,
    save_teacher_assist_export_bytes,
)
from oziebot_api.services.teacher_assist_v2.quiz_export_context import build_quiz_export_context
from oziebot_api.services.teacher_assist_v2.quiz_exports import (
    render_quiz_answer_key_docx_bytes,
    safe_export_filename,
)
from oziebot_api.services.teacher_assist_v2.student_packet_content import (
    compute_pages_per_student,
    split_assessment_content_pages,
)
from oziebot_api.services.teacher_assist_v2.student_packet_docx import (
    build_student_packet_docx_bytes,
    render_qr_png_bytes,
    student_number_label,
)

ASSESSMENT_EXPORT_ARTIFACT_TYPES = frozenset({"quiz", "assignment", "writing_response"})

STUDENT_DOCX_LABELS = {
    "quiz": "Download Student Quiz DOCX",
    "assignment": "Download Student Assignment DOCX",
    "writing_response": "Download Writing Response DOCX",
}


def _assignment_content_for_packet(*, artifact_type: str, content: dict[str, Any]) -> dict[str, Any]:
    if artifact_type == "assignment":
        return content
    mapping = content.get("objective_mapping") or {}
    return {
        "objective_text": mapping.get("objective_text") or content.get("summary") or content.get("description"),
        "student_instructions": content.get("instructions") or content.get("student_instructions") or [],
        "passage_title": content.get("passage_title") or content.get("title"),
        "passage_text": content.get("passage_text") or content.get("prompt") or "",
    }


def build_student_packet_docx_for_artifact(
    db: Session,
    *,
    settings: Settings,
    assignment: TeacherAssistV2Assignment,
    artifact_type: str,
    content: dict[str, Any],
) -> bytes:
    content_pages = split_assessment_content_pages(artifact_type=artifact_type, content=content)
    pages_per_student = len(content_pages)
    generate_assignment_print_packet(
        db,
        settings=settings,
        assignment=assignment,
        assignment_content=_assignment_content_for_packet(artifact_type=artifact_type, content=content),
        pages_per_student=pages_per_student,
    )
    print_pages = db.scalars(
        select(TeacherAssistV2AssignmentPrintPage)
        .where(TeacherAssistV2AssignmentPrintPage.assignment_id == assignment.id)
        .order_by(
            TeacherAssistV2AssignmentPrintPage.student_number,
            TeacherAssistV2AssignmentPrintPage.page_number,
        )
    ).all()
    pages_by_student: dict[int, list[TeacherAssistV2AssignmentPrintPage]] = {}
    for row in print_pages:
        pages_by_student.setdefault(row.student_number, []).append(row)

    title = str(content.get("title") or "Assessment")
    docx_pages: list[dict[str, Any]] = []
    student_numbers = sorted(pages_by_student)
    total_pages = sum(len(pages_by_student[number]) for number in student_numbers)
    rendered = 0

    for student_number in student_numbers:
        student_pages = pages_by_student[student_number]
        for index, print_page in enumerate(student_pages):
            content_index = min(index, len(content_pages) - 1)
            rendered += 1
            docx_pages.append(
                {
                    "qr_png": render_qr_png_bytes(print_page.qr_payload_json),
                    "student_label": student_number_label(student_number),
                    "paragraphs": content_pages[content_index],
                    "page_break_after": rendered < total_pages,
                }
            )
    return build_student_packet_docx_bytes(title=title, pages=docx_pages)


def save_assessment_student_exports(
    db: Session,
    *,
    settings: Settings,
    package: TeacherAssistV2InstructionalPackage,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    assignment: TeacherAssistV2Assignment | None = None,
) -> list[dict[str, str]]:
    if artifact.artifact_type not in ASSESSMENT_EXPORT_ARTIFACT_TYPES:
        return []

    assignment_row = assignment
    if assignment_row is None and artifact.assignment_id:
        assignment_row = db.get(TeacherAssistV2Assignment, artifact.assignment_id)
    if assignment_row is None:
        return []

    content = artifact.content_json if isinstance(artifact.content_json, dict) else {}
    export_context = build_quiz_export_context(db, package=package, artifact=artifact, assignment=assignment_row)
    title = str(content.get("title") or artifact.title or "Assessment")
    exports: list[dict[str, str]] = []

    student_docx = build_student_packet_docx_for_artifact(
        db,
        settings=settings,
        assignment=assignment_row,
        artifact_type=artifact.artifact_type,
        content=content,
    )
    student_filename = safe_export_filename(title, "Student_Packet", "docx")
    key = save_teacher_assist_export_bytes(
        settings=settings,
        tenant_id=package.tenant_id,
        filename=f"v2-package-{artifact.id}-{student_filename}",
        contents=student_docx,
        mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    exports.append(
        {
            "label": STUDENT_DOCX_LABELS[artifact.artifact_type],
            "format": "docx",
            "storage_key": key,
            "original_filename": student_filename,
            "pages_per_student": str(compute_pages_per_student(artifact_type=artifact.artifact_type, content=content)),
        }
    )

    if artifact.artifact_type == "quiz":
        answer_docx = render_quiz_answer_key_docx_bytes(content, export_context=export_context)
        answer_filename = safe_export_filename(title, "Quiz_Answer_Key", "docx")
        key = save_teacher_assist_export_bytes(
            settings=settings,
            tenant_id=package.tenant_id,
            filename=f"v2-package-{artifact.id}-{answer_filename}",
            contents=answer_docx,
            mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )
        exports.append(
            {
                "label": "Download Answer Key DOCX",
                "format": "docx",
                "storage_key": key,
                "original_filename": answer_filename,
            }
        )

        answer_html = render_answer_key_html(content)
        answer_preview_filename = safe_export_filename(title, "Answer_Key_Preview", "html")
        key = save_teacher_assist_export_bytes(
            settings=settings,
            tenant_id=package.tenant_id,
            filename=f"v2-package-{artifact.id}-answer-key.html",
            contents=answer_html.encode("utf-8"),
            mime_type="text/html",
        )
        exports.append(
            {
                "label": "Preview Answer Key",
                "format": "html",
                "storage_key": key,
                "original_filename": answer_preview_filename,
            }
        )

        forms_payload = build_google_forms_package_payload(content, export_context=export_context)
        json_filename = safe_export_filename(title, "Google_Forms", "json")
        key = save_teacher_assist_export_bytes(
            settings=settings,
            tenant_id=package.tenant_id,
            filename=f"v2-package-{artifact.id}-{json_filename}",
            contents=json.dumps(forms_payload, indent=2).encode("utf-8"),
            mime_type="application/json",
        )
        exports.append(
            {
                "label": "Download Google Forms JSON",
                "format": "json",
                "storage_key": key,
                "original_filename": json_filename,
            }
        )
    return exports


def refresh_assessment_student_exports(
    db: Session,
    *,
    settings: Settings,
    package: TeacherAssistV2InstructionalPackage,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    assignment: TeacherAssistV2Assignment | None = None,
) -> None:
    additional = save_assessment_student_exports(
        db,
        settings=settings,
        package=package,
        artifact=artifact,
        assignment=assignment,
    )
    metadata = dict(artifact.metadata_json or {})
    metadata["additional_exports"] = additional
    metadata["pages_per_student"] = (
        compute_pages_per_student(
            artifact_type=artifact.artifact_type,
            content=artifact.content_json if isinstance(artifact.content_json, dict) else {},
        )
        if artifact.artifact_type in ASSESSMENT_EXPORT_ARTIFACT_TYPES
        else metadata.get("pages_per_student")
    )
    artifact.metadata_json = metadata

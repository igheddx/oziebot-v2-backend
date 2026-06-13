"""HTML and DOCX exports for rubric score reports."""

from __future__ import annotations

import html
import io
from typing import Any
import zipfile

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.grade_review_constants import OFFICIAL_ASSIGNMENT_GRADE_STATUSES
from oziebot_api.services.teacher_assist_v2.grading_rubric import (
    assignment_supports_rubric_scorecards,
    grading_template_from_package_rubric,
    resolve_assignment_rubric_content,
)
from oziebot_api.services.teacher_assist_v2.mastery_constants import serialize_mastery_level_fields
from oziebot_api.services.teacher_assist_v2.package_export import _BASE_STYLE, _esc, _print_button
from oziebot_api.services.teacher_assist_v2.quiz_exports import safe_export_filename
from oziebot_api.services.teacher_assist_v2.submission_intake import _get_assignment_or_404, get_student_submission_or_404
from oziebot_api.services.teacher_assist_v2.submission_workflow import pending_submission_student_numbers

RUBRIC_SCORE_REPORT_DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


def assignment_rubric_score_report_status(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
    ensure_roster_slots: bool = False,
) -> tuple[bool, str | None]:
    if not assignment_supports_rubric_scorecards(assignment_type=assignment.assignment_type):
        return False, "Class rubric reports are only available for writing and written assignments."

    submissions = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment.id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
    ).all()
    if not submissions:
        return False, "Upload and confirm at least one student submission before printing the class report."

    pending_numbers = pending_submission_student_numbers(db, user=user, assignment=assignment)
    if pending_numbers:
        preview = ", ".join(str(number) for number in pending_numbers[:5])
        extra = f" (+{len(pending_numbers) - 5} more)" if len(pending_numbers) > 5 else ""
        student_hint = f"student # {preview}{extra}" if preview else "some students"
        return False, (
            f"{len(pending_numbers)} submission(s) still need review before printing the class report "
            f"({student_hint}). Confirm grades or mark missing work as incomplete."
        )

    official_count = db.scalar(
        select(func.count(TeacherAssistV2AssignmentGrade.id)).where(
            TeacherAssistV2AssignmentGrade.assignment_id == assignment.id,
            TeacherAssistV2AssignmentGrade.teacher_user_id == user.id,
            TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES),
        )
    ) or 0
    if official_count <= 0:
        return False, "Confirm at least one student grade before printing the class report."

    return True, None


def _render_rubric_sections_table(sections: list[dict[str, Any]]) -> str:
    rows = []
    for section in sections:
        rows.append(
            "<tr>"
            f"<td><strong>{_esc(section.get('name'))}</strong></td>"
            f"<td>{_esc(section.get('score'))} / {_esc(section.get('max_score'))}</td>"
            f"<td>{_esc(section.get('feedback'))}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>Criterion</th><th>Score</th><th>Feedback</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def render_student_rubric_scorecard_html(
    *,
    assignment: TeacherAssistV2Assignment,
    submission: TeacherAssistV2StudentSubmission,
    grade: TeacherAssistV2AssignmentGrade,
    rubric_content: dict[str, Any] | None,
) -> str:
    template = grading_template_from_package_rubric(rubric_content)
    sections = (grade.rubric_json or {}).get("sections") if isinstance(grade.rubric_json, dict) else None
    if not isinstance(sections, list) or not sections:
        sections = template.get("sections") or []
    mastery = serialize_mastery_level_fields(percentage=grade.percentage)
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(assignment.title)} — Student #{submission.student_number}</title>"
        f"<style>{_BASE_STYLE} table{{width:100%;border-collapse:collapse;margin-top:1rem}}"
        f" td,th{{border:1px solid #cbd5e1;padding:.5rem;text-align:left}}</style></head><body>"
        f"{_print_button()}"
        f"<h1>{_esc(assignment.title)}</h1>"
        f"<p><strong>Student #:</strong> {_esc(submission.student_number)}</p>"
        f"<p><strong>Total score:</strong> {_esc(grade.score)} / {_esc(grade.max_score)} ({_esc(grade.percentage)}%)</p>"
        f"<p><strong>Mastery:</strong> {_esc(mastery.get('mastery_level_label'))}</p>"
        f"<h2>{_esc(template.get('title') or 'Rubric score card')}</h2>"
        f"{_render_rubric_sections_table(sections)}"
        f"<h2>Teacher comment</h2><p>{_esc(grade.teacher_comment)}</p>"
        "</body></html>"
    )


def render_class_rubric_score_report_html(
    *,
    assignment: TeacherAssistV2Assignment,
    rubric_content: dict[str, Any] | None,
    rows: list[dict[str, Any]],
) -> str:
    template = grading_template_from_package_rubric(rubric_content)
    student_blocks = []
    for row in rows:
        sections = row.get("sections") or []
        student_blocks.append(
            "<section style='margin-top:1.5rem;page-break-inside:avoid'>"
            f"<h2>Student #{_esc(row.get('student_number'))}</h2>"
            f"<p><strong>Score:</strong> {_esc(row.get('score'))} / {_esc(row.get('max_score'))} "
            f"({_esc(row.get('percentage'))}%) · {_esc(row.get('mastery_level_label'))}</p>"
            f"{_render_rubric_sections_table(sections)}"
            f"<p><strong>Comment:</strong> {_esc(row.get('teacher_comment'))}</p>"
            "</section>"
        )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(assignment.title)} — Rubric score report</title>"
        f"<style>{_BASE_STYLE} table{{width:100%;border-collapse:collapse;margin-top:.75rem}}"
        f" td,th{{border:1px solid #cbd5e1;padding:.5rem;text-align:left}}</style></head><body>"
        f"{_print_button()}"
        f"<h1>{_esc(assignment.title)}</h1>"
        f"<p>Class rubric score report for student feedback.</p>"
        f"<p><strong>Rubric:</strong> {_esc(template.get('title'))}</p>"
        + "".join(student_blocks)
        + "</body></html>"
    )


def _docx_paragraph(text: str, *, bold: bool = False) -> str:
    escaped = html.escape(text)
    if bold:
        return f"<w:p><w:r><w:rPr><w:b/></w:rPr><w:t xml:space='preserve'>{escaped}</w:t></w:r></w:p>"
    return f"<w:p><w:r><w:t xml:space='preserve'>{escaped}</w:t></w:r></w:p>"


def _docx_page_break() -> str:
    return "<w:p><w:r><w:br w:type='page'/></w:r></w:p>"


def _build_docx_bytes(parts: list[str]) -> bytes:
    document_xml = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<w:document xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'>"
        "<w:body>"
        + "".join(parts)
        + "</w:body></w:document>"
    ).encode("utf-8")
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


def render_class_rubric_score_report_docx(
    *,
    assignment: TeacherAssistV2Assignment,
    rubric_content: dict[str, Any] | None,
    rows: list[dict[str, Any]],
) -> bytes:
    template = grading_template_from_package_rubric(rubric_content)
    parts: list[str] = []
    for index, row in enumerate(rows):
        parts.append(_docx_paragraph(str(assignment.title or "Rubric score report"), bold=True))
        parts.append(_docx_paragraph(f"Student #: {row.get('student_number')}", bold=True))
        parts.append(
            _docx_paragraph(
                f"Total score: {row.get('score')} / {row.get('max_score')} ({row.get('percentage')}%)"
            )
        )
        parts.append(_docx_paragraph(f"Mastery: {row.get('mastery_level_label') or '—'}"))
        parts.append(_docx_paragraph(f"Rubric: {template.get('title') or 'Rubric score card'}"))
        parts.append(_docx_paragraph(""))
        for section in row.get("sections") or []:
            parts.append(
                _docx_paragraph(
                    f"{section.get('name')}: {section.get('score')} / {section.get('max_score')}",
                    bold=True,
                )
            )
            feedback = section.get("feedback")
            if feedback:
                parts.append(_docx_paragraph(f"Feedback: {feedback}"))
            parts.append(_docx_paragraph(""))
        teacher_comment = row.get("teacher_comment")
        if teacher_comment:
            parts.append(_docx_paragraph("Teacher comment", bold=True))
            parts.append(_docx_paragraph(str(teacher_comment)))
        if index < len(rows) - 1:
            parts.append(_docx_page_break())
    return _build_docx_bytes(parts)


def _build_assignment_rubric_score_report_rows(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
) -> list[dict[str, Any]]:
    submissions = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment.id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
    ).all()
    rows: list[dict[str, Any]] = []
    for submission in submissions:
        grade = db.scalars(
            select(TeacherAssistV2AssignmentGrade)
            .where(
                TeacherAssistV2AssignmentGrade.student_submission_id == submission.id,
                TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES),
            )
            .order_by(TeacherAssistV2AssignmentGrade.updated_at.desc())
        ).first()
        if grade is None:
            continue
        sections = (grade.rubric_json or {}).get("sections") if isinstance(grade.rubric_json, dict) else []
        mastery = serialize_mastery_level_fields(percentage=grade.percentage)
        rows.append(
            {
                "student_number": submission.student_number,
                "score": grade.score,
                "max_score": grade.max_score,
                "percentage": grade.percentage,
                "mastery_level_label": mastery.get("mastery_level_label"),
                "teacher_comment": grade.teacher_comment,
                "sections": sections,
            }
        )
    rows.sort(key=lambda row: (row["student_number"] is None, row["student_number"] or 0))
    return rows


def build_submission_rubric_scorecard_html(
    db: Session,
    *,
    user: User,
    submission_id,
) -> str:
    submission = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    assignment = _get_assignment_or_404(db, user=user, assignment_id=submission.assignment_id)
    if not assignment_supports_rubric_scorecards(assignment_type=assignment.assignment_type):
        raise ValueError("This assignment type does not use rubric score cards.")
    grade = db.scalars(
        select(TeacherAssistV2AssignmentGrade)
        .where(
            TeacherAssistV2AssignmentGrade.student_submission_id == submission.id,
            TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES),
        )
        .order_by(TeacherAssistV2AssignmentGrade.updated_at.desc())
    ).first()
    if grade is None:
        raise ValueError("Confirm a grade before generating the rubric score card.")
    rubric_content = resolve_assignment_rubric_content(db, assignment=assignment)
    return render_student_rubric_scorecard_html(
        assignment=assignment,
        submission=submission,
        grade=grade,
        rubric_content=rubric_content,
    )


def build_assignment_rubric_score_report_html(
    db: Session,
    *,
    user: User,
    assignment_id,
) -> str:
    assignment = _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    available, blocker = assignment_rubric_score_report_status(
        db,
        user=user,
        assignment=assignment,
        ensure_roster_slots=False,
    )
    if not available:
        raise ValueError(blocker or "Class rubric score report is not available yet.")
    rubric_content = resolve_assignment_rubric_content(db, assignment=assignment)
    rows = _build_assignment_rubric_score_report_rows(db, user=user, assignment=assignment)
    if not rows:
        raise ValueError("No confirmed grades are available for this assignment.")
    return render_class_rubric_score_report_html(
        assignment=assignment,
        rubric_content=rubric_content,
        rows=rows,
    )


def build_assignment_rubric_score_report_docx(
    db: Session,
    *,
    user: User,
    assignment_id,
) -> tuple[str, bytes]:
    assignment = _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    available, blocker = assignment_rubric_score_report_status(
        db,
        user=user,
        assignment=assignment,
        ensure_roster_slots=False,
    )
    if not available:
        raise ValueError(blocker or "Class rubric score report is not available yet.")
    rubric_content = resolve_assignment_rubric_content(db, assignment=assignment)
    rows = _build_assignment_rubric_score_report_rows(db, user=user, assignment=assignment)
    if not rows:
        raise ValueError("No confirmed grades are available for this assignment.")
    filename = safe_export_filename(str(assignment.title or "Rubric_Report"), "Class_Rubric_Score_Report", "docx")
    payload = render_class_rubric_score_report_docx(
        assignment=assignment,
        rubric_content=rubric_content,
        rows=rows,
    )
    return filename, payload

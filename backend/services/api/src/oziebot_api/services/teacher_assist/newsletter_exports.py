from __future__ import annotations

from datetime import UTC, datetime
import html
import io
import uuid
import zipfile
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_newsletter_export import TeacherAssistNewsletterExport
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.constants import validate_newsletter_export_format
from oziebot_api.services.teacher_assist.newsletters import get_newsletter_or_404
from oziebot_api.services.teacher_assist.storage import (
    get_teacher_assist_download_url,
    save_teacher_assist_bytes,
)


def _content_sections(content: dict[str, Any]) -> list[tuple[str, str | list[str]]]:
    sections: list[tuple[str, str | list[str]]] = []
    if content.get("overview"):
        sections.append(("Overview", str(content["overview"])))
    for label, key in (
        ("What We Learned", "what_we_learned"),
        ("Standards Covered", "standards_covered"),
        ("Upcoming Topics", "upcoming_topics"),
        ("Reminders", "reminders"),
        ("Celebration Highlights", "celebration_highlights"),
    ):
        values = content.get(key)
        if isinstance(values, list) and values:
            sections.append((label, values))
    if content.get("teacher_message"):
        sections.append(("Teacher Message", str(content["teacher_message"])))
    return sections


def render_newsletter_html_bytes(*, title: str, content: dict[str, Any]) -> bytes:
    parts = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        f"<title>{html.escape(title)}</title>",
        "<style>body{font-family:Georgia,serif;max-width:720px;margin:2rem auto;line-height:1.5;color:#1e293b}"
        "h1,h2{color:#0f172a}ul{padding-left:1.25rem}</style></head><body>",
        f"<h1>{html.escape(title)}</h1>",
        "<p><em>Draft for teacher review. TeacherAssist does not send this automatically.</em></p>",
    ]
    for heading, body in _content_sections(content):
        parts.append(f"<h2>{html.escape(heading)}</h2>")
        if isinstance(body, list):
            parts.append("<ul>")
            for item in body:
                parts.append(f"<li>{html.escape(str(item))}</li>")
            parts.append("</ul>")
        else:
            parts.append(f"<p>{html.escape(body)}</p>")
    parts.append("</body></html>")
    return "".join(parts).encode("utf-8")


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def render_newsletter_pdf_bytes(*, title: str, content: dict[str, Any]) -> bytes:
    lines = [title, "", "Draft for teacher review. TeacherAssist does not send automatically.", ""]
    for heading, body in _content_sections(content):
        lines.append(heading)
        if isinstance(body, list):
            lines.extend(f"- {item}" for item in body)
        else:
            lines.append(body)
        lines.append("")
    stream_lines = ["BT", "/F1 11 Tf", "50 750 Td", "14 TL"]
    for line in lines[:80]:
        stream_lines.append(f"({_pdf_escape(line[:200])}) Tj")
        stream_lines.append("T*")
    stream_lines.append("ET")
    stream = "\n".join(stream_lines).encode("latin-1", errors="replace")
    objects = [
        b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n",
        b"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n",
        b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>endobj\n",
        b"4 0 obj<< /Length "
        + str(len(stream)).encode("ascii")
        + b" >>stream\n"
        + stream
        + b"\nendstream\nendobj\n",
        b"5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n",
    ]
    pdf = [b"%PDF-1.4\n"]
    offsets = [0]
    for obj in objects:
        offsets.append(sum(len(part) for part in pdf))
        pdf.append(obj)
    xref_offset = sum(len(part) for part in pdf)
    pdf.append(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.append(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.append(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.append(
        f"trailer<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode(
            "ascii"
        )
    )
    return b"".join(pdf)


def render_newsletter_docx_bytes(*, title: str, content: dict[str, Any]) -> bytes:
    paragraphs: list[str] = [
        title,
        "Draft for teacher review. TeacherAssist does not send automatically.",
    ]
    for heading, body in _content_sections(content):
        paragraphs.append(heading)
        if isinstance(body, list):
            paragraphs.extend(f"• {item}" for item in body)
        else:
            paragraphs.append(body)

    document_xml_parts = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">',
        "<w:body>",
    ]
    for paragraph in paragraphs:
        escaped = html.escape(paragraph)
        document_xml_parts.append(
            f"<w:p><w:r><w:t xml:space='preserve'>{escaped}</w:t></w:r></w:p>"
        )
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


def _render_newsletter_export_bytes(
    *, title: str, content: dict[str, Any], export_format: str
) -> bytes:
    if export_format == "html":
        return render_newsletter_html_bytes(title=title, content=content)
    if export_format == "pdf":
        return render_newsletter_pdf_bytes(title=title, content=content)
    if export_format == "docx":
        return render_newsletter_docx_bytes(title=title, content=content)
    raise ValueError("Unsupported newsletter export format")


def _export_mime_type(export_format: str) -> str:
    if export_format == "html":
        return "text/html"
    if export_format == "pdf":
        return "application/pdf"
    if export_format == "docx":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return "application/octet-stream"


def _export_filename(title: str, export_format: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in title.lower()).strip(
        "-"
    )
    safe = safe[:60] or "newsletter"
    return f"{safe}.{export_format}"


def create_newsletter_export(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter_id: uuid.UUID,
    export_format: str,
    settings: Settings | None = None,
) -> TeacherAssistNewsletterExport:
    settings = settings or Settings()
    normalized_format = validate_newsletter_export_format(export_format)
    newsletter = get_newsletter_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter_id=newsletter_id,
        load_versions=True,
    )
    if newsletter.current_version is None:
        raise ValueError("Newsletter must have content before export")
    content = dict(newsletter.current_version.content_json or {})
    file_bytes = _render_newsletter_export_bytes(
        title=newsletter.title,
        content=content,
        export_format=normalized_format,
    )
    filename = _export_filename(newsletter.title, normalized_format)
    stored = save_teacher_assist_bytes(
        settings,
        tenant_id=tenant_id,
        area="exports",
        original_filename=filename,
        contents=file_bytes,
        mime_type=_export_mime_type(normalized_format),
    )
    now = datetime.now(UTC)
    export_row = TeacherAssistNewsletterExport(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        newsletter_id=newsletter.id,
        newsletter_version_id=newsletter.current_version_id,
        export_format=normalized_format,
        storage_key=stored.storage_key,
        file_size_bytes=stored.file_size,
        created_at=now,
    )
    db.add(export_row)
    db.flush()
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="newsletter_export_created",
        event_category="export",
        entity_type="newsletter",
        entity_id=newsletter.id,
        school_year_id=newsletter.school_year_id,
        grading_period_id=newsletter.grading_period_id,
        class_id=newsletter.class_id,
        subject_id=newsletter.subject_id,
        summary_text=f"Created newsletter {normalized_format.upper()} export.",
        details_json={
            "newsletter_id": str(newsletter.id),
            "export_id": str(export_row.id),
            "export_format": normalized_format,
        },
    )
    db.flush()
    db.refresh(export_row)
    return export_row


def serialize_newsletter_export(
    export_row: TeacherAssistNewsletterExport, *, title: str
) -> dict[str, Any]:
    return {
        "id": export_row.id,
        "newsletter_id": export_row.newsletter_id,
        "newsletter_version_id": export_row.newsletter_version_id,
        "export_format": export_row.export_format,
        "file_size_bytes": export_row.file_size_bytes,
        "created_at": export_row.created_at,
        "download_filename": _export_filename(title, export_row.export_format),
    }


def get_newsletter_export_download(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter_id: uuid.UUID,
    export_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or Settings()
    get_newsletter_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter_id=newsletter_id,
    )
    export_row = db.get(TeacherAssistNewsletterExport, export_id)
    if (
        export_row is None
        or export_row.tenant_id != tenant_id
        or export_row.owner_user_id != user_id
        or export_row.newsletter_id != newsletter_id
    ):
        raise LookupError("Newsletter export not found")
    newsletter = get_newsletter_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter_id=newsletter_id,
    )
    filename = _export_filename(newsletter.title, export_row.export_format)
    return {
        "export_id": export_row.id,
        "newsletter_id": export_row.newsletter_id,
        "export_format": export_row.export_format,
        "mime_type": _export_mime_type(export_row.export_format),
        "download_filename": filename,
        "download_url": get_teacher_assist_download_url(
            settings,
            storage_key=export_row.storage_key,
            original_filename=filename,
            mime_type=_export_mime_type(export_row.export_format),
        ),
    }

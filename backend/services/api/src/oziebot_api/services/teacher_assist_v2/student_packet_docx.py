"""DOCX builder for per-student printable assessment packets with QR codes."""

from __future__ import annotations

import html
import io
import json
import zipfile
from typing import Any

import qrcode


def student_number_label(number: int) -> str:
    return f"Student #{number:03d}"


def render_qr_png_bytes(payload: dict[str, Any]) -> bytes:
    qr = qrcode.make(
        json.dumps(payload, sort_keys=True, separators=(",", ":")), box_size=4, border=1
    )
    buffer = io.BytesIO()
    qr.save(buffer, format="PNG")
    return buffer.getvalue()


def _docx_paragraph(text: str, *, bold: bool = False) -> str:
    escaped = html.escape(text)
    if bold:
        return (
            f"<w:p><w:r><w:rPr><w:b/></w:rPr><w:t xml:space='preserve'>{escaped}</w:t></w:r></w:p>"
        )
    return f"<w:p><w:r><w:t xml:space='preserve'>{escaped}</w:t></w:r></w:p>"


def _docx_page_break() -> str:
    return "<w:p><w:r><w:br w:type='page'/></w:r></w:p>"


def _docx_header(image_rel_id: str, *, image_id: int, student_label: str, title: str) -> str:
    return (
        "<w:tbl><w:tblPr><w:tblW w:w='5000' w:type='pct'/></w:tblPr><w:tr>"
        "<w:tc><w:tcPr><w:tcW w:w='1200' w:type='dxa'/></w:tcPr><w:p><w:r><w:drawing>"
        "<wp:inline distT='0' distB='0' distL='0' distR='0'>"
        "<wp:extent cx='914400' cy='914400'/>"
        f"<wp:docPr id='{image_id}' name='QR Code'/>"
        "<wp:cNvGraphicFramePr><a:graphicFrameLocks noChangeAspect='1'/></wp:cNvGraphicFramePr>"
        "<a:graphic><a:graphicData uri='http://schemas.openxmlformats.org/drawingml/2006/picture'>"
        "<pic:pic><pic:nvPicPr>"
        f"<pic:cNvPr id='{image_id}' name='qr.png'/>"
        "<pic:cNvPicPr/></pic:nvPicPr>"
        f"<pic:blipFill><a:blip r:embed='{image_rel_id}'/><a:stretch><a:fillRect/></a:stretch></pic:blipFill>"
        "<pic:spPr><a:xfrm><a:off x='0' y='0'/><a:ext cx='914400' cy='914400'/></a:xfrm>"
        "<a:prstGeom prst='rect'><a:avLst/></a:prstGeom></pic:spPr>"
        "</pic:pic></a:graphicData></a:graphic></wp:inline></w:drawing></w:r></w:p></w:tc>"
        "<w:tc><w:tcPr><w:tcW w:w='8000' w:type='dxa'/></w:tcPr>"
        f"{_docx_paragraph(student_label, bold=True)}"
        f"{_docx_paragraph(title, bold=False)}"
        "</w:tc></w:tr></w:tbl>"
    )


def build_student_packet_docx_bytes(
    *,
    title: str,
    pages: list[dict[str, Any]],
) -> bytes:
    """
    pages: ordered printable pages, each with:
      - qr_png: bytes
      - student_label: str
      - paragraphs: list[tuple[str, bool]]
      - page_break_after: bool
    """
    images: list[bytes] = []
    body_parts: list[str] = []

    for index, page in enumerate(pages, start=1):
        images.append(page["qr_png"])
        rel_id = f"rId{index + 1}"
        body_parts.append(
            _docx_header(
                rel_id, image_id=index, student_label=str(page["student_label"]), title=title
            )
        )
        for text, bold in page.get("paragraphs") or []:
            body_parts.append(_docx_paragraph(str(text), bold=bold))
        if page.get("page_break_after"):
            body_parts.append(_docx_page_break())

    document_xml = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<w:document "
        "xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main' "
        "xmlns:r='http://schemas.openxmlformats.org/officeDocument/2006/relationships' "
        "xmlns:wp='http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing' "
        "xmlns:a='http://schemas.openxmlformats.org/drawingml/2006/main' "
        "xmlns:pic='http://schemas.openxmlformats.org/drawingml/2006/picture'>"
        "<w:body>" + "".join(body_parts) + "</w:body></w:document>"
    )

    rels = [
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="word/document.xml"/>'
    ]
    document_rels = [
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="document.xml"/>'
    ]
    content_types = [
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '<Default Extension="xml" ContentType="application/xml"/>',
        '<Default Extension="png" ContentType="image/png"/>',
        '<Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>',
    ]

    for index, _image in enumerate(images, start=1):
        rel_id = f"rId{index + 1}"
        media_name = f"image{index}.png"
        document_rels.append(
            f'<Relationship Id="{rel_id}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" '
            f'Target="media/{media_name}"/>'
        )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "[Content_Types].xml",
            "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
            "<Types xmlns='http://schemas.openxmlformats.org/package/2006/content-types'>"
            + "".join(content_types)
            + "</Types>",
        )
        archive.writestr(
            "_rels/.rels",
            "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
            "<Relationships xmlns='http://schemas.openxmlformats.org/package/2006/relationships'>"
            + "".join(rels)
            + "</Relationships>",
        )
        archive.writestr("word/document.xml", document_xml.encode("utf-8"))
        archive.writestr(
            "word/_rels/document.xml.rels",
            "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
            "<Relationships xmlns='http://schemas.openxmlformats.org/package/2006/relationships'>"
            + "".join(document_rels)
            + "</Relationships>",
        )
        for index, image in enumerate(images, start=1):
            archive.writestr(f"word/media/image{index}.png", image)
    return buffer.getvalue()


COVER_SHEET_DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


def build_cover_sheet_docx_bytes(
    *,
    assignment_title: str,
    pages: list[dict[str, Any]],
) -> bytes:
    """Build one cover-sheet page per student with QR code and stapling instructions."""
    docx_pages: list[dict[str, Any]] = []
    for index, page in enumerate(pages):
        student_number = int(page["student_number"])
        docx_pages.append(
            {
                "qr_png": render_qr_png_bytes(page["qr_payload_json"]),
                "student_label": student_number_label(student_number),
                "paragraphs": [
                    ("Cover sheet", True),
                    ("", False),
                    ("Teacher instructions:", True),
                    (
                        "Staple this cover sheet to the front of the student's completed assignment.",
                        False,
                    ),
                    ("Keep each student's pages together before scanning.", False),
                    (
                        "Upload the class batch in TeacherAssist to match grades automatically.",
                        False,
                    ),
                    ("", False),
                    ("Attach external assignment pages below this cover sheet.", False),
                ],
                "page_break_after": index < len(pages) - 1,
            }
        )
    return build_student_packet_docx_bytes(title=assignment_title, pages=docx_pages)

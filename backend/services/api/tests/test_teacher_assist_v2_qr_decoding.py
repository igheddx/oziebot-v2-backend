from __future__ import annotations

import uuid

import fitz

from oziebot_api.services.teacher_assist_v2.qr_decoding import (
    extract_qr_identifiers_from_file,
    parse_qr_identifier_from_content,
)
from oziebot_api.services.teacher_assist_v2.student_packet_docx import render_qr_png_bytes
from oziebot_api.services.teacher_assist_v2.submission_intake_constants import QR_PACKET_VERSION


def test_parse_qr_identifier_from_json_payload():
    import json

    token = uuid.uuid4().hex
    payload = {
        "qr_version": QR_PACKET_VERSION,
        "qr_token": token,
        "student_number": 2,
    }
    assert (
        parse_qr_identifier_from_content(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        == token
    )


def test_extract_qr_identifiers_from_png_image():
    tokens = [uuid.uuid4().hex for _ in range(3)]
    payloads = [
        {
            "qr_version": QR_PACKET_VERSION,
            "qr_token": token,
            "student_number": index + 1,
        }
        for index, token in enumerate(tokens)
    ]

    doc = fitz.open()
    page = doc.new_page(width=612, height=792)
    y_offset = 36
    for payload in payloads:
        qr_png = render_qr_png_bytes(payload)
        rect = fitz.Rect(36, y_offset, 156, y_offset + 120)
        page.insert_image(rect, stream=qr_png)
        y_offset += 140
    png_bytes = doc[0].get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False).tobytes("png")
    doc.close()

    identifiers = extract_qr_identifiers_from_file(
        file_bytes=png_bytes,
        mime_type="image/png",
        original_filename="scan.png",
    )
    assert identifiers
    assert identifiers[0] in tokens


def test_extract_qr_identifiers_from_pdf_with_multiple_students():
    tokens = [uuid.uuid4().hex for _ in range(3)]
    pdf_doc = fitz.open()
    for index, token in enumerate(tokens):
        page = pdf_doc.new_page(width=612, height=792)
        payload = {
            "qr_version": QR_PACKET_VERSION,
            "qr_token": token,
            "student_number": index + 1,
        }
        page.insert_image(fitz.Rect(36, 36, 156, 156), stream=render_qr_png_bytes(payload))
    pdf_bytes = pdf_doc.tobytes()
    pdf_doc.close()

    identifiers = extract_qr_identifiers_from_file(
        file_bytes=pdf_bytes,
        mime_type="application/pdf",
        original_filename="class-scan.pdf",
    )
    assert set(identifiers) == set(tokens)

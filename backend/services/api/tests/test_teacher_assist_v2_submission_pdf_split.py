from __future__ import annotations

import uuid

import fitz

from oziebot_api.services.teacher_assist_v2.student_packet_docx import render_qr_png_bytes
from oziebot_api.services.teacher_assist_v2.submission_intake_constants import QR_PACKET_VERSION
from oziebot_api.services.teacher_assist_v2.submission_pdf_split import extract_pdf_pages


def test_extract_pdf_pages_returns_requested_pages():
    tokens = [uuid.uuid4().hex for _ in range(3)]
    pdf_doc = fitz.open()
    for index, token in enumerate(tokens):
        page = pdf_doc.new_page(width=612, height=792)
        payload = {
            "qr_version": QR_PACKET_VERSION,
            "qr_token": token,
            "student_number": index + 1,
        }
        page.insert_text((200, 400), f"Student {index + 1}")
        page.insert_image(fitz.Rect(36, 36, 156, 156), stream=render_qr_png_bytes(payload))
    pdf_bytes = pdf_doc.tobytes()
    pdf_doc.close()

    student_two_pdf = extract_pdf_pages(pdf_bytes, [2])
    split_doc = fitz.open(stream=student_two_pdf, filetype="pdf")
    try:
        assert split_doc.page_count == 1
        assert "Student 2" in split_doc[0].get_text()
    finally:
        split_doc.close()

    students_one_and_three = extract_pdf_pages(pdf_bytes, [1, 3])
    split_doc = fitz.open(stream=students_one_and_three, filetype="pdf")
    try:
        assert split_doc.page_count == 2
        text = "".join(page.get_text() for page in split_doc)
        assert "Student 1" in text
        assert "Student 3" in text
        assert "Student 2" not in text
    finally:
        split_doc.close()

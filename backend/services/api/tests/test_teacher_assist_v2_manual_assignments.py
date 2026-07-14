from __future__ import annotations

import io
import uuid
import zipfile

from oziebot_api.services.teacher_assist_v2.student_packet_docx import (
    build_cover_sheet_docx_bytes,
    student_number_label,
)


def test_build_cover_sheet_docx_one_page_per_student() -> None:
    pages = [
        {
            "student_number": student_number,
            "qr_payload_json": {
                "assignment_id": str(uuid.uuid4()),
                "student_number": student_number,
                "page_number": 1,
            },
        }
        for student_number in (1, 2, 3)
    ]
    docx = build_cover_sheet_docx_bytes(
        assignment_title="Partner Teacher Fractions Sheet", pages=pages
    )
    with zipfile.ZipFile(io.BytesIO(docx)) as archive:
        names = archive.namelist()
        assert "word/document.xml" in names
        assert "word/media/image1.png" in names
        assert "word/media/image3.png" in names
        document = archive.read("word/document.xml").decode("utf-8")
        assert student_number_label(1) in document
        assert student_number_label(3) in document
        assert "Staple this cover sheet" in document
        assert document.count("w:br w:type='page'") == 2

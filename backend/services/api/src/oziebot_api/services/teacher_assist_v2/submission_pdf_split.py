"""Split multi-student scanned PDF uploads into per-student files using QR page markers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.storage import (
    StoredTeacherAssistUpload,
    save_teacher_assist_bytes,
)
from oziebot_api.services.teacher_assist_v2.qr_decoding import (
    decode_qr_strings_from_fitz_page,
    is_pdf_upload,
)
from oziebot_api.services.teacher_assist_v2.qr_matching import (
    QrMatchResult,
    resolve_qr_match_from_content,
)


@dataclass
class StudentPdfSegment:
    student_number: int
    pdf_page_numbers: list[int]
    qr_match: QrMatchResult

    @property
    def page_range(self) -> str:
        return ",".join(str(page_number) for page_number in self.pdf_page_numbers)


def _decode_page_match(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    doc,
    page,
) -> QrMatchResult | None:
    for content in decode_qr_strings_from_fitz_page(doc, page):
        match = resolve_qr_match_from_content(db, assignment_id=assignment_id, content=content)
        if match is not None:
            return match
    return None


def _append_page_to_segments(
    segments: list[StudentPdfSegment],
    *,
    current_segment: StudentPdfSegment | None,
    pdf_page_number: int,
    match: QrMatchResult,
) -> StudentPdfSegment:
    if current_segment is not None and current_segment.student_number == match.student_number:
        current_segment.pdf_page_numbers.append(pdf_page_number)
        return current_segment
    current_segment = StudentPdfSegment(
        student_number=match.student_number,
        pdf_page_numbers=[pdf_page_number],
        qr_match=match,
    )
    segments.append(current_segment)
    return current_segment


def build_student_pdf_segments(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    file_bytes: bytes,
    mime_type: str | None,
    original_filename: str | None,
) -> list[StudentPdfSegment]:
    if not is_pdf_upload(
        file_bytes=file_bytes, mime_type=mime_type, original_filename=original_filename
    ):
        return []

    import fitz

    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
    except (fitz.FileDataError, fitz.mupdf.FzErrorFormat, ValueError):
        return []
    segments: list[StudentPdfSegment] = []
    current_segment: StudentPdfSegment | None = None
    try:
        for page_index in range(doc.page_count):
            pdf_page_number = page_index + 1
            page = doc[page_index]
            match = _decode_page_match(db, assignment_id=assignment_id, doc=doc, page=page)
            if match is not None:
                current_segment = _append_page_to_segments(
                    segments,
                    current_segment=current_segment,
                    pdf_page_number=pdf_page_number,
                    match=match,
                )
            elif current_segment is not None:
                current_segment.pdf_page_numbers.append(pdf_page_number)
    finally:
        doc.close()
    return segments


def extract_pdf_pages(file_bytes: bytes, page_numbers: list[int]) -> bytes:
    import fitz

    if not page_numbers:
        raise ValueError("At least one PDF page is required")

    source = fitz.open(stream=file_bytes, filetype="pdf")
    target = fitz.open()
    try:
        for page_number in page_numbers:
            if page_number < 1 or page_number > source.page_count:
                raise ValueError(f"PDF page {page_number} is out of range")
            target.insert_pdf(source, from_page=page_number - 1, to_page=page_number - 1)
        return target.tobytes()
    finally:
        target.close()
        source.close()


def save_student_pdf_segment(
    settings: Settings,
    *,
    tenant_id: uuid.UUID,
    batch_filename: str,
    student_number: int,
    pdf_bytes: bytes,
) -> StoredTeacherAssistUpload:
    stem = batch_filename.rsplit(".", 1)[0] if "." in batch_filename else batch_filename
    return save_teacher_assist_bytes(
        settings,
        tenant_id=tenant_id,
        area="student-work",
        original_filename=f"{stem}-student-{student_number:03d}.pdf",
        contents=pdf_bytes,
        mime_type="application/pdf",
    )


def persist_student_pdf_segment(
    settings: Settings,
    *,
    tenant_id: uuid.UUID,
    batch_filename: str,
    source_pdf_bytes: bytes,
    segment: StudentPdfSegment,
) -> StoredTeacherAssistUpload:
    segment_bytes = extract_pdf_pages(source_pdf_bytes, segment.pdf_page_numbers)
    return save_student_pdf_segment(
        settings,
        tenant_id=tenant_id,
        batch_filename=batch_filename,
        student_number=segment.student_number,
        pdf_bytes=segment_bytes,
    )

"""TeacherAssist v2 QR matching foundation for student work intake."""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_v2_assignment_print_page import TeacherAssistV2AssignmentPrintPage

QR_TOKEN_FILENAME_PATTERN = re.compile(r"(?:^|[-_])([a-f0-9]{32})(?:[-_.]|$)", re.IGNORECASE)
STUDENT_NUMBER_FILENAME_PATTERN = re.compile(r"student[-_ ]?(\d+)", re.IGNORECASE)
QR_MARKER_PATTERN = re.compile(r"qr[-_ ]?([a-f0-9]{32})", re.IGNORECASE)


@dataclass(frozen=True)
class QrMatchResult:
    qr_identifier: str
    packet_id: uuid.UUID
    student_number: int
    page_number: int


def parse_student_number_from_filename(filename: str) -> int | None:
    match = STUDENT_NUMBER_FILENAME_PATTERN.search(filename)
    if match is None:
        return None
    value = int(match.group(1))
    return value if value > 0 else None


def extract_qr_identifiers(*, original_filename: str, file_bytes: bytes) -> list[str]:
    del file_bytes  # Reserved for future image/PDF QR decoding.
    identifiers: list[str] = []
    for pattern in (QR_MARKER_PATTERN, QR_TOKEN_FILENAME_PATTERN):
        for match in pattern.finditer(original_filename):
            token = match.group(1).lower()
            if token not in identifiers:
                identifiers.append(token)
    return identifiers


def lookup_qr_match(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    qr_identifier: str,
) -> QrMatchResult | None:
    normalized = qr_identifier.strip().lower()
    if not normalized:
        return None
    row = db.scalars(
        select(TeacherAssistV2AssignmentPrintPage).where(
            TeacherAssistV2AssignmentPrintPage.assignment_id == assignment_id,
            TeacherAssistV2AssignmentPrintPage.qr_token == normalized,
        )
    ).first()
    if row is None:
        return None
    return QrMatchResult(
        qr_identifier=normalized,
        packet_id=row.packet_id,
        student_number=row.student_number,
        page_number=row.page_number,
    )

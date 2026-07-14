"""TeacherAssist v2 QR matching foundation for student work intake."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_v2_assignment_print_packet import (
    TeacherAssistV2AssignmentPrintPacket,
)
from oziebot_api.models.teacher_assist_v2_assignment_print_page import (
    TeacherAssistV2AssignmentPrintPage,
)

QR_TOKEN_FILENAME_PATTERN = re.compile(r"(?:^|[-_])([a-f0-9]{32})(?:[-_.]|$)", re.IGNORECASE)
STUDENT_NUMBER_FILENAME_PATTERN = re.compile(r"student[-_ ]?(\d+)", re.IGNORECASE)
QR_MARKER_PATTERN = re.compile(r"qr[-_ ]?([a-f0-9]{32})", re.IGNORECASE)


@dataclass(frozen=True)
class QrMatchResult:
    qr_identifier: str
    packet_id: uuid.UUID | None
    student_number: int
    page_number: int


def _resolve_assignment_packet_id(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    packet_id: uuid.UUID | str | None = None,
) -> uuid.UUID | None:
    if packet_id is not None:
        try:
            normalized_packet_id = uuid.UUID(str(packet_id))
        except (TypeError, ValueError):
            normalized_packet_id = None
        if normalized_packet_id is not None:
            row = db.scalars(
                select(TeacherAssistV2AssignmentPrintPacket.id).where(
                    TeacherAssistV2AssignmentPrintPacket.id == normalized_packet_id,
                    TeacherAssistV2AssignmentPrintPacket.assignment_id == assignment_id,
                )
            ).first()
            if row is not None:
                return row
    return db.scalars(
        select(TeacherAssistV2AssignmentPrintPacket.id)
        .where(TeacherAssistV2AssignmentPrintPacket.assignment_id == assignment_id)
        .order_by(TeacherAssistV2AssignmentPrintPacket.created_at.desc())
    ).first()


def parse_student_number_from_filename(filename: str) -> int | None:
    match = STUDENT_NUMBER_FILENAME_PATTERN.search(filename)
    if match is None:
        return None
    value = int(match.group(1))
    return value if value > 0 else None


def extract_qr_identifiers(
    *,
    original_filename: str,
    file_bytes: bytes,
    mime_type: str | None = None,
) -> list[str]:
    from oziebot_api.services.teacher_assist_v2.qr_decoding import extract_qr_identifiers_from_file

    identifiers = extract_qr_identifiers_from_file(
        file_bytes=file_bytes,
        mime_type=mime_type,
        original_filename=original_filename,
    )
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


def resolve_qr_match_from_content(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    content: str,
) -> QrMatchResult | None:
    from oziebot_api.services.teacher_assist_v2.qr_decoding import parse_qr_identifier_from_content

    token = parse_qr_identifier_from_content(content)
    if token is not None:
        match = lookup_qr_match(db, assignment_id=assignment_id, qr_identifier=token)
        if match is not None:
            return match

    try:
        payload = json.loads(content.strip())
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("assignment_id")) != str(assignment_id):
        return None

    qr_token = payload.get("qr_token")
    student_number = payload.get("student_number")
    page_number = payload.get("page_number", 1)
    packet_id = payload.get("packet_id")
    if not isinstance(qr_token, str) or not isinstance(student_number, int) or student_number <= 0:
        return None
    normalized = qr_token.strip().lower()
    if len(normalized) != 32:
        return None

    row = db.scalars(
        select(TeacherAssistV2AssignmentPrintPage).where(
            TeacherAssistV2AssignmentPrintPage.assignment_id == assignment_id,
            TeacherAssistV2AssignmentPrintPage.qr_token == normalized,
        )
    ).first()
    if row is not None:
        return QrMatchResult(
            qr_identifier=normalized,
            packet_id=row.packet_id,
            student_number=row.student_number,
            page_number=row.page_number,
        )

    resolved_page_number = (
        int(page_number) if isinstance(page_number, int) and page_number > 0 else 1
    )
    resolved_packet_id = _resolve_assignment_packet_id(
        db,
        assignment_id=assignment_id,
        packet_id=packet_id,
    )
    return QrMatchResult(
        qr_identifier=normalized,
        packet_id=resolved_packet_id,
        student_number=student_number,
        page_number=resolved_page_number,
    )

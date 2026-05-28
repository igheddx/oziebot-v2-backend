from __future__ import annotations

import base64
from datetime import UTC, datetime
import io
import json
import uuid
from typing import Any

import qrcode
from qrcode.image.svg import SvgPathImage
from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_print_packet import TeacherAssistAssignmentPrintPacket
from oziebot_api.models.teacher_assist_assignment_print_page import TeacherAssistAssignmentPrintPage
from oziebot_api.services.teacher_assist.assignments import get_assignment_or_404
from oziebot_api.services.teacher_assist.constants import (
    validate_assignment_print_output_format,
    validate_assignment_print_packet_status,
    validate_assignment_print_template_type,
)
from oziebot_api.services.teacher_assist.instructional_plan_validator import contains_pii_like_content
from oziebot_api.services.teacher_assist.setup import get_class_or_404

QR_PACKET_VERSION = "teacher_assist_assignment_packet_v1"


def _compact_qr_content(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def render_qr_svg_data_uri(payload: dict[str, Any]) -> str:
    qr = qrcode.make(_compact_qr_content(payload), image_factory=SvgPathImage, box_size=6, border=2)
    output = io.BytesIO()
    qr.save(output)
    svg_bytes = output.getvalue()
    return "data:image/svg+xml;base64," + base64.b64encode(svg_bytes).decode("ascii")


def _build_qr_payload(
    *,
    packet_id: uuid.UUID,
    assignment: TeacherAssistAssignment,
    student_number: int,
    page_number: int,
    qr_token: str,
) -> dict[str, Any]:
    payload = {
        "qr_version": QR_PACKET_VERSION,
        "packet_id": str(packet_id),
        "assignment_id": str(assignment.id),
        "teacher_user_id": str(assignment.teacher_user_id),
        "tenant_id": str(assignment.tenant_id),
        "school_year_id": str(assignment.school_year_id),
        "grading_period_id": str(assignment.grading_period_id) if assignment.grading_period_id else None,
        "class_id": str(assignment.class_id),
        "subject_id": str(assignment.subject_id),
        "student_number": student_number,
        "page_number": page_number,
        "qr_token": qr_token,
    }
    if contains_pii_like_content(payload):
        raise ValueError("QR payload contains disallowed PII-like content")
    return payload


def get_print_packet_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    packet_id: uuid.UUID,
) -> TeacherAssistAssignmentPrintPacket:
    row = db.scalars(
        select(TeacherAssistAssignmentPrintPacket).where(
            TeacherAssistAssignmentPrintPacket.id == packet_id,
            TeacherAssistAssignmentPrintPacket.tenant_id == tenant_id,
            TeacherAssistAssignmentPrintPacket.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Assignment print packet not found")
    return row


def get_print_packet_page_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    page_id: uuid.UUID,
) -> TeacherAssistAssignmentPrintPage:
    row = db.scalars(
        select(TeacherAssistAssignmentPrintPage)
        .join(
            TeacherAssistAssignmentPrintPacket,
            TeacherAssistAssignmentPrintPacket.id == TeacherAssistAssignmentPrintPage.packet_id,
        )
        .where(
            TeacherAssistAssignmentPrintPage.id == page_id,
            TeacherAssistAssignmentPrintPacket.tenant_id == tenant_id,
            TeacherAssistAssignmentPrintPacket.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Assignment print page not found")
    return row


def list_assignment_print_packets(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
) -> list[TeacherAssistAssignmentPrintPacket]:
    get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    return db.scalars(
        select(TeacherAssistAssignmentPrintPacket)
        .where(
            TeacherAssistAssignmentPrintPacket.tenant_id == tenant_id,
            TeacherAssistAssignmentPrintPacket.teacher_user_id == user_id,
            TeacherAssistAssignmentPrintPacket.assignment_id == assignment_id,
        )
        .order_by(
            TeacherAssistAssignmentPrintPacket.created_at.desc(),
            TeacherAssistAssignmentPrintPacket.updated_at.desc(),
        )
    ).all()


def list_print_packet_pages(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    packet_id: uuid.UUID,
) -> list[TeacherAssistAssignmentPrintPage]:
    get_print_packet_or_404(db, tenant_id=tenant_id, user_id=user_id, packet_id=packet_id)
    return db.scalars(
        select(TeacherAssistAssignmentPrintPage)
        .where(TeacherAssistAssignmentPrintPage.packet_id == packet_id)
        .order_by(
            TeacherAssistAssignmentPrintPage.student_number.asc(),
            TeacherAssistAssignmentPrintPage.page_number.asc(),
        )
    ).all()


def create_assignment_print_packet(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    pages_per_student: int,
    template_type: str | None,
    output_format: str | None,
) -> TeacherAssistAssignmentPrintPacket:
    if pages_per_student <= 0:
        raise ValueError("Pages per student must be greater than zero")
    assignment = get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    teacher_class = get_class_or_404(db, tenant_id=tenant_id, class_id=assignment.class_id)
    if teacher_class.student_count <= 0:
        raise ValueError("Assignments require a class with at least one student")

    normalized_template_type = validate_assignment_print_template_type(template_type)
    normalized_output_format = validate_assignment_print_output_format(output_format)
    now = datetime.now(UTC)
    packet = TeacherAssistAssignmentPrintPacket(
        tenant_id=assignment.tenant_id,
        teacher_user_id=assignment.teacher_user_id,
        assignment_id=assignment.id,
        class_id=assignment.class_id,
        school_year_id=assignment.school_year_id,
        grading_period_id=assignment.grading_period_id,
        subject_id=assignment.subject_id,
        packet_status=validate_assignment_print_packet_status("generated"),
        pages_per_student=pages_per_student,
        student_count=teacher_class.student_count,
        template_type=normalized_template_type,
        output_format=normalized_output_format,
        storage_key=None,
        created_at=now,
        updated_at=now,
    )
    db.add(packet)
    db.flush()

    pages: list[TeacherAssistAssignmentPrintPage] = []
    for student_number in range(1, teacher_class.student_count + 1):
        for page_number in range(1, pages_per_student + 1):
            qr_token = uuid.uuid4().hex
            payload = _build_qr_payload(
                packet_id=packet.id,
                assignment=assignment,
                student_number=student_number,
                page_number=page_number,
                qr_token=qr_token,
            )
            pages.append(
                TeacherAssistAssignmentPrintPage(
                    packet_id=packet.id,
                    assignment_id=assignment.id,
                    student_number=student_number,
                    page_number=page_number,
                    qr_payload_json=payload,
                    qr_token=qr_token,
                    created_at=now,
                )
            )
    db.add_all(pages)
    db.flush()
    db.refresh(packet)
    return packet

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_print_packet import TeacherAssistAssignmentPrintPacket
from oziebot_api.models.teacher_assist_assignment_print_page import TeacherAssistAssignmentPrintPage
from oziebot_api.models.teacher_assist_student_work_submission import TeacherAssistStudentWorkSubmission
from oziebot_api.services.teacher_assist.assignments import get_assignment_or_404
from oziebot_api.services.teacher_assist.constants import (
    validate_assignment_student_work_processing_status,
    validate_assignment_student_work_upload_status,
)
from oziebot_api.services.teacher_assist.instructional_plan_validator import contains_pii_like_content
from oziebot_api.services.teacher_assist.print_packets import (
    get_print_packet_or_404,
    get_print_packet_page_or_404,
)
from oziebot_api.services.teacher_assist.setup import get_class_or_404

UPLOAD_STATUS_TRANSITIONS: dict[str, set[str]] = {
    "uploaded": {"uploaded", "archived"},
    "archived": {"archived"},
}

PROCESSING_STATUS_TRANSITIONS: dict[str, set[str]] = {
    "pending_review": {"pending_review", "ready_for_processing", "processing_deferred", "archived"},
    "ready_for_processing": {"ready_for_processing", "processing_deferred", "archived"},
    "processing_deferred": {"processing_deferred", "ready_for_processing", "archived"},
    "archived": {"archived"},
}


def _validate_student_number(*, student_number: int, assignment: TeacherAssistAssignment, db: Session) -> int:
    teacher_class = get_class_or_404(db, tenant_id=assignment.tenant_id, class_id=assignment.class_id)
    if student_number < 1:
        raise ValueError("Student number must be greater than zero")
    if student_number > teacher_class.student_count:
        raise ValueError("Student number must fall within the selected class roster range")
    return student_number


def _validate_submission_metadata(
    *,
    original_filename: str,
    mime_type: str,
    file_size: int,
    storage_key: str,
) -> tuple[str, str, int, str]:
    normalized_filename = Path(original_filename.strip()).name
    normalized_mime_type = mime_type.strip()
    normalized_storage_key = storage_key.strip()
    if not normalized_filename:
        raise ValueError("Uploaded file must include a filename")
    if not normalized_mime_type:
        raise ValueError("Uploaded file must include a mime type")
    if not normalized_storage_key:
        raise ValueError("Uploaded file must include a storage key")
    if file_size <= 0:
        raise ValueError("Uploaded file is empty")
    if contains_pii_like_content(
        {
            "original_filename": normalized_filename,
            "mime_type": normalized_mime_type,
            "storage_key": normalized_storage_key,
        }
    ):
        raise ValueError("Student-work metadata contains disallowed PII-like content")
    return normalized_filename, normalized_mime_type, file_size, normalized_storage_key


def _normalize_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment: TeacherAssistAssignment,
    student_number: int,
    assignment_print_packet_id: uuid.UUID | None,
    assignment_print_page_id: uuid.UUID | None,
) -> tuple[TeacherAssistAssignmentPrintPacket | None, TeacherAssistAssignmentPrintPage | None]:
    packet: TeacherAssistAssignmentPrintPacket | None = None
    page: TeacherAssistAssignmentPrintPage | None = None
    if assignment_print_packet_id is not None:
        packet = get_print_packet_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            packet_id=assignment_print_packet_id,
        )
        if packet.assignment_id != assignment.id:
            raise ValueError("Print packet must belong to the selected assignment")
    if assignment_print_page_id is not None:
        page = get_print_packet_page_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            page_id=assignment_print_page_id,
        )
        if page.assignment_id != assignment.id:
            raise ValueError("Print page must belong to the selected assignment")
        if page.student_number != student_number:
            raise ValueError("Print page student number must match the submission student number")
        if packet is not None and packet.id != page.packet_id:
            raise ValueError("Print page must belong to the selected print packet")
        if packet is None:
            packet = page.packet
    return packet, page


def _validate_upload_status_transition(*, current_status: str, next_status: str) -> str:
    normalized_next = validate_assignment_student_work_upload_status(next_status)
    allowed = UPLOAD_STATUS_TRANSITIONS[validate_assignment_student_work_upload_status(current_status)]
    if normalized_next not in allowed:
        raise ValueError(f"Upload status cannot transition from {current_status} to {normalized_next}")
    return normalized_next


def _validate_processing_status_transition(*, current_status: str, next_status: str) -> str:
    normalized_next = validate_assignment_student_work_processing_status(next_status)
    allowed = PROCESSING_STATUS_TRANSITIONS[
        validate_assignment_student_work_processing_status(current_status)
    ]
    if normalized_next not in allowed:
        raise ValueError(f"Processing status cannot transition from {current_status} to {normalized_next}")
    return normalized_next


def get_student_work_submission_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    submission_id: uuid.UUID,
) -> TeacherAssistStudentWorkSubmission:
    row = db.scalars(
        select(TeacherAssistStudentWorkSubmission).where(
            TeacherAssistStudentWorkSubmission.id == submission_id,
            TeacherAssistStudentWorkSubmission.tenant_id == tenant_id,
            TeacherAssistStudentWorkSubmission.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Student work submission not found")
    return row


def list_assignment_student_work_submissions(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
) -> list[TeacherAssistStudentWorkSubmission]:
    get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    return db.scalars(
        select(TeacherAssistStudentWorkSubmission)
        .where(
            TeacherAssistStudentWorkSubmission.tenant_id == tenant_id,
            TeacherAssistStudentWorkSubmission.teacher_user_id == user_id,
            TeacherAssistStudentWorkSubmission.assignment_id == assignment_id,
        )
        .order_by(
            TeacherAssistStudentWorkSubmission.student_number.asc(),
            TeacherAssistStudentWorkSubmission.created_at.desc(),
        )
    ).all()


def create_student_work_submission(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    student_number: int,
    original_filename: str,
    mime_type: str,
    file_size: int,
    storage_key: str,
    assignment_print_packet_id: uuid.UUID | None = None,
    assignment_print_page_id: uuid.UUID | None = None,
) -> TeacherAssistStudentWorkSubmission:
    assignment = get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    normalized_student_number = _validate_student_number(
        student_number=student_number,
        assignment=assignment,
        db=db,
    )
    normalized_filename, normalized_mime_type, normalized_file_size, normalized_storage_key = (
        _validate_submission_metadata(
            original_filename=original_filename,
            mime_type=mime_type,
            file_size=file_size,
            storage_key=storage_key,
        )
    )
    packet, page = _normalize_context(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment=assignment,
        student_number=normalized_student_number,
        assignment_print_packet_id=assignment_print_packet_id,
        assignment_print_page_id=assignment_print_page_id,
    )
    now = datetime.now(UTC)
    row = TeacherAssistStudentWorkSubmission(
        tenant_id=assignment.tenant_id,
        teacher_user_id=assignment.teacher_user_id,
        assignment_id=assignment.id,
        assignment_print_packet_id=packet.id if packet is not None else None,
        assignment_print_page_id=page.id if page is not None else None,
        school_year_id=assignment.school_year_id,
        grading_period_id=assignment.grading_period_id,
        class_id=assignment.class_id,
        subject_id=assignment.subject_id,
        student_number=normalized_student_number,
        original_filename=normalized_filename,
        mime_type=normalized_mime_type,
        file_size=normalized_file_size,
        storage_key=normalized_storage_key,
        upload_status=validate_assignment_student_work_upload_status("uploaded"),
        processing_status=validate_assignment_student_work_processing_status("pending_review"),
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_student_work_submission_processing_status(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    submission_id: uuid.UUID,
    processing_status: str,
) -> TeacherAssistStudentWorkSubmission:
    row = get_student_work_submission_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_id=submission_id,
    )
    row.processing_status = _validate_processing_status_transition(
        current_status=row.processing_status,
        next_status=processing_status,
    )
    if row.processing_status == "archived":
        row.upload_status = _validate_upload_status_transition(
            current_status=row.upload_status,
            next_status="archived",
        )
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row


def link_student_work_submission_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    submission_id: uuid.UUID,
    assignment_print_packet_id: uuid.UUID | None,
    assignment_print_page_id: uuid.UUID | None,
) -> TeacherAssistStudentWorkSubmission:
    row = get_student_work_submission_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_id=submission_id,
    )
    assignment = get_assignment_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=row.assignment_id,
    )
    packet, page = _normalize_context(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment=assignment,
        student_number=row.student_number,
        assignment_print_packet_id=assignment_print_packet_id,
        assignment_print_page_id=assignment_print_page_id,
    )
    row.assignment_print_packet_id = packet.id if packet is not None else None
    row.assignment_print_page_id = page.id if page is not None else None
    row.updated_at = datetime.now(UTC)
    db.flush()
    return row

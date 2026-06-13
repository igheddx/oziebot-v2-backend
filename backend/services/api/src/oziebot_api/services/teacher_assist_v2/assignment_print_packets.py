"""V2 assignment QR print packet generation."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_print_packet import TeacherAssistV2AssignmentPrintPacket
from oziebot_api.models.teacher_assist_v2_assignment_print_page import TeacherAssistV2AssignmentPrintPage
from oziebot_api.models.teacher_assist_v2_onboarding import TeacherAssistV2Onboarding
from oziebot_api.services.teacher_assist.print_packets import render_qr_svg_data_uri
from oziebot_api.services.teacher_assist_v2.assignment_constants import PRINT_PACKET_KINDS
from oziebot_api.services.teacher_assist_v2.package_export import save_teacher_assist_export_bytes
from oziebot_api.services.teacher_assist_v2.quiz_exports import safe_export_filename
from oziebot_api.services.teacher_assist_v2.student_packet_docx import (
    COVER_SHEET_DOCX_MIME,
    build_cover_sheet_docx_bytes,
)
from oziebot_api.services.teacher_assist_v2.submission_intake_constants import QR_PACKET_VERSION


def _now() -> datetime:
    return datetime.now(UTC)


def resolve_student_count(db: Session, *, teacher_user_id: uuid.UUID, fallback: int = 5) -> int:
    onboarding = db.scalars(
        select(TeacherAssistV2Onboarding).where(TeacherAssistV2Onboarding.user_id == teacher_user_id)
    ).one_or_none()
    if onboarding and onboarding.student_count and onboarding.student_count > 0:
        return onboarding.student_count
    return fallback


def _build_qr_payload(
    *,
    packet_id: uuid.UUID,
    assignment: TeacherAssistV2Assignment,
    student_number: int,
    page_number: int,
    qr_token: str,
) -> dict[str, Any]:
    return {
        "qr_version": QR_PACKET_VERSION,
        "packet_id": str(packet_id),
        "assignment_id": str(assignment.id),
        "teacher_user_id": str(assignment.teacher_user_id),
        "tenant_id": str(assignment.tenant_id),
        "school_year_id": str(assignment.platform_school_year_id),
        "catalog_district_id": str(assignment.catalog_district_id),
        "catalog_school_id": str(assignment.catalog_school_id) if assignment.catalog_school_id else None,
        "catalog_grade_id": str(assignment.catalog_grade_id),
        "catalog_subject_id": str(assignment.catalog_subject_id),
        "student_number": student_number,
        "page_number": page_number,
        "qr_token": qr_token,
    }


def render_qr_cover_sheet_html(
    *,
    assignment: TeacherAssistV2Assignment,
    packet: TeacherAssistV2AssignmentPrintPacket,
    pages: list[TeacherAssistV2AssignmentPrintPage],
) -> str:
    from oziebot_api.services.teacher_assist_v2.package_export import _esc

    page_blocks = []
    for page in pages:
        qr_uri = render_qr_svg_data_uri(page.qr_payload_json)
        student_label = f"Student #{page.student_number:03d}"
        page_blocks.append(
            f"<section style='page-break-after:always;border:2px dashed #94a3b8;border-radius:16px;padding:1.5rem;margin:1rem 0;min-height:9.5in;box-sizing:border-box'>"
            f"<div style='display:flex;justify-content:space-between;align-items:flex-start;gap:1rem'>"
            f"<div><p style='font-size:12px;color:#64748b;text-transform:uppercase;letter-spacing:.08em'>Cover sheet</p>"
            f"<h2 style='margin:.25rem 0 0'>{_esc(assignment.title)}</h2>"
            f"<p style='font-size:1.25rem;font-weight:700;margin:.75rem 0 0'>{student_label}</p></div>"
            f"<img src='{qr_uri}' alt='QR code' width='132' height='132'/></div>"
            f"<div style='margin-top:2rem;border-top:1px solid #cbd5e1;padding-top:1rem'>"
            f"<p><strong>Teacher instructions</strong></p>"
            f"<ol style='margin:.5rem 0 0 1.25rem;line-height:1.6'>"
            f"<li>Staple this cover sheet to the front of the student's completed assignment.</li>"
            f"<li>Keep each student's pages together before scanning.</li>"
            f"<li>Upload the class batch in TeacherAssist to match grades automatically.</li>"
            f"</ol></div>"
            f"<div style='margin-top:2.5rem;border:1px dashed #cbd5e1;border-radius:12px;padding:1rem;background:#f8fafc'>"
            f"<p style='font-size:12px;color:#64748b;margin:0 0 .5rem'>Attach external assignment pages below this line</p>"
            f"<div style='height:6rem'></div></div>"
            f"<p style='font-size:11px;color:#64748b;margin-top:1.5rem'>"
            f"Assignment ID: {_esc(assignment.id)} · {student_label}</p></section>"
        )
    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<title>{_esc(assignment.title)} Cover Sheets</title>"
        "<style>body{font-family:system-ui,sans-serif;max-width:900px;margin:2rem auto;line-height:1.5}"
        "@media print{.print-btn{display:none}} .print-btn{margin:1rem 0}</style></head><body>"
        "<button class='print-btn' onclick='window.print()'>Print cover sheets</button>"
        f"<h1>{_esc(assignment.title)} — Student Cover Sheets</h1>"
        f"<p>{packet.student_count} students · staple one cover sheet per student before scanning</p>"
        + "".join(page_blocks)
        + "</body></html>"
    )


def render_qr_student_packet_html(
    *,
    assignment: TeacherAssistV2Assignment,
    packet: TeacherAssistV2AssignmentPrintPacket,
    pages: list[TeacherAssistV2AssignmentPrintPage],
    assignment_content: dict[str, Any],
) -> str:
    from oziebot_api.services.teacher_assist_v2.package_export import _esc

    page_blocks = []
    for page in pages:
        qr_uri = render_qr_svg_data_uri(page.qr_payload_json)
        student_label = f"Student #{page.student_number:03d}"
        page_blocks.append(
            f"<section style='page-break-after:always;border:1px solid #cbd5e1;border-radius:16px;padding:1.25rem;margin:1rem 0'>"
            f"<div style='display:flex;justify-content:flex-start;align-items:flex-start;gap:1rem'>"
            f"<img src='{qr_uri}' alt='QR code' width='120' height='120'/>"
            f"<div><p style='font-size:12px;color:#64748b'>Assignment ID: {_esc(assignment.id)}</p>"
            f"<h2>{_esc(assignment.title)}</h2><p><strong>{student_label}</strong></p></div></div>"
            f"<p><strong>Objective:</strong> {_esc(assignment_content.get('objective_text') or assignment.description)}</p>"
            f"<h3>Instructions</h3><ul>"
            + "".join(f"<li>{_esc(item)}</li>" for item in assignment_content.get("student_instructions") or [])
            + "</ul>"
            f"<h3>{_esc(assignment_content.get('passage_title'))}</h3>"
            f"<p>{_esc(assignment_content.get('passage_text'))}</p>"
            f"<h3>Writing Page</h3>"
            + "".join("<div style='border-bottom:1px solid #cbd5e1;height:1.75rem;margin:.5rem 0'></div>" for _ in range(10))
            + f"<p style='font-size:11px;color:#64748b;margin-top:1rem'>"
            f"Assignment ID: {_esc(assignment.id)} · {student_label} · Page {page.page_number}</p></section>"
        )
    return (
        "<!DOCTYPE html><html><head><meta charset='utf-8'>"
        f"<title>{_esc(assignment.title)} QR Student Packet</title>"
        "<style>body{font-family:system-ui,sans-serif;max-width:900px;margin:2rem auto;line-height:1.5}"
        "@media print{.print-btn{display:none}} .print-btn{margin:1rem 0}</style></head><body>"
        "<button class='print-btn' onclick='window.print()'>Print</button>"
        f"<h1>{_esc(assignment.title)} — QR Student Packet</h1>"
        f"<p>{packet.student_count} students · {packet.pages_per_student} page(s) each</p>"
        + "".join(page_blocks)
        + "</body></html>"
    )


def _cover_sheet_storage_filename(assignment_id: uuid.UUID) -> str:
    return f"v2-assignment-{assignment_id}-cover-sheets.docx"


def _create_print_packet_with_pages(
    db: Session,
    *,
    assignment: TeacherAssistV2Assignment,
    student_count: int | None,
    pages_per_student: int,
    packet_kind: str,
) -> tuple[TeacherAssistV2AssignmentPrintPacket, list[TeacherAssistV2AssignmentPrintPage], int]:
    if packet_kind not in PRINT_PACKET_KINDS:
        raise ValueError(f"Unsupported print packet kind '{packet_kind}'")

    count = student_count or resolve_student_count(db, teacher_user_id=assignment.teacher_user_id)
    if count < 1:
        count = 5

    _delete_assignment_print_packets(db, assignment_id=assignment.id, packet_kind=packet_kind)

    now = _now()
    packet = TeacherAssistV2AssignmentPrintPacket(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=assignment.teacher_user_id,
        assignment_id=assignment.id,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        packet_status="GENERATED",
        packet_kind=packet_kind,
        pages_per_student=pages_per_student,
        student_count=count,
        created_at=now,
        updated_at=now,
    )
    db.add(packet)
    db.flush()

    pages: list[TeacherAssistV2AssignmentPrintPage] = []
    for student_number in range(1, count + 1):
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
                TeacherAssistV2AssignmentPrintPage(
                    id=uuid.uuid4(),
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
    return packet, pages, count


def _delete_assignment_print_packets(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    packet_kind: str,
) -> None:
    packet_ids = db.scalars(
        select(TeacherAssistV2AssignmentPrintPacket.id).where(
            TeacherAssistV2AssignmentPrintPacket.assignment_id == assignment_id,
            TeacherAssistV2AssignmentPrintPacket.packet_kind == packet_kind,
        )
    ).all()
    if not packet_ids:
        return
    db.execute(
        delete(TeacherAssistV2AssignmentPrintPage).where(
            TeacherAssistV2AssignmentPrintPage.packet_id.in_(packet_ids)
        )
    )
    db.execute(
        delete(TeacherAssistV2AssignmentPrintPacket).where(
            TeacherAssistV2AssignmentPrintPacket.id.in_(packet_ids)
        )
    )


def _generate_print_packet(
    db: Session,
    *,
    settings: Settings,
    assignment: TeacherAssistV2Assignment,
    assignment_content: dict[str, Any] | None,
    student_count: int | None,
    pages_per_student: int,
    packet_kind: str,
    html_renderer,
    storage_filename: str,
    download_title: str,
) -> dict[str, Any]:
    packet, pages, count = _create_print_packet_with_pages(
        db,
        assignment=assignment,
        student_count=student_count,
        pages_per_student=pages_per_student,
        packet_kind=packet_kind,
    )

    html_body = html_renderer(
        assignment=assignment,
        packet=packet,
        pages=pages,
        assignment_content=assignment_content or {},
    )
    storage_key = save_teacher_assist_export_bytes(
        settings=settings,
        tenant_id=assignment.tenant_id,
        filename=storage_filename,
        contents=html_body.encode("utf-8"),
        mime_type="text/html",
    )
    from oziebot_api.services.teacher_assist.storage import get_teacher_assist_download_url

    download_url = get_teacher_assist_download_url(
        settings,
        storage_key=storage_key,
        original_filename=download_title,
        mime_type="text/html",
    )
    return {
        "packet_id": str(packet.id),
        "packet_kind": packet_kind,
        "student_count": count,
        "pages_per_student": pages_per_student,
        "preview_html": html_body,
        "download_url": download_url,
        "storage_key": storage_key,
        "title": download_title,
    }


def generate_assignment_print_packet(
    db: Session,
    *,
    settings: Settings,
    assignment: TeacherAssistV2Assignment,
    assignment_content: dict[str, Any],
    student_count: int | None = None,
    pages_per_student: int = 1,
) -> dict[str, Any]:
    return _generate_print_packet(
        db,
        settings=settings,
        assignment=assignment,
        assignment_content=assignment_content,
        student_count=student_count,
        pages_per_student=pages_per_student,
        packet_kind="STUDENT_PACKET",
        html_renderer=lambda **kwargs: render_qr_student_packet_html(
            assignment=kwargs["assignment"],
            packet=kwargs["packet"],
            pages=kwargs["pages"],
            assignment_content=kwargs["assignment_content"],
        ),
        storage_filename=f"v2-assignment-{assignment.id}-qr-packet.html",
        download_title=f"{assignment.title} QR Student Packet.html",
    )


def generate_assignment_cover_sheets(
    db: Session,
    *,
    settings: Settings,
    assignment: TeacherAssistV2Assignment,
    student_count: int | None = None,
) -> dict[str, Any]:
    packet, pages, count = _create_print_packet_with_pages(
        db,
        assignment=assignment,
        student_count=student_count,
        pages_per_student=1,
        packet_kind="COVER_SHEET",
    )
    docx_pages = [
        {
            "student_number": page.student_number,
            "qr_payload_json": page.qr_payload_json,
        }
        for page in pages
    ]
    docx_bytes = build_cover_sheet_docx_bytes(
        assignment_title=assignment.title,
        pages=docx_pages,
    )
    storage_filename = _cover_sheet_storage_filename(assignment.id)
    download_title = safe_export_filename(assignment.title, "Cover_Sheets", "docx")
    storage_key = save_teacher_assist_export_bytes(
        settings=settings,
        tenant_id=assignment.tenant_id,
        filename=storage_filename,
        contents=docx_bytes,
        mime_type=COVER_SHEET_DOCX_MIME,
    )
    from oziebot_api.services.teacher_assist.storage import get_teacher_assist_download_url

    download_url = get_teacher_assist_download_url(
        settings,
        storage_key=storage_key,
        original_filename=download_title,
        mime_type=COVER_SHEET_DOCX_MIME,
    )
    return {
        "packet_id": str(packet.id),
        "packet_kind": "COVER_SHEET",
        "format": "docx",
        "student_count": count,
        "pages_per_student": 1,
        "download_url": download_url,
        "storage_key": storage_key,
        "title": download_title,
    }


def get_assignment_cover_sheets(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    settings: Settings,
) -> dict[str, Any] | None:
    packet = db.scalars(
        select(TeacherAssistV2AssignmentPrintPacket)
        .where(
            TeacherAssistV2AssignmentPrintPacket.assignment_id == assignment_id,
            TeacherAssistV2AssignmentPrintPacket.packet_kind == "COVER_SHEET",
        )
        .order_by(TeacherAssistV2AssignmentPrintPacket.created_at.desc())
    ).first()
    if packet is None:
        return None
    from oziebot_api.services.teacher_assist.storage import get_teacher_assist_download_url

    assignment = db.get(TeacherAssistV2Assignment, assignment_id)
    download_title = safe_export_filename(
        assignment.title if assignment is not None else "Assignment",
        "Cover_Sheets",
        "docx",
    )
    storage_key = _cover_sheet_storage_filename(assignment_id)
    return {
        "packet_id": str(packet.id),
        "packet_kind": packet.packet_kind,
        "format": "docx",
        "student_count": packet.student_count,
        "pages_per_student": packet.pages_per_student,
        "download_url": get_teacher_assist_download_url(
            settings,
            storage_key=storage_key,
            original_filename=download_title,
            mime_type=COVER_SHEET_DOCX_MIME,
        ),
        "title": download_title,
    }

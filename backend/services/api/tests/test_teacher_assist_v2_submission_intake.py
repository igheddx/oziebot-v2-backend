from __future__ import annotations

import io
import uuid
from datetime import UTC, datetime

from sqlalchemy import select

from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_print_packet import (
    TeacherAssistV2AssignmentPrintPacket,
)
from oziebot_api.models.teacher_assist_v2_assignment_print_page import (
    TeacherAssistV2AssignmentPrintPage,
)
from tests.test_teacher_assist_v2_planning import _ready_teacher_token


def _written_assignment_id(client, headers) -> str:
    rows = client.get("/v1/teacher-assist-v2/teacher/assignments", headers=headers).json()
    written = next(row for row in rows if row["assignment_type"] == "WRITTEN_ASSIGNMENT")
    return written["id"]


def _generate_week1_package(client, headers) -> None:
    form = client.get("/v1/teacher-assist-v2/teacher/planning/form", headers=headers)
    teaching_order = form.json()["default_teaching_order"]
    package = client.post(
        "/v1/teacher-assist-v2/teacher/planning/packages/generate",
        headers=headers,
        json={
            "week_start": 1,
            "week_end": 1,
            "teaching_order": teaching_order,
            "selected_outputs": ["daily_lesson_plan", "subject_slide_deck", "assignment"],
        },
    )
    assert package.status_code == 201, package.text


def test_v2_submission_intake_single_student_upload(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)

    upload = client.post(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/submission-batches",
        headers=headers,
        files={"file": ("student-2-work.pdf", io.BytesIO(b"%PDF-1.4 test"), "application/pdf")},
        data={"student_number": "2"},
    )
    assert upload.status_code == 201, upload.text
    payload = upload.json()
    assert payload["submissions"]
    submission = payload["submissions"][0]
    assert submission["student_number"] == 2
    assert submission["match_method"] == "MANUAL"
    assert submission["status"] in {"PROCESSING", "READY_FOR_REVIEW"}


def test_v2_submission_intake_rejects_unmatched_batch(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)

    upload = client.post(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/submission-batches",
        headers=headers,
        files={"file": ("scan-batch.pdf", io.BytesIO(b"%PDF-1.4 batch"), "application/pdf")},
    )
    assert upload.status_code == 400, upload.text
    assert "QR codes" in upload.json()["detail"] or "matched" in upload.json()["detail"]


def test_v2_submission_intake_qr_filename_match(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    assignment = db_session.scalar(
        select(TeacherAssistV2Assignment).where(
            TeacherAssistV2Assignment.id == uuid.UUID(assignment_id)
        )
    )
    assert assignment is not None

    now = datetime.now(UTC)
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
        packet_kind="STUDENT_PACKET",
        pages_per_student=1,
        student_count=20,
        created_at=now,
        updated_at=now,
    )
    db_session.add(packet)
    db_session.flush()
    qr_token = uuid.uuid4().hex
    page = TeacherAssistV2AssignmentPrintPage(
        id=uuid.uuid4(),
        packet_id=packet.id,
        assignment_id=assignment.id,
        student_number=7,
        page_number=1,
        qr_payload_json={
            "qr_token": qr_token,
            "student_number": 7,
            "assignment_id": str(assignment.id),
        },
        qr_token=qr_token,
        created_at=now,
    )
    db_session.add(page)
    db_session.commit()

    upload = client.post(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/submission-batches",
        headers=headers,
        files={
            "file": (
                f"packet-qr_{qr_token}.pdf",
                io.BytesIO(b"%PDF-1.4 qr"),
                "application/pdf",
            )
        },
    )
    assert upload.status_code == 201, upload.text
    submission = upload.json()["submissions"][0]
    assert submission["student_number"] == 7
    assert submission["match_method"] == "QR"
    assert submission["status"] in {"PROCESSING", "READY_FOR_REVIEW"}

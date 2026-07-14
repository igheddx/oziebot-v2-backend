from __future__ import annotations

import io

from tests.test_teacher_assist_v2_submission_intake import (
    _generate_week1_package,
    _written_assignment_id,
)
from tests.test_teacher_assist_v2_planning import _ready_teacher_token


def test_v2_generate_ai_grading_draft_for_ready_submission(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)

    upload = client.post(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/submission-batches",
        headers=headers,
        files={"file": ("student-4-work.pdf", io.BytesIO(b"%PDF-1.4 test"), "application/pdf")},
        data={"student_number": "4"},
    )
    assert upload.status_code == 201, upload.text
    submission_id = upload.json()["submissions"][0]["id"]

    ready = client.patch(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/status",
        headers=headers,
        json={"status": "READY_FOR_GRADING"},
    )
    assert ready.status_code == 200, ready.text

    job = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grading-jobs",
        headers=headers,
    )
    assert job.status_code == 201, job.text
    payload = job.json()
    assert payload["status"] == "COMPLETED"
    assert payload["draft"]["score"] is not None
    assert payload["draft"]["rubric_json"]["sections"]
    assert payload["draft"]["teacher_comment_draft"]
    assert payload["draft"]["objective_evidence"]
    assert payload["draft"]["teacher_review_required"] is True

    detail = client.get(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}", headers=headers
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["grading_draft"]

    assignment_detail = client.get(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}",
        headers=headers,
    )
    assert assignment_detail.status_code == 200, assignment_detail.text
    summary = assignment_detail.json()["submission_summary"]
    assert summary["grading_complete_count"] >= 1
    assert summary["teacher_reviewed_count"] == 0


def test_v2_rejects_grading_for_non_ready_submission(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)

    upload = client.post(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/submission-batches",
        headers=headers,
        files={"file": ("student-1-work.pdf", io.BytesIO(b"%PDF-1.4 test"), "application/pdf")},
        data={"student_number": "1"},
    )
    submission_id = upload.json()["submissions"][0]["id"]

    job = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grading-jobs",
        headers=headers,
    )
    assert job.status_code == 400, job.text

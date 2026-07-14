from __future__ import annotations

import io

from tests.test_teacher_assist_v2_submission_intake import (
    _generate_week1_package,
    _written_assignment_id,
)
from tests.test_teacher_assist_v2_planning import _ready_teacher_token


def _upload_ready_submission(client, headers, assignment_id: str, student_number: int = 4) -> str:
    upload = client.post(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/submission-batches",
        headers=headers,
        files={
            "file": (
                f"student-{student_number}-work.pdf",
                io.BytesIO(b"%PDF-1.4 test"),
                "application/pdf",
            )
        },
        data={"student_number": str(student_number)},
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
    return submission_id


def test_v2_submission_review_view_is_idempotent(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    submission_id = _upload_ready_submission(client, headers, assignment_id)

    first = client.get(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}", headers=headers
    )
    second = client.get(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}", headers=headers
    )
    assert first.status_code == 200, first.text
    assert second.status_code == 200, second.text
    assert first.json()["teacher_viewed_for_review"] is True
    assert second.json()["teacher_viewed_for_review"] is True


def test_v2_accept_ai_grade_after_viewing_submission(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    submission_id = _upload_ready_submission(client, headers, assignment_id)

    detail = client.get(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}", headers=headers
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["teacher_viewed_for_review"] is True
    assert detail.json()["grading_draft"] is not None
    assert detail.json()["assignment_grade"] is None

    accept = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grade-review/accept",
        headers=headers,
    )
    assert accept.status_code == 201, accept.text
    grade = accept.json()
    assert grade["status"] == "CONFIRMED"
    assert grade["review_action"] == "ACCEPT"
    assert grade["is_official"] is True
    assert grade["confirmed_by"] is not None

    audit = client.get(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grade-audit-history",
        headers=headers,
    )
    assert audit.status_code == 200, audit.text
    events = audit.json()
    assert len(events) == 1
    assert events[0]["original_ai_score"] is not None
    assert events[0]["final_score"] == grade["score"]

    assignment_detail = client.get(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}",
        headers=headers,
    )
    assert assignment_detail.status_code == 200, assignment_detail.text
    payload = assignment_detail.json()
    assert payload["completion_summary"]["grades_confirmed_count"] == 1
    assert payload["submission_summary"]["teacher_reviewed_count"] == 1


def test_v2_modify_grade_requires_override_reason(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    submission_id = _upload_ready_submission(client, headers, assignment_id, student_number=5)

    client.get(f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}", headers=headers)

    draft = client.get(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grading-draft",
        headers=headers,
    ).json()

    modify = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grade-review/modify",
        headers=headers,
        json={
            "score": draft["score"] - 5,
            "max_score": draft["max_score"],
            "teacher_comment": "Adjusted after review.",
            "rubric_json": draft["rubric_json"],
            "teacher_override_reason": "Student showed partial understanding.",
        },
    )
    assert modify.status_code == 201, modify.text
    grade = modify.json()
    assert grade["status"] == "REVISED"
    assert grade["review_action"] == "MODIFY"
    assert grade["teacher_override_reason"] == "Student showed partial understanding."


def test_v2_bulk_accept_requires_viewed_submissions(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    submission_id = _upload_ready_submission(client, headers, assignment_id, student_number=6)

    bulk = client.post(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/grade-review/accept-all-viewed",
        headers=headers,
    )
    assert bulk.status_code == 400, bulk.text

    client.get(f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}", headers=headers)

    bulk = client.post(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/grade-review/accept-all-viewed",
        headers=headers,
    )
    assert bulk.status_code == 200, bulk.text
    assert bulk.json()["accepted_count"] == 1

    home = client.get("/v1/teacher-assist-v2/teacher/home", headers=headers)
    assert home.status_code == 200, home.text
    assert home.json()["assignments_requiring_review_count"] == 0

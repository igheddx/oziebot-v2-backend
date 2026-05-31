from __future__ import annotations

import io

from tests.test_teacher_assist_v2_grade_reviews import _upload_ready_submission
from tests.test_teacher_assist_v2_submission_intake import (
    _generate_week1_package,
    _written_assignment_id,
)
from tests.test_teacher_assist_v2_planning import _ready_teacher_token


def test_v2_confirmed_grade_syncs_gradebook_and_mastery(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    submission_id = _upload_ready_submission(client, headers, assignment_id, student_number=1)

    accept = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grade-review/accept",
        headers=headers,
    )
    assert accept.status_code == 201, accept.text

    gradebook = client.get("/v1/teacher-assist-v2/teacher/gradebook", headers=headers)
    assert gradebook.status_code == 200, gradebook.text
    records = gradebook.json()
    assert len(records) == 1
    assert records[0]["assignment_id"] == assignment_id
    assert records[0]["student_number"] == 1
    assert records[0]["sync_status"] == "SYNCED"

    mastery = client.get("/v1/teacher-assist-v2/teacher/mastery", headers=headers)
    assert mastery.status_code == 200, mastery.text
    evidence = mastery.json()
    assert len(evidence) >= 1
    assert evidence[0]["student_number"] == 1
    assert evidence[0]["teacher_confirmed"] is True
    assert evidence[0]["is_current"] is True

    assignment_detail = client.get(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}",
        headers=headers,
    )
    assert assignment_detail.status_code == 200, assignment_detail.text
    payload = assignment_detail.json()
    assert payload["gradebook_summary"]["confirmed_grades_count"] == 1
    assert payload["gradebook_summary"]["gradebook_sync_status"] == "SYNCED"
    assert len(payload["objective_performance"]) >= 1
    assert payload["objective_performance"][0]["students_assessed"] == 1


def test_v2_ai_draft_alone_does_not_sync_gradebook(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    submission_id = _upload_ready_submission(client, headers, assignment_id, student_number=2)

    gradebook = client.get(
        f"/v1/teacher-assist-v2/teacher/gradebook?assignment_id={assignment_id}",
        headers=headers,
    )
    assert gradebook.status_code == 200, gradebook.text
    assert gradebook.json() == []


def test_v2_modify_confirmed_grade_updates_gradebook(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    submission_id = _upload_ready_submission(client, headers, assignment_id, student_number=3)

    accept = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grade-review/accept",
        headers=headers,
    )
    assert accept.status_code == 201, accept.text
    original_score = accept.json()["score"]

    modify = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grade-review/modify",
        headers=headers,
        json={
            "score": max(original_score - 5, 0),
            "max_score": accept.json()["max_score"],
            "teacher_comment": "Adjusted after review.",
            "rubric_json": accept.json()["rubric_json"],
            "teacher_override_reason": "Teacher adjusted score after second look.",
        },
    )
    assert modify.status_code == 201, modify.text

    gradebook = client.get(
        f"/v1/teacher-assist-v2/teacher/gradebook?assignment_id={assignment_id}",
        headers=headers,
    )
    assert gradebook.status_code == 200, gradebook.text
    records = [row for row in gradebook.json() if row["student_number"] == 3]
    assert len(records) == 1
    assert records[0]["score"] == modify.json()["score"]

from __future__ import annotations

import uuid

from sqlalchemy import select

from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.teacher_onboarding import get_v2_onboarding
from tests.test_teacher_assist_v2_grade_reviews import _upload_ready_submission
from tests.test_teacher_assist_v2_submission_intake import (
    _generate_week1_package,
    _written_assignment_id,
)
from tests.test_teacher_assist_v2_planning import _ready_teacher_token


def test_assignment_detail_enables_class_report_when_existing_submissions_are_confirmed(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)
    submission_id = _upload_ready_submission(client, headers, assignment_id, student_number=4)

    accept = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grade-review/accept",
        headers=headers,
    )
    assert accept.status_code == 201, accept.text

    detail = client.get(f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}", headers=headers)
    assert detail.status_code == 200, detail.text
    payload = detail.json()
    assert payload["rubric_score_report_available"] is True
    assert payload["rubric_score_report_blocker"] is None

    report = client.get(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/rubric-score-report",
        headers=headers,
    )
    assert report.status_code == 200, report.text
    assert "Rubric score report" in report.text


def test_assignment_detail_enables_class_report_when_roster_is_fully_resolved(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}
    _generate_week1_package(client, headers)
    assignment_id = _written_assignment_id(client, headers)

    user = db_session.scalar(select(User).where(User.email == "v2-planning-teacher@example.com"))
    assignment = db_session.get(TeacherAssistV2Assignment, uuid.UUID(assignment_id))
    assert user is not None
    assert assignment is not None

    onboarding = get_v2_onboarding(db_session, user_id=user.id)
    assert onboarding is not None
    onboarding.student_count = 1
    db_session.flush()

    submission_id = _upload_ready_submission(client, headers, assignment_id, student_number=1)
    accept = client.post(
        f"/v1/teacher-assist-v2/teacher/submissions/{submission_id}/grade-review/accept",
        headers=headers,
    )
    assert accept.status_code == 201, accept.text

    detail = client.get(f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}", headers=headers)
    assert detail.status_code == 200, detail.text
    payload = detail.json()
    assert payload["status"] == "COMPLETED"
    assert payload["rubric_score_report_available"] is True
    assert payload["rubric_score_report_blocker"] is None

    report = client.get(
        f"/v1/teacher-assist-v2/teacher/assignments/{assignment_id}/rubric-score-report",
        headers=headers,
    )
    assert report.status_code == 200, report.text
    assert "Rubric score report" in report.text

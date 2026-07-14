from __future__ import annotations

import uuid

from sqlalchemy import select

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.scripts.seed_education_catalog import GOLDEN_PATH_ELA_OBJECTIVE_ID
from tests.test_teacher_assist_v2_planning import _ready_teacher_token


def test_v2_assignments_created_from_package_generation(client, db_session):
    token = _ready_teacher_token(client, db_session)
    headers = {"Authorization": f"Bearer {token}"}

    form = client.get("/v1/teacher-assist-v2/teacher/planning/form", headers=headers)
    teaching_order = form.json()["default_teaching_order"]

    package = client.post(
        "/v1/teacher-assist-v2/teacher/planning/packages/generate",
        headers=headers,
        json={
            "week_start": 1,
            "week_end": 1,
            "teaching_order": teaching_order,
            "selected_outputs": [
                "daily_lesson_plan",
                "subject_slide_deck",
                "quiz",
                "assignment",
            ],
        },
    )
    assert package.status_code == 201, package.text

    listed = client.get("/v1/teacher-assist-v2/teacher/assignments", headers=headers)
    assert listed.status_code == 200, listed.text
    rows = listed.json()
    assert len(rows) >= 2
    types = {row["assignment_type"] for row in rows}
    assert "QUIZ" in types
    assert "WRITTEN_ASSIGNMENT" in types

    detail = client.get(
        f"/v1/teacher-assist-v2/teacher/assignments/{rows[0]['id']}", headers=headers
    )
    assert detail.status_code == 200, detail.text
    payload = detail.json()
    assert payload["instructional_plan_id"]
    assert payload["pacing_guide_id"]
    assert payload["school_year_id"]
    assert payload["grade_id"]
    assert payload["subject_id"]
    assert payload["week_number"] == 1
    assert payload["objectives"]
    assert payload["artifacts"]

    home = client.get("/v1/teacher-assist-v2/teacher/home", headers=headers)
    assert home.status_code == 200, home.text
    assert home.json()["recent_assignments"]

    ela_rows = [row for row in rows if row.get("subject_name") == "ELA"]
    assert ela_rows
    db_row = db_session.scalar(
        select(TeacherAssistV2Assignment).where(
            TeacherAssistV2Assignment.id == uuid.UUID(ela_rows[0]["id"])
        )
    )
    assert db_row is not None
    assert db_row.status == "GENERATED"
    objective_ids = [uuid.UUID(value) for value in db_row.education_objective_ids_json]
    objectives = db_session.scalars(
        select(EducationObjective).where(EducationObjective.id.in_(objective_ids))
    ).all()
    assert any(objective.objective_id == GOLDEN_PATH_ELA_OBJECTIVE_ID for objective in objectives)

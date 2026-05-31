from __future__ import annotations

from sqlalchemy.orm import Session

from tests.test_education_catalog import _root_token
from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user


def _catalog_scope(client, root_token: str) -> dict[str, str]:
    state = client.post(
        "/v1/teacher-assist/education-catalog/states",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"name": "Texas", "abbreviation": "TX", "active": True},
    ).json()
    district = client.post(
        "/v1/teacher-assist/education-catalog/districts",
        headers={"Authorization": f"Bearer {root_token}"},
        json={"state_id": state["id"], "name": "Leander Independent School District", "active": True},
    ).json()
    school = client.post(
        "/v1/teacher-assist/education-catalog/schools",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "district_id": district["id"],
            "name": "Mason Elementary",
            "school_type": "elementary",
            "active": True,
        },
    ).json()
    grade = client.post(
        "/v1/teacher-assist/education-catalog/grades",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "school_id": school["id"],
            "grade_code": "5",
            "display_name": "Grade 5",
            "active": True,
        },
    ).json()
    math = client.post(
        "/v1/teacher-assist/education-catalog/subjects",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "grade_id": grade["id"],
            "subject_code": "Math",
            "display_name": "Math",
            "active": True,
        },
    ).json()
    science = client.post(
        "/v1/teacher-assist/education-catalog/subjects",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "grade_id": grade["id"],
            "subject_code": "Science",
            "display_name": "Science",
            "active": True,
        },
    ).json()
    return {
        "state_id": state["id"],
        "district_id": district["id"],
        "school_id": school["id"],
        "grade_id": grade["id"],
        "math_subject_id": math["id"],
        "science_subject_id": science["id"],
    }


def test_teacher_my_school_setup_syncs_catalog_subjects(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _register_user(client, email="school-setup-teacher@example.com", tenant_name="School Setup Tenant")
    _grant_teacher_assist_access(db_session, email="school-setup-teacher@example.com")
    scope = _catalog_scope(client, root_token)

    empty = client.get(
        "/v1/teacher-assist/education-catalog/my-school-setup",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert empty.status_code == 200, empty.text
    assert empty.json()["assignment"] is None

    saved = client.put(
        "/v1/teacher-assist/education-catalog/my-school-setup",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "state_id": scope["state_id"],
            "district_id": scope["district_id"],
            "school_id": scope["school_id"],
            "catalog_grade_id": scope["grade_id"],
            "catalog_subject_ids": [scope["math_subject_id"], scope["science_subject_id"]],
        },
    )
    assert saved.status_code == 200, saved.text
    payload = saved.json()
    assert payload["assignment"]["school_name"] == "Mason Elementary"
    assert payload["catalog_grade_code"] == "5"
    assert set(payload["selected_catalog_subject_ids"]) == {scope["math_subject_id"], scope["science_subject_id"]}
    assert len(payload["synced_subjects"]) == 2

    narrowed = client.put(
        "/v1/teacher-assist/education-catalog/my-school-setup",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "state_id": scope["state_id"],
            "district_id": scope["district_id"],
            "school_id": scope["school_id"],
            "catalog_grade_id": scope["grade_id"],
            "catalog_subject_ids": [scope["math_subject_id"]],
        },
    )
    assert narrowed.status_code == 200, narrowed.text
    narrowed_payload = narrowed.json()
    assert narrowed_payload["selected_catalog_subject_ids"] == [scope["math_subject_id"]]
    assert len(narrowed_payload["synced_subjects"]) == 1
    assert narrowed_payload["synced_subjects"][0]["subject_code"] == "Math"

    subjects = client.get(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert subjects.status_code == 200, subjects.text
    subject_codes = {row["code"] for row in subjects.json()}
    assert subject_codes == {"Math"}

    profile = client.get(
        "/v1/teacher-assist/profile",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert profile.status_code == 200, profile.text
    assert profile.json()["preferred_grade_level"] == "5"

    home = client.get(
        "/v1/teacher-assist/home",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert home.status_code == 200, home.text
    assert home.json()["onboarding"]["total_count"] == 3
    school_step = next(row for row in home.json()["onboarding"]["steps"] if row["key"] == "school_placement")
    assert school_step["complete"] is True


def test_classroom_subjects_follow_school_setup_selection(client, db_session: Session):
    root_token = _root_token(client, db_session)
    teacher_token = _register_user(client, email="classroom-sync@example.com", tenant_name="Classroom Sync Tenant")
    _grant_teacher_assist_access(db_session, email="classroom-sync@example.com")
    scope = _catalog_scope(client, root_token)

    client.put(
        "/v1/teacher-assist/education-catalog/my-school-setup",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "state_id": scope["state_id"],
            "district_id": scope["district_id"],
            "school_id": scope["school_id"],
            "catalog_grade_id": scope["grade_id"],
            "catalog_subject_ids": [scope["math_subject_id"], scope["science_subject_id"]],
        },
    )
    client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    )
    client.put(
        "/v1/teacher-assist/my-classroom",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={"homeroom_name": "Grade 5 Homeroom", "student_count": 22, "timezone": None},
    )

    classroom = client.get(
        "/v1/teacher-assist/my-classroom",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert classroom.status_code == 200, classroom.text
    classroom_payload = classroom.json()
    assert {row["subject_code"] for row in classroom_payload["synced_subjects"]} == {"Math", "Science"}
    assert classroom_payload["active_school_year_title"] == "2026-2027"
    assert classroom_payload["class_id"] is not None

    client.put(
        "/v1/teacher-assist/education-catalog/my-school-setup",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "state_id": scope["state_id"],
            "district_id": scope["district_id"],
            "school_id": scope["school_id"],
            "catalog_grade_id": scope["grade_id"],
            "catalog_subject_ids": [scope["math_subject_id"]],
        },
    )

    classroom_after = client.get(
        "/v1/teacher-assist/my-classroom",
        headers={"Authorization": f"Bearer {teacher_token}"},
    )
    assert classroom_after.status_code == 200, classroom_after.text
    assert [row["subject_code"] for row in classroom_after.json()["synced_subjects"]] == ["Math"]

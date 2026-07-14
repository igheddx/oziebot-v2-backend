from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.platform_product import PlatformProduct
from oziebot_api.models.tenant_product_access import TenantProductAccess
from oziebot_api.models.user import User
from oziebot_api.services.product_access import TEACHER_ASSIST_PRODUCT_KEY


def _register_user(client, *, email: str, tenant_name: str) -> str:
    response = client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "full_name": email.split("@")[0].title(),
            "password": "password-123",
            "tenant_name": tenant_name,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["access_token"]


def _grant_teacher_assist_access(
    db_session: Session, *, email: str, status: str = "active"
) -> None:
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    product = db_session.scalar(
        select(PlatformProduct).where(PlatformProduct.product_key == TEACHER_ASSIST_PRODUCT_KEY)
    )
    assert product is not None
    existing = db_session.scalar(
        select(TenantProductAccess).where(
            TenantProductAccess.tenant_id == membership.tenant_id,
            TenantProductAccess.product_id == product.id,
        )
    )
    if existing is None:
        db_session.add(
            TenantProductAccess(
                tenant_id=membership.tenant_id,
                product_id=product.id,
                status=status,
                created_at=membership.created_at,
                updated_at=membership.created_at,
            )
        )
    else:
        existing.status = status
    db_session.commit()


def test_teacher_assist_requires_product_access(client):
    token = _register_user(
        client, email="teacher-no-access@example.com", tenant_name="No Access Tenant"
    )
    response = client.get(
        "/v1/teacher-assist/profile", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "TeacherAssist is not enabled for this user"


def test_teacher_profile_and_school_year_setup_round_trip(client, db_session: Session):
    email = "teacher-setup@example.com"
    token = _register_user(client, email=email, tenant_name="Teacher Setup Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    profile = client.put(
        "/v1/teacher-assist/profile",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "preferred_grade_level": "5",
            "default_student_count": 23,
            "preferred_grading_period_type": "nine_weeks",
            "timezone": "America/Chicago",
        },
    )
    assert profile.status_code == 200, profile.text
    assert profile.json()["preferred_grade_level"] == "5"

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    )
    assert school_year.status_code == 201, school_year.text
    school_year_payload = school_year.json()
    assert school_year_payload["is_active"] is True

    fetched = client.get(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()[0]["title"] == "2026-2027"


def test_grading_period_validation_rejects_outside_year_and_overlap(client, db_session: Session):
    email = "grading-periods@example.com"
    token = _register_user(client, email=email, tenant_name="Grading Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()

    first = client.post(
        "/v1/teacher-assist/grading-periods",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "9 Weeks 1",
            "grading_period_type": "nine_weeks",
            "start_date": "2026-08-10",
            "end_date": "2026-10-10",
            "sort_order": 1,
        },
    )
    assert first.status_code == 201, first.text

    overlap = client.post(
        "/v1/teacher-assist/grading-periods",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "Overlap",
            "grading_period_type": "nine_weeks",
            "start_date": "2026-10-01",
            "end_date": "2026-11-01",
            "sort_order": 2,
        },
    )
    assert overlap.status_code == 400
    assert "overlap" in overlap.json()["detail"].lower()

    outside_year = client.post(
        "/v1/teacher-assist/grading-periods",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "Outside",
            "grading_period_type": "custom",
            "start_date": "2027-06-01",
            "end_date": "2027-06-15",
            "sort_order": 3,
        },
    )
    assert outside_year.status_code == 400
    assert "within the school year" in outside_year.json()["detail"].lower()


def test_class_student_count_validation_and_subject_standard_creation(client, db_session: Session):
    email = "classes@example.com"
    token = _register_user(client, email=email, tenant_name="Classes Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()

    invalid_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "name": "5th Grade Homeroom",
            "grade_level": "5",
            "student_count": 0,
        },
    )
    assert invalid_class.status_code == 422

    subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"code": "MATH", "name": "Math"},
    )
    assert subject.status_code == 201, subject.text
    subject_payload = subject.json()

    created_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "name": "5th Grade Homeroom",
            "grade_level": "5",
            "student_count": 23,
        },
    )
    assert created_class.status_code == 201, created_class.text
    class_payload = created_class.json()
    assert class_payload["student_number_range_start"] == 1
    assert class_payload["student_number_range_end"] == 23

    attached = client.post(
        "/v1/teacher-assist/class-subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"class_id": class_payload["id"], "subject_id": subject_payload["id"]},
    )
    assert attached.status_code == 201, attached.text

    standard = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": subject_payload["id"],
            "standard_type": "TEKS",
            "code": "5.3H",
            "description": "Represent and solve addition and subtraction problems.",
            "grade_level": "5",
            "school_year_id": school_year["id"],
        },
    )
    assert standard.status_code == 201, standard.text

    classes = client.get("/v1/teacher-assist/classes", headers={"Authorization": f"Bearer {token}"})
    assert classes.status_code == 200, classes.text
    assert classes.json()[0]["subject_ids"] == [subject_payload["id"]]

    standards = client.get(
        "/v1/teacher-assist/standards", headers={"Authorization": f"Bearer {token}"}
    )
    assert standards.status_code == 200, standards.text
    assert standards.json()[0]["code"] == "5.3H"


def test_teacher_assist_tenant_isolation_for_school_years_and_standards(
    client, db_session: Session
):
    first_email = "teacher-a@example.com"
    second_email = "teacher-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    )
    assert school_year.status_code == 201, school_year.text
    school_year_id = school_year.json()["id"]

    foreign_update = client.put(
        f"/v1/teacher-assist/school-years/{school_year_id}",
        headers={"Authorization": f"Bearer {second_token}"},
        json={
            "title": "Not Yours",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    )
    assert foreign_update.status_code == 404

    subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"name": "Science"},
    ).json()
    standard = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "subject_id": subject["id"],
            "standard_type": "TEKS",
            "code": "5.7A",
            "description": "Explore ecosystems.",
        },
    )
    assert standard.status_code == 201, standard.text

    second_standards = client.get(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert second_standards.status_code == 200, second_standards.text
    assert second_standards.json() == []


def test_standard_requires_subject_and_supports_update_and_import(client, db_session: Session):
    email = "standards-ux@example.com"
    token = _register_user(client, email=email, tenant_name="Standards UX Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()

    math_subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"code": "MATH", "name": "Math"},
    ).json()
    ela_subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"code": "ELA", "name": "ELA"},
    ).json()

    missing_subject = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "standard_type": "TEKS",
            "code": "5.MATH.1",
            "description": "Add decimals.",
        },
    )
    assert missing_subject.status_code == 422, missing_subject.text

    created = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": math_subject["id"],
            "standard_type": "TEKS",
            "code": "5.MATH.1",
            "description": "Add decimals.",
            "grade_level": "5",
            "school_year_id": school_year["id"],
        },
    )
    assert created.status_code == 201, created.text
    standard_payload = created.json()

    duplicate = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": math_subject["id"],
            "standard_type": "TEKS",
            "code": "5.MATH.1",
            "description": "Duplicate code.",
        },
    )
    assert duplicate.status_code == 400
    assert "already exists" in duplicate.json()["detail"].lower()

    updated = client.put(
        f"/v1/teacher-assist/standards/{standard_payload['id']}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": ela_subject["id"],
            "standard_type": "TEKS",
            "code": "5.ELA.1",
            "description": "Updated description.",
            "grade_level": "5",
            "school_year_id": school_year["id"],
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["code"] == "5.ELA.1"
    assert updated.json()["subject_id"] == ela_subject["id"]

    other_token = _register_user(
        client, email="standards-other@example.com", tenant_name="Other Standards Tenant"
    )
    _grant_teacher_assist_access(db_session, email="standards-other@example.com")
    forbidden = client.put(
        f"/v1/teacher-assist/standards/{standard_payload['id']}",
        headers={"Authorization": f"Bearer {other_token}"},
        json={
            "subject_id": ela_subject["id"],
            "standard_type": "TEKS",
            "code": "5.ELA.1",
            "description": "Cross tenant update.",
        },
    )
    assert forbidden.status_code == 404

    csv_content = "\n".join(
        [
            "code,type,subject,description",
            '5.MATH.2,TEKS,Math,"Multiply decimals."',
            '5.MISSING.1,TEKS,Science,"Unknown subject row."',
            '5.ELA.1,TEKS,ELA,"Duplicate row."',
        ]
    )
    preview = client.post(
        "/v1/teacher-assist/standards/import/preview",
        headers={"Authorization": f"Bearer {token}"},
        json={"csv_content": csv_content},
    )
    assert preview.status_code == 200, preview.text
    preview_payload = preview.json()
    assert preview_payload["total_rows"] == 3
    assert preview_payload["valid_count"] == 1
    assert preview_payload["invalid_count"] == 1
    assert preview_payload["duplicate_count"] == 1
    assert any("Science" in error["message"] for error in preview_payload["errors"])

    commit = client.post(
        "/v1/teacher-assist/standards/import/commit",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "rows": [
                {
                    "code": "5.MATH.2",
                    "standard_type": "TEKS",
                    "subject_id": math_subject["id"],
                    "description": "Multiply decimals.",
                }
            ]
        },
    )
    assert commit.status_code == 200, commit.text
    assert commit.json()["created_count"] == 1

    standards = client.get(
        "/v1/teacher-assist/standards", headers={"Authorization": f"Bearer {token}"}
    )
    assert standards.status_code == 200, standards.text
    codes = {row["code"] for row in standards.json()}
    assert "5.MATH.2" in codes
    assert "5.ELA.1" in codes

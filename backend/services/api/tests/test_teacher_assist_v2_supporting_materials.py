from __future__ import annotations

import io

from sqlalchemy import select

from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.user import User
from oziebot_api.scripts.seed_teacher_assist_v2 import seed_teacher_assist_v2
from oziebot_api.services.teacher_assist.pacing_guide_foundation import get_catalog_pacing_guide_detail
from tests.test_teacher_assist_setup import _grant_teacher_assist_access, _register_user


def _make_root_admin(db_session, client, email: str) -> str:
    token = _register_user(client, email=email, tenant_name=f"Tenant {email}")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    user.teacher_assist_role = "root_admin"
    db_session.commit()
    return token


def _make_teacher(db_session, client, email: str) -> str:
    token = _register_user(client, email=email, tenant_name=f"Tenant {email}")
    _grant_teacher_assist_access(db_session, email=email)
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.teacher_assist_role = "teacher"
    db_session.commit()
    return token


def _first_math_guide(db_session) -> tuple[TeacherAssistPacingGuide, str]:
    row = db_session.scalars(
        select(TeacherAssistPacingGuide).where(
            TeacherAssistPacingGuide.guide_type == "DISTRICT",
            TeacherAssistPacingGuide.title.like("%Math%"),
        )
    ).first()
    assert row is not None
    guide = get_catalog_pacing_guide_detail(db_session, tenant_id=row.tenant_id, pacing_guide_id=row.id)
    period = sorted(guide.periods, key=lambda item: item.sequence_number)[0]
    return guide, str(period.id)


def test_v2_supporting_materials_link_and_planning_context(client, db_session):
    token = _make_root_admin(db_session, client, "v2-materials-root@example.com")
    seed_teacher_assist_v2(db_session)
    db_session.commit()

    guide, period_id = _first_math_guide(db_session)
    headers = {"Authorization": f"Bearer {token}"}

    create_link = client.post(
        f"/v1/teacher-assist-v2/instructional/pacing-guides/{guide.id}/supporting-materials/links",
        headers=headers,
        json={
            "title": "Decimal models reference",
            "external_url": "https://example.com/math/decimals",
            "resource_type": "reference_link",
            "period_id": period_id,
        },
    )
    assert create_link.status_code == 201, create_link.text

    create_note = client.post(
        f"/v1/teacher-assist-v2/instructional/pacing-guides/{guide.id}/supporting-materials/notes",
        headers=headers,
        json={
            "title": "Week 1 reminder",
            "note_body": "Review place value before computation.",
            "period_id": period_id,
        },
    )
    assert create_note.status_code == 201, create_note.text

    upload = client.post(
        f"/v1/teacher-assist-v2/instructional/pacing-guides/{guide.id}/supporting-materials/upload",
        headers=headers,
        files={"file": ("week1-overview.txt", io.BytesIO(b"Sample district curriculum"), "text/plain")},
        data={"resource_type": "curriculum_file", "title": "Week 1 overview", "period_id": period_id},
    )
    assert upload.status_code == 201, upload.text

    listing = client.get(
        f"/v1/teacher-assist-v2/instructional/pacing-guides/{guide.id}/supporting-materials?period_id={period_id}",
        headers=headers,
    )
    assert listing.status_code == 200, listing.text
    rows = listing.json()
    assert len(rows) >= 3

    context = client.get(
        f"/v1/teacher-assist-v2/instructional/pacing-guides/{guide.id}/planning-context?period_id={period_id}",
        headers=headers,
    )
    assert context.status_code == 200, context.text
    payload = context.json()
    assert payload["week_title"]
    assert len(payload["reference_links"]) >= 1
    assert len(payload["notes"]) >= 1
    assert len(payload["curriculum_files"]) >= 1


def test_v2_supporting_materials_teacher_cannot_mutate(client, db_session):
    root_token = _make_root_admin(db_session, client, "v2-materials-root2@example.com")
    teacher_token = _make_teacher(db_session, client, "v2-materials-teacher@example.com")
    seed_teacher_assist_v2(db_session)
    db_session.commit()

    guide, period_id = _first_math_guide(db_session)

    response = client.post(
        f"/v1/teacher-assist-v2/instructional/pacing-guides/{guide.id}/supporting-materials/links",
        headers={"Authorization": f"Bearer {teacher_token}"},
        json={
            "title": "Blocked link",
            "external_url": "https://example.com/blocked",
            "resource_type": "reference_link",
            "period_id": period_id,
        },
    )
    assert response.status_code == 403

    ok = client.post(
        f"/v1/teacher-assist-v2/instructional/pacing-guides/{guide.id}/supporting-materials/links",
        headers={"Authorization": f"Bearer {root_token}"},
        json={
            "title": "Allowed link",
            "external_url": "https://example.com/allowed",
            "resource_type": "reference_link",
            "period_id": period_id,
        },
    )
    assert ok.status_code == 201, ok.text


def test_v2_supporting_materials_invalid_url(client, db_session):
    token = _make_root_admin(db_session, client, "v2-materials-url@example.com")
    seed_teacher_assist_v2(db_session)
    db_session.commit()
    guide, period_id = _first_math_guide(db_session)

    response = client.post(
        f"/v1/teacher-assist-v2/instructional/pacing-guides/{guide.id}/supporting-materials/links",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "Bad link",
            "external_url": "not-a-url",
            "resource_type": "reference_link",
            "period_id": period_id,
        },
    )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "field_errors" in detail
    assert "external_url" in detail["field_errors"]

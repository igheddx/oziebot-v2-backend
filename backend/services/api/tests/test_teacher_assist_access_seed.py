from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.platform_product import PlatformProduct
from oziebot_api.models.teacher_assist_user_preference import TeacherAssistUserPreference
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_product_access import TenantProductAccess
from oziebot_api.models.user import User
from oziebot_api.models.user_product_preference import UserProductPreference
from oziebot_api.services.product_access import TEACHER_ASSIST_PRODUCT_KEY, TRADING_PRODUCT_KEY, get_user_products
from oziebot_api.services.teacher_assist.access_seed import (
    ensure_existing_user_teacher_assist_access,
    ensure_user_teacher_assist_access,
)
from oziebot_api.services.teacher_assist import user_preferences as user_preferences_module
from oziebot_api.services.teacher_assist.user_preferences import get_user_preferences_or_create


def test_teacher_assist_access_seed_is_idempotent_and_preserves_trading_access(
    client,
    db_session: Session,
):
    dominic_password = "dominic-password-123"
    register = client.post(
        "/v1/auth/register",
        json={
            "email": "Dominic@oziebot.com",
            "full_name": "Dominic",
            "password": dominic_password,
            "tenant_name": "Dominic Tenant",
        },
    )
    assert register.status_code == 201, register.text
    dominic_token = register.json()["access_token"]

    teacher_assist_product = db_session.scalar(
        select(PlatformProduct).where(PlatformProduct.product_key == TEACHER_ASSIST_PRODUCT_KEY)
    )
    assert teacher_assist_product is not None
    teacher_assist_product.display_name = "Wrong Name"
    db_session.commit()

    dominic_result = ensure_existing_user_teacher_assist_access(db_session, email="Dominic@oziebot.com")
    awele_result = ensure_user_teacher_assist_access(
        db_session,
        email="aweleu@gmail.com",
        full_name="Awele Ighedosa",
        tenant_name="Awele Ighedosa",
        password="awele-password-123",
    )
    ozie_result = ensure_user_teacher_assist_access(
        db_session,
        email="dvaten.1992@gmail.com",
        full_name="Ozie Ighedosa",
        tenant_name="Ozie Ighedosa",
        password="ozie-password-123",
    )
    db_session.commit()

    me = client.get("/v1/me", headers={"Authorization": f"Bearer {dominic_token}"})
    assert me.status_code == 200, me.text
    me_payload = me.json()
    assert me_payload["default_product"] == TEACHER_ASSIST_PRODUCT_KEY
    assert {row["product_key"] for row in me_payload["products"]} == {
        TRADING_PRODUCT_KEY,
        TEACHER_ASSIST_PRODUCT_KEY,
    }

    teacher_assist_row = next(
        row for row in me_payload["products"] if row["product_key"] == TEACHER_ASSIST_PRODUCT_KEY
    )
    assert teacher_assist_row["display_name"] == "TeacherAssist AI"
    assert teacher_assist_row["is_default"] is True

    dominic = db_session.scalar(select(User).where(func.lower(User.email) == "dominic@oziebot.com"))
    assert dominic is not None
    dominic_products, dominic_default = get_user_products(db_session, dominic)
    assert {row.product_key for row in dominic_products} == {
        TRADING_PRODUCT_KEY,
        TEACHER_ASSIST_PRODUCT_KEY,
    }
    assert dominic_default == TEACHER_ASSIST_PRODUCT_KEY
    assert dominic_result.default_product == TEACHER_ASSIST_PRODUCT_KEY

    awele = db_session.scalar(select(User).where(func.lower(User.email) == "aweleu@gmail.com"))
    assert awele is not None
    assert awele.full_name == "Awele Ighedosa"
    assert awele.is_active is True
    awele_products, awele_default = get_user_products(db_session, awele)
    assert {row.product_key for row in awele_products} == {TEACHER_ASSIST_PRODUCT_KEY}
    assert awele_default == TEACHER_ASSIST_PRODUCT_KEY
    assert awele_result.created_user is True
    assert awele_result.created_tenant is True

    ozie = db_session.scalar(select(User).where(func.lower(User.email) == "dvaten.1992@gmail.com"))
    assert ozie is not None
    assert ozie.full_name == "Ozie Ighedosa"
    assert ozie.is_active is True
    ozie_products, ozie_default = get_user_products(db_session, ozie)
    assert {row.product_key for row in ozie_products} == {TEACHER_ASSIST_PRODUCT_KEY}
    assert ozie_default == TEACHER_ASSIST_PRODUCT_KEY
    assert ozie_result.created_user is True
    assert ozie_result.created_tenant is True

    counts_before = {
        "users": db_session.scalar(select(func.count()).select_from(User)),
        "tenants": db_session.scalar(select(func.count()).select_from(Tenant)),
        "memberships": db_session.scalar(select(func.count()).select_from(TenantMembership)),
        "tenant_access": db_session.scalar(select(func.count()).select_from(TenantProductAccess)),
        "preferences": db_session.scalar(select(func.count()).select_from(UserProductPreference)),
    }

    second_dominic = ensure_existing_user_teacher_assist_access(db_session, email="dominic@oziebot.com")
    second_awele = ensure_user_teacher_assist_access(
        db_session,
        email="aweleu@gmail.com",
        full_name="Awele Ighedosa",
        tenant_name="Awele Ighedosa",
    )
    second_ozie = ensure_user_teacher_assist_access(
        db_session,
        email="dvaten.1992@gmail.com",
        full_name="Ozie Ighedosa",
        tenant_name="Ozie Ighedosa",
    )
    db_session.commit()

    counts_after = {
        "users": db_session.scalar(select(func.count()).select_from(User)),
        "tenants": db_session.scalar(select(func.count()).select_from(Tenant)),
        "memberships": db_session.scalar(select(func.count()).select_from(TenantMembership)),
        "tenant_access": db_session.scalar(select(func.count()).select_from(TenantProductAccess)),
        "preferences": db_session.scalar(select(func.count()).select_from(UserProductPreference)),
    }
    assert counts_after == counts_before
    assert second_dominic.created_user is False
    assert second_dominic.created_tenant is False
    assert second_awele.created_user is False
    assert second_awele.created_tenant is False
    assert second_awele.temporary_password_generated is False
    assert second_ozie.created_user is False
    assert second_ozie.created_tenant is False
    assert second_ozie.temporary_password_generated is False


def test_teacher_assist_access_seed_requires_existing_dominic_membership(db_session: Session):
    try:
        ensure_existing_user_teacher_assist_access(db_session, email="missing-dominic@example.com")
    except LookupError as exc:
        assert str(exc) == "User not found: missing-dominic@example.com"
    else:
        raise AssertionError("Expected LookupError for missing Dominic user")


def test_user_preferences_or_create_is_idempotent(db_session: Session, client):
    email = "teacher-preferences-idempotent@example.com"
    token = client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "full_name": "Preferences Idempotent",
            "password": "password-123",
            "tenant_name": "Preferences Tenant",
        },
    )
    assert token.status_code == 201, token.text
    user = db_session.scalar(select(User).where(func.lower(User.email) == email.lower()))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    first = get_user_preferences_or_create(
        db_session,
        tenant_id=membership.tenant_id,
        user_id=user.id,
    )
    second = get_user_preferences_or_create(
        db_session,
        tenant_id=membership.tenant_id,
        user_id=user.id,
    )
    db_session.commit()
    assert first.id == second.id


def test_user_preferences_or_create_recovers_from_duplicate_insert(db_session: Session, client):
    email = "teacher-preferences-race@example.com"
    token = client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "full_name": "Preferences Race",
            "password": "password-123",
            "tenant_name": "Preferences Race Tenant",
        },
    )
    assert token.status_code == 201, token.text
    user = db_session.scalar(select(User).where(func.lower(User.email) == email.lower()))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None

    now = datetime.now(UTC)
    existing = TeacherAssistUserPreference(
        tenant_id=membership.tenant_id,
        user_id=user.id,
        preferred_landing="home",
        recently_viewed_json=[],
        onboarding_progress_json={},
        created_at=now,
        updated_at=now,
    )
    db_session.add(existing)
    db_session.flush()

    with patch.object(
        user_preferences_module,
        "_get_user_preferences",
        side_effect=[None, existing],
    ), patch.object(db_session, "begin_nested", db_session.begin_nested), patch.object(
        db_session,
        "flush",
        side_effect=IntegrityError("duplicate key", None, None),
    ):
        recovered = get_user_preferences_or_create(
            db_session,
            tenant_id=membership.tenant_id,
            user_id=user.id,
        )

    assert recovered.id == existing.id

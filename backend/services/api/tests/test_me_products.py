from __future__ import annotations

from datetime import UTC, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.platform_product import PlatformProduct
from oziebot_api.models.tenant_product_access import TenantProductAccess
from oziebot_api.models.user import User


def _grant_product_access(
    db_session: Session,
    *,
    user_email: str,
    product_key: str,
    status: str,
) -> None:
    user = db_session.scalar(select(User).where(User.email == user_email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    product = db_session.scalar(
        select(PlatformProduct).where(PlatformProduct.product_key == product_key)
    )
    assert product is not None
    now = datetime.now(UTC)
    db_session.add(
        TenantProductAccess(
            id=uuid.uuid4(),
            tenant_id=membership.tenant_id,
            product_id=product.id,
            status=status,
            created_at=now,
            updated_at=now,
        )
    )
    db_session.commit()


def test_me_includes_products_and_default_product(client):
    register = client.post(
        "/v1/auth/register",
        json={
            "email": "products@example.com",
            "full_name": "Products Example",
            "password": "password-123",
            "tenant_name": "Products Tenant",
        },
    )
    assert register.status_code == 201, register.text

    token = register.json()["access_token"]
    response = client.get("/v1/me", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["default_product"] == "trading"
    assert payload["products"] == [
        {
            "product_key": "trading",
            "display_name": "Oziebot Trading",
            "status": "active",
            "is_default": True,
        }
    ]


def test_user_can_switch_default_product_with_access(client, db_session: Session):
    register = client.post(
        "/v1/auth/register",
        json={
            "email": "switch@example.com",
            "full_name": "Switch Example",
            "password": "password-123",
            "tenant_name": "Switch Tenant",
        },
    )
    assert register.status_code == 201, register.text
    token = register.json()["access_token"]

    _grant_product_access(
        db_session,
        user_email="switch@example.com",
        product_key="teacher_assist",
        status="trial",
    )

    products_response = client.get("/v1/me/products", headers={"Authorization": f"Bearer {token}"})
    assert products_response.status_code == 200, products_response.text
    assert {row["product_key"] for row in products_response.json()["products"]} == {
        "trading",
        "teacher_assist",
    }

    updated = client.patch(
        "/v1/me/default-product",
        headers={"Authorization": f"Bearer {token}"},
        json={"product_key": "teacher_assist"},
    )
    assert updated.status_code == 200, updated.text
    payload = updated.json()
    assert payload["default_product"] == "teacher_assist"
    teacher_assist = next(
        row for row in payload["products"] if row["product_key"] == "teacher_assist"
    )
    assert teacher_assist["status"] == "trial"
    assert teacher_assist["is_default"] is True

    me = client.get("/v1/me", headers={"Authorization": f"Bearer {token}"})
    assert me.status_code == 200, me.text
    assert me.json()["default_product"] == "teacher_assist"


def test_disabled_product_cannot_be_selected_as_default(client, db_session: Session):
    register = client.post(
        "/v1/auth/register",
        json={
            "email": "disabled@example.com",
            "full_name": "Disabled Example",
            "password": "password-123",
            "tenant_name": "Disabled Tenant",
        },
    )
    assert register.status_code == 201, register.text
    token = register.json()["access_token"]

    _grant_product_access(
        db_session,
        user_email="disabled@example.com",
        product_key="teacher_assist",
        status="disabled",
    )

    denied = client.patch(
        "/v1/me/default-product",
        headers={"Authorization": f"Bearer {token}"},
        json={"product_key": "teacher_assist"},
    )
    assert denied.status_code == 403
    assert denied.json()["detail"] == "Product is not available for this user"

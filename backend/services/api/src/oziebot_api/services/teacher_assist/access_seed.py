from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import secrets
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_api.models.user import User
from oziebot_api.services.passwords import hash_password
from oziebot_api.services.product_access import (
    TEACHER_ASSIST_PRODUCT_KEY,
    ensure_platform_products,
    grant_tenant_product_access,
    set_user_default_product,
)


@dataclass(frozen=True)
class TeacherAssistAccessSeedResult:
    email: str
    user_id: uuid.UUID
    tenant_id: uuid.UUID
    created_user: bool
    created_tenant: bool
    teacher_assist_granted: bool
    default_product: str
    temporary_password_generated: bool


def _normalized_email(value: str) -> str:
    return value.strip().lower()


def _get_user_by_email(db: Session, email: str) -> User | None:
    normalized = _normalized_email(email)
    return db.scalars(select(User).where(func.lower(User.email) == normalized)).one_or_none()


def _primary_membership(db: Session, *, user_id: uuid.UUID) -> TenantMembership | None:
    return db.scalars(
        select(TenantMembership)
        .where(TenantMembership.user_id == user_id)
        .order_by(TenantMembership.created_at.asc())
        .limit(1)
    ).first()


def _ensure_tenant_integration(db: Session, *, tenant_id: uuid.UUID, now: datetime) -> None:
    existing = db.get(TenantIntegration, tenant_id)
    if existing is not None:
        return
    db.add(
        TenantIntegration(
            tenant_id=tenant_id,
            coinbase_connected=False,
            updated_at=now,
        )
    )
    db.flush()


def _create_user(
    db: Session,
    *,
    email: str,
    full_name: str,
    password: str | None,
    now: datetime,
) -> tuple[User, bool]:
    generated_password = password is None
    user = User(
        id=uuid.uuid4(),
        email=_normalized_email(email),
        full_name=full_name.strip(),
        password_hash=hash_password(password or secrets.token_urlsafe(32)),
        is_root_admin=False,
        is_active=True,
        email_verified_at=None,
        current_trading_mode="paper",
        created_at=now,
        updated_at=now,
    )
    db.add(user)
    db.flush()
    return user, generated_password


def _ensure_user_and_membership(
    db: Session,
    *,
    email: str,
    full_name: str,
    tenant_name: str,
    password: str | None,
) -> tuple[User, TenantMembership, bool, bool, bool]:
    now = datetime.now(UTC)
    user = _get_user_by_email(db, email)
    created_user = False
    temporary_password_generated = False
    if user is None:
        user, temporary_password_generated = _create_user(
            db,
            email=email,
            full_name=full_name,
            password=password,
            now=now,
        )
        created_user = True
    membership = _primary_membership(db, user_id=user.id)
    created_tenant = False
    if membership is None:
        tenant = Tenant(
            id=uuid.uuid4(),
            name=tenant_name.strip(),
            created_at=now,
            default_trading_mode="paper",
        )
        db.add(tenant)
        db.flush()
        _ensure_tenant_integration(db, tenant_id=tenant.id, now=now)
        membership = TenantMembership(
            id=uuid.uuid4(),
            user_id=user.id,
            tenant_id=tenant.id,
            role="user",
            created_at=now,
        )
        db.add(membership)
        db.flush()
        created_tenant = True
    else:
        _ensure_tenant_integration(db, tenant_id=membership.tenant_id, now=now)
    return user, membership, created_user, created_tenant, temporary_password_generated


def ensure_existing_user_teacher_assist_access(
    db: Session,
    *,
    email: str,
) -> TeacherAssistAccessSeedResult:
    ensure_platform_products(db)
    user = _get_user_by_email(db, email)
    if user is None:
        raise LookupError(f"User not found: {email}")
    membership = _primary_membership(db, user_id=user.id)
    if membership is None:
        raise LookupError(f"User has no tenant membership: {email}")
    now = datetime.now(UTC)
    _ensure_tenant_integration(db, tenant_id=membership.tenant_id, now=now)
    access_row = grant_tenant_product_access(
        db,
        tenant_id=membership.tenant_id,
        product_key=TEACHER_ASSIST_PRODUCT_KEY,
        status="active",
    )
    _, default_product = set_user_default_product(
        db,
        user=user,
        product_key=TEACHER_ASSIST_PRODUCT_KEY,
    )
    return TeacherAssistAccessSeedResult(
        email=user.email,
        user_id=user.id,
        tenant_id=membership.tenant_id,
        created_user=False,
        created_tenant=False,
        teacher_assist_granted=access_row.status == "active",
        default_product=default_product,
        temporary_password_generated=False,
    )


def ensure_user_teacher_assist_access(
    db: Session,
    *,
    email: str,
    full_name: str,
    tenant_name: str,
    password: str | None = None,
) -> TeacherAssistAccessSeedResult:
    ensure_platform_products(db)
    user, membership, created_user, created_tenant, temporary_password_generated = (
        _ensure_user_and_membership(
            db,
            email=email,
            full_name=full_name,
            tenant_name=tenant_name,
            password=password,
        )
    )
    access_row = grant_tenant_product_access(
        db,
        tenant_id=membership.tenant_id,
        product_key=TEACHER_ASSIST_PRODUCT_KEY,
        status="active",
    )
    _, default_product = set_user_default_product(
        db,
        user=user,
        product_key=TEACHER_ASSIST_PRODUCT_KEY,
    )
    return TeacherAssistAccessSeedResult(
        email=user.email,
        user_id=user.id,
        tenant_id=membership.tenant_id,
        created_user=created_user,
        created_tenant=created_tenant,
        teacher_assist_granted=access_row.status == "active",
        default_product=default_product,
        temporary_password_generated=temporary_password_generated,
    )


from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.platform_product import PlatformProduct
from oziebot_api.models.tenant_product_access import TenantProductAccess
from oziebot_api.models.user import User
from oziebot_api.models.user_product_preference import UserProductPreference

TRADING_PRODUCT_KEY = "trading"
TEACHER_ASSIST_PRODUCT_KEY = "teacher_assist"
SELECTABLE_PRODUCT_STATUSES = {"active", "trial"}
PRODUCT_STATUS_PRIORITY = {"disabled": 0, "trial": 1, "active": 2}

DEFAULT_PLATFORM_PRODUCTS = (
    {
        "product_key": TRADING_PRODUCT_KEY,
        "display_name": "Oziebot Trading",
        "description": "Oziebot trading console and portfolio workflows.",
    },
    {
        "product_key": TEACHER_ASSIST_PRODUCT_KEY,
        "display_name": "TeacherAssist AI",
        "description": "Teacher planning and classroom workflow product module.",
    },
)


@dataclass(frozen=True)
class ProductAccessSnapshot:
    product_key: str
    display_name: str
    status: str
    is_default: bool


def ensure_platform_products(db: Session) -> None:
    existing = {
        row.product_key: row for row in db.scalars(select(PlatformProduct)).all()
    }
    now = datetime.now(UTC)
    for definition in DEFAULT_PLATFORM_PRODUCTS:
        row = existing.get(definition["product_key"])
        if row is None:
            db.add(
                PlatformProduct(
                    product_key=definition["product_key"],
                    display_name=definition["display_name"],
                    description=definition["description"],
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                )
            )
            continue
        changed = False
        if row.display_name != definition["display_name"]:
            row.display_name = definition["display_name"]
            changed = True
        if row.description != definition["description"]:
            row.description = definition["description"]
            changed = True
        if not row.is_active:
            row.is_active = True
            changed = True
        if changed:
            row.updated_at = now
    db.flush()


def product_route_key(pathname: str) -> str | None:
    if pathname.startswith("/teacher-assist"):
        return TEACHER_ASSIST_PRODUCT_KEY
    if pathname.startswith("/admin"):
        return None
    if pathname == "/login":
        return None
    return TRADING_PRODUCT_KEY


def grant_tenant_product_access(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    product_key: str,
    status: str = "active",
) -> TenantProductAccess:
    ensure_platform_products(db)
    product = db.scalars(
        select(PlatformProduct).where(
            PlatformProduct.product_key == product_key,
            PlatformProduct.is_active.is_(True),
        )
    ).one()
    row = db.scalars(
        select(TenantProductAccess).where(
            TenantProductAccess.tenant_id == tenant_id,
            TenantProductAccess.product_id == product.id,
        )
    ).one_or_none()
    now = datetime.now(UTC)
    if row is None:
        row = TenantProductAccess(
            tenant_id=tenant_id,
            product_id=product.id,
            status=status,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    elif row.status != status:
        row.status = status
        row.updated_at = now
    db.flush()
    return row


def _preferred_default_product_key(
    db: Session,
    *,
    user_id: uuid.UUID,
    products_by_key: dict[str, dict[str, object]],
) -> str | None:
    if not products_by_key:
        return None
    pref = db.scalars(
        select(UserProductPreference).where(UserProductPreference.user_id == user_id)
    ).one_or_none()
    if pref is not None and pref.default_product_id is not None:
        product = db.get(PlatformProduct, pref.default_product_id)
        if product is not None:
            candidate = products_by_key.get(product.product_key)
            if candidate and candidate["status"] in SELECTABLE_PRODUCT_STATUSES:
                return product.product_key
    first = min(
        products_by_key.values(),
        key=lambda row: (row["first_seen_at"], row["product_key"]),
    )
    return str(first["product_key"])


def get_user_products(db: Session, user: User) -> tuple[list[ProductAccessSnapshot], str | None]:
    rows = db.execute(
        select(
            PlatformProduct.product_key,
            PlatformProduct.display_name,
            TenantProductAccess.status,
            TenantProductAccess.created_at,
        )
        .join(TenantProductAccess, TenantProductAccess.product_id == PlatformProduct.id)
        .join(TenantMembership, TenantMembership.tenant_id == TenantProductAccess.tenant_id)
        .where(
            TenantMembership.user_id == user.id,
            PlatformProduct.is_active.is_(True),
        )
        .order_by(TenantProductAccess.created_at.asc(), PlatformProduct.product_key.asc())
    ).all()
    products_by_key: dict[str, dict[str, object]] = {}
    for product_key, display_name, status, created_at in rows:
        if status not in SELECTABLE_PRODUCT_STATUSES:
            continue
        current = products_by_key.get(product_key)
        if current is None:
            products_by_key[product_key] = {
                "product_key": product_key,
                "display_name": display_name,
                "status": status,
                "first_seen_at": created_at,
            }
            continue
        if PRODUCT_STATUS_PRIORITY[status] > PRODUCT_STATUS_PRIORITY[str(current["status"])]:
            current["status"] = status
        if created_at < current["first_seen_at"]:
            current["first_seen_at"] = created_at

    default_product_key = _preferred_default_product_key(
        db,
        user_id=user.id,
        products_by_key=products_by_key,
    )
    snapshots = [
        ProductAccessSnapshot(
            product_key=str(row["product_key"]),
            display_name=str(row["display_name"]),
            status=str(row["status"]),
            is_default=str(row["product_key"]) == default_product_key,
        )
        for row in sorted(
            products_by_key.values(),
            key=lambda item: (item["first_seen_at"], item["product_key"]),
        )
    ]
    return snapshots, default_product_key


def tenant_ids_for_product(
    db: Session,
    *,
    user: User,
    product_key: str,
    selectable_only: bool = True,
) -> list[uuid.UUID]:
    rows = db.execute(
        select(
            TenantMembership.tenant_id,
            TenantMembership.created_at,
            TenantProductAccess.status,
            TenantProductAccess.created_at,
        )
        .join(TenantProductAccess, TenantProductAccess.tenant_id == TenantMembership.tenant_id)
        .join(PlatformProduct, PlatformProduct.id == TenantProductAccess.product_id)
        .where(
            TenantMembership.user_id == user.id,
            PlatformProduct.product_key == product_key,
            PlatformProduct.is_active.is_(True),
        )
        .order_by(TenantMembership.created_at.asc(), TenantProductAccess.created_at.asc())
    ).all()
    tenant_ids: list[uuid.UUID] = []
    seen: set[uuid.UUID] = set()
    for tenant_id, _, status, _ in rows:
        if selectable_only and status not in SELECTABLE_PRODUCT_STATUSES:
            continue
        if tenant_id in seen:
            continue
        seen.add(tenant_id)
        tenant_ids.append(tenant_id)
    return tenant_ids


def resolve_tenant_id_for_product(db: Session, *, user: User, product_key: str) -> uuid.UUID | None:
    tenant_ids = tenant_ids_for_product(db, user=user, product_key=product_key, selectable_only=True)
    return tenant_ids[0] if tenant_ids else None


def set_user_default_product(db: Session, *, user: User, product_key: str) -> tuple[list[ProductAccessSnapshot], str]:
    ensure_platform_products(db)
    product = db.scalars(
        select(PlatformProduct).where(
            PlatformProduct.product_key == product_key,
            PlatformProduct.is_active.is_(True),
        )
    ).one_or_none()
    if product is None:
        raise LookupError("Product not found")

    products, _ = get_user_products(db, user)
    accessible = next((row for row in products if row.product_key == product_key), None)
    if accessible is None or accessible.status not in SELECTABLE_PRODUCT_STATUSES:
        raise PermissionError("Product is not available for this user")

    pref = db.scalars(
        select(UserProductPreference).where(UserProductPreference.user_id == user.id)
    ).one_or_none()
    now = datetime.now(UTC)
    if pref is None:
        pref = UserProductPreference(
            user_id=user.id,
            default_product_id=product.id,
            created_at=now,
            updated_at=now,
        )
        db.add(pref)
    else:
        pref.default_product_id = product.id
        pref.updated_at = now
    db.flush()
    updated_products, default_product_key = get_user_products(db, user)
    if default_product_key is None:
        raise PermissionError("No selectable products are available")
    return updated_products, default_product_key

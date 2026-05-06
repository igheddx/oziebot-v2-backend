"""Create or update the platform root admin user (is_root_admin=True).

If the user has no tenant membership (typical after an empty DB or volume swap),
creates a minimal tenant + integration + membership + trial + token permissions,
matching normal registration bootstrap so /me and the web app work.

Usage (Compose, from repo root on the host):

  docker compose -f docker-compose.lean.yml -f docker-compose.lean.edge.yml \\
    --env-file .env.lean exec -T \\
    -e SEED_ROOT_EMAIL=you@example.com \\
    -e SEED_ROOT_PASSWORD='choose-a-strong-password' \\
    api python -m oziebot_api.scripts.seed_root_admin

Optional: SEED_ROOT_FULL_NAME, SEED_TENANT_NAME (default: Personal).
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.config import get_settings
from oziebot_api.db.session import make_engine, make_session_factory
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_api.models.user import User
from oziebot_api.services.passwords import hash_password
from oziebot_api.services.root_admin_defaults import ensure_root_admin_strategy_access
from oziebot_api.services.strategy_catalog import ensure_platform_strategy_catalog
from oziebot_api.services.token_permissions import TokenPermissionService
from oziebot_api.services.trial import start_trial_for_new_tenant


def _ensure_minimal_tenant(session: Session, user: User, now: datetime) -> None:
    has_membership = (
        session.scalars(
            select(TenantMembership).where(TenantMembership.user_id == user.id).limit(1)
        ).first()
        is not None
    )
    if has_membership:
        return

    tenant_label = os.environ.get("SEED_TENANT_NAME", "Personal").strip() or "Personal"
    tenant = Tenant(
        id=uuid.uuid4(),
        name=tenant_label,
        created_at=now,
        default_trading_mode="paper",
    )
    session.add(tenant)
    session.flush()
    session.add(
        TenantIntegration(
            tenant_id=tenant.id,
            coinbase_connected=False,
            updated_at=now,
        )
    )
    session.add(
        TenantMembership(
            id=uuid.uuid4(),
            user_id=user.id,
            tenant_id=tenant.id,
            role="user",
            created_at=now,
        )
    )
    session.flush()
    ensure_platform_strategy_catalog(session)
    start_trial_for_new_tenant(session, tenant.id)
    TokenPermissionService.initialize_user_tokens(session, user.id, enabled=True)
    print(f"Bootstrapped tenant {tenant.id} and membership for {user.email}")


def run() -> None:
    settings = get_settings()
    if not settings.database_url:
        raise SystemExit("DATABASE_URL is required")
    email = os.environ.get("SEED_ROOT_EMAIL", "root@localhost").lower().strip()
    full_name = os.environ.get("SEED_ROOT_FULL_NAME")
    password = os.environ.get("SEED_ROOT_PASSWORD")
    if not password:
        raise SystemExit("SEED_ROOT_PASSWORD is required")
    engine = make_engine(settings)
    if engine is None:
        raise SystemExit("Could not create database engine")
    factory = make_session_factory(settings)
    if factory is None:
        raise SystemExit("Could not create session factory")
    session: Session = factory()
    try:
        existing = session.scalars(
            select(User).where(func.lower(User.email) == email)
        ).one_or_none()
        now = datetime.now(UTC)
        if existing:
            existing.full_name = (
                full_name.strip() if full_name and full_name.strip() else existing.full_name
            )
            existing.password_hash = hash_password(password)
            existing.is_root_admin = True
            existing.is_active = True
            existing.updated_at = now
            print(f"Updated root admin: {email}")
        else:
            session.add(
                User(
                    id=uuid.uuid4(),
                    email=email,
                    full_name=full_name.strip() if full_name and full_name.strip() else None,
                    password_hash=hash_password(password),
                    is_root_admin=True,
                    is_active=True,
                    email_verified_at=None,
                    current_trading_mode="paper",
                    created_at=now,
                    updated_at=now,
                )
            )
            print(f"Created root admin: {email}")
        session.flush()
        user = (
            existing
            if existing is not None
            else session.scalars(select(User).where(func.lower(User.email) == email)).one()
        )
        _ensure_minimal_tenant(session, user, now)
        ensure_root_admin_strategy_access(session, user)
        session.commit()
    finally:
        session.close()


if __name__ == "__main__":
    run()

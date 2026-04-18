"""Create or update the platform root admin user (is_root_admin=True)."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.config import get_settings
from oziebot_api.db.session import make_engine, make_session_factory
from oziebot_api.models.user import User
from oziebot_api.services.passwords import hash_password
from oziebot_api.services.root_admin_defaults import ensure_root_admin_strategy_access


def run() -> None:
    settings = get_settings()
    if not settings.database_url:
        raise SystemExit("DATABASE_URL is required")
    email = os.environ.get("SEED_ROOT_EMAIL", "root@localhost").lower().strip()
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
        ensure_root_admin_strategy_access(session, user)
        session.commit()
    finally:
        session.close()


if __name__ == "__main__":
    run()

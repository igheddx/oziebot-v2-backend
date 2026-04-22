"""Pytest fixtures: in-memory SQLite, dependency overrides."""

from __future__ import annotations

import os

os.environ.setdefault("JWT_SECRET", "test-jwt-secret-key-for-pytest-only-32chars")
os.environ.setdefault("DATABASE_URL", "sqlite+pysqlite:///:memory:")

from cryptography.fernet import Fernet

os.environ.setdefault(
    "EXCHANGE_CREDENTIALS_ENCRYPTION_KEY",
    Fernet.generate_key().decode(),
)

import uuid
from collections.abc import Generator
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from oziebot_api.db.base import Base
from oziebot_api.deps import require_db
from oziebot_api.main import app
import oziebot_api.models  # noqa: F401 — register all mappers / metadata
from oziebot_api.models.user import User
from oziebot_api.services.passwords import hash_password


@pytest.fixture
def engine():
    eng = create_engine(
        "sqlite+pysqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db_session(engine) -> Generator[Session, None, None]:
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    s = SessionLocal()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


@pytest.fixture
def client(engine) -> Generator[TestClient, None, None]:
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    def override_require_db() -> Generator[Session, None, None]:
        s = SessionLocal()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()

    app.dependency_overrides[require_db] = override_require_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def root_user_and_token(client: TestClient, db_session: Session) -> tuple[str, str]:
    """Root admin email and access token."""
    email = "root@example.com"
    password = "root-password-123"
    now = datetime.now(UTC)
    u = User(
        id=uuid.uuid4(),
        email=email,
        full_name="Root Example",
        password_hash=hash_password(password),
        is_root_admin=True,
        is_active=True,
        email_verified_at=None,
        current_trading_mode="paper",
        created_at=now,
        updated_at=now,
    )
    db_session.add(u)
    db_session.commit()
    r = client.post("/v1/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return email, r.json()["access_token"]


@pytest.fixture
def regular_user_and_token(client: TestClient) -> tuple[str, str]:
    """Regular user email and access token via public register flow."""
    email = "user@example.com"
    password = "user-password-123"
    r = client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "full_name": "User Example",
            "password": password,
            "tenant_name": "User Tenant",
        },
    )
    assert r.status_code == 201, r.text
    return email, r.json()["access_token"]


@pytest.fixture
def tenant_root_user_and_token(client: TestClient, db_session: Session) -> tuple[str, str]:
    """Root admin account that also retains a tenant membership."""
    email = "tenant-root@example.com"
    password = "tenant-root-password-123"
    r = client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "full_name": "Tenant Root",
            "password": password,
            "tenant_name": "Tenant Root",
        },
    )
    assert r.status_code == 201, r.text

    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    user.is_root_admin = True
    db_session.add(user)
    db_session.commit()

    return email, r.json()["access_token"]

"""Ensure TeacherAssist AI access for the requested operational users.

Usage:

  cd backend/services/api && python -m oziebot_api.scripts.seed_teacher_assist_access

Optional environment variables:
  TEACHER_ASSIST_DOMINIC_EMAIL
  TEACHER_ASSIST_AWELE_EMAIL
  TEACHER_ASSIST_AWELE_FULL_NAME
  TEACHER_ASSIST_AWELE_TENANT_NAME
  TEACHER_ASSIST_AWELE_PASSWORD
  TEACHER_ASSIST_OZIE_EMAIL
  TEACHER_ASSIST_OZIE_FULL_NAME
  TEACHER_ASSIST_OZIE_TENANT_NAME
  TEACHER_ASSIST_OZIE_PASSWORD
"""

from __future__ import annotations

import os

from sqlalchemy.orm import Session

from oziebot_api.config import get_settings
from oziebot_api.db.session import make_engine, make_session_factory
from oziebot_api.services.teacher_assist.access_seed import (
    ensure_existing_user_teacher_assist_access,
    ensure_user_teacher_assist_access,
)

DEFAULT_DOMINIC_EMAIL = "Dominic@oziebot.com"
DEFAULT_AWELE_EMAIL = "aweleu@gmail.com"
DEFAULT_AWELE_FULL_NAME = "Awele Ighedosa"
DEFAULT_AWELE_TENANT_NAME = "Awele Ighedosa"
DEFAULT_OZIE_EMAIL = "dvaten.1992@gmail.com"
DEFAULT_OZIE_FULL_NAME = "Ozie Ighedosa"
DEFAULT_OZIE_TENANT_NAME = "Ozie Ighedosa"


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    return value or default


def run() -> None:
    settings = get_settings()
    if not settings.database_url:
        raise SystemExit("DATABASE_URL is required")
    engine = make_engine(settings)
    if engine is None:
        raise SystemExit("Could not create database engine")
    factory = make_session_factory(settings)
    if factory is None:
        raise SystemExit("Could not create session factory")

    dominic_email = _env("TEACHER_ASSIST_DOMINIC_EMAIL", DEFAULT_DOMINIC_EMAIL)
    awele_email = _env("TEACHER_ASSIST_AWELE_EMAIL", DEFAULT_AWELE_EMAIL)
    awele_full_name = _env("TEACHER_ASSIST_AWELE_FULL_NAME", DEFAULT_AWELE_FULL_NAME)
    awele_tenant_name = _env("TEACHER_ASSIST_AWELE_TENANT_NAME", DEFAULT_AWELE_TENANT_NAME)
    awele_password = os.environ.get("TEACHER_ASSIST_AWELE_PASSWORD")
    ozie_email = _env("TEACHER_ASSIST_OZIE_EMAIL", DEFAULT_OZIE_EMAIL)
    ozie_full_name = _env("TEACHER_ASSIST_OZIE_FULL_NAME", DEFAULT_OZIE_FULL_NAME)
    ozie_tenant_name = _env("TEACHER_ASSIST_OZIE_TENANT_NAME", DEFAULT_OZIE_TENANT_NAME)
    ozie_password = os.environ.get("TEACHER_ASSIST_OZIE_PASSWORD")

    session: Session = factory()
    try:
        dominic = ensure_existing_user_teacher_assist_access(session, email=dominic_email)
        awele = ensure_user_teacher_assist_access(
            session,
            email=awele_email,
            full_name=awele_full_name,
            tenant_name=awele_tenant_name,
            password=awele_password,
        )
        ozie = ensure_user_teacher_assist_access(
            session,
            email=ozie_email,
            full_name=ozie_full_name,
            tenant_name=ozie_tenant_name,
            password=ozie_password,
        )
        session.commit()
        for result in (dominic, awele, ozie):
            print(
                "Ensured TeacherAssist access:",
                {
                    "email": result.email,
                    "tenant_id": str(result.tenant_id),
                    "created_user": result.created_user,
                    "created_tenant": result.created_tenant,
                    "default_product": result.default_product,
                    "temporary_password_generated": result.temporary_password_generated,
                },
            )
    finally:
        session.close()


if __name__ == "__main__":
    run()

"""Reset TeacherAssist setup for specific users while preserving accounts and seed catalog.

Usage:

  cd backend/services/api && python -m oziebot_api.scripts.reset_teacher_assist_user_setup

Optional:
  TEACHER_ASSIST_RESET_EMAILS=aweleu@yahoo.com,dvaten.1992@gmail.com
  TEACHER_ASSIST_BLANK_SLATE=1
"""

from __future__ import annotations

import os

from sqlalchemy.orm import Session

from oziebot_api.config import get_settings
from oziebot_api.db.session import make_engine, make_session_factory
from oziebot_api.services.teacher_assist.reset_user_setup import reset_teacher_assist_user_setup

DEFAULT_EMAILS = ("aweleu@yahoo.com", "dvaten.1992@gmail.com")


def _emails() -> list[str]:
    raw = os.environ.get("TEACHER_ASSIST_RESET_EMAILS", ",".join(DEFAULT_EMAILS))
    return [part.strip() for part in raw.split(",") if part.strip()]


def _blank_slate() -> bool:
    return os.environ.get("TEACHER_ASSIST_BLANK_SLATE", "").strip().lower() in {"1", "true", "yes"}


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

    session: Session = factory()
    try:
        for email in _emails():
            result = reset_teacher_assist_user_setup(
                session,
                email=email,
                blank_slate=_blank_slate(),
            )
            print("Reset TeacherAssist setup:", result)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


if __name__ == "__main__":
    run()

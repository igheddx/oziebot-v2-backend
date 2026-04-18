from collections.abc import Generator
from functools import lru_cache
from typing import Annotated

from fastapi import Depends, HTTPException
from sqlalchemy.orm import Session

from oziebot_api.config import Settings, get_settings
from oziebot_api.db.session import get_db


@lru_cache
def cached_settings() -> Settings:
    return get_settings()


def settings_dep() -> Settings:
    return cached_settings()


def db_dep(
    settings: Settings = Depends(settings_dep),
) -> Generator[Session | None, None, None]:
    yield from get_db(settings)


def require_db(session: Session | None = Depends(db_dep)) -> Session:
    if session is None:
        raise HTTPException(status_code=503, detail="Database not configured")
    return session


DbSession = Annotated[Session, Depends(require_db)]

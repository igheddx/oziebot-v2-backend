from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.deps import db_dep, settings_dep

router = APIRouter(tags=["health"])


@router.get("/health")
def health(settings: Settings = Depends(settings_dep)) -> dict:
    return {"status": "ok", "env": settings.app_env}


@router.get("/ready")
def ready(
    settings: Settings = Depends(settings_dep),
    db: Session | None = Depends(db_dep),
) -> dict:
    if db is None or not settings.database_url:
        return {"status": "degraded", "database": "not_configured"}
    db.execute(text("SELECT 1"))
    return {"status": "ok", "database": "connected"}

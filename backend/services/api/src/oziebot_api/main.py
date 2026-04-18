from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from oziebot_api.api.v1.router import api_router
from oziebot_api.config import get_settings
from oziebot_api.deps import cached_settings


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Oziebot API", version="0.1.0")

    @app.get("/health")
    def root_health() -> dict:
        return {"status": "ok"}

    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.on_event("startup")
    def _warm_settings() -> None:
        cached_settings.cache_clear()
        cached_settings()

    app.include_router(api_router)
    return app


app = create_app()

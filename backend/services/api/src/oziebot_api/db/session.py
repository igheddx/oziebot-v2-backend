from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

from oziebot_api.config import Settings
from oziebot_api.services.performance_observability import register_query_observers


@lru_cache(maxsize=8)
def _cached_engine(database_url: str, slow_query_ms: int):
    url = make_url(database_url)
    engine_kwargs = {"pool_pre_ping": True}
    if not url.drivername.startswith("sqlite"):
        engine_kwargs["pool_size"] = 5
        engine_kwargs["max_overflow"] = 10
    engine = create_engine(database_url, **engine_kwargs)
    register_query_observers(engine, slow_query_ms=slow_query_ms)
    return engine


def make_engine(settings: Settings):
    if not settings.database_url:
        return None
    return _cached_engine(settings.database_url, settings.api_slow_query_ms)


@lru_cache(maxsize=8)
def _cached_session_factory(database_url: str, slow_query_ms: int):
    engine = _cached_engine(database_url, slow_query_ms)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def make_session_factory(settings: Settings):
    if not settings.database_url:
        return None
    return _cached_session_factory(settings.database_url, settings.api_slow_query_ms)


def get_db(settings: Settings) -> Generator[Session | None, None, None]:
    factory = make_session_factory(settings)
    if factory is None:
        yield None
        return
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

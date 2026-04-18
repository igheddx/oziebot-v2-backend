from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from oziebot_api.config import Settings


def make_engine(settings: Settings):
    if not settings.database_url:
        return None
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )


def make_session_factory(settings: Settings):
    engine = make_engine(settings)
    if engine is None:
        return None
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


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

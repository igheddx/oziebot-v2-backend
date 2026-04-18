from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.platform_strategy import PlatformStrategy


def ensure_platform_strategy_catalog(db: Session) -> None:
    from oziebot_strategy_engine.registry import StrategyRegistry

    existing = {row.slug: row for row in db.scalars(select(PlatformStrategy)).all()}
    now = datetime.now(UTC)

    for strategy in StrategyRegistry.list_strategies():
        slug = str(strategy["strategy_id"]).strip().lower()
        if slug in existing:
            continue
        db.add(
            PlatformStrategy(
                id=uuid.uuid4(),
                slug=slug,
                display_name=str(strategy["display_name"]),
                description=str(strategy["description"]),
                is_enabled=True,
                entry_point=None,
                config_schema=dict(strategy["config_schema"]),
                sort_order=0,
                created_at=now,
                updated_at=now,
            )
        )

    db.flush()

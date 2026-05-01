from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine, text

from oziebot_common.postgres_runtime_kv import PostgresRuntimeKV
from oziebot_market_data_ingestor.normalizer import normalize_bbo
from oziebot_market_data_ingestor.postgres_market_cache import PostgresMarketCache


def test_postgres_market_cache_writes_bbo_to_runtime_kv_memory_sqlite(tmp_path):
    db_path = tmp_path / "md.sqlite"
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE runtime_kv ("
                "cache_key TEXT PRIMARY KEY, value_text TEXT NOT NULL, expires_at TEXT NOT NULL)"
            )
        )

    kv = PostgresRuntimeKV(eng)
    cache = PostgresMarketCache(kv, ttl_seconds=60)
    item = normalize_bbo(
        {
            "product_id": "BTC-USD",
            "best_bid": "1",
            "best_bid_size": "2",
            "best_ask": "3",
            "best_ask_size": "4",
            "time": datetime.now(UTC).isoformat(),
        }
    )
    cache.put_bbo(item)
    raw = kv.get(PostgresMarketCache.key_bbo("BTC-USD"))
    assert raw is not None
    assert "BTC-USD" in raw or "best_bid" in raw

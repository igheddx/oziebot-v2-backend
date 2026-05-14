from __future__ import annotations

import json
from datetime import UTC, datetime

from sqlalchemy import create_engine, text

from oziebot_market_data_ingestor.normalizer import normalize_candle
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


def test_postgres_market_cache_deduplicates_and_sorts_candle_history(tmp_path):
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
    cache = PostgresMarketCache(kv, ttl_seconds=60, candle_history_limit=3)
    symbol = "BTC-USD"

    cache.put_candle(
        normalize_candle(
            {
                "product_id": symbol,
                "start": 1_700_000_120,
                "open": "101",
                "high": "102",
                "low": "100",
                "close": "101",
                "volume": "10",
            },
            granularity_sec=60,
        )
    )
    cache.put_candle(
        normalize_candle(
            {
                "product_id": symbol,
                "start": 1_700_000_000,
                "open": "91",
                "high": "92",
                "low": "90",
                "close": "91",
                "volume": "8",
            },
            granularity_sec=60,
        )
    )
    cache.put_candle(
        normalize_candle(
            {
                "product_id": symbol,
                "start": 1_700_000_060,
                "open": "96",
                "high": "97",
                "low": "95",
                "close": "96",
                "volume": "9",
            },
            granularity_sec=60,
        )
    )
    cache.put_candle(
        normalize_candle(
            {
                "product_id": symbol,
                "start": 1_700_000_060,
                "open": "196",
                "high": "197",
                "low": "195",
                "close": "196",
                "volume": "11",
            },
            granularity_sec=60,
        )
    )

    history = kv.lrange_strings(f"oziebot:md:candles:60:{symbol}", 0, 9)
    parsed = [json.loads(entry) for entry in history]

    assert [entry["bucket_start"] for entry in parsed] == [
        "2023-11-14T22:15:20Z",
        "2023-11-14T22:14:20Z",
        "2023-11-14T22:13:20Z",
    ]
    assert parsed[1]["close"] == "196"

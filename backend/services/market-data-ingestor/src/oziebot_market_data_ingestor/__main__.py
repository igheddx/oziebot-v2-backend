from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import redis
from sqlalchemy import create_engine

from oziebot_common.health import start_health_server
from oziebot_market_data_ingestor.coinbase_client import CoinbaseRestClient, CoinbaseWsClient
from oziebot_market_data_ingestor.config import get_settings
from oziebot_market_data_ingestor.normalizer import (
    normalize_bbo,
    normalize_candle,
    normalize_orderbook_top,
    normalize_trade,
)
from oziebot_market_data_ingestor.policy_refresh import TokenPolicyRefresher
from oziebot_market_data_ingestor.redis_cache import RedisMarketCache
from oziebot_market_data_ingestor.stale import StaleDataDetector, StaleThresholds
from oziebot_market_data_ingestor.storage import MarketDataStore
from oziebot_market_data_ingestor.universe import SymbolUniverseProvider

log = logging.getLogger("market-data-ingestor")
logging.basicConfig(level=logging.INFO)


async def _reconcile_candles(
    rest: CoinbaseRestClient,
    store: MarketDataStore,
    cache: RedisMarketCache,
    stale: StaleDataDetector,
    products: list[str],
    granularity: int,
) -> None:
    now = datetime.now(UTC)
    for p in products:
        try:
            candles = await rest.get_candles(p, granularity_sec=granularity, limit=50)
            for c in candles:
                item = normalize_candle(c, granularity)
                cache.put_candle(item)
                store.insert_candle(item)
            stale.mark_candle(p, now)
        except Exception as exc:
            log.warning("candle reconciliation failed product=%s err=%s", p, exc)


async def _reconcile_trades(
    rest: CoinbaseRestClient,
    store: MarketDataStore,
    cache: RedisMarketCache,
    stale: StaleDataDetector,
    products: list[str],
    limit: int,
) -> None:
    for p in products:
        try:
            trades = await rest.get_recent_trades(p, limit=limit)
            for t in trades:
                item = normalize_trade(t)
                cache.put_trade(item)
                store.insert_trade_snapshot(item)
                stale.mark_trade(item.product_id, item.ingest_time)
        except Exception as exc:
            log.warning("trade reconciliation failed product=%s err=%s", p, exc)


async def _reconcile_bbo(
    rest: CoinbaseRestClient,
    store: MarketDataStore,
    cache: RedisMarketCache,
    stale: StaleDataDetector,
    products: list[str],
) -> None:
    for p in products:
        try:
            ticker = await rest.get_ticker(p)
            item = normalize_bbo(ticker)
            cache.put_bbo(item)
            store.insert_bbo_snapshot(item)
            stale.mark_bbo(item.product_id, item.ingest_time)
        except Exception as exc:
            log.warning("bbo reconciliation failed product=%s err=%s", p, exc)


async def main() -> None:
    s = get_settings()
    engine = create_engine(s.database_url)
    r = redis.Redis.from_url(s.redis_url, decode_responses=True)

    universe = SymbolUniverseProvider(engine)
    products = universe.list_active_product_ids()
    if not products:
        log.info("no enabled products found (platform + user token filters)")
        return

    cache = RedisMarketCache(r, ttl_seconds=s.cache_ttl_seconds)
    store = MarketDataStore(engine)
    refresher = TokenPolicyRefresher(engine)
    stale = StaleDataDetector(
        thresholds=StaleThresholds(
            trade=s.stale_trade_seconds,
            bbo=s.stale_bbo_seconds,
            candle=s.stale_candle_seconds,
        )
    )

    ws = CoinbaseWsClient(s.coinbase_ws_url)
    rest = CoinbaseRestClient(s.coinbase_rest_url)
    health = start_health_server("market-data-ingestor")

    log.info("subscribing to products=%s", products)

    # Seed candle history on startup with 50 candles so MAs can be computed immediately
    log.info("seeding market cache for products=%s", products)
    await _reconcile_trades(
        rest,
        store,
        cache,
        stale,
        products,
        s.trade_recovery_limit,
    )
    health.touch()
    await _reconcile_bbo(rest, store, cache, stale, products)
    health.touch()
    await _reconcile_candles(rest, store, cache, stale, products, s.candles_granularity_sec)
    refresher.refresh_active_tokens()
    health.mark_ready()

    channels = ["ticker", "matches", "level2"]
    last_candle_reconcile = datetime.now(UTC)
    last_policy_refresh = datetime.now(UTC)
    try:
        async for msg in ws.subscribe_and_stream(products, channels):
            typ = msg.get("type")
            if typ == "match":
                trade = normalize_trade(msg)
                cache.put_trade(trade)
                store.insert_trade_snapshot(trade)
                stale.mark_trade(trade.product_id, trade.ingest_time)
            elif typ == "ticker":
                bbo = normalize_bbo(msg)
                cache.put_bbo(bbo)
                store.insert_bbo_snapshot(bbo)
                stale.mark_bbo(bbo.product_id, bbo.ingest_time)
            elif typ in {"snapshot", "l2update"} and msg.get("product_id"):
                top = normalize_orderbook_top(msg, depth=s.orderbook_depth)
                cache.put_orderbook(top)
            elif typ == "candle":
                candle = normalize_candle(msg, s.candles_granularity_sec)
                cache.put_candle(candle)
                store.insert_candle(candle)
                stale.mark_candle(candle.product_id, candle.ingest_time)
            health.touch()

            now = datetime.now(UTC)
            stale_map = stale.stale_products(now, products)
            if any(stale_map.values()):
                cache.publish_stale("oziebot:md:stale", {"at": now.isoformat(), "stale": stale_map})
                await _reconcile_trades(
                    rest,
                    store,
                    cache,
                    stale,
                    stale_map["trade"],
                    s.trade_recovery_limit,
                )
                await _reconcile_bbo(rest, store, cache, stale, stale_map["bbo"])
                await _reconcile_candles(rest, store, cache, stale, stale_map["candle"], s.candles_granularity_sec)
                health.touch()

            if (now - last_candle_reconcile).total_seconds() >= s.candles_granularity_sec:
                await _reconcile_candles(rest, store, cache, stale, products, s.candles_granularity_sec)
                last_candle_reconcile = now
                health.touch()
            if (now - last_policy_refresh).total_seconds() >= s.token_policy_recalc_interval_seconds:
                refresher.refresh_active_tokens()
                last_policy_refresh = now
                health.touch()
    finally:
        await rest.close()


if __name__ == "__main__":
    asyncio.run(main())

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import create_engine

from oziebot_common import redis_from_url
from oziebot_common.health import start_health_server
from oziebot_common.trade_log import append_trade_log_event
from oziebot_market_data_ingestor.coinbase_client import (
    CoinbaseRestClient,
    CoinbaseWsClient,
)
from oziebot_market_data_ingestor.config import get_settings
from oziebot_market_data_ingestor.normalizer import (
    normalize_bbo,
    normalize_candle,
    normalize_orderbook_top,
    normalize_trade,
)
from oziebot_market_data_ingestor.policy_refresh import TokenPolicyRefresher
from oziebot_market_data_ingestor.redis_cache import RedisMarketCache
from oziebot_market_data_ingestor.signal_panel import SignalPanelEmitter
from oziebot_market_data_ingestor.stale import StaleDataDetector, StaleThresholds
from oziebot_market_data_ingestor.storage import MarketDataStore
from oziebot_market_data_ingestor.universe import SymbolUniverseProvider

log = logging.getLogger("market-data-ingestor")
logging.basicConfig(level=logging.INFO)
RAW_STREAM_LOG_SAMPLE_SECONDS = 15


class TradeLogSampler:
    def __init__(self, interval_seconds: int = RAW_STREAM_LOG_SAMPLE_SECONDS) -> None:
        self._interval_seconds = interval_seconds
        self._last_emit: dict[tuple[str, str], datetime] = {}

    def should_emit(
        self, *, symbol: str, event_type: str, now: datetime | None = None
    ) -> bool:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        key = (symbol.upper(), event_type)
        previous = self._last_emit.get(key)
        if (
            previous is not None
            and (current - previous).total_seconds() < self._interval_seconds
        ):
            return False
        self._last_emit[key] = current
        return True


def _format_decimal(value: Decimal, *, places: int = 6) -> str:
    quantized = value.quantize(Decimal(f"1e-{places}"))
    text = format(quantized.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _spread_pct(bid: Decimal, ask: Decimal) -> Decimal:
    if bid <= 0 or ask <= 0:
        return Decimal("0")
    mid = (bid + ask) / Decimal("2")
    if mid <= 0:
        return Decimal("0")
    return ((ask - bid) / mid) * Decimal("100")


def _trade_snapshot_summary(trades) -> tuple[str, dict[str, object]]:
    latest = max(trades, key=lambda item: item.event_time)
    high = max(item.price for item in trades)
    low = min(item.price for item in trades)
    total_size = sum((item.size for item in trades), Decimal("0"))
    total_notional = sum((item.price * item.size for item in trades), Decimal("0"))
    details = {
        "sample_count": len(trades),
        "last_price": _format_decimal(latest.price, places=2),
        "last_size": _format_decimal(latest.size),
        "last_side": latest.side,
        "high_price": _format_decimal(high, places=2),
        "low_price": _format_decimal(low, places=2),
        "total_size": _format_decimal(total_size),
        "notional_usd": _format_decimal(total_notional, places=2),
    }
    message = (
        f"{latest.product_id} market snapshot pulled | "
        f"last {_format_decimal(latest.price, places=2)} "
        f"({latest.side} {_format_decimal(latest.size)}), "
        f"range {_format_decimal(low, places=2)}-{_format_decimal(high, places=2)}"
    )
    return message, details


def _bbo_summary(item, *, streamed: bool) -> tuple[str, dict[str, object]]:
    spread_pct = _spread_pct(item.best_bid_price, item.best_ask_price)
    mid = (item.best_bid_price + item.best_ask_price) / Decimal("2")
    details = {
        "best_bid": _format_decimal(item.best_bid_price, places=2),
        "bid_size": _format_decimal(item.best_bid_size),
        "best_ask": _format_decimal(item.best_ask_price, places=2),
        "ask_size": _format_decimal(item.best_ask_size),
        "mid_price": _format_decimal(mid, places=2),
        "spread_pct": _format_decimal(spread_pct, places=4),
    }
    label = "BBO stream sampled" if streamed else "BBO refreshed"
    message = (
        f"{item.product_id} {label} | "
        f"bid {_format_decimal(item.best_bid_price, places=2)} x {_format_decimal(item.best_bid_size)}, "
        f"ask {_format_decimal(item.best_ask_price, places=2)} x {_format_decimal(item.best_ask_size)}, "
        f"spread {_format_decimal(spread_pct, places=4)}%"
    )
    return message, details


def _candle_summary(
    candles, *, streamed: bool, granularity_sec: int
) -> tuple[str, dict[str, object]]:
    latest = max(candles, key=lambda item: item.bucket_start)
    details = {
        "granularity_sec": granularity_sec,
        "sample_count": len(candles),
        "open": _format_decimal(latest.open, places=2),
        "high": _format_decimal(latest.high, places=2),
        "low": _format_decimal(latest.low, places=2),
        "close": _format_decimal(latest.close, places=2),
        "volume": _format_decimal(latest.volume),
        "bucket_start": latest.bucket_start,
    }
    label = "candle streamed" if streamed else "candles refreshed"
    message = (
        f"{latest.product_id} {label} | "
        f"O {_format_decimal(latest.open, places=2)} "
        f"H {_format_decimal(latest.high, places=2)} "
        f"L {_format_decimal(latest.low, places=2)} "
        f"C {_format_decimal(latest.close, places=2)} "
        f"V {_format_decimal(latest.volume)}"
    )
    return message, details


def _trade_tick_summary(trade) -> tuple[str, dict[str, object]]:
    details = {
        "trade_id": trade.trade_id,
        "price": _format_decimal(trade.price, places=2),
        "size": _format_decimal(trade.size),
        "side": trade.side,
        "event_time": trade.event_time,
    }
    message = (
        f"{trade.product_id} trade tick sampled | "
        f"{trade.side} {_format_decimal(trade.size)} @ {_format_decimal(trade.price, places=2)}"
    )
    return message, details


def _orderbook_summary(top) -> tuple[str, dict[str, object]]:
    best_bid_price, best_bid_size = (
        top.bids[0] if top.bids else (Decimal("0"), Decimal("0"))
    )
    best_ask_price, best_ask_size = (
        top.asks[0] if top.asks else (Decimal("0"), Decimal("0"))
    )
    spread_pct = _spread_pct(best_bid_price, best_ask_price)
    details = {
        "depth": top.depth,
        "best_bid": _format_decimal(best_bid_price, places=2),
        "bid_size": _format_decimal(best_bid_size),
        "best_ask": _format_decimal(best_ask_price, places=2),
        "ask_size": _format_decimal(best_ask_size),
        "spread_pct": _format_decimal(spread_pct, places=4),
        "bid_levels": len(top.bids),
        "ask_levels": len(top.asks),
    }
    message = (
        f"{top.product_id} orderbook top sampled | "
        f"bid {_format_decimal(best_bid_price, places=2)} x {_format_decimal(best_bid_size)}, "
        f"ask {_format_decimal(best_ask_price, places=2)} x {_format_decimal(best_ask_size)}, "
        f"depth {top.depth}"
    )
    return message, details


async def _reconcile_candles(
    rest: CoinbaseRestClient,
    store: MarketDataStore,
    cache: RedisMarketCache,
    log_client,
    stale: StaleDataDetector,
    products: list[str],
    granularity: int,
) -> None:
    now = datetime.now(UTC)
    for p in products:
        try:
            normalized = []
            candles = await rest.get_candles(p, granularity_sec=granularity, limit=50)
            for c in candles:
                item = normalize_candle(c, granularity)
                normalized.append(item)
                cache.put_candle(item)
                store.insert_candle(item)
            if not normalized:
                continue
            stale.mark_candle(p, now)
            message, details = _candle_summary(
                normalized,
                streamed=False,
                granularity_sec=granularity,
            )
        except Exception as exc:
            log.warning("candle reconciliation failed product=%s err=%s", p, exc)


async def _reconcile_trades(
    rest: CoinbaseRestClient,
    store: MarketDataStore,
    cache: RedisMarketCache,
    log_client,
    stale: StaleDataDetector,
    products: list[str],
    limit: int,
    signal_panel: SignalPanelEmitter | None = None,
) -> None:
    for p in products:
        try:
            normalized = []
            trades = await rest.get_recent_trades(p, limit=limit)
            for t in trades:
                item = normalize_trade(t)
                normalized.append(item)
                cache.put_trade(item)
                store.insert_trade_snapshot(item)
                stale.mark_trade(item.product_id, item.ingest_time)
                if signal_panel is not None:
                    signal_panel.observe_trade(item)
            if not normalized:
                continue
        except Exception as exc:
            log.warning("trade reconciliation failed product=%s err=%s", p, exc)


async def _reconcile_bbo(
    rest: CoinbaseRestClient,
    store: MarketDataStore,
    cache: RedisMarketCache,
    log_client,
    stale: StaleDataDetector,
    products: list[str],
    signal_panel: SignalPanelEmitter | None = None,
) -> None:
    for p in products:
        try:
            ticker = await rest.get_ticker(p)
            item = normalize_bbo(ticker)
            cache.put_bbo(item)
            store.insert_bbo_snapshot(item)
            stale.mark_bbo(item.product_id, item.ingest_time)
            if signal_panel is not None:
                signal_panel.observe_bbo(item)
            message, details = _bbo_summary(item, streamed=False)
            append_trade_log_event(
                log_client,
                symbol=p,
                event_type="bbo_update",
                message=message,
                details=details,
            )
            if signal_panel is not None:
                signal_panel.force_emit(p, now=item.ingest_time)
        except Exception as exc:
            log.warning("bbo reconciliation failed product=%s err=%s", p, exc)


async def main() -> None:
    s = get_settings()
    engine = create_engine(s.database_url)
    r = redis_from_url(
        s.redis_url,
        probe=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    )

    universe = SymbolUniverseProvider(engine)
    products = universe.list_active_product_ids()
    if not products:
        log.info("no enabled products found (platform + user token filters)")
        return

    cache = RedisMarketCache(r, ttl_seconds=s.cache_ttl_seconds)
    store = MarketDataStore(engine)
    refresher = TokenPolicyRefresher(engine)
    signal_panel = SignalPanelEmitter(r)
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
        r,
        stale,
        products,
        s.trade_recovery_limit,
        signal_panel,
    )
    health.touch()
    await _reconcile_bbo(rest, store, cache, r, stale, products, signal_panel)
    health.touch()
    await _reconcile_candles(
        rest, store, cache, r, stale, products, s.candles_granularity_sec
    )
    refresher.refresh_active_tokens()
    health.mark_ready()

    channels = ["ticker", "matches", "level2"]
    last_candle_reconcile = datetime.now(UTC)
    last_policy_refresh = datetime.now(UTC)
    trade_log_sampler = TradeLogSampler()
    try:
        async for msg in ws.subscribe_and_stream(products, channels):
            typ = msg.get("type")
            if typ == "match":
                trade = normalize_trade(msg)
                cache.put_trade(trade)
                store.insert_trade_snapshot(trade)
                stale.mark_trade(trade.product_id, trade.ingest_time)
                if trade_log_sampler.should_emit(
                    symbol=trade.product_id,
                    event_type="trade_tick",
                    now=trade.ingest_time,
                ):
                    message, details = _trade_tick_summary(trade)
                    append_trade_log_event(
                        r,
                        symbol=trade.product_id,
                        event_type="trade_tick",
                        message=message,
                        timestamp=trade.ingest_time,
                        details=details,
                    )
                signal_panel.observe_trade(trade)
            elif typ == "ticker":
                bbo = normalize_bbo(msg)
                cache.put_bbo(bbo)
                store.insert_bbo_snapshot(bbo)
                stale.mark_bbo(bbo.product_id, bbo.ingest_time)
                if trade_log_sampler.should_emit(
                    symbol=bbo.product_id,
                    event_type="bbo_stream",
                    now=bbo.ingest_time,
                ):
                    message, details = _bbo_summary(bbo, streamed=True)
                    append_trade_log_event(
                        r,
                        symbol=bbo.product_id,
                        event_type="bbo_update",
                        message=message,
                        timestamp=bbo.ingest_time,
                        details=details,
                    )
                signal_panel.observe_bbo(bbo)
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
                cache.publish_stale(
                    "oziebot:md:stale", {"at": now.isoformat(), "stale": stale_map}
                )
                await _reconcile_trades(
                    rest,
                    store,
                    cache,
                    r,
                    stale,
                    stale_map["trade"],
                    s.trade_recovery_limit,
                    signal_panel,
                )
                await _reconcile_bbo(
                    rest,
                    store,
                    cache,
                    r,
                    stale,
                    stale_map["bbo"],
                    signal_panel,
                )
                await _reconcile_candles(
                    rest,
                    store,
                    cache,
                    r,
                    stale,
                    stale_map["candle"],
                    s.candles_granularity_sec,
                )
                health.touch()

            if (
                now - last_candle_reconcile
            ).total_seconds() >= s.candles_granularity_sec:
                await _reconcile_candles(
                    rest, store, cache, r, stale, products, s.candles_granularity_sec
                )
                last_candle_reconcile = now
                health.touch()
            if (
                now - last_policy_refresh
            ).total_seconds() >= s.token_policy_recalc_interval_seconds:
                refresher.refresh_active_tokens()
                last_policy_refresh = now
                health.touch()
    finally:
        await rest.close()


if __name__ == "__main__":
    asyncio.run(main())

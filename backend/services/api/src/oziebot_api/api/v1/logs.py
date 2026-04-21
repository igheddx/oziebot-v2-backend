from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query
import redis
from sqlalchemy import select

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.market_data import MarketDataBboSnapshot, MarketDataTradeSnapshot
from oziebot_common import redis_from_url
from oziebot_common.trade_log import (
    MAX_TRADE_LOG_LIMIT,
    MAX_TRADE_LOG_WINDOW_SECONDS,
    build_trade_log_event,
    read_trade_log_events,
)
from oziebot_common.trade_log_intelligence import (
    build_market_signal_snapshot,
    read_trade_log_summaries,
)

router = APIRouter(prefix="/logs", tags=["logs"])


def _build_db_trade_log_fallback(
    db: DbSession,
    *,
    window_seconds: int,
    limit: int,
    symbol: str | None,
    event_type: str | None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    cutoff = datetime.now(UTC) - timedelta(seconds=window_seconds)
    normalized_symbol = str(symbol or "").upper() or None
    normalized_event_type = str(event_type or "").lower() or None

    trade_query = (
        select(MarketDataTradeSnapshot)
        .where(MarketDataTradeSnapshot.event_time >= cutoff)
        .order_by(MarketDataTradeSnapshot.event_time.desc())
        .limit(limit)
    )
    bbo_query = (
        select(MarketDataBboSnapshot)
        .where(MarketDataBboSnapshot.event_time >= cutoff)
        .order_by(MarketDataBboSnapshot.event_time.desc())
        .limit(limit)
    )
    if normalized_symbol:
        trade_query = trade_query.where(MarketDataTradeSnapshot.product_id == normalized_symbol)
        bbo_query = bbo_query.where(MarketDataBboSnapshot.product_id == normalized_symbol)

    trade_rows = list(reversed(db.scalars(trade_query).all()))
    bbo_rows = list(reversed(db.scalars(bbo_query).all()))
    latest_trade_by_symbol: dict[str, MarketDataTradeSnapshot] = {}
    for row in trade_rows:
        latest_trade_by_symbol[str(row.product_id).upper()] = row

    events: list[dict[str, object]] = []
    if normalized_event_type in (None, "trade_tick"):
        for row in trade_rows:
            events.append(
                build_trade_log_event(
                    symbol=row.product_id,
                    event_type="trade_tick",
                    message=(
                        f"{row.product_id} trade tick fallback | "
                        f"{row.side} {Decimal(str(row.size)).normalize()} @ "
                        f"{Decimal(str(row.price)).quantize(Decimal('0.01'))}"
                    ),
                    timestamp=row.event_time,
                    details={
                        "trade_id": row.trade_id,
                        "price": row.price,
                        "size": row.size,
                        "side": row.side,
                        "event_time": row.event_time,
                    },
                )
            )
    if normalized_event_type in (None, "bbo_update"):
        for row in bbo_rows:
            bid = Decimal(str(row.best_bid_price))
            ask = Decimal(str(row.best_ask_price))
            mid = (bid + ask) / Decimal("2") if bid > 0 and ask > 0 else Decimal("0")
            spread_pct = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
            events.append(
                build_trade_log_event(
                    symbol=row.product_id,
                    event_type="bbo_update",
                    message=(
                        f"{row.product_id} BBO fallback | "
                        f"bid {bid.quantize(Decimal('0.01'))}, ask {ask.quantize(Decimal('0.01'))}"
                    ),
                    timestamp=row.event_time,
                    details={
                        "best_bid": row.best_bid_price,
                        "bid_size": row.best_bid_size,
                        "best_ask": row.best_ask_price,
                        "ask_size": row.best_ask_size,
                        "mid_price": mid,
                        "spread_pct": spread_pct,
                    },
                )
            )
    events.sort(key=lambda item: str(item["timestamp"]))
    events = events[-limit:]

    bbo_by_symbol: dict[str, list[MarketDataBboSnapshot]] = {}
    for row in bbo_rows:
        bbo_by_symbol.setdefault(str(row.product_id).upper(), []).append(row)

    summaries: list[dict[str, object]] = []
    for product_id, rows in bbo_by_symbol.items():
        latest_trade = latest_trade_by_symbol.get(product_id)
        samples = []
        for row in rows[-30:]:
            bid = Decimal(str(row.best_bid_price))
            ask = Decimal(str(row.best_ask_price))
            mid = (bid + ask) / Decimal("2") if bid > 0 and ask > 0 else Decimal("0")
            spread_pct = ((ask - bid) / mid) * Decimal("100") if mid > 0 else Decimal("0")
            trade_size = (
                Decimal(str(latest_trade.size)) if latest_trade is not None else Decimal("0")
            )
            trade_price = Decimal(str(latest_trade.price)) if latest_trade is not None else mid
            samples.append(
                {
                    "timestamp": row.event_time.isoformat(),
                    "symbol": product_id,
                    "sample": {
                        "mid_price": mid,
                        "spread_pct": spread_pct,
                        "best_bid": row.best_bid_price,
                        "best_ask": row.best_ask_price,
                        "bid_size": row.best_bid_size,
                        "ask_size": row.best_ask_size,
                        "trade_volume": trade_size,
                        "trade_notional_usd": trade_price * trade_size,
                        "buy_volume": trade_size
                        if latest_trade is not None and str(latest_trade.side).lower() == "buy"
                        else Decimal("0"),
                        "sell_volume": trade_size
                        if latest_trade is not None and str(latest_trade.side).lower() == "sell"
                        else Decimal("0"),
                        "trade_count": 1 if latest_trade is not None else 0,
                        "last_price": trade_price,
                        "price_high": mid,
                        "price_low": mid,
                    },
                }
            )
        snapshot = build_market_signal_snapshot(symbol=product_id, samples=samples)
        if snapshot is not None:
            summaries.append(snapshot)

    return events, summaries


@router.get("/trade")
def get_trade_log(
    _user: CurrentUser,
    db: DbSession,
    window_seconds: int = Query(default=120, ge=1, le=MAX_TRADE_LOG_WINDOW_SECONDS),
    limit: int = Query(default=200, ge=1, le=MAX_TRADE_LOG_LIMIT),
    symbol: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    settings: Settings = Depends(settings_dep),
) -> dict[str, object]:
    redis_error: redis.RedisError | ValueError | None = None
    try:
        client = redis_from_url(
            settings.redis_url,
            probe=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        events = read_trade_log_events(
            client,
            window_seconds=window_seconds,
            limit=limit,
            symbol=symbol,
            event_type=event_type,
        )
        summaries = read_trade_log_summaries(client, symbol=symbol)
    except (redis.RedisError, ValueError) as exc:
        redis_error = exc
        events = []
        summaries = []
    if not events and not summaries:
        events, summaries = _build_db_trade_log_fallback(
            db,
            window_seconds=window_seconds,
            limit=limit,
            symbol=symbol,
            event_type=event_type,
        )
    if redis_error is not None and not events and not summaries:
        raise HTTPException(
            status_code=503, detail="Trade log temporarily unavailable"
        ) from redis_error
    available_symbols = sorted(
        {str(event["symbol"]) for event in events}
        | {str(summary.get("symbol") or "") for summary in summaries if summary.get("symbol")}
    )
    available_event_types = sorted({str(event["event_type"]) for event in events})
    return {
        "window_seconds": window_seconds,
        "limit": limit,
        "symbol": symbol.upper() if symbol else None,
        "event_type": event_type,
        "count": len(events),
        "available_symbols": available_symbols,
        "available_event_types": available_event_types,
        "summaries": summaries,
        "events": events,
    }

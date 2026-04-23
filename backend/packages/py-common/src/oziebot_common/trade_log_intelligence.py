from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from math import sqrt
from typing import Any, Mapping

from oziebot_common.trade_log import (
    DEFAULT_TRADE_LOG_RETENTION_SECONDS,
    MAX_TRADE_LOG_WINDOW_SECONDS,
    normalize_trade_log_payload,
)

TRADE_LOG_SAMPLE_KEY_PREFIX = "oziebot:logs:trade:samples:"
TRADE_LOG_SUMMARY_KEY_PREFIX = "oziebot:logs:trade:summary:"
TRADE_LOG_SYMBOLS_KEY = "oziebot:logs:trade:symbols"


def trade_log_sample_key(symbol: str) -> str:
    return f"{TRADE_LOG_SAMPLE_KEY_PREFIX}{str(symbol).upper()}"


def trade_log_summary_key(symbol: str) -> str:
    return f"{TRADE_LOG_SUMMARY_KEY_PREFIX}{str(symbol).upper()}"


def append_trade_log_sample(
    client: Any,
    *,
    symbol: str,
    sample: Mapping[str, Any],
    timestamp: datetime | None = None,
    retention_seconds: int = DEFAULT_TRADE_LOG_RETENTION_SECONDS,
) -> dict[str, Any]:
    event_time = (timestamp or datetime.now(UTC)).astimezone(UTC)
    normalized_symbol = str(symbol).upper()
    clamped_retention = max(
        1, min(int(retention_seconds), MAX_TRADE_LOG_WINDOW_SECONDS)
    )
    payload = {
        "timestamp": event_time.isoformat(),
        "symbol": normalized_symbol,
        "sample": normalize_trade_log_payload(sample),
    }
    score = event_time.timestamp()
    cutoff = (event_time - timedelta(seconds=clamped_retention)).timestamp()
    sample_key = trade_log_sample_key(normalized_symbol)

    pipeline = client.pipeline()
    pipeline.zadd(sample_key, {json.dumps(payload, separators=(",", ":")): score})
    pipeline.zremrangebyscore(sample_key, "-inf", cutoff)
    pipeline.expire(sample_key, clamped_retention + 30)
    pipeline.sadd(TRADE_LOG_SYMBOLS_KEY, normalized_symbol)
    pipeline.expire(TRADE_LOG_SYMBOLS_KEY, clamped_retention + 30)
    pipeline.execute()
    return payload


def read_trade_log_samples(
    client: Any,
    *,
    symbol: str,
    window_seconds: int = 60,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    clamped_window = max(1, min(int(window_seconds), MAX_TRADE_LOG_WINDOW_SECONDS))
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    min_score = (current_time - timedelta(seconds=clamped_window)).timestamp()
    rows = client.zrevrangebyscore(
        trade_log_sample_key(symbol),
        "+inf",
        min_score,
        start=0,
        num=max(1, clamped_window * 2),
    )

    samples: list[dict[str, Any]] = []
    for raw in reversed(rows):
        try:
            payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        sample = payload.get("sample")
        if not isinstance(sample, dict):
            continue
        samples.append(
            {
                "timestamp": str(payload.get("timestamp") or ""),
                "symbol": str(payload.get("symbol") or "").upper(),
                "sample": sample,
            }
        )
    return samples


def write_trade_log_summary(
    client: Any,
    *,
    symbol: str,
    summary: Mapping[str, Any],
    retention_seconds: int = DEFAULT_TRADE_LOG_RETENTION_SECONDS,
) -> dict[str, Any]:
    normalized_symbol = str(symbol).upper()
    clamped_retention = max(
        1, min(int(retention_seconds), MAX_TRADE_LOG_WINDOW_SECONDS)
    )
    normalized_summary = normalize_trade_log_payload(summary)
    pipeline = client.pipeline()
    pipeline.setex(
        trade_log_summary_key(normalized_symbol),
        clamped_retention + 30,
        json.dumps(normalized_summary, separators=(",", ":")),
    )
    pipeline.sadd(TRADE_LOG_SYMBOLS_KEY, normalized_symbol)
    pipeline.expire(TRADE_LOG_SYMBOLS_KEY, clamped_retention + 30)
    pipeline.execute()
    return normalized_summary


def read_trade_log_summaries(
    client: Any,
    *,
    symbol: str | None = None,
) -> list[dict[str, Any]]:
    symbols = [str(symbol).upper()] if symbol else _read_symbols(client)
    if not symbols:
        return []

    pipeline = client.pipeline()
    for item in symbols:
        pipeline.get(trade_log_summary_key(item))
    rows = pipeline.execute()

    summaries: list[dict[str, Any]] = []
    for raw in rows:
        if raw is None:
            continue
        try:
            payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            summaries.append(payload)

    return sorted(
        summaries,
        key=lambda item: (
            -int(item.get("signal_quality_score") or 0),
            str(item.get("symbol") or ""),
        ),
    )


def _read_symbols(client: Any) -> list[str]:
    members = client.smembers(TRADE_LOG_SYMBOLS_KEY)
    symbols: list[str] = []
    for raw in members:
        value = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        if value:
            symbols.append(value.upper())
    return sorted(set(symbols))


def build_market_signal_snapshot(
    *,
    symbol: str,
    samples: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not samples:
        return None

    normalized_samples: list[dict[str, Any]] = []
    for item in samples:
        sample = item.get("sample")
        if not isinstance(sample, dict):
            continue
        normalized_samples.append(
            {
                "timestamp": str(item.get("timestamp") or ""),
                "mid_price": _decimal(sample.get("mid_price")),
                "spread_pct": _decimal(sample.get("spread_pct")),
                "best_bid": _decimal(sample.get("best_bid")),
                "best_ask": _decimal(sample.get("best_ask")),
                "bid_size": _decimal(sample.get("bid_size")),
                "ask_size": _decimal(sample.get("ask_size")),
                "trade_volume": _decimal(sample.get("trade_volume")),
                "trade_notional_usd": _decimal(sample.get("trade_notional_usd")),
                "buy_volume": _decimal(sample.get("buy_volume")),
                "sell_volume": _decimal(sample.get("sell_volume")),
                "trade_count": int(sample.get("trade_count") or 0),
                "last_price": _decimal(sample.get("last_price")),
                "price_high": _decimal(sample.get("price_high")),
                "price_low": _decimal(sample.get("price_low")),
            }
        )
    if not normalized_samples:
        return None

    latest = normalized_samples[-1]
    latest_mid = latest["mid_price"] or latest["last_price"]
    if latest_mid <= 0:
        return None

    now = _parse_timestamp(latest["timestamp"])
    samples_10s = _filter_samples(normalized_samples, now=now, window_seconds=10)
    samples_30s = _filter_samples(normalized_samples, now=now, window_seconds=30)
    samples_60s = _filter_samples(normalized_samples, now=now, window_seconds=60)

    change_10s = _price_change_pct(samples_10s)
    change_30s = _price_change_pct(samples_30s)
    volume_10s = _sum_field(samples_10s, "trade_notional_usd")
    volume_60s = _sum_field(samples_60s, "trade_notional_usd")
    volatility_30s = _volatility_pct(samples_30s)
    buy_volume_30s = _sum_field(samples_30s, "buy_volume")
    sell_volume_30s = _sum_field(samples_30s, "sell_volume")
    total_volume_30s = buy_volume_30s + sell_volume_30s
    imbalance_ratio = (
        Decimal("999")
        if buy_volume_30s > 0 and sell_volume_30s == 0
        else (
            Decimal("0")
            if buy_volume_30s == 0 and sell_volume_30s > 0
            else buy_volume_30s / sell_volume_30s
            if sell_volume_30s > 0
            else Decimal("1")
        )
    )
    buy_share_pct = (
        (buy_volume_30s / total_volume_30s) * Decimal("100")
        if total_volume_30s > 0
        else Decimal("50")
    )

    trend = _trend_label(change_10s if len(samples_10s) >= 2 else change_30s)
    volatility = _volatility_label(volatility_30s)
    liquidity = _liquidity_label(
        spread_pct=latest["spread_pct"],
        best_bid=latest["best_bid"],
        best_ask=latest["best_ask"],
        bid_size=latest["bid_size"],
        ask_size=latest["ask_size"],
        mid_price=latest_mid,
    )
    trade_bias = _trade_bias_label(buy_share_pct)
    signal_quality_score = _signal_quality_score(
        spread_pct=latest["spread_pct"],
        volume_10s=volume_10s,
        volume_60s=volume_60s,
        change_10s=change_10s,
        volatility_30s=volatility_30s,
    )
    signal_quality_label = _signal_quality_label(signal_quality_score)

    summary_line = (
        f"Trend: {trend} | Volatility: {volatility} | "
        f"Liquidity: {liquidity} | Bias: {trade_bias}"
    )
    return {
        "timestamp": latest["timestamp"],
        "symbol": str(symbol).upper(),
        "summary_line": summary_line,
        "market_state": {
            "trend": trend,
            "volatility": volatility,
            "liquidity": liquidity,
            "trade_bias": trade_bias,
        },
        "signal_quality_score": signal_quality_score,
        "signal_quality_label": signal_quality_label,
        "raw_metrics": {
            "mid_price": _format_decimal(latest_mid, places=2),
            "spread_pct": _format_decimal(latest["spread_pct"], places=4),
            "short_term_price_change_pct_10s": _format_decimal(change_10s, places=4),
            "short_term_price_change_pct_30s": _format_decimal(change_30s, places=4),
            "rolling_volume_10s_usd": _format_decimal(volume_10s, places=2),
            "rolling_volume_60s_usd": _format_decimal(volume_60s, places=2),
            "volatility_30s_pct": _format_decimal(volatility_30s, places=4),
            "buy_volume_30s": _format_decimal(buy_volume_30s, places=6),
            "sell_volume_30s": _format_decimal(sell_volume_30s, places=6),
            "trade_imbalance_ratio_30s": _format_decimal(imbalance_ratio, places=4),
            "buy_share_pct_30s": _format_decimal(buy_share_pct, places=2),
            "bid_size": _format_decimal(latest["bid_size"], places=6),
            "ask_size": _format_decimal(latest["ask_size"], places=6),
            "best_bid": _format_decimal(latest["best_bid"], places=2),
            "best_ask": _format_decimal(latest["best_ask"], places=2),
            "trade_count_30s": sum(item["trade_count"] for item in samples_30s),
        },
    }


def _filter_samples(
    samples: list[dict[str, Any]], *, now: datetime, window_seconds: int
) -> list[dict[str, Any]]:
    cutoff = now - timedelta(seconds=window_seconds)
    return [
        sample for sample in samples if _parse_timestamp(sample["timestamp"]) >= cutoff
    ]


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def _sum_field(samples: list[dict[str, Any]], field: str) -> Decimal:
    return sum((sample[field] for sample in samples), Decimal("0"))


def _price_change_pct(samples: list[dict[str, Any]]) -> Decimal:
    if len(samples) < 2:
        return Decimal("0")
    first = samples[0]["mid_price"] or samples[0]["last_price"]
    last = samples[-1]["mid_price"] or samples[-1]["last_price"]
    if first <= 0 or last <= 0:
        return Decimal("0")
    return ((last - first) / first) * Decimal("100")


def _volatility_pct(samples: list[dict[str, Any]]) -> Decimal:
    prices = [
        sample["mid_price"] or sample["last_price"]
        for sample in samples
        if (sample["mid_price"] or sample["last_price"]) > 0
    ]
    if len(prices) < 2:
        return Decimal("0")
    mean = sum(prices, Decimal("0")) / Decimal(len(prices))
    if mean <= 0:
        return Decimal("0")
    variance = sum((price - mean) ** 2 for price in prices) / Decimal(len(prices))
    stddev = Decimal(str(sqrt(float(variance))))
    return (stddev / mean) * Decimal("100")


def _trend_label(change_pct: Decimal) -> str:
    if change_pct >= Decimal("0.08"):
        return "UP"
    if change_pct <= Decimal("-0.08"):
        return "DOWN"
    return "FLAT"


def _volatility_label(volatility_pct: Decimal) -> str:
    if volatility_pct >= Decimal("0.35"):
        return "HIGH"
    if volatility_pct >= Decimal("0.12"):
        return "MEDIUM"
    return "LOW"


def _liquidity_label(
    *,
    spread_pct: Decimal,
    best_bid: Decimal,
    best_ask: Decimal,
    bid_size: Decimal,
    ask_size: Decimal,
    mid_price: Decimal,
) -> str:
    top_notional = ((best_bid * bid_size) + (best_ask * ask_size)) / Decimal("2")
    if spread_pct <= Decimal("0.05") and top_notional >= Decimal("25000"):
        return "HIGH"
    if spread_pct <= Decimal("0.18") and top_notional >= Decimal("5000"):
        return "MEDIUM"
    if mid_price <= 0:
        return "LOW"
    return "LOW"


def _trade_bias_label(buy_share_pct: Decimal) -> str:
    if buy_share_pct >= Decimal("58"):
        return "BUY"
    if buy_share_pct <= Decimal("42"):
        return "SELL"
    return "NEUTRAL"


def _signal_quality_score(
    *,
    spread_pct: Decimal,
    volume_10s: Decimal,
    volume_60s: Decimal,
    change_10s: Decimal,
    volatility_30s: Decimal,
) -> int:
    spread_score = _score_inverse(
        value=spread_pct,
        low=Decimal("0.02"),
        high=Decimal("0.35"),
        max_points=35,
    )
    volume_score = _score_positive(
        value=volume_10s + (volume_60s / Decimal("6")),
        low=Decimal("10000"),
        high=Decimal("750000"),
        max_points=25,
    )
    move_score = _score_positive(
        value=abs(change_10s),
        low=Decimal("0.03"),
        high=Decimal("0.45"),
        max_points=20,
    )
    breakout_context = abs(change_10s) >= Decimal("0.15")
    if breakout_context:
        volatility_score = _score_positive(
            value=volatility_30s,
            low=Decimal("0.10"),
            high=Decimal("0.55"),
            max_points=20,
        )
    else:
        volatility_score = _score_band(
            volatility_30s,
            ideal_low=Decimal("0.06"),
            ideal_high=Decimal("0.22"),
            max_points=20,
        )
    total = spread_score + volume_score + move_score + volatility_score
    return max(0, min(100, int(round(total))))


def _signal_quality_label(score: int) -> str:
    if score >= 70:
        return "HIGH"
    if score >= 40:
        return "MODERATE"
    return "LOW"


def _score_inverse(
    *, max_points: int, low: Decimal, high: Decimal, value: Decimal
) -> float:
    if value <= low:
        return float(max_points)
    if value >= high:
        return 0.0
    return float((high - value) / (high - low) * Decimal(max_points))


def _score_positive(
    *, max_points: int, low: Decimal, high: Decimal, value: Decimal
) -> float:
    if value <= low:
        return 0.0
    if value >= high:
        return float(max_points)
    return float((value - low) / (high - low) * Decimal(max_points))


def _score_band(
    value: Decimal,
    *,
    ideal_low: Decimal,
    ideal_high: Decimal,
    max_points: int,
) -> float:
    if ideal_low <= value <= ideal_high:
        return float(max_points)
    if value < ideal_low:
        return float(
            _score_positive(
                value=value,
                low=Decimal("0"),
                high=ideal_low,
                max_points=max_points,
            )
        )
    return float(
        _score_inverse(
            value=value,
            low=ideal_high,
            high=Decimal("0.60"),
            max_points=max_points,
        )
    )


def _decimal(value: Any) -> Decimal:
    try:
        if value in (None, ""):
            return Decimal("0")
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _format_decimal(value: Decimal, *, places: int = 4) -> str:
    quantized = value.quantize(Decimal(f"1e-{places}"))
    text = format(quantized.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text

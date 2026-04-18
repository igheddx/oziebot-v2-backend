from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

TOKEN_POLICY_STRATEGIES = ("momentum", "reversion", "day_trading", "dca")
TOKEN_POLICY_RECOMMENDATIONS = ("preferred", "allowed", "discouraged", "blocked")
DISCOURAGED_SIZE_MULTIPLIER = Decimal("0.60")


@dataclass(frozen=True)
class CandleSample:
    close: float
    high: float
    low: float
    volume: float


@dataclass(frozen=True)
class BboSample:
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float


@dataclass(frozen=True)
class TradeSample:
    price: float
    size: float


@dataclass(frozen=True)
class TokenMarketProfileResult:
    liquidity_score: float
    spread_score: float
    volatility_score: float
    trend_score: float
    reversion_score: float
    slippage_score: float
    avg_daily_volume_usd: float
    avg_spread_pct: float
    avg_intraday_volatility_pct: float
    raw_metrics_json: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategySuitabilityResult:
    strategy_id: str
    suitability_score: float
    recommendation_status: str
    recommendation_reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _avg(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _score_increasing(value: float, *, floor: float, ceiling: float) -> float:
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 100.0
    return _clamp(((value - floor) / (ceiling - floor)) * 100.0)


def _score_decreasing(value: float, *, floor: float, ceiling: float) -> float:
    if value <= floor:
        return 100.0
    if value >= ceiling:
        return 0.0
    return _clamp(((ceiling - value) / (ceiling - floor)) * 100.0)


def _ema(values: Sequence[float], window: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (window + 1)
    current = float(values[0])
    for value in values[1:]:
        current = (float(value) * alpha) + (current * (1.0 - alpha))
    return current


def _recommendation_for_score(score: float) -> str:
    if score >= 80:
        return "preferred"
    if score >= 60:
        return "allowed"
    if score >= 40:
        return "discouraged"
    return "blocked"


def compute_market_profile(
    *,
    candles: Sequence[CandleSample],
    bbos: Sequence[BboSample],
    trades: Sequence[TradeSample],
) -> TokenMarketProfileResult:
    closes = [float(item.close) for item in candles if float(item.close) > 0]
    intraday_ranges = [
        max(0.0, (float(item.high) - float(item.low)) / float(item.close))
        for item in candles
        if float(item.close) > 0
    ]
    avg_intraday_volatility_pct = _avg(intraday_ranges)

    avg_candle_notional = _avg(
        [
            float(item.close) * float(item.volume)
            for item in candles
            if float(item.close) > 0
        ]
    )
    avg_daily_volume_usd = (
        avg_candle_notional * 1440.0 if avg_candle_notional > 0 else 0.0
    )

    spreads: list[float] = []
    depth_usd: list[float] = []
    for item in bbos:
        bid = float(item.bid_price)
        ask = float(item.ask_price)
        if bid <= 0 or ask <= 0:
            continue
        mid = (bid + ask) / 2.0
        if mid <= 0:
            continue
        spreads.append(max(0.0, (ask - bid) / mid))
        depth_usd.append(
            mid * max(0.0, min(float(item.bid_size), float(item.ask_size)))
        )
    avg_spread_pct = _avg(spreads)
    avg_depth_usd = _avg(depth_usd)

    trade_notional = _avg(
        [
            float(item.price) * float(item.size)
            for item in trades
            if float(item.price) > 0
        ]
    )
    volume_score = _score_increasing(
        avg_daily_volume_usd, floor=25_000, ceiling=5_000_000
    )
    depth_score = _score_increasing(
        max(avg_depth_usd, trade_notional), floor=500, ceiling=50_000
    )
    liquidity_score = _clamp((volume_score * 0.65) + (depth_score * 0.35))
    spread_score = _score_decreasing(avg_spread_pct, floor=0.001, ceiling=0.02)

    volatility_score = _score_increasing(
        avg_intraday_volatility_pct,
        floor=0.003,
        ceiling=0.04,
    )

    trend_bias = 0.0
    if len(closes) >= 5:
        short = _ema(closes, min(9, len(closes)))
        long = _ema(closes, min(21, len(closes)))
        if long > 0:
            trend_bias = (short - long) / long
    elif len(closes) >= 2 and closes[0] > 0:
        trend_bias = (closes[-1] - closes[0]) / closes[0]
    trend_score = _score_increasing(abs(trend_bias), floor=0.002, ceiling=0.03)

    returns = []
    for previous, current in zip(closes, closes[1:]):
        if previous > 0:
            returns.append((current - previous) / previous)
    sign_flips = 0
    for previous, current in zip(returns, returns[1:]):
        if previous == 0 or current == 0:
            continue
        if (previous > 0) != (current > 0):
            sign_flips += 1
    mean_cross_rate = (sign_flips / max(1, len(returns) - 1)) if returns else 0.0
    stability_score = _score_decreasing(
        avg_intraday_volatility_pct, floor=0.005, ceiling=0.08
    )
    reversion_score = _clamp(
        (_score_increasing(mean_cross_rate, floor=0.15, ceiling=0.7) * 0.6)
        + (stability_score * 0.4)
    )

    est_slippage_pct = 0.0
    if avg_depth_usd > 0:
        est_slippage_pct = avg_spread_pct + min(0.03, 2_000.0 / max(avg_depth_usd, 1.0))
    slippage_score = _score_decreasing(est_slippage_pct, floor=0.002, ceiling=0.04)

    raw_metrics_json = {
        "sample_counts": {
            "candles": len(candles),
            "bbos": len(bbos),
            "trades": len(trades),
        },
        "avg_bbo_depth_usd": round(avg_depth_usd, 6),
        "avg_trade_notional_usd": round(trade_notional, 6),
        "trend_bias": round(trend_bias, 6),
        "mean_cross_rate": round(mean_cross_rate, 6),
        "stability_score": round(stability_score, 4),
        "estimated_slippage_pct": round(est_slippage_pct, 6),
        "has_minimum_data": bool(candles) and bool(bbos),
        "bearish_regime": trend_bias < -0.01,
        "severe_downtrend": trend_bias < -0.03,
    }

    return TokenMarketProfileResult(
        liquidity_score=round(liquidity_score, 4),
        spread_score=round(spread_score, 4),
        volatility_score=round(volatility_score, 4),
        trend_score=round(trend_score, 4),
        reversion_score=round(reversion_score, 4),
        slippage_score=round(slippage_score, 4),
        avg_daily_volume_usd=round(avg_daily_volume_usd, 6),
        avg_spread_pct=round(avg_spread_pct, 8),
        avg_intraday_volatility_pct=round(avg_intraday_volatility_pct, 8),
        raw_metrics_json=raw_metrics_json,
    )


def score_strategy_suitability(
    *,
    strategy_id: str,
    profile: TokenMarketProfileResult,
    token_extra: Mapping[str, Any] | None = None,
) -> StrategySuitabilityResult:
    token_extra = token_extra or {}
    raw = profile.raw_metrics_json
    if not raw.get("has_minimum_data"):
        return StrategySuitabilityResult(
            strategy_id=strategy_id,
            suitability_score=0.0,
            recommendation_status="blocked",
            recommendation_reason="Insufficient market data for policy computation",
        )

    avg_daily_volume_usd = float(profile.avg_daily_volume_usd)
    relative_volume_score = _score_increasing(
        avg_daily_volume_usd, floor=100_000, ceiling=8_000_000
    )
    stability_score = float(raw.get("stability_score") or 0.0)
    trend_bias = float(raw.get("trend_bias") or 0.0)
    bearish_regime = bool(raw.get("bearish_regime"))
    severe_downtrend = bool(raw.get("severe_downtrend"))

    if strategy_id == "momentum":
        score = (
            (profile.trend_score * 0.35)
            + (profile.liquidity_score * 0.25)
            + (profile.spread_score * 0.20)
            + (relative_volume_score * 0.20)
        )
        if trend_bias < 0:
            score -= 20.0
        reason = "Favors trend strength, liquidity, and relative volume"
    elif strategy_id == "reversion":
        score = (
            (profile.liquidity_score * 0.30)
            + (profile.spread_score * 0.25)
            + (profile.reversion_score * 0.30)
            + (stability_score * 0.15)
        )
        if severe_downtrend:
            return StrategySuitabilityResult(
                strategy_id=strategy_id,
                suitability_score=round(_clamp(score), 4),
                recommendation_status="blocked",
                recommendation_reason="Severe downtrend blocks mean reversion trades",
            )
        if bearish_regime:
            score -= 10.0
        reason = "Favors liquid tokens with tight spread and repeatable mean reversion"
    elif strategy_id == "day_trading":
        score = (
            (relative_volume_score * 0.30)
            + (profile.spread_score * 0.25)
            + (profile.volatility_score * 0.25)
            + (profile.slippage_score * 0.20)
        )
        if profile.avg_intraday_volatility_pct < 0.004:
            score -= 15.0
        reason = "Favors intraday volume, volatility, and controllable spread/slippage"
    elif strategy_id == "dca":
        score = (
            (profile.liquidity_score * 0.35)
            + (profile.spread_score * 0.20)
            + (stability_score * 0.25)
            + (profile.slippage_score * 0.20)
        )
        requires_admin = not bool(
            token_extra.get("core_token") or token_extra.get("dca_approved")
        )
        if severe_downtrend:
            score -= 10.0
        if requires_admin:
            return StrategySuitabilityResult(
                strategy_id=strategy_id,
                suitability_score=round(_clamp(score), 4),
                recommendation_status="discouraged" if score >= 40 else "blocked",
                recommendation_reason="DCA requires admin core-token approval for this token",
            )
        reason = (
            "Favors liquid, lower-instability core tokens for scheduled accumulation"
        )
    else:
        raise ValueError(f"Unsupported strategy_id: {strategy_id}")

    score = _clamp(score)
    return StrategySuitabilityResult(
        strategy_id=strategy_id,
        suitability_score=round(score, 4),
        recommendation_status=_recommendation_for_score(score),
        recommendation_reason=reason,
    )


def resolve_effective_token_policy(policy: Mapping[str, Any] | None) -> dict[str, Any]:
    if not policy:
        return {
            "admin_enabled": True,
            "computed_recommendation_status": "allowed",
            "effective_recommendation_status": "allowed",
            "computed_recommendation_reason": None,
            "effective_recommendation_reason": None,
            "size_multiplier": Decimal("1"),
            "max_position_pct_override": None,
        }

    computed_status = str(policy.get("recommendation_status") or "allowed")
    override_status = policy.get("recommendation_status_override")
    effective_status = str(override_status or computed_status)
    raw_admin_enabled = policy.get("admin_enabled", True)
    admin_enabled = True if raw_admin_enabled is None else bool(raw_admin_enabled)
    effective_reason = policy.get("recommendation_reason_override") or policy.get(
        "recommendation_reason"
    )
    size_multiplier = Decimal("1")
    if not admin_enabled or effective_status == "blocked":
        size_multiplier = Decimal("0")
    elif effective_status == "discouraged":
        size_multiplier = DISCOURAGED_SIZE_MULTIPLIER

    max_position_pct_override = policy.get("max_position_pct_override")
    if max_position_pct_override is not None:
        max_position_pct_override = Decimal(str(max_position_pct_override))

    return {
        "admin_enabled": admin_enabled,
        "computed_recommendation_status": computed_status,
        "effective_recommendation_status": effective_status,
        "computed_recommendation_reason": policy.get("recommendation_reason"),
        "effective_recommendation_reason": effective_reason,
        "size_multiplier": size_multiplier,
        "max_position_pct_override": max_position_pct_override,
    }

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

from oziebot_common.token_policy import (
    BboSample,
    CandleSample,
    TOKEN_POLICY_STRATEGIES,
    TradeSample,
    compute_market_profile,
    score_strategy_suitability,
)

log = logging.getLogger("market-data-ingestor.policy-refresh")


class TokenPolicyRefresher:
    def __init__(self, engine: Engine):
        self._engine = engine

    def refresh_active_tokens(self) -> int:
        with self._engine.begin() as conn:
            tokens = conn.execute(
                text(
                    """
                    SELECT id, symbol, extra
                    FROM platform_token_allowlist
                    WHERE is_enabled = true
                    ORDER BY sort_order, symbol
                    """
                )
            ).mappings().all()

        refreshed = 0
        for token in tokens:
            try:
                self._refresh_one(
                    token_id=str(token["id"]),
                    symbol=str(token["symbol"]),
                    extra=self._json_dict(token.get("extra")),
                )
                refreshed += 1
            except Exception as exc:
                log.warning("token policy refresh failed symbol=%s err=%s", token["symbol"], exc)
        return refreshed

    def _refresh_one(self, *, token_id: str, symbol: str, extra: dict[str, object]) -> None:
        candles = self._load_candles(symbol)
        bbos = self._load_bbos(symbol)
        trades = self._load_trades(symbol)
        profile = compute_market_profile(candles=candles, bbos=bbos, trades=trades)
        now = datetime.now(UTC)

        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO token_market_profile (
                      id, token_id, liquidity_score, spread_score, volatility_score,
                      trend_score, reversion_score, slippage_score,
                      avg_daily_volume_usd, avg_spread_pct, avg_intraday_volatility_pct,
                      last_computed_at, raw_metrics_json
                    ) VALUES (
                      :id, :token_id, :liquidity_score, :spread_score, :volatility_score,
                      :trend_score, :reversion_score, :slippage_score,
                      :avg_daily_volume_usd, :avg_spread_pct, :avg_intraday_volatility_pct,
                      :last_computed_at, :raw_metrics_json
                    )
                    ON CONFLICT (token_id)
                    DO UPDATE SET
                      liquidity_score = excluded.liquidity_score,
                      spread_score = excluded.spread_score,
                      volatility_score = excluded.volatility_score,
                      trend_score = excluded.trend_score,
                      reversion_score = excluded.reversion_score,
                      slippage_score = excluded.slippage_score,
                      avg_daily_volume_usd = excluded.avg_daily_volume_usd,
                      avg_spread_pct = excluded.avg_spread_pct,
                      avg_intraday_volatility_pct = excluded.avg_intraday_volatility_pct,
                      last_computed_at = excluded.last_computed_at,
                      raw_metrics_json = excluded.raw_metrics_json
                    """
                ),
                {
                    "id": str(uuid.uuid4()),
                    "token_id": token_id,
                    "liquidity_score": profile.liquidity_score,
                    "spread_score": profile.spread_score,
                    "volatility_score": profile.volatility_score,
                    "trend_score": profile.trend_score,
                    "reversion_score": profile.reversion_score,
                    "slippage_score": profile.slippage_score,
                    "avg_daily_volume_usd": profile.avg_daily_volume_usd,
                    "avg_spread_pct": profile.avg_spread_pct,
                    "avg_intraday_volatility_pct": profile.avg_intraday_volatility_pct,
                    "last_computed_at": now,
                    "raw_metrics_json": json.dumps(profile.raw_metrics_json),
                },
            )

            for strategy_id in TOKEN_POLICY_STRATEGIES:
                suitability = score_strategy_suitability(
                    strategy_id=strategy_id,
                    profile=profile,
                    token_extra=extra,
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO token_strategy_policy (
                          id, token_id, strategy_id, admin_enabled, suitability_score,
                          recommendation_status, recommendation_reason,
                          recommendation_status_override, recommendation_reason_override,
                          max_position_pct_override, notes, computed_at, updated_at
                        ) VALUES (
                          :id, :token_id, :strategy_id, true, :suitability_score,
                          :recommendation_status, :recommendation_reason,
                          NULL, NULL, NULL, NULL, :computed_at, :updated_at
                        )
                        ON CONFLICT (token_id, strategy_id)
                        DO UPDATE SET
                          suitability_score = excluded.suitability_score,
                          recommendation_status = excluded.recommendation_status,
                          recommendation_reason = excluded.recommendation_reason,
                          computed_at = excluded.computed_at,
                          updated_at = excluded.updated_at
                        """
                    ),
                    {
                        "id": str(uuid.uuid4()),
                        "token_id": token_id,
                        "strategy_id": strategy_id,
                        "suitability_score": suitability.suitability_score,
                        "recommendation_status": suitability.recommendation_status,
                        "recommendation_reason": suitability.recommendation_reason,
                        "computed_at": now,
                        "updated_at": now,
                    },
                )

    def _load_candles(self, symbol: str) -> list[CandleSample]:
        with self._engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT close, high, low, volume
                    FROM market_data_candles
                    WHERE product_id = :symbol
                    ORDER BY bucket_start DESC
                    LIMIT 240
                    """
                ),
                {"symbol": symbol},
            ).mappings().all()
        ordered = list(reversed(rows))
        return [
            CandleSample(
                close=float(row["close"]),
                high=float(row["high"]),
                low=float(row["low"]),
                volume=float(row["volume"]),
            )
            for row in ordered
        ]

    def _load_bbos(self, symbol: str) -> list[BboSample]:
        with self._engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT best_bid_price, best_ask_price, best_bid_size, best_ask_size
                    FROM market_data_bbo_snapshots
                    WHERE product_id = :symbol
                    ORDER BY event_time DESC
                    LIMIT 240
                    """
                ),
                {"symbol": symbol},
            ).mappings().all()
        return [
            BboSample(
                bid_price=float(row["best_bid_price"]),
                ask_price=float(row["best_ask_price"]),
                bid_size=float(row["best_bid_size"]),
                ask_size=float(row["best_ask_size"]),
            )
            for row in rows
        ]

    def _load_trades(self, symbol: str) -> list[TradeSample]:
        with self._engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT price, size
                    FROM market_data_trade_snapshots
                    WHERE product_id = :symbol
                    ORDER BY event_time DESC
                    LIMIT 240
                    """
                ),
                {"symbol": symbol},
            ).mappings().all()
        return [TradeSample(price=float(row["price"]), size=float(row["size"])) for row in rows]

    @staticmethod
    def _json_dict(value) -> dict[str, object]:
        if value is None:
            return {}
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except Exception:
                return {}
        return value if isinstance(value, dict) else {}

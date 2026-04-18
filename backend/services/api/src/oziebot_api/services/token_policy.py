from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_common.token_policy import (
    BboSample,
    CandleSample,
    TOKEN_POLICY_STRATEGIES,
    TradeSample,
    compute_market_profile,
    resolve_effective_token_policy,
    score_strategy_suitability,
)
from oziebot_api.models.market_data import (
    MarketDataBboSnapshot,
    MarketDataCandle,
    MarketDataTradeSnapshot,
)
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.token_market_profile import TokenMarketProfile
from oziebot_api.models.token_strategy_policy import TokenStrategyPolicy


class TokenPolicyService:
    def __init__(self, db: Session):
        self._db = db

    def list_market_profiles(self) -> list[dict[str, Any]]:
        tokens = self._db.scalars(
            select(PlatformTokenAllowlist).order_by(
                PlatformTokenAllowlist.sort_order,
                PlatformTokenAllowlist.symbol,
            )
        ).all()
        profile_map = {
            row.token_id: row
            for row in self._db.scalars(select(TokenMarketProfile)).all()
        }
        return [
            {
                "token": {
                    "id": str(token.id),
                    "symbol": token.symbol,
                    "quote_currency": token.quote_currency,
                    "display_name": token.display_name,
                    "is_enabled": token.is_enabled,
                    "extra": token.extra,
                },
                "market_profile": self._profile_out(profile_map.get(token.id)),
            }
            for token in tokens
        ]

    def list_token_matrix(self, *, symbol: str | None = None) -> list[dict[str, Any]]:
        stmt = select(PlatformTokenAllowlist).order_by(
            PlatformTokenAllowlist.sort_order,
            PlatformTokenAllowlist.symbol,
        )
        if symbol:
            stmt = stmt.where(PlatformTokenAllowlist.symbol.ilike(f"%{symbol.strip().upper()}%"))
        tokens = self._db.scalars(stmt).all()
        return [self.describe_token(token) for token in tokens]

    def recalculate_token(self, token: PlatformTokenAllowlist) -> dict[str, Any]:
        candles = self._load_candles(token.symbol)
        bbos = self._load_bbos(token.symbol)
        trades = self._load_trades(token.symbol)
        profile_result = compute_market_profile(candles=candles, bbos=bbos, trades=trades)
        now = datetime.now(UTC)

        profile = self._db.scalar(
            select(TokenMarketProfile).where(TokenMarketProfile.token_id == token.id)
        )
        if profile is None:
            profile = TokenMarketProfile(id=uuid.uuid4(), token_id=token.id, last_computed_at=now)
            self._db.add(profile)

        profile.liquidity_score = profile_result.liquidity_score
        profile.spread_score = profile_result.spread_score
        profile.volatility_score = profile_result.volatility_score
        profile.trend_score = profile_result.trend_score
        profile.reversion_score = profile_result.reversion_score
        profile.slippage_score = profile_result.slippage_score
        profile.avg_daily_volume_usd = profile_result.avg_daily_volume_usd
        profile.avg_spread_pct = profile_result.avg_spread_pct
        profile.avg_intraday_volatility_pct = profile_result.avg_intraday_volatility_pct
        profile.last_computed_at = now
        profile.raw_metrics_json = profile_result.raw_metrics_json

        policies = []
        for strategy_id in TOKEN_POLICY_STRATEGIES:
            result = score_strategy_suitability(
                strategy_id=strategy_id,
                profile=profile_result,
                token_extra=token.extra or {},
            )
            policy = self._db.scalar(
                select(TokenStrategyPolicy).where(
                    TokenStrategyPolicy.token_id == token.id,
                    TokenStrategyPolicy.strategy_id == strategy_id,
                )
            )
            if policy is None:
                policy = TokenStrategyPolicy(
                    id=uuid.uuid4(),
                    token_id=token.id,
                    strategy_id=strategy_id,
                    admin_enabled=True,
                    computed_at=now,
                    updated_at=now,
                )
                self._db.add(policy)

            policy.suitability_score = result.suitability_score
            policy.recommendation_status = result.recommendation_status
            policy.recommendation_reason = result.recommendation_reason
            policy.computed_at = now
            policy.updated_at = now
            policies.append(policy)

        self._db.flush()
        return self.describe_token(token)

    def describe_token(self, token: PlatformTokenAllowlist) -> dict[str, Any]:
        profile = self._db.scalar(
            select(TokenMarketProfile).where(TokenMarketProfile.token_id == token.id)
        )
        strategy_map = {
            row.slug: row.display_name for row in self._db.scalars(select(PlatformStrategy)).all()
        }
        policies = self._db.scalars(
            select(TokenStrategyPolicy)
            .where(TokenStrategyPolicy.token_id == token.id)
            .order_by(TokenStrategyPolicy.strategy_id)
        ).all()
        return {
            "token": {
                "id": str(token.id),
                "symbol": token.symbol,
                "quote_currency": token.quote_currency,
                "display_name": token.display_name,
                "is_enabled": token.is_enabled,
                "extra": token.extra,
            },
            "market_profile": self._profile_out(profile),
            "strategy_policies": [
                self._policy_out(policy, strategy_display_name=strategy_map.get(policy.strategy_id))
                for policy in policies
            ],
        }

    def update_policy_override(
        self,
        *,
        token: PlatformTokenAllowlist,
        strategy_id: str,
        admin_enabled: bool | None,
        recommendation_status: str | None,
        recommendation_reason: str | None,
        max_position_pct_override: float | None,
        notes: str | None,
    ) -> dict[str, Any]:
        policy = self._db.scalar(
            select(TokenStrategyPolicy).where(
                TokenStrategyPolicy.token_id == token.id,
                TokenStrategyPolicy.strategy_id == strategy_id,
            )
        )
        if policy is None:
            self.recalculate_token(token)
            policy = self._db.scalar(
                select(TokenStrategyPolicy).where(
                    TokenStrategyPolicy.token_id == token.id,
                    TokenStrategyPolicy.strategy_id == strategy_id,
                )
            )
        if policy is None:
            raise ValueError("Token strategy policy was not created")

        if admin_enabled is not None:
            policy.admin_enabled = admin_enabled
        if recommendation_status is not None:
            policy.recommendation_status_override = recommendation_status
        if recommendation_reason is not None:
            policy.recommendation_reason_override = recommendation_reason
        if max_position_pct_override is not None:
            policy.max_position_pct_override = max_position_pct_override
        if notes is not None:
            policy.notes = notes
        policy.updated_at = datetime.now(UTC)
        self._db.flush()
        strategy_display_name = self._db.scalar(
            select(PlatformStrategy.display_name).where(PlatformStrategy.slug == strategy_id)
        )
        return self._policy_out(policy, strategy_display_name=strategy_display_name)

    def _load_candles(self, symbol: str) -> list[CandleSample]:
        rows = self._db.scalars(
            select(MarketDataCandle)
            .where(MarketDataCandle.product_id == symbol)
            .order_by(MarketDataCandle.bucket_start.desc())
            .limit(240)
        ).all()
        ordered = list(reversed(rows))
        return [
            CandleSample(
                close=float(row.close),
                high=float(row.high),
                low=float(row.low),
                volume=float(row.volume),
            )
            for row in ordered
        ]

    def _load_bbos(self, symbol: str) -> list[BboSample]:
        rows = self._db.scalars(
            select(MarketDataBboSnapshot)
            .where(MarketDataBboSnapshot.product_id == symbol)
            .order_by(MarketDataBboSnapshot.event_time.desc())
            .limit(240)
        ).all()
        return [
            BboSample(
                bid_price=float(row.best_bid_price),
                ask_price=float(row.best_ask_price),
                bid_size=float(row.best_bid_size),
                ask_size=float(row.best_ask_size),
            )
            for row in rows
        ]

    def _load_trades(self, symbol: str) -> list[TradeSample]:
        rows = self._db.scalars(
            select(MarketDataTradeSnapshot)
            .where(MarketDataTradeSnapshot.product_id == symbol)
            .order_by(MarketDataTradeSnapshot.event_time.desc())
            .limit(240)
        ).all()
        return [TradeSample(price=float(row.price), size=float(row.size)) for row in rows]

    @staticmethod
    def _profile_out(profile: TokenMarketProfile | None) -> dict[str, Any] | None:
        if profile is None:
            return None
        return {
            "liquidity_score": float(profile.liquidity_score),
            "spread_score": float(profile.spread_score),
            "volatility_score": float(profile.volatility_score),
            "trend_score": float(profile.trend_score),
            "reversion_score": float(profile.reversion_score),
            "slippage_score": float(profile.slippage_score),
            "avg_daily_volume_usd": float(profile.avg_daily_volume_usd),
            "avg_spread_pct": float(profile.avg_spread_pct),
            "avg_intraday_volatility_pct": float(profile.avg_intraday_volatility_pct),
            "last_computed_at": profile.last_computed_at.isoformat(),
            "raw_metrics_json": profile.raw_metrics_json,
        }

    @staticmethod
    def _policy_out(
        policy: TokenStrategyPolicy,
        *,
        strategy_display_name: str | None,
    ) -> dict[str, Any]:
        effective = resolve_effective_token_policy(
            {
                "admin_enabled": policy.admin_enabled,
                "recommendation_status": policy.recommendation_status,
                "recommendation_reason": policy.recommendation_reason,
                "recommendation_status_override": policy.recommendation_status_override,
                "recommendation_reason_override": policy.recommendation_reason_override,
                "max_position_pct_override": policy.max_position_pct_override,
            }
        )
        return {
            "id": str(policy.id),
            "strategy_id": policy.strategy_id,
            "strategy_display_name": strategy_display_name,
            "admin_enabled": policy.admin_enabled,
            "suitability_score": float(policy.suitability_score),
            "computed_recommendation_status": policy.recommendation_status,
            "computed_recommendation_reason": policy.recommendation_reason,
            "effective_recommendation_status": effective["effective_recommendation_status"],
            "effective_recommendation_reason": effective["effective_recommendation_reason"],
            "recommendation_status": effective["effective_recommendation_status"],
            "recommendation_reason": effective["effective_recommendation_reason"],
            "recommendation_status_override": policy.recommendation_status_override,
            "recommendation_reason_override": policy.recommendation_reason_override,
            "max_position_pct_override": float(policy.max_position_pct_override)
            if policy.max_position_pct_override is not None
            else None,
            "notes": policy.notes,
            "computed_at": policy.computed_at.isoformat(),
            "updated_at": policy.updated_at.isoformat(),
        }

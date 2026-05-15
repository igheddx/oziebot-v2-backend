from __future__ import annotations

import os
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
    default_size_multiplier_for_status,
    normalize_missing_policy_behavior,
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
from oziebot_api.models.user_strategy import UserStrategy
from oziebot_api.models.user_token_permission import UserTokenPermission
from oziebot_api.models.user import User

RECOMMENDED_TOKEN_STRATEGY_MATRIX: dict[str, dict[str, str]] = {
    "BTC-USD": {
        "momentum": "preferred",
        "day_trading": "allowed",
        "reversion": "allowed",
        "dca": "preferred",
        "strategic_aggressive_allocation": "preferred",
    },
    "ETH-USD": {
        "momentum": "preferred",
        "day_trading": "allowed",
        "reversion": "allowed",
        "dca": "preferred",
        "strategic_aggressive_allocation": "preferred",
    },
    "SOL-USD": {
        "momentum": "preferred",
        "day_trading": "allowed",
        "reversion": "discouraged",
        "dca": "blocked",
        "strategic_aggressive_allocation": "preferred",
    },
    "LINK-USD": {
        "momentum": "allowed",
        "day_trading": "discouraged",
        "reversion": "allowed",
        "dca": "blocked",
        "strategic_aggressive_allocation": "allowed",
    },
    "AVAX-USD": {
        "momentum": "allowed",
        "day_trading": "allowed",
        "reversion": "discouraged",
        "dca": "blocked",
        "strategic_aggressive_allocation": "allowed",
    },
    "SUI-USD": {
        "momentum": "allowed",
        "day_trading": "discouraged",
        "reversion": "blocked",
        "dca": "blocked",
        "strategic_aggressive_allocation": "allowed",
    },
    "AERO-USD": {
        "momentum": "allowed",
        "day_trading": "blocked",
        "reversion": "blocked",
        "dca": "blocked",
        "strategic_aggressive_allocation": "preferred",
    },
}

RECOMMENDED_TOKEN_DISPLAY_NAMES = {
    "BTC-USD": "Bitcoin / USD",
    "ETH-USD": "Ethereum / USD",
    "SOL-USD": "Solana / USD",
    "LINK-USD": "Chainlink / USD",
    "AVAX-USD": "Avalanche / USD",
    "SUI-USD": "Sui / USD",
    "AERO-USD": "Aerodrome / USD",
}


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
            row.token_id: row for row in self._db.scalars(select(TokenMarketProfile)).all()
        }
        return [
            {
                "token": self._token_out(token),
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

    def export_token_matrix(self) -> dict[str, Any]:
        default_missing_policy_behavior = normalize_missing_policy_behavior(
            os.environ.get("TOKEN_STRATEGY_POLICY_DEFAULT_BEHAVIOR")
        )
        tokens = self.list_token_matrix()
        export_tokens: list[dict[str, Any]] = []
        matrix: dict[str, dict[str, Any]] = {}

        for entry in tokens:
            strategy_map = {policy["strategy_id"]: policy for policy in entry["strategy_policies"]}
            export_tokens.append(
                {
                    "token": entry["token"],
                    "market_profile": entry["market_profile"],
                    "strategies": strategy_map,
                }
            )
            matrix[entry["token"]["symbol"]] = strategy_map

        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "default_missing_policy_behavior": default_missing_policy_behavior,
            "tokens": export_tokens,
            "matrix": matrix,
        }

    def list_user_matrix(self, *, user: User) -> list[dict[str, Any]]:
        tokens = self._db.scalars(
            select(PlatformTokenAllowlist).order_by(
                PlatformTokenAllowlist.sort_order,
                PlatformTokenAllowlist.symbol,
            )
        ).all()
        permissions = {
            row.platform_token_id: row
            for row in self._db.scalars(
                select(UserTokenPermission).where(UserTokenPermission.user_id == user.id)
            ).all()
        }
        user_strategies = self._db.scalars(
            select(UserStrategy)
            .where(UserStrategy.user_id == user.id)
            .order_by(UserStrategy.strategy_id)
        ).all()
        platform_strategy_names = {row.slug: row.display_name for row in self._list_strategy_rows()}
        strategy_ids = [row.strategy_id for row in user_strategies] or list(platform_strategy_names)
        entries: list[dict[str, Any]] = []
        for token in tokens:
            perm = permissions.get(token.id)
            token_entry = self.describe_token(token, strategy_ids=strategy_ids)
            token_entry["user_token_enabled"] = bool(perm and perm.is_enabled)
            token_entry["platform_token_enabled"] = bool(token.is_enabled)
            token_entry["strategies"] = [
                {
                    "strategy_id": row.strategy_id,
                    "strategy_display_name": platform_strategy_names.get(row.strategy_id)
                    or self._format_strategy_name(row.strategy_id),
                    "is_user_enabled": bool(row.is_enabled),
                }
                for row in user_strategies
            ]
            entries.append(token_entry)
        return entries

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

        strategy_ids = [row.slug for row in self._list_strategy_rows()]
        if not strategy_ids:
            strategy_ids = list(TOKEN_POLICY_STRATEGIES)
        for strategy_id in strategy_ids:
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
                continue
            policy.suitability_score = result.suitability_score
            policy.computed_at = now
            policy.updated_at = now

        self._db.flush()
        return self.describe_token(token)

    def describe_token(
        self,
        token: PlatformTokenAllowlist,
        *,
        strategy_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        profile = self._db.scalar(
            select(TokenMarketProfile).where(TokenMarketProfile.token_id == token.id)
        )
        strategy_rows = self._list_strategy_rows()
        strategy_map = {row.slug: row.display_name for row in strategy_rows}
        ordered_strategy_ids = strategy_ids or [row.slug for row in strategy_rows]
        if not ordered_strategy_ids:
            ordered_strategy_ids = list(TOKEN_POLICY_STRATEGIES)

        policies = self._db.scalars(
            select(TokenStrategyPolicy)
            .where(TokenStrategyPolicy.token_id == token.id)
            .order_by(TokenStrategyPolicy.strategy_id)
        ).all()
        policy_map = {policy.strategy_id: policy for policy in policies}
        strategy_policies = []
        for strategy_id in ordered_strategy_ids:
            strategy_display_name = strategy_map.get(strategy_id)
            policy = policy_map.get(strategy_id)
            if policy is None:
                strategy_policies.append(
                    self._virtual_policy_out(
                        strategy_id=strategy_id,
                        strategy_display_name=strategy_display_name,
                    )
                )
                continue
            strategy_policies.append(
                self._policy_out(policy, strategy_display_name=strategy_display_name)
            )

        return {
            "token": self._token_out(token),
            "market_profile": self._profile_out(profile),
            "strategy_policies": strategy_policies,
        }

    def update_policy_override(
        self,
        *,
        token: PlatformTokenAllowlist,
        strategy_id: str,
        is_enabled: bool | None,
        admin_enabled: bool | None,
        recommendation_status: str | None,
        recommendation_reason: str | None,
        size_multiplier: float | None,
        max_position_usd_override: float | None,
        max_position_pct_override: float | None,
        notes: str | None,
    ) -> dict[str, Any]:
        normalized_strategy_id = strategy_id.strip().lower()
        now = datetime.now(UTC)
        policy = self._db.scalar(
            select(TokenStrategyPolicy).where(
                TokenStrategyPolicy.token_id == token.id,
                TokenStrategyPolicy.strategy_id == normalized_strategy_id,
            )
        )
        if policy is None:
            default_status = normalize_missing_policy_behavior("allowed")
            policy = TokenStrategyPolicy(
                id=uuid.uuid4(),
                token_id=token.id,
                strategy_id=normalized_strategy_id,
                admin_enabled=True,
                suitability_score=0,
                recommendation_status=default_status,
                recommendation_reason=None,
                recommendation_status_override=None,
                recommendation_reason_override=None,
                size_multiplier=default_size_multiplier_for_status(default_status),
                max_position_usd_override=None,
                max_position_pct_override=None,
                notes=None,
                created_at=now,
                computed_at=now,
                updated_at=now,
            )
            self._db.add(policy)

        if is_enabled is not None:
            policy.admin_enabled = is_enabled
        elif admin_enabled is not None:
            policy.admin_enabled = admin_enabled
        if recommendation_status is not None:
            policy.recommendation_status = recommendation_status
            policy.recommendation_status_override = None
        if recommendation_reason is not None:
            policy.recommendation_reason = recommendation_reason
            policy.recommendation_reason_override = None
        if size_multiplier is not None:
            policy.size_multiplier = size_multiplier
        elif recommendation_status is not None:
            policy.size_multiplier = default_size_multiplier_for_status(recommendation_status)
        if max_position_usd_override is not None:
            policy.max_position_usd_override = max_position_usd_override
        if max_position_pct_override is not None:
            policy.max_position_pct_override = max_position_pct_override
        if notes is not None:
            policy.notes = notes
        policy.updated_at = now
        self._db.flush()
        strategy_display_name = self._db.scalar(
            select(PlatformStrategy.display_name).where(
                PlatformStrategy.slug == normalized_strategy_id
            )
        )
        return self._policy_out(policy, strategy_display_name=strategy_display_name)

    def initialize_recommended_defaults(self, *, reset_existing: bool) -> dict[str, Any]:
        now = datetime.now(UTC)
        tokens_processed = 0
        policies_written = 0
        updated_symbols: list[str] = []
        for symbol, strategy_map in RECOMMENDED_TOKEN_STRATEGY_MATRIX.items():
            token = self._db.scalar(
                select(PlatformTokenAllowlist).where(PlatformTokenAllowlist.symbol == symbol)
            )
            if token is None:
                token = PlatformTokenAllowlist(
                    id=uuid.uuid4(),
                    symbol=symbol,
                    quote_currency="USD",
                    network="mainnet",
                    contract_address=None,
                    display_name=RECOMMENDED_TOKEN_DISPLAY_NAMES.get(symbol),
                    is_enabled=True,
                    sort_order=0,
                    extra=None,
                    created_at=now,
                    updated_at=now,
                )
                self._db.add(token)
                self._db.flush()
            elif not token.display_name and symbol in RECOMMENDED_TOKEN_DISPLAY_NAMES:
                token.display_name = RECOMMENDED_TOKEN_DISPLAY_NAMES[symbol]
                token.updated_at = now
            tokens_processed += 1
            updated_symbols.append(symbol)

            for strategy_id, status in strategy_map.items():
                policy = self._db.scalar(
                    select(TokenStrategyPolicy).where(
                        TokenStrategyPolicy.token_id == token.id,
                        TokenStrategyPolicy.strategy_id == strategy_id,
                    )
                )
                if policy is not None and not reset_existing:
                    continue
                if policy is None:
                    policy = TokenStrategyPolicy(
                        id=uuid.uuid4(),
                        token_id=token.id,
                        strategy_id=strategy_id,
                        admin_enabled=True,
                        suitability_score=0,
                        recommendation_status=status,
                        recommendation_reason="Recommended default token-strategy policy",
                        recommendation_status_override=None,
                        recommendation_reason_override=None,
                        size_multiplier=default_size_multiplier_for_status(status),
                        max_position_usd_override=None,
                        max_position_pct_override=None,
                        notes=None,
                        created_at=now,
                        computed_at=now,
                        updated_at=now,
                    )
                    self._db.add(policy)
                else:
                    policy.admin_enabled = True
                    policy.recommendation_status = status
                    policy.recommendation_reason = "Recommended default token-strategy policy"
                    policy.recommendation_status_override = None
                    policy.recommendation_reason_override = None
                    policy.size_multiplier = default_size_multiplier_for_status(status)
                    policy.max_position_usd_override = None
                    policy.max_position_pct_override = None
                    policy.notes = None
                    policy.updated_at = now
                policies_written += 1
            self.recalculate_token(token)

        self._db.flush()
        return {
            "tokens_processed": tokens_processed,
            "policies_written": policies_written,
            "updated_symbols": updated_symbols,
        }

    def _list_strategy_rows(self) -> list[PlatformStrategy]:
        return self._db.scalars(
            select(PlatformStrategy).order_by(PlatformStrategy.sort_order, PlatformStrategy.slug)
        ).all()

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
    def _token_out(token: PlatformTokenAllowlist) -> dict[str, Any]:
        return {
            "id": str(token.id),
            "symbol": token.symbol,
            "quote_currency": token.quote_currency,
            "display_name": token.display_name,
            "is_enabled": token.is_enabled,
            "extra": token.extra,
        }

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
    def _format_strategy_name(strategy_id: str) -> str:
        parts = [p for p in strategy_id.replace(".", "-").replace("_", "-").split("-") if p]
        return " ".join(part[:1].upper() + part[1:] for part in parts) if parts else strategy_id

    def _virtual_policy_out(
        self,
        *,
        strategy_id: str,
        strategy_display_name: str | None,
    ) -> dict[str, Any]:
        effective = resolve_effective_token_policy(None)
        return {
            "id": f"virtual:{strategy_id}",
            "strategy_id": strategy_id,
            "strategy_display_name": strategy_display_name
            or self._format_strategy_name(strategy_id),
            "is_enabled": True,
            "admin_enabled": True,
            "suitability_score": 0.0,
            "computed_recommendation_status": effective["computed_recommendation_status"],
            "computed_recommendation_reason": effective["computed_recommendation_reason"],
            "effective_recommendation_status": effective["effective_recommendation_status"],
            "effective_recommendation_reason": effective["effective_recommendation_reason"],
            "recommendation_status": effective["recommendation_status"],
            "recommendation_reason": effective["recommendation_reason"],
            "recommendation_status_override": None,
            "recommendation_reason_override": None,
            "size_multiplier": float(effective["size_multiplier"]),
            "configured_size_multiplier": float(effective["configured_size_multiplier"]),
            "max_position_usd_override": None,
            "max_position_pct_override": None,
            "notes": None,
            "created_at": None,
            "computed_at": None,
            "updated_at": None,
        }

    @staticmethod
    def _policy_out(
        policy: TokenStrategyPolicy,
        *,
        strategy_display_name: str | None,
    ) -> dict[str, Any]:
        effective = resolve_effective_token_policy(
            {
                "is_enabled": policy.admin_enabled,
                "admin_enabled": policy.admin_enabled,
                "recommendation_status": policy.recommendation_status,
                "recommendation_reason": policy.recommendation_reason,
                "recommendation_status_override": policy.recommendation_status_override,
                "recommendation_reason_override": policy.recommendation_reason_override,
                "size_multiplier": policy.size_multiplier,
                "max_position_usd_override": policy.max_position_usd_override,
                "max_position_pct_override": policy.max_position_pct_override,
            }
        )
        return {
            "id": str(policy.id),
            "strategy_id": policy.strategy_id,
            "strategy_display_name": strategy_display_name,
            "is_enabled": policy.admin_enabled,
            "admin_enabled": policy.admin_enabled,
            "suitability_score": float(policy.suitability_score),
            "computed_recommendation_status": policy.recommendation_status,
            "computed_recommendation_reason": policy.recommendation_reason,
            "effective_recommendation_status": effective["effective_recommendation_status"],
            "effective_recommendation_reason": effective["effective_recommendation_reason"],
            "recommendation_status": effective["recommendation_status"],
            "recommendation_reason": effective["recommendation_reason"],
            "recommendation_status_override": policy.recommendation_status_override,
            "recommendation_reason_override": policy.recommendation_reason_override,
            "size_multiplier": float(effective["size_multiplier"]),
            "configured_size_multiplier": float(effective["configured_size_multiplier"]),
            "max_position_usd_override": float(policy.max_position_usd_override)
            if policy.max_position_usd_override is not None
            else None,
            "max_position_pct_override": float(policy.max_position_pct_override)
            if policy.max_position_pct_override is not None
            else None,
            "notes": policy.notes,
            "created_at": policy.created_at.isoformat(),
            "computed_at": policy.computed_at.isoformat(),
            "updated_at": policy.updated_at.isoformat(),
        }

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from oziebot_common.queues import QueueNames, strategy_signal_to_json
from oziebot_common.token_policy import resolve_effective_token_policy
from oziebot_common.worker_outbox import enqueue_worker_payload
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_api.config import Settings
from oziebot_api.models.execution import ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.exchange_connection import ExchangeConnection
from oziebot_api.models.market_data import MarketDataBboSnapshot, MarketDataCandle
from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.strategic_aggressive_allocation import (
    StrategicAggressiveAllocationConfig,
    StrategicAggressiveAllocationProfitEvent,
)
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.strategy_signal_pipeline import StrategySignalRecord
from oziebot_api.models.token_market_profile import TokenMarketProfile
from oziebot_api.models.token_strategy_policy import TokenStrategyPolicy
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy
from oziebot_api.models.user_token_permission import UserTokenPermission
from oziebot_api.services.live_coinbase import load_live_coinbase_accounts, sum_coinbase_cash_cents
from oziebot_api.services.strategy_allocation import StrategyAllocationService
from oziebot_api.services.strategy_catalog import ensure_platform_strategy_catalog
from oziebot_api.services.tenant_scope import primary_tenant_id
from oziebot_strategy_engine.strategy import MarketSnapshot, PositionState
from oziebot_strategy_engine.strategies.strategic_aggressive_allocation import (
    STRATEGY_ID,
    build_portfolio_plan,
    default_strategy_config,
    normalize_strategy_config,
    selected_trade_symbols,
    StrategicAggressiveAllocationStrategy,
)

LOOKBACK_GRANULARITY_SEC = 3600
LOOKBACK_CANDLE_LIMIT = 48


class StrategicAggressiveAllocationError(ValueError):
    pass


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _cents(amount: Decimal) -> int:
    return int((amount * Decimal("100")).quantize(Decimal("1")))


def _latest_symbol_marks(db: Session, symbols: list[str]) -> dict[str, Decimal]:
    normalized_symbols = sorted({str(symbol or "").upper() for symbol in symbols if symbol})
    if not normalized_symbols:
        return {}
    recent_cutoff = datetime.now(UTC) - timedelta(minutes=30)
    rn = (
        func.row_number()
        .over(
            partition_by=MarketDataBboSnapshot.product_id,
            order_by=MarketDataBboSnapshot.event_time.desc(),
        )
        .label("rn")
    )
    ranked = (
        select(
            MarketDataBboSnapshot.product_id.label("pid"),
            MarketDataBboSnapshot.best_bid_price.label("bid"),
            MarketDataBboSnapshot.best_ask_price.label("ask"),
            rn,
        )
        .where(
            MarketDataBboSnapshot.product_id.in_(normalized_symbols),
            MarketDataBboSnapshot.event_time >= recent_cutoff,
        )
        .subquery("ranked_bbo")
    )
    rows = db.execute(
        select(ranked.c.pid, ranked.c.bid, ranked.c.ask).where(ranked.c.rn == 1)
    ).all()
    marks: dict[str, Decimal] = {}
    for product_id, bid_price, ask_price in rows:
        symbol = str(product_id).upper()
        bid = _to_decimal(bid_price)
        ask = _to_decimal(ask_price)
        if bid > 0 and ask > 0:
            marks[symbol] = (bid + ask) / Decimal("2")
        elif bid > 0:
            marks[symbol] = bid
        elif ask > 0:
            marks[symbol] = ask
    return marks


def _load_market_snapshots(db: Session, symbols: list[str]) -> dict[str, MarketSnapshot]:
    normalized_symbols = sorted({str(symbol).upper() for symbol in symbols if symbol})
    if not normalized_symbols:
        return {}

    recent_cutoff = datetime.now(UTC) - timedelta(minutes=30)
    bbo_rn = (
        func.row_number()
        .over(
            partition_by=MarketDataBboSnapshot.product_id,
            order_by=MarketDataBboSnapshot.event_time.desc(),
        )
        .label("rn")
    )
    ranked_bbo = (
        select(
            MarketDataBboSnapshot.product_id.label("product_id"),
            MarketDataBboSnapshot.best_bid_price.label("best_bid_price"),
            MarketDataBboSnapshot.best_ask_price.label("best_ask_price"),
            MarketDataBboSnapshot.event_time.label("event_time"),
            bbo_rn,
        )
        .where(
            MarketDataBboSnapshot.product_id.in_(normalized_symbols),
            MarketDataBboSnapshot.event_time >= recent_cutoff,
        )
        .subquery("ranked_bbo")
    )
    bbo_rows = db.execute(
        select(
            ranked_bbo.c.product_id,
            ranked_bbo.c.best_bid_price,
            ranked_bbo.c.best_ask_price,
            ranked_bbo.c.event_time,
        ).where(ranked_bbo.c.rn == 1)
    ).all()
    bbo_map = {
        str(product_id).upper(): {
            "bid": _to_decimal(best_bid_price),
            "ask": _to_decimal(best_ask_price),
            "event_time": event_time,
        }
        for product_id, best_bid_price, best_ask_price, event_time in bbo_rows
    }

    candle_rows = db.scalars(
        select(MarketDataCandle)
        .where(
            MarketDataCandle.product_id.in_(normalized_symbols),
            MarketDataCandle.granularity_sec == LOOKBACK_GRANULARITY_SEC,
        )
        .order_by(MarketDataCandle.product_id, MarketDataCandle.bucket_start.desc())
    ).all()
    candle_map: dict[str, list[MarketDataCandle]] = defaultdict(list)
    for row in candle_rows:
        symbol = str(row.product_id).upper()
        bucket_rows = candle_map[symbol]
        if len(bucket_rows) < LOOKBACK_CANDLE_LIMIT:
            bucket_rows.append(row)

    markets: dict[str, MarketSnapshot] = {}
    for symbol in normalized_symbols:
        bbo = bbo_map.get(symbol)
        candles = list(reversed(candle_map.get(symbol, [])))
        if bbo is None or not candles:
            continue
        current_price = (
            (bbo["bid"] + bbo["ask"]) / Decimal("2")
            if bbo["bid"] and bbo["ask"]
            else bbo["bid"] or bbo["ask"]
        )
        latest_candle = candles[-1]
        markets[symbol] = MarketSnapshot(
            timestamp=bbo["event_time"] or latest_candle.event_time,
            symbol=symbol,
            current_price=current_price,
            bid_price=bbo["bid"],
            ask_price=bbo["ask"],
            volume_24h=sum(_to_decimal(row.volume) for row in candles[-24:]),
            open_price=_to_decimal(latest_candle.open),
            high_price=max(_to_decimal(row.high) for row in candles[-24:]),
            low_price=min(_to_decimal(row.low) for row in candles[-24:]),
            close_price=_to_decimal(latest_candle.close),
            candle_closes=[float(_to_decimal(row.close)) for row in candles],
            candle_volumes=[float(_to_decimal(row.volume)) for row in candles],
            candle_highs=[float(_to_decimal(row.high)) for row in candles],
            candle_lows=[float(_to_decimal(row.low)) for row in candles],
        )
    return markets


def _load_positions(
    db: Session, *, user_id: uuid.UUID, trading_mode: str
) -> dict[str, ExecutionPosition]:
    rows = db.scalars(
        select(ExecutionPosition)
        .where(
            ExecutionPosition.user_id == user_id,
            ExecutionPosition.strategy_id == STRATEGY_ID,
            ExecutionPosition.trading_mode == trading_mode,
        )
        .order_by(ExecutionPosition.updated_at.desc())
    ).all()
    return {str(row.symbol).upper(): row for row in rows if _to_decimal(row.quantity) > 0}


def _position_state_map(rows: dict[str, ExecutionPosition]) -> dict[str, PositionState]:
    return {
        symbol: PositionState(
            symbol=symbol,
            quantity=_to_decimal(row.quantity),
            entry_price=_to_decimal(row.avg_entry_price),
            opened_at=row.opened_at,
        )
        for symbol, row in rows.items()
    }


def _profile_and_token_map(
    db: Session,
    *,
    user: User,
) -> tuple[dict[str, dict[str, Any]], dict[str, PlatformTokenAllowlist]]:
    tokens = db.scalars(
        select(PlatformTokenAllowlist)
        .where(PlatformTokenAllowlist.is_enabled)
        .order_by(
            PlatformTokenAllowlist.sort_order,
            PlatformTokenAllowlist.symbol,
        )
    ).all()
    token_map = {str(token.symbol).upper(): token for token in tokens}
    permissions = {
        row.platform_token_id: row
        for row in db.scalars(
            select(UserTokenPermission).where(UserTokenPermission.user_id == user.id)
        ).all()
    }
    profiles = {row.token_id: row for row in db.scalars(select(TokenMarketProfile)).all()}
    policies = {
        (row.token_id, row.strategy_id): row
        for row in db.scalars(
            select(TokenStrategyPolicy).where(TokenStrategyPolicy.strategy_id == STRATEGY_ID)
        ).all()
    }
    payload: dict[str, dict[str, Any]] = {}
    for symbol, token in token_map.items():
        perm = permissions.get(token.id)
        policy = policies.get((token.id, STRATEGY_ID))
        effective = resolve_effective_token_policy(
            {
                "admin_enabled": getattr(policy, "admin_enabled", True) if policy else True,
                "recommendation_status": getattr(policy, "recommendation_status", None),
                "recommendation_reason": getattr(policy, "recommendation_reason", None),
                "recommendation_status_override": getattr(
                    policy, "recommendation_status_override", None
                ),
                "recommendation_reason_override": getattr(
                    policy, "recommendation_reason_override", None
                ),
                "size_multiplier": getattr(policy, "size_multiplier", None),
                "max_position_pct_override": getattr(policy, "max_position_pct_override", None),
                "max_position_usd_override": getattr(policy, "max_position_usd_override", None),
            }
            if policy
            else None
        )
        profile = profiles.get(token.id)
        payload[symbol] = {
            "display_name": token.display_name,
            "user_enabled": bool(perm and perm.is_enabled),
            "admin_enabled": bool(token.is_enabled and effective["admin_enabled"]),
            "effective_recommendation_status": str(effective["effective_recommendation_status"]),
            "effective_recommendation_reason": effective.get("effective_recommendation_reason"),
            "trend_score": float(profile.trend_score)
            if profile and profile.trend_score is not None
            else None,
            "liquidity_score": float(profile.liquidity_score)
            if profile and profile.liquidity_score is not None
            else None,
            "ecosystem": str((token.extra or {}).get("ecosystem") or "") or None,
            "tags": list((token.extra or {}).get("tags") or []),
        }
    return payload, token_map


def _allowed_token_options(db: Session, *, user: User) -> list[dict[str, Any]]:
    profiles, _token_map = _profile_and_token_map(db, user=user)
    out: list[dict[str, Any]] = []
    for symbol, entry in sorted(profiles.items()):
        if not entry["user_enabled"] or not entry["admin_enabled"]:
            continue
        if entry["effective_recommendation_status"] == "blocked":
            continue
        out.append(
            {
                "symbol": symbol,
                "display_name": entry["display_name"],
                "bucket_ids": [],
                "ecosystem": entry["ecosystem"],
                "strategy_policy_status": entry["effective_recommendation_status"],
                "strategy_policy_reason": entry["effective_recommendation_reason"],
                "user_enabled": entry["user_enabled"],
                "admin_enabled": entry["admin_enabled"],
                "trend_score": entry["trend_score"],
                "liquidity_score": entry["liquidity_score"],
            }
        )
    return out


def _mode_capital_cents(db: Session, *, user: User, trading_mode: str, settings: Settings) -> int:
    if trading_mode == "live":
        accounts = load_live_coinbase_accounts(db, user=user, settings=settings)
        return sum_coinbase_cash_cents(accounts or [], include_hold=False)
    plan = StrategyAllocationService.get_plan(db, user_id=user.id, trading_mode=trading_mode)
    return int(plan.total_capital_cents if plan is not None else 0)


def _sync_user_strategy(
    db: Session,
    *,
    user: User,
    paper_config: dict[str, Any] | None,
    live_config: dict[str, Any] | None,
) -> None:
    ensure_platform_strategy_catalog(db)
    row = db.scalar(
        select(UserStrategy).where(
            UserStrategy.user_id == user.id,
            UserStrategy.strategy_id == STRATEGY_ID,
        )
    )
    now = datetime.now(UTC)
    merged = {
        "paper": paper_config or default_strategy_config(),
        "live": live_config or default_strategy_config(),
        "symbols": sorted(
            set(selected_trade_symbols(paper_config) + selected_trade_symbols(live_config))
        ),
    }
    enabled = bool((paper_config or {}).get("enabled") or (live_config or {}).get("enabled"))
    if row is None:
        row = UserStrategy(
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            is_enabled=enabled,
            config=merged,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.is_enabled = enabled
        row.config = merged
        row.updated_at = now
    db.flush()


def _validate_requested_config(
    db: Session,
    *,
    user: User,
    settings: Settings,
    payload: dict[str, Any],
) -> dict[str, Any]:
    strategy = StrategicAggressiveAllocationStrategy()
    normalized = normalize_strategy_config(payload)
    strategy.validate_config(normalized)
    trading_mode = str(normalized["trading_mode"]).lower()
    capital_target = _to_decimal((normalized.get("total_allocated_amount_usd") or {}).get("target"))
    available_mode_cents = _mode_capital_cents(
        db, user=user, trading_mode=trading_mode, settings=settings
    )
    if capital_target > 0 and _cents(capital_target) > available_mode_cents:
        raise StrategicAggressiveAllocationError(
            "Requested capital exceeds available cash for this mode"
        )
    if trading_mode == "live":
        tenant_id = primary_tenant_id(db, user)
        if tenant_id is None:
            raise StrategicAggressiveAllocationError("Live trading requires a tenant membership")
        connection = db.scalar(
            select(ExchangeConnection).where(
                ExchangeConnection.tenant_id == tenant_id,
                ExchangeConnection.provider == "coinbase",
            )
        )
        if (
            connection is None
            or connection.validation_status != "valid"
            or connection.health_status != "healthy"
            or connection.can_trade is not True
        ):
            raise StrategicAggressiveAllocationError(
                "Live mode requires a healthy Coinbase connection with trading enabled"
            )
    profiles, _token_map = _profile_and_token_map(db, user=user)
    for symbol in selected_trade_symbols(normalized):
        profile = profiles.get(symbol)
        if profile is None or not profile["user_enabled"] or not profile["admin_enabled"]:
            raise StrategicAggressiveAllocationError(
                f"Token '{symbol}' is not enabled for this user"
            )
        if profile["effective_recommendation_status"] == "blocked":
            raise StrategicAggressiveAllocationError(
                f"Token '{symbol}' is blocked by admin token policy for {STRATEGY_ID}"
            )
    return normalized


def get_config(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    row = db.scalar(
        select(StrategicAggressiveAllocationConfig).where(
            StrategicAggressiveAllocationConfig.user_id == user.id,
            StrategicAggressiveAllocationConfig.strategy_id == STRATEGY_ID,
            StrategicAggressiveAllocationConfig.trading_mode == trading_mode,
        )
    )
    config = (
        {
            "trading_mode": row.trading_mode,
            "enabled": row.is_enabled,
            "total_allocated_amount_usd": row.total_allocated_amount_usd,
            "bucket_allocations": row.bucket_allocations,
            "selected_tokens": row.selected_tokens,
            "max_allocation_per_token": row.max_allocation_per_token,
            "profit_taking_rules": row.profit_taking_rules,
            "stop_loss_rules": row.stop_loss_rules,
            "trailing_stop_rules": row.trailing_stop_rules,
            "rebalance_settings": row.rebalance_settings,
            "mode_settings": row.mode_settings,
        }
        if row is not None
        else {"trading_mode": trading_mode, **default_strategy_config()}
    )
    config["trading_mode"] = trading_mode
    return {
        "strategy_id": STRATEGY_ID,
        "config": normalize_strategy_config(config),
        "available_tokens": _allowed_token_options(db, user=user),
    }


def upsert_config(
    db: Session,
    *,
    user: User,
    settings: Settings,
    payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = _validate_requested_config(db, user=user, settings=settings, payload=payload)
    trading_mode = str(normalized["trading_mode"]).lower()
    row = db.scalar(
        select(StrategicAggressiveAllocationConfig).where(
            StrategicAggressiveAllocationConfig.user_id == user.id,
            StrategicAggressiveAllocationConfig.strategy_id == STRATEGY_ID,
            StrategicAggressiveAllocationConfig.trading_mode == trading_mode,
        )
    )
    now = datetime.now(UTC)
    if row is None:
        row = StrategicAggressiveAllocationConfig(
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode=trading_mode,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    row.is_enabled = bool(normalized.get("enabled"))
    row.total_allocated_amount_usd = dict(normalized.get("total_allocated_amount_usd") or {})
    row.bucket_allocations = list(normalized.get("bucket_allocations") or [])
    row.selected_tokens = dict(normalized.get("selected_tokens") or {})
    row.max_allocation_per_token = dict(normalized.get("max_allocation_per_token") or {})
    row.profit_taking_rules = dict(normalized.get("profit_taking_rules") or {})
    row.stop_loss_rules = dict(normalized.get("stop_loss_rules") or {})
    row.trailing_stop_rules = dict(normalized.get("trailing_stop_rules") or {})
    row.rebalance_settings = dict(normalized.get("rebalance_settings") or {})
    row.mode_settings = dict(normalized.get("mode_settings") or {})
    row.updated_at = now
    db.flush()

    paper_row = db.scalar(
        select(StrategicAggressiveAllocationConfig).where(
            StrategicAggressiveAllocationConfig.user_id == user.id,
            StrategicAggressiveAllocationConfig.strategy_id == STRATEGY_ID,
            StrategicAggressiveAllocationConfig.trading_mode == "paper",
        )
    )
    live_row = db.scalar(
        select(StrategicAggressiveAllocationConfig).where(
            StrategicAggressiveAllocationConfig.user_id == user.id,
            StrategicAggressiveAllocationConfig.strategy_id == STRATEGY_ID,
            StrategicAggressiveAllocationConfig.trading_mode == "live",
        )
    )
    _sync_user_strategy(
        db,
        user=user,
        paper_config=getattr(paper_row, "to_dict", lambda: None)()
        if hasattr(paper_row, "to_dict")
        else (
            {
                "trading_mode": "paper",
                "enabled": paper_row.is_enabled,
                "total_allocated_amount_usd": paper_row.total_allocated_amount_usd,
                "bucket_allocations": paper_row.bucket_allocations,
                "selected_tokens": paper_row.selected_tokens,
                "max_allocation_per_token": paper_row.max_allocation_per_token,
                "profit_taking_rules": paper_row.profit_taking_rules,
                "stop_loss_rules": paper_row.stop_loss_rules,
                "trailing_stop_rules": paper_row.trailing_stop_rules,
                "rebalance_settings": paper_row.rebalance_settings,
                "mode_settings": paper_row.mode_settings,
            }
            if paper_row
            else None
        ),
        live_config=getattr(live_row, "to_dict", lambda: None)()
        if hasattr(live_row, "to_dict")
        else (
            {
                "trading_mode": "live",
                "enabled": live_row.is_enabled,
                "total_allocated_amount_usd": live_row.total_allocated_amount_usd,
                "bucket_allocations": live_row.bucket_allocations,
                "selected_tokens": live_row.selected_tokens,
                "max_allocation_per_token": live_row.max_allocation_per_token,
                "profit_taking_rules": live_row.profit_taking_rules,
                "stop_loss_rules": live_row.stop_loss_rules,
                "trailing_stop_rules": live_row.trailing_stop_rules,
                "rebalance_settings": live_row.rebalance_settings,
                "mode_settings": live_row.mode_settings,
            }
            if live_row
            else None
        ),
    )
    db.commit()
    return get_config(db, user=user, trading_mode=trading_mode)


def set_enabled(
    db: Session,
    *,
    user: User,
    trading_mode: str,
    enabled: bool,
) -> dict[str, Any]:
    row = db.scalar(
        select(StrategicAggressiveAllocationConfig).where(
            StrategicAggressiveAllocationConfig.user_id == user.id,
            StrategicAggressiveAllocationConfig.strategy_id == STRATEGY_ID,
            StrategicAggressiveAllocationConfig.trading_mode == trading_mode,
        )
    )
    if row is None:
        row = StrategicAggressiveAllocationConfig(
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode=trading_mode,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        base = normalize_strategy_config(
            {"trading_mode": trading_mode, **default_strategy_config()}
        )
        row.total_allocated_amount_usd = dict(base["total_allocated_amount_usd"])
        row.bucket_allocations = list(base["bucket_allocations"])
        row.selected_tokens = dict(base["selected_tokens"])
        row.max_allocation_per_token = dict(base["max_allocation_per_token"])
        row.profit_taking_rules = dict(base["profit_taking_rules"])
        row.stop_loss_rules = dict(base["stop_loss_rules"])
        row.trailing_stop_rules = dict(base["trailing_stop_rules"])
        row.rebalance_settings = dict(base["rebalance_settings"])
        row.mode_settings = dict(base["mode_settings"])
        db.add(row)
    row.is_enabled = enabled
    row.updated_at = datetime.now(UTC)
    db.flush()
    paper = get_config(db, user=user, trading_mode="paper")["config"]
    live = get_config(db, user=user, trading_mode="live")["config"]
    _sync_user_strategy(db, user=user, paper_config=paper, live_config=live)
    db.commit()
    return get_config(db, user=user, trading_mode=trading_mode)


def list_positions(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    rows = list(_load_positions(db, user_id=user.id, trading_mode=trading_mode).values())
    marks = _latest_symbol_marks(db, [row.symbol for row in rows])
    position_items: list[dict[str, Any]] = []
    total_unrealized = Decimal("0")
    total_exposure = Decimal("0")
    for row in rows:
        quantity = _to_decimal(row.quantity)
        entry = _to_decimal(row.avg_entry_price)
        mark = marks.get(str(row.symbol).upper(), entry)
        exposure = quantity * mark
        unrealized = (mark - entry) * quantity
        total_exposure += exposure
        total_unrealized += unrealized
        position_items.append(
            {
                "id": str(row.id),
                "symbol": row.symbol,
                "quantity": row.quantity,
                "entry_price": row.avg_entry_price,
                "mark_price": str(mark),
                "exposure_usd": float(exposure),
                "unrealized_pnl_usd": float(unrealized),
                "realized_pnl_cents": row.realized_pnl_cents,
                "opened_at": row.opened_at.isoformat() if row.opened_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
        )
    return {
        "strategy_id": STRATEGY_ID,
        "trading_mode": trading_mode,
        "total_exposure_usd": float(total_exposure),
        "unrealized_pnl_usd": float(total_unrealized),
        "positions": position_items,
    }


def get_performance(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    positions = _load_positions(db, user_id=user.id, trading_mode=trading_mode)
    marks = _latest_symbol_marks(db, list(positions.keys()))
    unrealized_cents = 0
    for symbol, row in positions.items():
        quantity = _to_decimal(row.quantity)
        entry = _to_decimal(row.avg_entry_price)
        mark = marks.get(symbol, entry)
        unrealized_cents += _cents((mark - entry) * quantity)
    realized_cents = sum(
        int(value or 0)
        for value in db.scalars(
            select(ExecutionTradeRecord.realized_pnl_cents).where(
                ExecutionTradeRecord.user_id == user.id,
                ExecutionTradeRecord.strategy_id == STRATEGY_ID,
                ExecutionTradeRecord.trading_mode == trading_mode,
            )
        ).all()
    )
    trade_count = (
        db.scalar(
            select(func.count())
            .select_from(ExecutionTradeRecord)
            .where(
                ExecutionTradeRecord.user_id == user.id,
                ExecutionTradeRecord.strategy_id == STRATEGY_ID,
                ExecutionTradeRecord.trading_mode == trading_mode,
            )
        )
        or 0
    )
    return {
        "strategy_id": STRATEGY_ID,
        "trading_mode": trading_mode,
        "realized_pnl_cents": realized_cents,
        "unrealized_pnl_cents": unrealized_cents,
        "total_pnl_cents": realized_cents + unrealized_cents,
        "trade_count": int(trade_count),
        "open_position_count": len(positions),
    }


def get_profit_history(
    db: Session, *, user: User, trading_mode: str
) -> list[StrategicAggressiveAllocationProfitEvent]:
    return db.scalars(
        select(StrategicAggressiveAllocationProfitEvent)
        .where(
            StrategicAggressiveAllocationProfitEvent.user_id == user.id,
            StrategicAggressiveAllocationProfitEvent.strategy_id == STRATEGY_ID,
            StrategicAggressiveAllocationProfitEvent.trading_mode == trading_mode,
        )
        .order_by(StrategicAggressiveAllocationProfitEvent.occurred_at.desc())
    ).all()


def _capital_context(db: Session, *, user_id: uuid.UUID, trading_mode: str) -> dict[str, Any]:
    bucket = db.scalar(
        select(StrategyCapitalBucket).where(
            StrategyCapitalBucket.user_id == user_id,
            StrategyCapitalBucket.strategy_id == STRATEGY_ID,
            StrategyCapitalBucket.trading_mode == trading_mode,
        )
    )
    plan = StrategyAllocationService.get_plan(db, user_id=user_id, trading_mode=trading_mode)
    if bucket is not None:
        return {
            "assigned_capital_usd": float(Decimal(bucket.assigned_capital_cents) / Decimal("100")),
            "available_capital_usd": float(Decimal(bucket.available_cash_cents) / Decimal("100")),
            "buying_power_usd": float(
                Decimal(bucket.available_buying_power_cents) / Decimal("100")
            ),
        }
    return {
        "assigned_capital_usd": float(
            Decimal((plan.total_capital_cents if plan else 0)) / Decimal("100")
        ),
        "available_capital_usd": float(
            Decimal((plan.total_capital_cents if plan else 0)) / Decimal("100")
        ),
        "buying_power_usd": float(
            Decimal((plan.total_capital_cents if plan else 0)) / Decimal("100")
        ),
    }


def _config_row_to_payload(
    row: StrategicAggressiveAllocationConfig | None, trading_mode: str
) -> dict[str, Any]:
    if row is None:
        return {"trading_mode": trading_mode, **default_strategy_config()}
    return {
        "trading_mode": trading_mode,
        "enabled": row.is_enabled,
        "total_allocated_amount_usd": row.total_allocated_amount_usd,
        "bucket_allocations": row.bucket_allocations,
        "selected_tokens": row.selected_tokens,
        "max_allocation_per_token": row.max_allocation_per_token,
        "profit_taking_rules": row.profit_taking_rules,
        "stop_loss_rules": row.stop_loss_rules,
        "trailing_stop_rules": row.trailing_stop_rules,
        "rebalance_settings": row.rebalance_settings,
        "mode_settings": row.mode_settings,
    }


def _load_runtime_state_map(
    db: Session, *, user_id: uuid.UUID, trading_mode: str
) -> dict[str, dict[str, Any]]:
    row = db.scalar(
        select(UserStrategy).where(
            UserStrategy.user_id == user_id,
            UserStrategy.strategy_id == STRATEGY_ID,
        )
    )
    config = row.config if row is not None and isinstance(row.config, dict) else {}
    runtime_rows = db.execute(
        select(UserStrategy.config).where(
            UserStrategy.user_id == user_id, UserStrategy.strategy_id == STRATEGY_ID
        )
    ).all()
    _ = runtime_rows, config
    state_row = db.execute(
        select(UserStrategy.config).where(
            UserStrategy.user_id == user_id, UserStrategy.strategy_id == STRATEGY_ID
        )
    ).first()
    _ = state_row
    from oziebot_api.models.user_strategy import UserStrategyState

    runtime = db.scalar(
        select(UserStrategyState).where(
            UserStrategyState.user_id == user_id,
            UserStrategyState.strategy_id == STRATEGY_ID,
            UserStrategyState.trading_mode == trading_mode,
        )
    )
    state = runtime.state if runtime is not None and isinstance(runtime.state, dict) else {}
    symbols = state.get("symbols") if isinstance(state, dict) else {}
    return dict(symbols) if isinstance(symbols, dict) else {}


def build_rebalance_preview(
    db: Session,
    *,
    user: User,
    trading_mode: str,
    aggressive_rebalance: bool | None = None,
) -> dict[str, Any]:
    row = db.scalar(
        select(StrategicAggressiveAllocationConfig).where(
            StrategicAggressiveAllocationConfig.user_id == user.id,
            StrategicAggressiveAllocationConfig.strategy_id == STRATEGY_ID,
            StrategicAggressiveAllocationConfig.trading_mode == trading_mode,
        )
    )
    config = normalize_strategy_config(_config_row_to_payload(row, trading_mode))
    if aggressive_rebalance is not None:
        config["rebalance_settings"]["aggressive_rebalance"] = aggressive_rebalance  # type: ignore[index]
    positions = _load_positions(db, user_id=user.id, trading_mode=trading_mode)
    market_map = _load_market_snapshots(
        db,
        list(set(selected_trade_symbols(config) + list(positions.keys()) + ["BTC-USD", "ETH-USD"])),
    )
    profiles, _token_map = _profile_and_token_map(db, user=user)
    runtime_state = _load_runtime_state_map(db, user_id=user.id, trading_mode=trading_mode)
    plan = build_portfolio_plan(
        config=config,
        market_map=market_map,
        positions=_position_state_map(positions),
        runtime_state=runtime_state,
        token_profiles=profiles,
        capital_context=_capital_context(db, user_id=user.id, trading_mode=trading_mode),
    )
    actions: list[dict[str, Any]] = []
    symbol_contexts = plan.get("symbol_contexts") or {}
    aggressive = bool((config.get("rebalance_settings") or {}).get("aggressive_rebalance"))
    for symbol, ctx in symbol_contexts.items():
        if bool(ctx.get("should_enter")) and symbol not in positions:
            actions.append(
                {
                    "symbol": symbol,
                    "action": "buy",
                    "bucket_id": ctx["bucket_id"],
                    "reason": "ranked_for_rebalance_entry",
                    "suggested_size_usd": ctx.get("per_new_symbol_capital_usd") or 0,
                }
            )
    if aggressive:
        active_symbols = {
            symbol
            for symbol, ctx in symbol_contexts.items()
            if symbol in (ctx.get("bucket_plan") or {}).get("active_symbols", [])
        }
        for symbol, row in positions.items():
            if symbol not in active_symbols:
                actions.append(
                    {
                        "symbol": symbol,
                        "action": "close",
                        "bucket_id": runtime_state.get(symbol, {}).get("saa_bucket_id")
                        or "unknown",
                        "reason": "aggressive_rebalance_rotation",
                        "suggested_size_usd": float(
                            _to_decimal(row.quantity)
                            * _to_decimal(
                                market_map.get(symbol).current_price
                                if symbol in market_map
                                else row.avg_entry_price
                            )
                        ),
                    }
                )
    return {
        "strategy_id": STRATEGY_ID,
        "trading_mode": trading_mode,
        "config": config,
        "plan": plan,
        "actions": actions,
    }


def _bind_engine(db: Session) -> Engine:
    bind = db.get_bind()
    if isinstance(bind, Engine):
        return bind
    if isinstance(bind, Connection):
        return bind.engine
    raise StrategicAggressiveAllocationError("Database engine unavailable")


def _enqueue_signal_generated(engine: Engine, event: StrategySignalEvent) -> None:
    payload = {
        "signal": strategy_signal_to_json(event),
        "trace_id": f"saa-rebalance-{event.signal_id}",
    }
    try:
        enqueue_worker_payload(
            engine,
            QueueNames.signal_generated(event.trading_mode),
            payload,
        )
        return
    except Exception:
        pass

    now = datetime.now(UTC)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO worker_message_outbox (
                  id, queue_name, payload, status, attempt_count,
                  retry_after, lease_expires_at, created_at, updated_at
                ) VALUES (
                  :id, :queue_name, :payload, 'pending', 0,
                  NULL, NULL, :created_at, :updated_at
                )
                """
            ),
            {
                "id": str(uuid.uuid4()),
                "queue_name": QueueNames.signal_generated(event.trading_mode),
                "payload": str(payload),
                "created_at": now,
                "updated_at": now,
            },
        )


def execute_rebalance(
    db: Session,
    *,
    user: User,
    trading_mode: str,
    aggressive_rebalance: bool | None = None,
) -> dict[str, Any]:
    preview = build_rebalance_preview(
        db, user=user, trading_mode=trading_mode, aggressive_rebalance=aggressive_rebalance
    )
    engine = _bind_engine(db)
    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        raise StrategicAggressiveAllocationError("Tenant scope is required for rebalance execution")
    now = datetime.now(UTC)
    queued: list[dict[str, Any]] = []
    run_id = uuid.uuid4()
    for action in preview["actions"]:
        signal_id = uuid.uuid4()
        symbol = str(action["symbol"]).upper()
        suggested_size_usd = _to_decimal(action.get("suggested_size_usd"))
        mark_map = _latest_symbol_marks(db, [symbol])
        mark = mark_map.get(symbol, Decimal("0"))
        quantity = (
            (suggested_size_usd / mark)
            if action["action"] == "buy" and suggested_size_usd > 0 and mark > 0
            else Decimal("0.00000001")
        )
        event = StrategySignalEvent(
            signal_id=signal_id,
            run_id=run_id,
            user_id=user.id,
            strategy_name=STRATEGY_ID,
            symbol=symbol,
            action=SignalType.BUY if action["action"] == "buy" else SignalType.CLOSE,
            confidence=0.72,
            suggested_size=max(quantity, Decimal("0.00000001")),
            reasoning_metadata={
                "manual_rebalance": True,
                "bucket_id": action["bucket_id"],
                "reason": action["reason"],
            },
            trading_mode=TradingMode(str(trading_mode).lower()),
            timestamp=now,
        )
        db.add(
            StrategySignalRecord(
                signal_id=signal_id,
                run_id=run_id,
                user_id=user.id,
                strategy_name=STRATEGY_ID,
                symbol=symbol,
                action=event.action.value,
                confidence=event.confidence,
                suggested_size=str(event.suggested_size),
                reasoning_metadata=event.reasoning_metadata,
                trading_mode=trading_mode,
                timestamp=now,
            )
        )
        _enqueue_signal_generated(engine, event)
        queued.append(
            {
                "signal_id": str(signal_id),
                "symbol": symbol,
                "action": event.action.value,
                "bucket_id": action["bucket_id"],
                "reason": action["reason"],
                "suggested_size": float(event.suggested_size),
                "confidence": event.confidence,
            }
        )
    db.commit()
    return {
        "strategy_id": STRATEGY_ID,
        "trading_mode": trading_mode,
        "queued": queued,
        "queued_count": len(queued),
    }

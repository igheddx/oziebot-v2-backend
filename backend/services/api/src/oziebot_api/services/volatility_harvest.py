from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from oziebot_common.queues import QueueNames, strategy_signal_to_json
from oziebot_common.reason_codes import normalize_reason_code
from oziebot_common.worker_outbox import enqueue_worker_payload
from oziebot_api.models.execution import ExecutionOrder, ExecutionPosition, ExecutionTradeRecord
from oziebot_api.models.market_data import MarketDataBboSnapshot, MarketDataCandle
from oziebot_api.models.platform_setting import PlatformSetting
from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.strategy_allocation import StrategyCapitalBucket
from oziebot_api.models.strategy_signal_pipeline import StrategySignalRecord
from oziebot_api.models.token_market_profile import TokenMarketProfile
from oziebot_api.models.token_strategy_policy import TokenStrategyPolicy
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy, UserStrategyState
from oziebot_api.models.user_token_permission import UserTokenPermission
from oziebot_api.models.volatility_harvest import (
    VolatilityHarvestConfig,
    VolatilityHarvestMetric,
    VolatilityHarvestPosition,
    VolatilityHarvestTransaction,
)
from oziebot_api.services.platform_management import upsert_setting
from oziebot_api.services.strategy_catalog import ensure_platform_strategy_catalog
from oziebot_api.services.tenant_scope import primary_tenant_id
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.strategy import MarketSnapshot, PositionState, StrategyContext
from oziebot_strategy_engine.strategies.volatility_harvest import (
    STRATEGY_ID,
    VolatilityHarvestStrategy,
    build_volatility_harvest_plan,
    default_strategy_config,
    normalize_strategy_config,
    selected_trade_symbols,
)

LOOKBACK_GRANULARITY_SEC = 3600
LOOKBACK_CANDLE_LIMIT = 48
ADMIN_DEFAULTS_KEY = "strategy.volatility_harvest.admin_defaults"


class VolatilityHarvestError(ValueError):
    pass


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(default)


def _cents(amount: Decimal) -> int:
    return int((amount * Decimal("100")).quantize(Decimal("1")))


def _default_admin_defaults() -> dict[str, Any]:
    config = default_strategy_config()
    return {
        "max_volatility_pct": 12.0,
        "default_harvest_bands": config["harvest_bands"],
        "fee_assumptions": config["fee_settings"],
        "emergency_disable": False,
        "suspend_rebuys_on_btc_breakdown": True,
    }


def get_admin_defaults(db: Session) -> dict[str, Any]:
    row = db.get(PlatformSetting, ADMIN_DEFAULTS_KEY)
    defaults = _default_admin_defaults()
    if row and isinstance(row.value, dict):
        defaults.update(row.value)
    return defaults


def set_admin_defaults(db: Session, *, user: User, payload: dict[str, Any]) -> dict[str, Any]:
    defaults = _default_admin_defaults()
    defaults.update(payload)
    upsert_setting(
        db,
        key=ADMIN_DEFAULTS_KEY,
        value=defaults,
        updated_by_user_id=user.id,
    )
    db.commit()
    return defaults


def _config_row(
    db: Session, *, user_id: uuid.UUID, trading_mode: str
) -> VolatilityHarvestConfig | None:
    return db.scalars(
        select(VolatilityHarvestConfig).where(
            VolatilityHarvestConfig.user_id == user_id,
            VolatilityHarvestConfig.strategy_id == STRATEGY_ID,
            VolatilityHarvestConfig.trading_mode == trading_mode,
        )
    ).first()


def _serialize_config(row: VolatilityHarvestConfig | None, *, trading_mode: str) -> dict[str, Any]:
    if row is None:
        config = default_strategy_config()
        config["trading_mode"] = trading_mode
        return config
    return normalize_strategy_config(
        {
            "trading_mode": trading_mode,
            "enabled": row.is_enabled,
            "selected_tokens": row.selected_tokens,
            "total_allocated_amount_usd": row.total_allocated_amount_usd,
            "core_position_percentage": float(row.core_position_percentage),
            "trading_position_percentage": float(row.trading_position_percentage),
            "entry_layers": row.entry_layers,
            "harvest_bands": row.harvest_bands,
            "rebuy_bands": row.rebuy_bands,
            "volatility_settings": row.volatility_settings,
            "risk_controls": row.risk_controls,
            "fee_settings": row.fee_settings,
            "mode_settings": row.mode_settings,
        }
    )


def _latest_symbol_marks(db: Session, symbols: list[str]) -> dict[str, Decimal]:
    if not symbols:
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
            MarketDataBboSnapshot.product_id.in_(symbols),
            MarketDataBboSnapshot.event_time >= recent_cutoff,
        )
        .subquery("ranked_bbo")
    )
    rows = db.execute(
        select(ranked.c.pid, ranked.c.bid, ranked.c.ask).where(ranked.c.rn == 1)
    ).all()
    marks: dict[str, Decimal] = {}
    for product_id, bid_price, ask_price in rows:
        bid = _to_decimal(bid_price)
        ask = _to_decimal(ask_price)
        if bid > 0 and ask > 0:
            marks[str(product_id).upper()] = (bid + ask) / Decimal("2")
        elif bid > 0:
            marks[str(product_id).upper()] = bid
        elif ask > 0:
            marks[str(product_id).upper()] = ask
    return marks


def _load_market_snapshots(db: Session, symbols: list[str]) -> dict[str, MarketSnapshot]:
    normalized = sorted({str(symbol).upper() for symbol in symbols if symbol})
    if not normalized:
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
            MarketDataBboSnapshot.product_id.in_(normalized),
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
            MarketDataCandle.product_id.in_(normalized),
            MarketDataCandle.granularity_sec == LOOKBACK_GRANULARITY_SEC,
        )
        .order_by(MarketDataCandle.product_id, MarketDataCandle.bucket_start.desc())
    ).all()
    candle_map: dict[str, list[MarketDataCandle]] = {}
    for row in candle_rows:
        symbol = str(row.product_id).upper()
        candle_map.setdefault(symbol, [])
        if len(candle_map[symbol]) < LOOKBACK_CANDLE_LIMIT:
            candle_map[symbol].append(row)
    markets: dict[str, MarketSnapshot] = {}
    for symbol in normalized:
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


def _load_token_profiles(
    db: Session, *, user: User
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    tokens = db.scalars(
        select(PlatformTokenAllowlist)
        .where(PlatformTokenAllowlist.is_enabled)
        .order_by(PlatformTokenAllowlist.symbol)
    ).all()
    permissions = {
        row.platform_token_id: row
        for row in db.scalars(
            select(UserTokenPermission).where(UserTokenPermission.user_id == user.id)
        ).all()
    }
    profiles = {row.token_id: row for row in db.scalars(select(TokenMarketProfile)).all()}
    policies = {
        row.token_id: row
        for row in db.scalars(
            select(TokenStrategyPolicy).where(TokenStrategyPolicy.strategy_id == STRATEGY_ID)
        ).all()
    }
    options: list[dict[str, Any]] = []
    profile_map: dict[str, dict[str, Any]] = {}
    for token in tokens:
        permission = permissions.get(token.id)
        policy = policies.get(token.id)
        profile = profiles.get(token.id)
        status = str(getattr(policy, "recommendation_status", "allowed") or "allowed")
        reason = getattr(policy, "recommendation_reason", None)
        admin_enabled = bool(token.is_enabled and getattr(policy, "admin_enabled", True))
        option = {
            "symbol": token.symbol,
            "display_name": token.display_name,
            "ecosystem": str((token.extra or {}).get("ecosystem") or "") or None,
            "strategy_policy_status": status,
            "strategy_policy_reason": reason,
            "user_enabled": bool(permission and permission.is_enabled),
            "admin_enabled": admin_enabled,
            "volatility_score": float(profile.volatility_score) if profile else None,
            "liquidity_score": float(profile.liquidity_score) if profile else None,
        }
        if option["user_enabled"] and option["admin_enabled"] and status != "blocked":
            options.append(option)
        profile_map[str(token.symbol).upper()] = {
            "volatility_score": float(profile.volatility_score) if profile else 0.0,
            "liquidity_score": float(profile.liquidity_score) if profile else 0.0,
            "trend_score": float(profile.trend_score) if profile else 0.0,
            "ecosystem": option["ecosystem"],
            "tags": list((token.extra or {}).get("tags") or []),
        }
    return options, profile_map


def _capital_context(db: Session, *, user_id: uuid.UUID, trading_mode: str) -> dict[str, object]:
    bucket = db.scalars(
        select(StrategyCapitalBucket).where(
            StrategyCapitalBucket.user_id == user_id,
            StrategyCapitalBucket.strategy_id == STRATEGY_ID,
            StrategyCapitalBucket.trading_mode == trading_mode,
        )
    ).first()
    if bucket is None:
        return {}
    return {
        "assigned_capital_usd": Decimal(str(bucket.assigned_capital_cents)) / Decimal("100"),
        "available_capital_usd": Decimal(str(bucket.available_cash_cents)) / Decimal("100"),
    }


def _runtime_symbol_states(
    db: Session, *, user_id: uuid.UUID, trading_mode: str
) -> dict[str, dict[str, object]]:
    row = db.scalars(
        select(UserStrategyState).where(
            UserStrategyState.user_id == user_id,
            UserStrategyState.strategy_id == STRATEGY_ID,
            UserStrategyState.trading_mode == trading_mode,
        )
    ).first()
    state = dict((row.state or {}) if row else {})
    symbols = state.get("symbols")
    return dict(symbols) if isinstance(symbols, dict) else {}


def _position_states(
    db: Session, *, user_id: uuid.UUID, trading_mode: str
) -> dict[str, PositionState]:
    rows = db.scalars(
        select(ExecutionPosition).where(
            ExecutionPosition.user_id == user_id,
            ExecutionPosition.strategy_id == STRATEGY_ID,
            ExecutionPosition.trading_mode == trading_mode,
        )
    ).all()
    return {
        row.symbol: PositionState(
            symbol=row.symbol,
            quantity=_to_decimal(row.quantity),
            entry_price=_to_decimal(row.avg_entry_price),
            peak_price=None,
            opened_at=row.opened_at,
        )
        for row in rows
        if _to_decimal(row.quantity) > 0
    }


def _upsert_user_strategy(
    db: Session, *, user: User, enabled: bool, config: dict[str, Any]
) -> None:
    row = db.scalars(
        select(UserStrategy).where(
            UserStrategy.user_id == user.id,
            UserStrategy.strategy_id == STRATEGY_ID,
        )
    ).first()
    now = datetime.now(UTC)
    if row is None:
        row = UserStrategy(
            id=uuid.uuid4(),
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            is_enabled=enabled,
            config=config,
            metadata_json={"strategy_type": STRATEGY_ID},
            created_at=now,
            updated_at=now,
        )
        db.add(row)
        return
    row.is_enabled = enabled
    row.config = config
    row.updated_at = now


def get_config(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    ensure_platform_strategy_catalog(db)
    row = _config_row(db, user_id=user.id, trading_mode=trading_mode)
    admin_defaults = get_admin_defaults(db)
    config = _serialize_config(row, trading_mode=trading_mode)
    options, _ = _load_token_profiles(db, user=user)
    return {
        "config": config,
        "available_tokens": options,
        "admin_defaults": admin_defaults,
        "example_configs": {
            "AERO-USD": {
                **config,
                "selected_tokens": ["AERO-USD"],
                "total_allocated_amount_usd": {"target": 1000, "source": "manual"},
            }
        },
    }


def upsert_config(db: Session, *, user: User, payload: dict[str, Any]) -> dict[str, Any]:
    trading_mode = str(payload.get("trading_mode") or "paper").lower()
    config = normalize_strategy_config(payload)
    strategy = VolatilityHarvestStrategy()
    strategy.validate_config(config)
    allowed_options, _ = _load_token_profiles(db, user=user)
    allowed_symbols = {item["symbol"] for item in allowed_options}
    for symbol in selected_trade_symbols(config):
        if symbol not in allowed_symbols:
            raise VolatilityHarvestError(f"{symbol} is not enabled for volatility harvesting")
    capital = _capital_context(db, user_id=user.id, trading_mode=trading_mode)
    available = _to_decimal(capital.get("available_capital_usd"))
    requested = _to_decimal((config.get("total_allocated_amount_usd") or {}).get("target"))
    if available > 0 and requested > available:
        raise VolatilityHarvestError(
            "Target capital exceeds available cash for this strategy bucket"
        )
    now = datetime.now(UTC)
    row = _config_row(db, user_id=user.id, trading_mode=trading_mode)
    if row is None:
        row = VolatilityHarvestConfig(
            id=uuid.uuid4(),
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode=trading_mode,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    row.is_enabled = bool(config.get("enabled"))
    row.total_allocated_amount_usd = dict(config.get("total_allocated_amount_usd") or {})
    row.selected_tokens = list(config.get("selected_tokens") or [])
    row.core_position_percentage = str(config.get("core_position_percentage") or 70)
    row.trading_position_percentage = str(config.get("trading_position_percentage") or 30)
    row.entry_layers = list(config.get("entry_layers") or [])
    row.harvest_bands = list(config.get("harvest_bands") or [])
    row.rebuy_bands = list(config.get("rebuy_bands") or [])
    row.volatility_settings = dict(config.get("volatility_settings") or {})
    row.risk_controls = dict(config.get("risk_controls") or {})
    row.fee_settings = dict(config.get("fee_settings") or {})
    row.mode_settings = dict(config.get("mode_settings") or {})
    row.admin_overrides = get_admin_defaults(db)
    row.updated_at = now
    _upsert_user_strategy(db, user=user, enabled=row.is_enabled, config=config)
    db.commit()
    return get_config(db, user=user, trading_mode=trading_mode)


def set_enabled(db: Session, *, user: User, trading_mode: str, enabled: bool) -> dict[str, Any]:
    row = _config_row(db, user_id=user.id, trading_mode=trading_mode)
    if row is None:
        config = default_strategy_config()
        config["trading_mode"] = trading_mode
        config["enabled"] = enabled
        return upsert_config(db, user=user, payload=config)
    row.is_enabled = enabled
    row.updated_at = datetime.now(UTC)
    _upsert_user_strategy(
        db,
        user=user,
        enabled=enabled,
        config=_serialize_config(row, trading_mode=trading_mode),
    )
    db.commit()
    return {"strategy_id": STRATEGY_ID, "trading_mode": trading_mode, "enabled": enabled}


def _sync_snapshots(db: Session, *, user: User, trading_mode: str) -> None:
    config = _serialize_config(
        _config_row(db, user_id=user.id, trading_mode=trading_mode), trading_mode=trading_mode
    )
    symbols = selected_trade_symbols(config)
    options, profile_map = _load_token_profiles(db, user=user)
    market_map = _load_market_snapshots(db, list({*symbols, "BTC-USD"}))
    positions = _position_states(db, user_id=user.id, trading_mode=trading_mode)
    runtime_state = _runtime_symbol_states(db, user_id=user.id, trading_mode=trading_mode)
    capital_context = _capital_context(db, user_id=user.id, trading_mode=trading_mode)
    plan = build_volatility_harvest_plan(
        config=config,
        market_map=market_map,
        positions=positions,
        runtime_state=runtime_state,
        token_profiles=profile_map,
        capital_context=capital_context,
    )
    now = datetime.now(UTC)
    marks = _latest_symbol_marks(db, symbols)
    existing_rows = {
        row.symbol: row
        for row in db.scalars(
            select(VolatilityHarvestPosition).where(
                VolatilityHarvestPosition.user_id == user.id,
                VolatilityHarvestPosition.strategy_id == STRATEGY_ID,
                VolatilityHarvestPosition.trading_mode == trading_mode,
            )
        ).all()
    }
    symbol_contexts = dict(plan.get("symbol_contexts") or {})
    total_core = Decimal("0")
    total_trading = Decimal("0")
    total_realized = 0
    total_unrealized = 0
    total_harvested_cash = 0
    total_harvested_gains = 0
    total_accumulation_qty = Decimal("0")
    for symbol, ctx in symbol_contexts.items():
        row = existing_rows.get(symbol)
        if row is None:
            row = VolatilityHarvestPosition(
                id=uuid.uuid4(),
                user_id=user.id,
                strategy_id=STRATEGY_ID,
                trading_mode=trading_mode,
                symbol=symbol,
                created_at=now,
                updated_at=now,
            )
            db.add(row)
        mark = marks.get(symbol, Decimal("0"))
        core_qty = _to_decimal(
            runtime_state.get(symbol, {}).get("core_quantity") or ctx.get("core_quantity")
        )
        trading_qty = _to_decimal(
            runtime_state.get(symbol, {}).get("trading_quantity") or ctx.get("trading_quantity")
        )
        avg_core = _to_decimal(
            runtime_state.get(symbol, {}).get("avg_core_entry_price")
            or ctx.get("avg_core_entry_price")
        )
        avg_trading = _to_decimal(
            runtime_state.get(symbol, {}).get("avg_trading_entry_price")
            or ctx.get("avg_trading_entry_price")
        )
        harvested_cash = int(runtime_state.get(symbol, {}).get("harvested_cash_cents") or 0)
        realized = int(runtime_state.get(symbol, {}).get("total_harvested_gains_cents") or 0)
        unrealized = _cents(
            max(Decimal("0"), (mark - avg_core) * core_qty)
            + max(Decimal("0"), (mark - avg_trading) * trading_qty)
        )
        token_accumulation_qty = _to_decimal(
            runtime_state.get(symbol, {}).get("token_accumulation_quantity")
        )
        token_accumulation_pct = _to_decimal(
            runtime_state.get(symbol, {}).get("token_accumulation_pct")
        )
        row.core_quantity = str(core_qty)
        row.trading_quantity = str(trading_qty)
        row.avg_core_entry_price = str(avg_core)
        row.avg_trading_entry_price = str(avg_trading)
        row.harvested_cash_cents = harvested_cash
        row.realized_gains_cents = realized
        row.unrealized_gains_cents = unrealized
        row.total_harvested_gains_cents = realized
        row.token_accumulation_quantity = str(token_accumulation_qty)
        row.token_accumulation_pct = str(token_accumulation_pct)
        row.total_harvest_count = len(
            runtime_state.get(symbol, {}).get("completed_harvest_bands") or []
        )
        row.total_rebuy_count = len(
            runtime_state.get(symbol, {}).get("completed_rebuy_bands") or []
        )
        row.last_local_high = str(runtime_state.get(symbol, {}).get("last_local_high") or mark or 0)
        raw_last_harvest = runtime_state.get(symbol, {}).get("last_harvest_at")
        raw_last_rebuy = runtime_state.get(symbol, {}).get("last_rebuy_at")
        row.last_harvest_at = (
            datetime.fromisoformat(str(raw_last_harvest).replace("Z", "+00:00"))
            if raw_last_harvest
            else None
        )
        row.last_rebuy_at = (
            datetime.fromisoformat(str(raw_last_rebuy).replace("Z", "+00:00"))
            if raw_last_rebuy
            else None
        )
        row.last_action_at = row.last_rebuy_at or row.last_harvest_at
        row.metadata_json = {"plan": ctx}
        row.updated_at = now
        total_core += core_qty
        total_trading += trading_qty
        total_realized += realized
        total_unrealized += unrealized
        total_harvested_cash += harvested_cash
        total_harvested_gains += realized
        total_accumulation_qty += token_accumulation_qty

    existing_tx_order_ids = {
        row.order_id
        for row in db.scalars(
            select(VolatilityHarvestTransaction).where(
                VolatilityHarvestTransaction.user_id == user.id,
                VolatilityHarvestTransaction.strategy_id == STRATEGY_ID,
                VolatilityHarvestTransaction.trading_mode == trading_mode,
            )
        ).all()
        if row.order_id is not None
    }
    trade_rows = db.execute(
        select(ExecutionTradeRecord, ExecutionOrder)
        .join(ExecutionOrder, ExecutionOrder.id == ExecutionTradeRecord.order_id)
        .where(
            ExecutionTradeRecord.user_id == user.id,
            ExecutionTradeRecord.strategy_id == STRATEGY_ID,
            ExecutionTradeRecord.trading_mode == trading_mode,
        )
        .order_by(ExecutionTradeRecord.executed_at.asc())
    ).all()
    for trade, order in trade_rows:
        if order.id in existing_tx_order_ids:
            continue
        signal_metadata = dict(
            ((order.intent_payload or {}).get("metadata") or {}).get("signal_metadata") or {}
        )
        transaction = VolatilityHarvestTransaction(
            id=uuid.uuid4(),
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode=trading_mode,
            symbol=trade.symbol,
            order_id=order.id,
            transaction_type=str(
                signal_metadata.get("event_type")
                or ("harvest_sell" if trade.side == "sell" else "entry_buy")
            ),
            bucket_type=signal_metadata.get("bucket_type"),
            band_code=signal_metadata.get("band_code"),
            quantity=trade.quantity,
            price=trade.price,
            gross_notional_cents=trade.gross_notional_cents,
            fee_cents=trade.fee_cents,
            slippage_cents=int(
                (Decimal(trade.gross_notional_cents) * Decimal(order.estimated_slippage_bps))
                / Decimal("10000")
            ),
            net_profit_cents=trade.realized_pnl_cents - trade.fee_cents,
            harvested_cash_balance_cents=int(
                runtime_state.get(trade.symbol, {}).get("harvested_cash_cents") or 0
            ),
            token_quantity_after=trade.position_quantity_after,
            signal_id=((order.risk_payload or {}).get("signal_id")),
            correlation_id=order.correlation_id,
            metadata_json=signal_metadata or None,
            occurred_at=trade.executed_at,
            created_at=now,
        )
        db.add(transaction)

    transactions = db.scalars(
        select(VolatilityHarvestTransaction)
        .where(
            VolatilityHarvestTransaction.user_id == user.id,
            VolatilityHarvestTransaction.strategy_id == STRATEGY_ID,
            VolatilityHarvestTransaction.trading_mode == trading_mode,
        )
        .order_by(VolatilityHarvestTransaction.occurred_at.asc())
    ).all()
    cumulative = Decimal("0")
    accumulation_history: list[dict[str, Any]] = []
    for tx in transactions:
        cumulative += (
            _to_decimal(tx.token_quantity_after)
            if tx.transaction_type in {"rebuy_buy", "entry_buy"}
            else Decimal("0")
        )
        accumulation_history.append(
            {
                "timestamp": tx.occurred_at.isoformat(),
                "token_accumulation_quantity": float(cumulative),
                "harvested_gains_usd": tx.net_profit_cents / 100,
            }
        )

    metric = db.scalars(
        select(VolatilityHarvestMetric).where(
            VolatilityHarvestMetric.user_id == user.id,
            VolatilityHarvestMetric.strategy_id == STRATEGY_ID,
            VolatilityHarvestMetric.trading_mode == trading_mode,
        )
    ).first()
    if metric is None:
        metric = VolatilityHarvestMetric(
            id=uuid.uuid4(),
            user_id=user.id,
            strategy_id=STRATEGY_ID,
            trading_mode=trading_mode,
            created_at=now,
            updated_at=now,
        )
        db.add(metric)
    metric.harvested_cash_cents = total_harvested_cash
    metric.total_harvested_gains_cents = total_harvested_gains
    metric.realized_gains_cents = total_realized
    metric.unrealized_gains_cents = total_unrealized
    metric.total_core_quantity = str(total_core)
    metric.total_trading_quantity = str(total_trading)
    metric.total_token_accumulation_quantity = str(total_accumulation_qty)
    baseline = total_core + total_trading - total_accumulation_qty
    metric.total_token_accumulation_pct = str(
        ((total_accumulation_qty / baseline) * Decimal("100")) if baseline > 0 else Decimal("0")
    )
    metric.lifetime_harvest_count = sum(
        1 for tx in transactions if tx.transaction_type == "harvest_sell"
    )
    metric.lifetime_rebuy_count = sum(
        1 for tx in transactions if tx.transaction_type == "rebuy_buy"
    )
    metric.avg_rebuy_efficiency_pct = str(
        (
            (
                Decimal(
                    sum(
                        max(0, tx.net_profit_cents)
                        for tx in transactions
                        if tx.transaction_type == "rebuy_buy"
                    )
                )
                / Decimal(
                    max(
                        1,
                        sum(
                            tx.gross_notional_cents
                            for tx in transactions
                            if tx.transaction_type == "rebuy_buy"
                        ),
                    )
                )
            )
            * Decimal("100")
        ).quantize(Decimal("0.01"))
    )
    metric.accumulation_history = accumulation_history[-60:]
    metric.last_synced_at = now
    metric.updated_at = now
    db.commit()


def get_overview(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    _sync_snapshots(db, user=user, trading_mode=trading_mode)
    metric = db.scalars(
        select(VolatilityHarvestMetric).where(
            VolatilityHarvestMetric.user_id == user.id,
            VolatilityHarvestMetric.strategy_id == STRATEGY_ID,
            VolatilityHarvestMetric.trading_mode == trading_mode,
        )
    ).first()
    config = get_config(db, user=user, trading_mode=trading_mode)["config"]
    return {
        "strategy_id": STRATEGY_ID,
        "trading_mode": trading_mode,
        "enabled": bool(config.get("enabled")),
        "selected_tokens": config.get("selected_tokens") or [],
        "harvested_cash_cents": metric.harvested_cash_cents if metric else 0,
        "lifetime_harvested_gains_cents": metric.total_harvested_gains_cents if metric else 0,
        "realized_gains_cents": metric.realized_gains_cents if metric else 0,
        "unrealized_gains_cents": metric.unrealized_gains_cents if metric else 0,
        "token_accumulation_quantity": metric.total_token_accumulation_quantity if metric else "0",
        "token_accumulation_pct": metric.total_token_accumulation_pct if metric else "0",
        "lifetime_harvest_count": metric.lifetime_harvest_count if metric else 0,
        "lifetime_rebuy_count": metric.lifetime_rebuy_count if metric else 0,
        "avg_rebuy_efficiency_pct": metric.avg_rebuy_efficiency_pct if metric else "0",
        "admin_defaults": get_admin_defaults(db),
    }


def get_positions(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    _sync_snapshots(db, user=user, trading_mode=trading_mode)
    rows = db.scalars(
        select(VolatilityHarvestPosition)
        .where(
            VolatilityHarvestPosition.user_id == user.id,
            VolatilityHarvestPosition.strategy_id == STRATEGY_ID,
            VolatilityHarvestPosition.trading_mode == trading_mode,
        )
        .order_by(VolatilityHarvestPosition.symbol)
    ).all()
    return {
        "positions": [
            {
                "symbol": row.symbol,
                "core_quantity": row.core_quantity,
                "trading_quantity": row.trading_quantity,
                "avg_core_entry_price": row.avg_core_entry_price,
                "avg_trading_entry_price": row.avg_trading_entry_price,
                "harvested_cash_cents": row.harvested_cash_cents,
                "realized_gains_cents": row.realized_gains_cents,
                "unrealized_gains_cents": row.unrealized_gains_cents,
                "total_harvested_gains_cents": row.total_harvested_gains_cents,
                "token_accumulation_quantity": row.token_accumulation_quantity,
                "token_accumulation_pct": row.token_accumulation_pct,
                "last_harvest_at": row.last_harvest_at,
                "last_rebuy_at": row.last_rebuy_at,
            }
            for row in rows
        ]
    }


def get_harvest_activity(db: Session, *, user: User, trading_mode: str) -> list[dict[str, Any]]:
    _sync_snapshots(db, user=user, trading_mode=trading_mode)
    rows = db.scalars(
        select(VolatilityHarvestTransaction)
        .where(
            VolatilityHarvestTransaction.user_id == user.id,
            VolatilityHarvestTransaction.strategy_id == STRATEGY_ID,
            VolatilityHarvestTransaction.trading_mode == trading_mode,
            VolatilityHarvestTransaction.transaction_type == "harvest_sell",
        )
        .order_by(VolatilityHarvestTransaction.occurred_at.desc())
    ).all()
    return [
        {
            "id": row.id,
            "symbol": row.symbol,
            "transaction_type": row.transaction_type,
            "bucket_type": row.bucket_type,
            "band_code": row.band_code,
            "quantity": row.quantity,
            "price": row.price,
            "gross_notional_cents": row.gross_notional_cents,
            "fee_cents": row.fee_cents,
            "slippage_cents": row.slippage_cents,
            "net_profit_cents": row.net_profit_cents,
            "harvested_cash_balance_cents": row.harvested_cash_balance_cents,
            "token_quantity_after": row.token_quantity_after,
            "occurred_at": row.occurred_at,
            "metadata": row.metadata_json,
        }
        for row in rows
    ]


def get_rebuy_history(db: Session, *, user: User, trading_mode: str) -> list[dict[str, Any]]:
    _sync_snapshots(db, user=user, trading_mode=trading_mode)
    rows = db.scalars(
        select(VolatilityHarvestTransaction)
        .where(
            VolatilityHarvestTransaction.user_id == user.id,
            VolatilityHarvestTransaction.strategy_id == STRATEGY_ID,
            VolatilityHarvestTransaction.trading_mode == trading_mode,
            VolatilityHarvestTransaction.transaction_type == "rebuy_buy",
        )
        .order_by(VolatilityHarvestTransaction.occurred_at.desc())
    ).all()
    return [
        {
            "id": row.id,
            "symbol": row.symbol,
            "transaction_type": row.transaction_type,
            "bucket_type": row.bucket_type,
            "band_code": row.band_code,
            "quantity": row.quantity,
            "price": row.price,
            "gross_notional_cents": row.gross_notional_cents,
            "fee_cents": row.fee_cents,
            "slippage_cents": row.slippage_cents,
            "net_profit_cents": row.net_profit_cents,
            "harvested_cash_balance_cents": row.harvested_cash_balance_cents,
            "token_quantity_after": row.token_quantity_after,
            "occurred_at": row.occurred_at,
            "metadata": row.metadata_json,
        }
        for row in rows
    ]


def get_accumulation_chart(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    _sync_snapshots(db, user=user, trading_mode=trading_mode)
    metric = db.scalars(
        select(VolatilityHarvestMetric).where(
            VolatilityHarvestMetric.user_id == user.id,
            VolatilityHarvestMetric.strategy_id == STRATEGY_ID,
            VolatilityHarvestMetric.trading_mode == trading_mode,
        )
    ).first()
    return {"points": metric.accumulation_history if metric else []}


def get_metrics(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    _sync_snapshots(db, user=user, trading_mode=trading_mode)
    metric = db.scalars(
        select(VolatilityHarvestMetric).where(
            VolatilityHarvestMetric.user_id == user.id,
            VolatilityHarvestMetric.strategy_id == STRATEGY_ID,
            VolatilityHarvestMetric.trading_mode == trading_mode,
        )
    ).first()
    if metric is None:
        return {}
    return {
        "harvested_cash_cents": metric.harvested_cash_cents,
        "total_harvested_gains_cents": metric.total_harvested_gains_cents,
        "realized_gains_cents": metric.realized_gains_cents,
        "unrealized_gains_cents": metric.unrealized_gains_cents,
        "total_core_quantity": metric.total_core_quantity,
        "total_trading_quantity": metric.total_trading_quantity,
        "total_token_accumulation_quantity": metric.total_token_accumulation_quantity,
        "total_token_accumulation_pct": metric.total_token_accumulation_pct,
        "lifetime_harvest_count": metric.lifetime_harvest_count,
        "lifetime_rebuy_count": metric.lifetime_rebuy_count,
        "avg_rebuy_efficiency_pct": metric.avg_rebuy_efficiency_pct,
        "accumulation_history": metric.accumulation_history,
        "last_synced_at": metric.last_synced_at,
    }


def build_cycle_preview(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    config = _serialize_config(
        _config_row(db, user_id=user.id, trading_mode=trading_mode), trading_mode=trading_mode
    )
    strategy = VolatilityHarvestStrategy()
    strategy.validate_config(config)
    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        raise VolatilityHarvestError("Tenant scope is required for volatility harvest preview")
    _, profile_map = _load_token_profiles(db, user=user)
    symbols = list({*selected_trade_symbols(config), "BTC-USD"})
    market_map = _load_market_snapshots(db, symbols)
    runtime_state = _runtime_symbol_states(db, user_id=user.id, trading_mode=trading_mode)
    positions = _position_states(db, user_id=user.id, trading_mode=trading_mode)
    plan = build_volatility_harvest_plan(
        config=config,
        market_map=market_map,
        positions=positions,
        runtime_state=runtime_state,
        token_profiles=profile_map,
        capital_context=_capital_context(db, user_id=user.id, trading_mode=trading_mode),
    )
    actions: list[dict[str, Any]] = []
    for symbol in selected_trade_symbols(config):
        market = market_map.get(symbol)
        if market is None:
            continue
        signal = strategy.generate_signal(
            StrategyContext(
                tenant_id=tenant_id,
                trading_mode=TradingMode(trading_mode),
                market_snapshot=market,
                position_state=positions.get(symbol) or PositionState(symbol=symbol),
                volatility_harvest=plan.get("symbol_contexts", {}).get(symbol, {}),
                runtime_symbol_state=runtime_state.get(symbol, {}),
            ),
            config,
            signal_id=uuid.uuid4(),
            correlation_id=uuid.uuid4(),
        )
        if signal.signal_type == SignalType.HOLD:
            continue
        actions.append(
            {
                "symbol": symbol,
                "action": signal.signal_type.value,
                "reason": signal.reason,
                "quantity": float(_to_decimal(signal.quantity.amount if signal.quantity else 0)),
                "metadata": signal.metadata or {},
            }
        )
    return {
        "strategy_id": STRATEGY_ID,
        "trading_mode": trading_mode,
        "config": config,
        "market_regime": plan.get("market_regime") or {},
        "actions": actions,
    }


def _bind_engine(db: Session) -> Engine:
    bind = db.get_bind()
    if isinstance(bind, Engine):
        return bind
    if isinstance(bind, Connection):
        return bind.engine
    raise VolatilityHarvestError("Database engine unavailable")


def _enqueue_signal_generated(engine: Engine, event: StrategySignalEvent) -> None:
    payload = {
        "signal": strategy_signal_to_json(event),
        "trace_id": f"vh-cycle-{event.signal_id}",
    }
    queue_name = (
        QueueNames.signal_generated_strategy(event.trading_mode, STRATEGY_ID)
        if STRATEGY_ID in QueueNames.DEDICATED_SIGNAL_STRATEGIES
        else QueueNames.signal_generated(event.trading_mode)
    )
    try:
        enqueue_worker_payload(engine, queue_name, payload)
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
                "queue_name": queue_name,
                "payload": str(payload),
                "created_at": now,
                "updated_at": now,
            },
        )


def execute_cycle(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    preview = build_cycle_preview(db, user=user, trading_mode=trading_mode)
    if not preview["actions"]:
        return {
            "strategy_id": STRATEGY_ID,
            "trading_mode": trading_mode,
            "queued": [],
            "queued_count": 0,
        }
    engine = _bind_engine(db)
    now = datetime.now(UTC)
    run_id = uuid.uuid4()
    queued: list[dict[str, Any]] = []
    for action in preview["actions"]:
        signal_id = uuid.uuid4()
        symbol = str(action["symbol"]).upper()
        quantity = _to_decimal(action["quantity"])
        event = StrategySignalEvent(
            signal_id=signal_id,
            run_id=run_id,
            user_id=user.id,
            strategy_name=STRATEGY_ID,
            symbol=symbol,
            action=SignalType(action["action"]),
            confidence=0.72,
            suggested_size=max(quantity, Decimal("0.00000001")),
            reasoning_metadata={
                "reason": action["reason"],
                "reason_code": normalize_reason_code(
                    "volatility_harvest", reason_detail=action["reason"]
                ),
                "signal_metadata": dict(action.get("metadata") or {}),
            },
            trading_mode=TradingMode(trading_mode),
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
                "reason": action["reason"],
                "suggested_size": float(event.suggested_size),
            }
        )
    db.commit()
    return {
        "strategy_id": STRATEGY_ID,
        "trading_mode": trading_mode,
        "queued": queued,
        "queued_count": len(queued),
    }


def backtest_preview(db: Session, *, user: User, trading_mode: str) -> dict[str, Any]:
    preview = build_cycle_preview(db, user=user, trading_mode=trading_mode)
    return {
        "strategy_id": STRATEGY_ID,
        "trading_mode": trading_mode,
        "selected_symbols": sorted(selected_trade_symbols(preview.get("config") or {})),
        "market_regime": preview.get("market_regime") or {},
        "actions": preview["actions"],
        "note": "Preview only. No trades were executed.",
    }

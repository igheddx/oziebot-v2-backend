from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text

from oziebot_domain.risk import RiskOutcome
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.strategy import SignalType
from oziebot_domain.trading_mode import TradingMode
from oziebot_risk_engine.config import Settings
from oziebot_risk_engine.service import RiskEngineService


class FakeRedis:
    def __init__(self):
        self._kv: dict[str, str] = {}
        self._lists: dict[str, list[str]] = {}

    def get(self, key: str):
        return self._kv.get(key)

    def set(self, key: str, value: str):
        self._kv[key] = value

    def lpush(self, key: str, value: str):
        self._lists.setdefault(key, []).insert(0, value)


def _setup_db(db_path: Path) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text("CREATE TABLE users (id TEXT PRIMARY KEY, is_active BOOLEAN NOT NULL)")
        )
        conn.execute(
            text(
                "CREATE TABLE tenant_memberships ("
                "id TEXT PRIMARY KEY, user_id TEXT, tenant_id TEXT, role TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE tenant_entitlements ("
                "id TEXT PRIMARY KEY, tenant_id TEXT, platform_strategy_id TEXT, source TEXT, "
                "valid_from TEXT, valid_until TEXT, is_active BOOLEAN, created_at TEXT, updated_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE platform_strategies ("
                "id TEXT PRIMARY KEY, slug TEXT, display_name TEXT, is_enabled BOOLEAN, created_at TEXT, updated_at TEXT, config_schema TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE platform_settings (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT, updated_by_user_id TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE user_strategies (id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, is_enabled BOOLEAN, config TEXT, created_at TEXT, updated_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE platform_token_allowlist (id TEXT PRIMARY KEY, symbol TEXT, quote_currency TEXT, is_enabled BOOLEAN)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE token_strategy_policy ("
                "id TEXT PRIMARY KEY, token_id TEXT, strategy_id TEXT, admin_enabled BOOLEAN, suitability_score REAL,"
                "recommendation_status TEXT, recommendation_reason TEXT, recommendation_status_override TEXT,"
                "recommendation_reason_override TEXT, max_position_pct_override REAL, notes TEXT,"
                "computed_at TEXT, updated_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE user_token_permissions (id TEXT PRIMARY KEY, user_id TEXT, platform_token_id TEXT, is_enabled BOOLEAN)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE strategy_capital_buckets ("
                "id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, trading_mode TEXT,"
                "assigned_capital_cents INTEGER, available_buying_power_cents INTEGER, locked_capital_cents INTEGER)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE execution_positions ("
                "id TEXT PRIMARY KEY, tenant_id TEXT, user_id TEXT, strategy_id TEXT, symbol TEXT, trading_mode TEXT,"
                "quantity TEXT, avg_entry_price TEXT, realized_pnl_cents INTEGER, created_at TEXT, updated_at TEXT, last_trade_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE strategy_capital_ledger ("
                "id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, trading_mode TEXT, event_type TEXT, metadata TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE risk_events ("
                "id TEXT PRIMARY KEY, signal_id TEXT, run_id TEXT, user_id TEXT, strategy_name TEXT, symbol TEXT,"
                "trading_mode TEXT, outcome TEXT, reason TEXT, detail TEXT, original_size TEXT, final_size TEXT,"
                "trace_id TEXT, rules_evaluated TEXT, signal_payload TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE user_strategy_states ("
                "id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, trading_mode TEXT, state TEXT, created_at TEXT, updated_at TEXT,"
                "UNIQUE(user_id, strategy_id, trading_mode))"
            )
        )


def _seed_common(
    db_path: Path, user_id: str, tenant_id: str, strategy_name: str = "momentum"
) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    now = datetime.now(UTC).isoformat()
    with eng.begin() as conn:
        conn.execute(
            text("INSERT INTO users (id, is_active) VALUES (:id, 1)"), {"id": user_id}
        )
        conn.execute(
            text(
                "INSERT INTO tenant_memberships (id, user_id, tenant_id, role, created_at) VALUES (:id,:u,:t,'user',:c)"
            ),
            {"id": str(uuid4()), "u": user_id, "t": tenant_id, "c": now},
        )
        conn.execute(
            text(
                "INSERT INTO platform_strategies (id, slug, display_name, is_enabled, created_at, updated_at, config_schema) "
                "VALUES (:id,:slug,:slug,1,:c,:c,'{}')"
            ),
            {"id": str(uuid4()), "slug": strategy_name, "c": now},
        )
        conn.execute(
            text(
                "INSERT INTO tenant_entitlements (id, tenant_id, platform_strategy_id, source, valid_from, valid_until, is_active, created_at, updated_at) "
                "VALUES (:id,:t,NULL,'test',:vf,NULL,1,:c,:c)"
            ),
            {"id": str(uuid4()), "t": tenant_id, "vf": now, "c": now},
        )
        conn.execute(
            text(
                "INSERT INTO user_strategies (id, user_id, strategy_id, is_enabled, config, created_at, updated_at) "
                "VALUES (:id,:u,:s,1,'{}',:c,:c)"
            ),
            {"id": str(uuid4()), "u": user_id, "s": strategy_name, "c": now},
        )
        token_id = str(uuid4())
        conn.execute(
            text(
                "INSERT INTO platform_token_allowlist (id, symbol, quote_currency, is_enabled) VALUES (:id,'BTC-USD','USD',1)"
            ),
            {"id": token_id},
        )
        conn.execute(
            text(
                "INSERT INTO user_token_permissions (id, user_id, platform_token_id, is_enabled) VALUES (:id,:u,:t,1)"
            ),
            {"id": str(uuid4()), "u": user_id, "t": token_id},
        )
        conn.execute(
            text(
                "INSERT INTO strategy_capital_buckets (id, user_id, strategy_id, trading_mode, assigned_capital_cents, available_buying_power_cents, locked_capital_cents) "
                "VALUES (:id,:u,:s,'live',200000,100000,0),(:id2,:u,:s,'paper',200000,100000,0)"
            ),
            {"id": str(uuid4()), "id2": str(uuid4()), "u": user_id, "s": strategy_name},
        )


def _insert_token_policy(
    db_path: Path,
    *,
    strategy_name: str,
    status: str = "allowed",
    admin_enabled: bool = True,
    max_position_pct_override: float | None = None,
) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    now = datetime.now(UTC).isoformat()
    with eng.begin() as conn:
        token_id = conn.execute(
            text(
                "SELECT id FROM platform_token_allowlist WHERE symbol = 'BTC-USD' LIMIT 1"
            )
        ).first()
        assert token_id is not None
        conn.execute(
            text(
                "INSERT INTO token_strategy_policy ("
                "id, token_id, strategy_id, admin_enabled, suitability_score, recommendation_status,"
                "recommendation_reason, recommendation_status_override, recommendation_reason_override,"
                "max_position_pct_override, notes, computed_at, updated_at"
                ") VALUES (:id,:token_id,:strategy_id,:admin_enabled,80,:status,:reason,NULL,NULL,:max_position_pct_override,NULL,:computed_at,:updated_at)"
            ),
            {
                "id": str(uuid4()),
                "token_id": token_id[0],
                "strategy_id": strategy_name,
                "admin_enabled": 1 if admin_enabled else 0,
                "status": status,
                "reason": f"{strategy_name}:{status}",
                "max_position_pct_override": max_position_pct_override,
                "computed_at": now,
                "updated_at": now,
            },
        )


def _signal(
    user_id: str,
    strategy_name: str = "momentum",
    mode: TradingMode = TradingMode.LIVE,
    size: str = "0.5",
) -> StrategySignalEvent:
    return StrategySignalEvent(
        signal_id=uuid4(),
        run_id=uuid4(),
        user_id=user_id,
        strategy_name=strategy_name,
        symbol="BTC-USD",
        action=SignalType.BUY,
        confidence=0.9,
        suggested_size=Decimal(size),
        reasoning_metadata={"reason": "test"},
        trading_mode=mode,
        timestamp=datetime.now(UTC),
    )


def _hold_signal(
    user_id: str, strategy_name: str = "momentum", mode: TradingMode = TradingMode.LIVE
) -> StrategySignalEvent:
    return StrategySignalEvent(
        signal_id=uuid4(),
        run_id=uuid4(),
        user_id=user_id,
        strategy_name=strategy_name,
        symbol="BTC-USD",
        action=SignalType.HOLD,
        confidence=0.5,
        suggested_size=Decimal("0"),
        reasoning_metadata={"reason": "hold"},
        trading_mode=mode,
        timestamp=datetime.now(UTC),
    )


def _redis_with_fresh_market() -> FakeRedis:
    r = FakeRedis()
    now = datetime.now(UTC).isoformat()
    r.set("oziebot:md:last_update:trade:BTC-USD", now)
    r.set("oziebot:md:last_update:bbo:BTC-USD", now)
    r.set("oziebot:md:last_update:candle:BTC-USD", now)
    r.set(
        "oziebot:md:bbo:BTC-USD",
        '{"best_bid_price":"50000","best_bid_size":"2","best_ask_price":"50010","best_ask_size":"2"}',
    )
    return r


def _redis_with_stale_market(
    *, trade_age_seconds: int, bbo_age_seconds: int, candle_age_seconds: int
) -> FakeRedis:
    r = _redis_with_fresh_market()
    now = datetime.now(UTC)
    r.set(
        "oziebot:md:last_update:trade:BTC-USD",
        (now - timedelta(seconds=trade_age_seconds)).isoformat(),
    )
    r.set(
        "oziebot:md:last_update:bbo:BTC-USD",
        (now - timedelta(seconds=bbo_age_seconds)).isoformat(),
    )
    r.set(
        "oziebot:md:last_update:candle:BTC-USD",
        (now - timedelta(seconds=candle_age_seconds)).isoformat(),
    )
    return r


def test_risk_approves_live_signal(tmp_path: Path):
    db_path = tmp_path / "risk1.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    decision, intent = svc.evaluate(_signal(user_id), trace_id="t1")
    assert decision.outcome in (RiskOutcome.APPROVE, RiskOutcome.REDUCE_SIZE)
    assert decision.trading_mode == TradingMode.LIVE
    assert intent is not None


def test_risk_rejects_when_platform_paused(tmp_path: Path):
    db_path = tmp_path / "risk2.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO platform_settings (key, value, updated_at, updated_by_user_id) VALUES ('trading.global.pause', '{\"paused\": true}', :u, NULL)"
            ),
            {"u": datetime.now(UTC).isoformat()},
        )

    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    decision, intent = svc.evaluate(_signal(user_id), trace_id="t2")
    assert decision.outcome == RiskOutcome.REJECT
    assert intent is None


def test_risk_reduces_size_by_buying_power(tmp_path: Path):
    db_path = tmp_path / "risk3.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    decision, intent = svc.evaluate(_signal(user_id, size="10"), trace_id="t3")
    assert decision.outcome in (RiskOutcome.REDUCE_SIZE, RiskOutcome.REJECT)
    if decision.outcome == RiskOutcome.REDUCE_SIZE:
        assert Decimal(decision.final_size) < Decimal(decision.original_size)
        assert intent is not None


def test_risk_approves_hold_without_trade_intent(tmp_path: Path):
    db_path = tmp_path / "risk-hold.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    decision, intent = svc.evaluate(_hold_signal(user_id), trace_id="t-hold")
    assert decision.outcome == RiskOutcome.APPROVE
    assert decision.approved is True
    assert intent is None


def test_risk_rejects_trade_when_fee_drag_exceeds_expected_edge(tmp_path: Path):
    db_path = tmp_path / "risk-fee.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(settings, _redis_with_fresh_market())
    signal = _signal(user_id).model_copy(
        update={
            "reasoning_metadata": {
                "reason": "thin edge",
                "fee_economics": {
                    "expected_gross_edge_bps": 20,
                    "estimated_fee_bps": 10,
                    "estimated_slippage_bps": 20,
                    "estimated_total_cost_bps": 40,
                    "expected_net_edge_bps": -20,
                    "execution_preference": "maker_preferred",
                    "fallback_behavior": "convert_to_taker",
                    "maker_timeout_seconds": 15,
                    "limit_price_offset_bps": 2,
                    "min_notional_per_trade": "25",
                    "min_expected_edge_bps": 25,
                    "min_expected_net_profit_dollars": "0.5",
                    "max_fee_percent_of_expected_profit": "0.65",
                    "max_slippage_bps": 35,
                    "skip_trade_if_fee_too_high": True,
                },
            }
        }
    )

    decision, intent = svc.evaluate(signal, trace_id="fee-risk")

    assert decision.outcome == RiskOutcome.REJECT
    assert decision.detail is not None
    assert decision.detail.startswith("fee_economics:")
    assert intent is None


def test_paper_can_trade_without_entitlement_when_allowed(tmp_path: Path):
    db_path = tmp_path / "risk-paper-entitlement.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text("DELETE FROM tenant_entitlements WHERE tenant_id = :tenant_id"),
            {"tenant_id": tenant_id},
        )

    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    paper_decision, paper_intent = svc.evaluate(
        _signal(user_id, mode=TradingMode.PAPER), trace_id="t-paper-entitled"
    )
    live_decision, live_intent = svc.evaluate(
        _signal(user_id, mode=TradingMode.LIVE), trace_id="t-live-entitled"
    )

    assert paper_decision.outcome in (RiskOutcome.APPROVE, RiskOutcome.REDUCE_SIZE)
    assert paper_intent is not None
    assert live_decision.outcome == RiskOutcome.REJECT
    assert live_intent is None


def test_paper_can_relax_daily_loss_but_live_rejects(tmp_path: Path):
    db_path = tmp_path / "risk4.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    now = datetime.now(UTC).isoformat()
    with eng.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO strategy_capital_ledger (id, user_id, strategy_id, trading_mode, event_type, metadata, created_at) "
                "VALUES (:id,:u,'momentum','live','settle',:m,:c),(:id2,:u,'momentum','paper','settle',:m,:c)"
            ),
            {
                "id": str(uuid4()),
                "id2": str(uuid4()),
                "u": user_id,
                "m": '{"realized_pnl_delta_cents": -100000}',
                "c": now,
            },
        )

    settings = Settings(
        database_url=f"sqlite+pysqlite:///{db_path}",
        risk_max_daily_loss_cents=1000,
        risk_relaxed_paper_rules="max_daily_loss,cooldown_after_losses",
    )
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    live_decision, _ = svc.evaluate(
        _signal(user_id, mode=TradingMode.LIVE), trace_id="t4-live"
    )
    paper_decision, _ = svc.evaluate(
        _signal(user_id, mode=TradingMode.PAPER), trace_id="t4-paper"
    )

    assert live_decision.outcome == RiskOutcome.REJECT
    assert paper_decision.outcome in (RiskOutcome.APPROVE, RiskOutcome.REDUCE_SIZE)


def test_risk_rejects_spread_from_strategy_quality_controls(tmp_path: Path):
    db_path = tmp_path / "risk-spread.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text(
                "UPDATE platform_strategies SET config_schema = :config WHERE slug = 'momentum'"
            ),
            {
                "config": json.dumps(
                    {
                        "strategy_params": {
                            "max_spread_pct": 0.001,
                            "max_slippage_pct": 0.005,
                        }
                    }
                )
            },
        )

    redis = _redis_with_fresh_market()
    redis.set(
        "oziebot:md:bbo:BTC-USD",
        '{"best_bid_price":"50000","best_bid_size":"2","best_ask_price":"50120","best_ask_size":"2"}',
    )
    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(settings, redis)

    decision, intent = svc.evaluate(_signal(user_id, size="0.01"), trace_id="t-spread")

    assert decision.outcome == RiskOutcome.REJECT
    assert intent is None
    assert "Spread too wide" in (decision.detail or "")


def test_risk_rejects_after_consecutive_strategy_losses(tmp_path: Path):
    db_path = tmp_path / "risk-cooldown.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    now = datetime.now(UTC).isoformat()
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text(
                "UPDATE platform_strategies SET config_schema = :config WHERE slug = 'momentum'"
            ),
            {
                "config": json.dumps(
                    {
                        "risk_caps": {
                            "max_consecutive_losses": 2,
                            "loss_cooldown_minutes": 120,
                        }
                    }
                )
            },
        )
        conn.execute(
            text(
                "INSERT INTO strategy_capital_ledger (id, user_id, strategy_id, trading_mode, event_type, metadata, created_at) "
                "VALUES (:id,:u,'momentum','live','settle',:m,:c),(:id2,:u,'momentum','live','settle',:m2,:c)"
            ),
            {
                "id": str(uuid4()),
                "id2": str(uuid4()),
                "u": user_id,
                "m": '{"realized_pnl_delta_cents": -5000}',
                "m2": '{"realized_pnl_delta_cents": -2500}',
                "c": now,
            },
        )

    settings = Settings(
        database_url=f"sqlite+pysqlite:///{db_path}",
        risk_max_daily_loss_cents=100_000_000,
    )
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    decision, intent = svc.evaluate(
        _signal(user_id, size="0.01"), trace_id="t-cooldown"
    )

    assert decision.outcome == RiskOutcome.REJECT
    assert intent is None
    assert "Cooldown active" in (decision.detail or "")


def test_risk_rejects_when_global_daily_loss_guard_triggered(tmp_path: Path):
    db_path = tmp_path / "risk-global-guard.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    now = datetime.now(UTC).isoformat()
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO platform_settings (key, value, updated_at, updated_by_user_id) "
                "VALUES ('trading.global.daily_loss_guard', :value, :updated_at, NULL)"
            ),
            {
                "value": '{"enabled": true, "daily_loss_pct": 5}',
                "updated_at": now,
            },
        )
        conn.execute(
            text(
                "INSERT INTO strategy_capital_ledger (id, user_id, strategy_id, trading_mode, event_type, metadata, created_at) "
                "VALUES (:id,:u,'momentum','live','settle',:m,:c)"
            ),
            {
                "id": str(uuid4()),
                "u": user_id,
                "m": '{"realized_pnl_delta_cents": -20000}',
                "c": now,
            },
        )

    redis = _redis_with_fresh_market()
    settings = Settings(
        database_url=f"sqlite+pysqlite:///{db_path}",
        risk_max_daily_loss_cents=100_000_000,
    )
    svc = RiskEngineService(settings, redis)

    decision, intent = svc.evaluate(
        _signal(user_id, size="0.01"), trace_id="t-global-guard"
    )

    assert decision.outcome == RiskOutcome.REJECT
    assert intent is None
    assert "Global daily loss guard active" in (decision.detail or "")
    assert redis._lists


def test_risk_rejects_blocked_token_strategy_policy(tmp_path: Path):
    db_path = tmp_path / "risk-token-policy-blocked.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)
    _insert_token_policy(db_path, strategy_name="momentum", status="blocked")

    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    decision, intent = svc.evaluate(_signal(user_id), trace_id="t-token-policy-blocked")

    assert decision.outcome == RiskOutcome.REJECT
    assert intent is None
    assert "token_strategy_policy" in (decision.detail or "")


def test_risk_reduces_size_for_discouraged_token_policy(tmp_path: Path):
    db_path = tmp_path / "risk-token-policy-discouraged.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)
    _insert_token_policy(db_path, strategy_name="momentum", status="discouraged")

    settings = Settings(
        database_url=f"sqlite+pysqlite:///{db_path}",
        risk_max_position_size_cents=1_000_000_000,
        risk_max_per_trade_risk_pct=1.0,
        risk_max_strategy_allocation_pct=1.0,
        risk_max_token_concentration_pct=1.0,
    )
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    decision, intent = svc.evaluate(
        _signal(user_id, size="0.02"), trace_id="t-token-policy-discouraged"
    )

    assert decision.outcome == RiskOutcome.REDUCE_SIZE
    assert intent is not None
    assert Decimal(decision.final_size) == Decimal("0.01200000")


def test_risk_applies_max_position_pct_override(tmp_path: Path):
    db_path = tmp_path / "risk-token-policy-cap.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)
    _insert_token_policy(
        db_path,
        strategy_name="momentum",
        status="allowed",
        max_position_pct_override=0.10,
    )

    settings = Settings(
        database_url=f"sqlite+pysqlite:///{db_path}",
        risk_max_position_size_cents=1_000_000_000,
        risk_max_per_trade_risk_pct=1.0,
        risk_max_strategy_allocation_pct=1.0,
        risk_max_token_concentration_pct=1.0,
    )
    svc = RiskEngineService(settings, _redis_with_fresh_market())

    decision, intent = svc.evaluate(
        _signal(user_id, size="0.1"), trace_id="t-token-policy-cap"
    )

    assert decision.outcome == RiskOutcome.REDUCE_SIZE
    assert intent is not None
    assert Decimal(decision.final_size) == Decimal("0.00399960")


def test_stale_data_degrades_signal_without_full_rejection(tmp_path: Path):
    db_path = tmp_path / "risk-stale-degraded.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    settings = Settings(
        database_url=f"sqlite+pysqlite:///{db_path}",
        risk_stale_degraded_confidence_multiplier=0.75,
        risk_max_per_trade_risk_pct=1.0,
        risk_max_position_size_cents=1_000_000_000,
        risk_max_strategy_allocation_pct=1.0,
        risk_max_token_concentration_pct=1.0,
    )
    svc = RiskEngineService(
        settings,
        _redis_with_stale_market(
            trade_age_seconds=25,
            bbo_age_seconds=10,
            candle_age_seconds=90,
        ),
    )

    decision, intent = svc.evaluate(
        _signal(user_id, size="0.01"), trace_id="t-stale-degraded"
    )

    assert decision.outcome == RiskOutcome.APPROVE
    assert intent is not None
    assert Decimal(decision.original_size) == Decimal("0.00750000")
    assert svc.metrics_snapshot()["signals_rejected"] == 0


def test_critical_stale_data_still_rejects(tmp_path: Path):
    db_path = tmp_path / "risk-stale-critical.sqlite"
    _setup_db(db_path)
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    _seed_common(db_path, user_id, tenant_id)

    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    svc = RiskEngineService(
        settings,
        _redis_with_stale_market(
            trade_age_seconds=10,
            bbo_age_seconds=61,
            candle_age_seconds=90,
        ),
    )

    decision, intent = svc.evaluate(
        _signal(user_id, size="0.1"), trace_id="t-stale-critical"
    )

    assert decision.outcome == RiskOutcome.REJECT
    assert intent is None
    assert "Critically stale market data" in (decision.detail or "")
    assert svc.metrics_snapshot()["signals_rejected"] == 1

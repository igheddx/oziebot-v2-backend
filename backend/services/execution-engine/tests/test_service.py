from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text

from oziebot_domain.execution import (
    ExecutionOrderStatus,
    ExecutionRequest,
    ExecutionSubmission,
)
from oziebot_domain.risk import RiskDecision, RiskOutcome
from oziebot_domain.trading import OrderType, Side, Venue
from oziebot_domain.trading_mode import TradingMode
from oziebot_execution_engine.adapters import (
    LiveCoinbaseExecutionAdapter,
    PaperExecutionAdapter,
)
from oziebot_execution_engine.config import Settings
from oziebot_execution_engine.service import ExecutionService
from oziebot_execution_engine.state_machine import ensure_transition


class FakeRedis:
    def __init__(self) -> None:
        self._kv: dict[str, str] = {}
        self._lists: dict[str, list[str]] = {}

    def get(self, key: str):
        return self._kv.get(key)

    def set(self, key: str, value: str) -> None:
        self._kv[key] = value

    def lpush(self, key: str, value: str) -> None:
        self._lists.setdefault(key, []).insert(0, value)

    def list_len(self, key: str) -> int:
        return len(self._lists.get(key, []))


class FakeLiveClient:
    def __init__(self, submission: ExecutionSubmission) -> None:
        self._submission = submission
        self.place_calls = 0

    def place_order(
        self, request: ExecutionRequest, *, api_key_name: str, private_key_pem: str
    ) -> ExecutionSubmission:
        self.place_calls += 1
        return self._submission

    def cancel_order(
        self, venue_order_id: str, *, api_key_name: str, private_key_pem: str
    ) -> dict:
        return {"cancelled": [venue_order_id]}


def _compact_id(value: str) -> str:
    return value.replace("-", "")


def _setup_db(db_path: Path) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(
            text("CREATE TABLE users (id TEXT PRIMARY KEY, is_active BOOLEAN NOT NULL)")
        )
        conn.execute(text("CREATE TABLE tenants (id TEXT PRIMARY KEY, slug TEXT)"))
        conn.execute(
            text(
                "CREATE TABLE strategy_capital_buckets ("
                "id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, trading_mode TEXT,"
                "assigned_capital_cents INTEGER, available_cash_cents INTEGER, reserved_cash_cents INTEGER,"
                "locked_capital_cents INTEGER, realized_pnl_cents INTEGER, unrealized_pnl_cents INTEGER,"
                "available_buying_power_cents INTEGER, version INTEGER, created_at TEXT, updated_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE execution_orders ("
                "id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, correlation_id TEXT NOT NULL, tenant_id TEXT NOT NULL,"
                "user_id TEXT NOT NULL, strategy_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,"
                "order_type TEXT NOT NULL, trading_mode TEXT NOT NULL, venue TEXT NOT NULL, state TEXT NOT NULL,"
                "quantity TEXT NOT NULL, requested_notional_cents INTEGER NOT NULL, reserved_cash_cents INTEGER NOT NULL,"
                "locked_cash_cents INTEGER NOT NULL, filled_quantity TEXT NOT NULL, avg_fill_price TEXT, fees_cents INTEGER NOT NULL,"
                "idempotency_key TEXT NOT NULL UNIQUE, client_order_id TEXT NOT NULL UNIQUE, venue_order_id TEXT,"
                "failure_code TEXT, failure_detail TEXT, trace_id TEXT NOT NULL, intent_payload TEXT NOT NULL, risk_payload TEXT NOT NULL,"
                "adapter_payload TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, submitted_at TEXT, completed_at TEXT,"
                "cancelled_at TEXT, failed_at TEXT, UNIQUE(intent_id, trading_mode))"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE execution_fills ("
                "id TEXT PRIMARY KEY, order_id TEXT NOT NULL, venue_fill_id TEXT NOT NULL, fill_sequence INTEGER NOT NULL,"
                "quantity TEXT NOT NULL, price TEXT NOT NULL, gross_notional_cents INTEGER NOT NULL, fee_cents INTEGER NOT NULL,"
                "liquidity TEXT, raw_payload TEXT, filled_at TEXT NOT NULL, UNIQUE(order_id, venue_fill_id))"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE execution_trades ("
                "id TEXT PRIMARY KEY, order_id TEXT NOT NULL, fill_id TEXT, tenant_id TEXT NOT NULL, user_id TEXT NOT NULL,"
                "strategy_id TEXT NOT NULL, symbol TEXT NOT NULL, trading_mode TEXT NOT NULL, side TEXT NOT NULL,"
                "quantity TEXT NOT NULL, price TEXT NOT NULL, gross_notional_cents INTEGER NOT NULL, fee_cents INTEGER NOT NULL,"
                "realized_pnl_cents INTEGER NOT NULL, position_quantity_after TEXT NOT NULL, avg_entry_price_after TEXT NOT NULL,"
                "executed_at TEXT NOT NULL, raw_payload TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE execution_positions ("
                "id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, user_id TEXT NOT NULL, strategy_id TEXT NOT NULL, symbol TEXT NOT NULL,"
                "trading_mode TEXT NOT NULL, quantity TEXT NOT NULL, avg_entry_price TEXT NOT NULL, realized_pnl_cents INTEGER NOT NULL,"
                "created_at TEXT NOT NULL, updated_at TEXT NOT NULL, last_trade_at TEXT,"
                "UNIQUE(tenant_id, user_id, strategy_id, symbol, trading_mode))"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE platform_strategies ("
                "id TEXT PRIMARY KEY, slug TEXT, config_schema TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE platform_token_allowlist ("
                "id TEXT PRIMARY KEY, symbol TEXT, quote_currency TEXT, is_enabled BOOLEAN)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE token_strategy_policy ("
                "id TEXT PRIMARY KEY, token_id TEXT, strategy_id TEXT, admin_enabled BOOLEAN,"
                "suitability_score REAL, recommendation_status TEXT, recommendation_reason TEXT,"
                "recommendation_status_override TEXT, recommendation_reason_override TEXT,"
                "max_position_pct_override REAL, notes TEXT, computed_at TEXT, updated_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE user_strategies ("
                "id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, is_enabled BOOLEAN, config TEXT, created_at TEXT, updated_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE user_strategy_states ("
                "id TEXT PRIMARY KEY, user_id TEXT, strategy_id TEXT, trading_mode TEXT, state TEXT, created_at TEXT, updated_at TEXT,"
                "UNIQUE(user_id, strategy_id, trading_mode))"
            )
        )


def _seed_bucket(
    db_path: Path,
    user_id: str,
    tenant_id: str,
    strategy_id: str,
    trading_mode: str,
    available_cash_cents: int = 3_000_000,
) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    now = datetime.now(UTC).isoformat()
    with eng.begin() as conn:
        conn.execute(
            text("INSERT OR IGNORE INTO users (id, is_active) VALUES (:id, 1)"),
            {"id": _compact_id(user_id)},
        )
        conn.execute(
            text("INSERT OR IGNORE INTO tenants (id, slug) VALUES (:id, 'tenant')"),
            {"id": _compact_id(tenant_id)},
        )
        conn.execute(
            text(
                "INSERT INTO strategy_capital_buckets (id, user_id, strategy_id, trading_mode, assigned_capital_cents, available_cash_cents, reserved_cash_cents, locked_capital_cents, realized_pnl_cents, unrealized_pnl_cents, available_buying_power_cents, version, created_at, updated_at) "
                "VALUES (:id, :user_id, :strategy_id, :trading_mode, :available_cash_cents, :available_cash_cents, 0, 0, 0, 0, :available_cash_cents, 1, :now, :now)"
            ),
            {
                "id": str(uuid4()),
                "user_id": _compact_id(user_id),
                "strategy_id": strategy_id,
                "trading_mode": trading_mode,
                "available_cash_cents": available_cash_cents,
                "now": now,
            },
        )


def _insert_token_policy(
    db_path: Path,
    *,
    strategy_id: str,
    symbol: str = "BTC-USD",
    admin_enabled: bool = True,
    recommendation_status: str = "allowed",
    recommendation_reason: str = "policy",
    max_position_pct_override: float | None = None,
) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    now = datetime.now(UTC).isoformat()
    with eng.begin() as conn:
        token_id = str(uuid4())
        conn.execute(
            text(
                "INSERT INTO platform_token_allowlist (id, symbol, quote_currency, is_enabled) "
                "VALUES (:id, :symbol, 'USD', 1)"
            ),
            {"id": token_id, "symbol": symbol},
        )
        conn.execute(
            text(
                "INSERT INTO token_strategy_policy ("
                "id, token_id, strategy_id, admin_enabled, suitability_score, recommendation_status, recommendation_reason,"
                "recommendation_status_override, recommendation_reason_override, max_position_pct_override, notes, computed_at, updated_at"
                ") VALUES ("
                ":id, :token_id, :strategy_id, :admin_enabled, 80, :recommendation_status, :recommendation_reason,"
                "NULL, NULL, :max_position_pct_override, NULL, :computed_at, :updated_at)"
            ),
            {
                "id": str(uuid4()),
                "token_id": token_id,
                "strategy_id": strategy_id,
                "admin_enabled": 1 if admin_enabled else 0,
                "recommendation_status": recommendation_status,
                "recommendation_reason": recommendation_reason,
                "max_position_pct_override": max_position_pct_override,
                "computed_at": now,
                "updated_at": now,
            },
        )


def _risk(user_id: str, strategy_id: str, mode: TradingMode) -> RiskDecision:
    return RiskDecision(
        outcome=RiskOutcome.APPROVE,
        approved=True,
        signal_id=uuid4(),
        run_id=uuid4(),
        user_id=user_id,
        strategy_name=strategy_id,
        symbol="BTC-USD",
        original_size="0.5",
        final_size="0.5",
        trading_mode=mode,
        trace_id="trace-test",
    )


def _request(
    user_id: str,
    tenant_id: str,
    strategy_id: str,
    mode: TradingMode,
    *,
    side: Side = Side.BUY,
    quantity: Decimal = Decimal("0.5"),
    price_hint: Decimal = Decimal("50000"),
) -> ExecutionRequest:
    intent_id = uuid4()
    return ExecutionRequest(
        intent_id=intent_id,
        trace_id="trace-test",
        user_id=user_id,
        risk=_risk(user_id, strategy_id, mode),
        tenant_id=tenant_id,
        trading_mode=mode,
        strategy_id=strategy_id,
        symbol="BTC-USD",
        side=side,
        order_type=OrderType.MARKET,
        quantity=quantity,
        venue=Venue.COINBASE,
        price_hint=price_hint,
        idempotency_key=ExecutionService.build_idempotency_key(str(intent_id), mode),
        client_order_id=ExecutionService.build_client_order_id(str(intent_id), mode),
        intent_payload={
            "intent_id": str(intent_id),
            "tenant_id": tenant_id,
            "trading_mode": mode.value,
            "strategy_id": strategy_id,
            "instrument": {"symbol": "BTC-USD"},
            "side": side.value,
            "order_type": OrderType.MARKET.value,
            "quantity": {"amount": str(quantity)},
        },
    )


def _service(
    db_path: Path, redis: FakeRedis, live_submission: ExecutionSubmission | None = None
) -> tuple[ExecutionService, FakeLiveClient]:
    settings = Settings(database_url=f"sqlite+pysqlite:///{db_path}")
    paper = PaperExecutionAdapter(redis, fee_bps=10, slippage_bps=0)
    live_client = FakeLiveClient(
        live_submission
        or ExecutionSubmission(
            status=ExecutionOrderStatus.PENDING,
            venue=Venue.COINBASE,
            venue_order_id="venue-1",
        )
    )
    live = LiveCoinbaseExecutionAdapter(
        live_client, credential_loader=lambda tenant_id: ("key", "secret")
    )
    return ExecutionService(
        settings, redis, paper_adapter=paper, live_adapter=live
    ), live_client


def _count(db_path: Path, table: str) -> int:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())


def _bucket(db_path: Path, user_id: str, strategy_id: str, trading_mode: str):
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        return (
            conn.execute(
                text(
                    "SELECT * FROM strategy_capital_buckets WHERE user_id = :user_id AND strategy_id = :strategy_id AND trading_mode = :trading_mode"
                ),
                {
                    "user_id": _compact_id(user_id),
                    "strategy_id": strategy_id,
                    "trading_mode": trading_mode,
                },
            )
            .mappings()
            .first()
        )


def _order_state(db_path: Path) -> str:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        return str(
            conn.execute(
                text("SELECT state FROM execution_orders LIMIT 1")
            ).scalar_one()
        )


def _position(db_path: Path) -> dict | None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        row = (
            conn.execute(text("SELECT * FROM execution_positions LIMIT 1"))
            .mappings()
            .first()
        )
    return dict(row) if row is not None else None


def _last_order(db_path: Path) -> dict:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        row = (
            conn.execute(
                text("SELECT * FROM execution_orders ORDER BY created_at DESC LIMIT 1")
            )
            .mappings()
            .one()
        )
    return dict(row)


def _last_trade(db_path: Path) -> dict:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        row = (
            conn.execute(
                text("SELECT * FROM execution_trades ORDER BY executed_at DESC LIMIT 1")
            )
            .mappings()
            .one()
        )
    return dict(row)


def test_duplicate_intent_prevents_duplicate_order_and_fill(tmp_path: Path):
    db_path = tmp_path / "execution1.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "momentum"
    _seed_bucket(
        db_path,
        user_id,
        tenant_id,
        strategy_id,
        TradingMode.PAPER.value,
        available_cash_cents=3_100_000,
    )

    service, _ = _service(db_path, redis)
    request = _request(user_id, tenant_id, strategy_id, TradingMode.PAPER)

    first = service.process_request(request)
    second = service.process_request(request)

    assert first.duplicated is False
    assert second.duplicated is True
    assert _count(db_path, "execution_orders") == 1
    assert _count(db_path, "execution_fills") == 1
    assert _count(db_path, "execution_trades") == 1
    assert _order_state(db_path) == ExecutionOrderStatus.FILLED.value


def test_paper_fills_immediately_while_live_stays_pending(tmp_path: Path):
    db_path = tmp_path / "execution2.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "momentum"
    _seed_bucket(
        db_path,
        user_id,
        tenant_id,
        strategy_id,
        TradingMode.PAPER.value,
        available_cash_cents=3_100_000,
    )
    _seed_bucket(db_path, user_id, tenant_id, strategy_id, TradingMode.LIVE.value)

    service, live_client = _service(db_path, redis)

    paper = service.process_request(
        _request(user_id, tenant_id, strategy_id, TradingMode.PAPER)
    )
    live = service.process_request(
        _request(user_id, tenant_id, strategy_id, TradingMode.LIVE)
    )

    assert paper.state == ExecutionOrderStatus.FILLED
    assert live.state == ExecutionOrderStatus.PENDING
    assert live_client.place_calls == 1
    assert _count(db_path, "execution_fills") == 1
    assert _count(db_path, "execution_trades") == 1
    paper_bucket = _bucket(db_path, user_id, strategy_id, TradingMode.PAPER.value)
    live_bucket = _bucket(db_path, user_id, strategy_id, TradingMode.LIVE.value)
    assert int(paper_bucket["locked_capital_cents"]) > 0
    assert int(live_bucket["locked_capital_cents"]) > 0
    assert redis.list_len("oziebot:queue:execution_events:paper") >= 3
    assert redis.list_len("oziebot:queue:execution_events:live") >= 3


def test_state_machine_rejects_invalid_transition():
    try:
        ensure_transition(ExecutionOrderStatus.FILLED, ExecutionOrderStatus.PENDING)
    except ValueError as exc:
        assert "Invalid execution state transition" in str(exc)
    else:
        raise AssertionError("expected invalid transition to raise")


def test_live_duplicate_does_not_resubmit_order(tmp_path: Path):
    db_path = tmp_path / "execution3.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "momentum"
    _seed_bucket(db_path, user_id, tenant_id, strategy_id, TradingMode.LIVE.value)
    service, live_client = _service(db_path, redis)
    request = _request(user_id, tenant_id, strategy_id, TradingMode.LIVE)

    first = service.process_request(request)
    second = service.process_request(request)

    assert first.state == ExecutionOrderStatus.PENDING
    assert second.duplicated is True
    assert live_client.place_calls == 1
    assert _count(db_path, "execution_orders") == 1


def test_paper_sell_closes_position_and_records_trade(tmp_path: Path):
    db_path = tmp_path / "execution4.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "momentum"
    _seed_bucket(
        db_path,
        user_id,
        tenant_id,
        strategy_id,
        TradingMode.PAPER.value,
        available_cash_cents=3_100_000,
    )
    service, _ = _service(db_path, redis)

    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    buy = service.process_request(
        _request(user_id, tenant_id, strategy_id, TradingMode.PAPER)
    )
    buy_order = _last_order(db_path)

    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"51000","best_ask_price":"51010"}'
    )
    sell = service.process_request(
        _request(
            user_id,
            tenant_id,
            strategy_id,
            TradingMode.PAPER,
            side=Side.SELL,
            price_hint=Decimal("51000"),
        )
    )

    assert buy.state == ExecutionOrderStatus.FILLED
    assert int(buy_order["locked_cash_cents"]) == 2_502_500

    assert sell.state == ExecutionOrderStatus.FILLED
    assert _count(db_path, "execution_fills") == 2
    assert _count(db_path, "execution_trades") == 2
    position = _position(db_path)
    assert position is not None
    assert Decimal(str(position["quantity"])) == Decimal("0")
    last_order = _last_order(db_path)
    assert last_order["side"] == Side.SELL.value
    assert Decimal(str(last_order["filled_quantity"])) == Decimal("0.5")
    assert Decimal(str(last_order["avg_fill_price"])) == Decimal("51000")
    last_trade = _last_trade(db_path)
    assert last_trade["side"] == Side.SELL.value
    assert int(last_trade["realized_pnl_cents"]) == 44_950


def test_day_trading_max_position_age_auto_closes_in_paper(tmp_path: Path):
    db_path = tmp_path / "execution-age.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "day_trading"
    _seed_bucket(db_path, user_id, tenant_id, strategy_id, TradingMode.PAPER.value)
    service, _ = _service(db_path, redis)

    buy = service.process_request(
        _request(user_id, tenant_id, strategy_id, TradingMode.PAPER)
    )
    assert buy.state == ExecutionOrderStatus.FILLED

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    aged_at = (datetime.now(UTC) - timedelta(hours=5)).isoformat()
    with eng.begin() as conn:
        conn.execute(
            text(
                "UPDATE execution_positions SET last_trade_at = :aged_at WHERE strategy_id = :strategy_id AND trading_mode = :trading_mode"
            ),
            {
                "aged_at": aged_at,
                "strategy_id": strategy_id,
                "trading_mode": TradingMode.PAPER.value,
            },
        )

    enforced = service.enforce_runtime_controls()

    assert enforced == 1
    position = _position(db_path)
    assert position is not None
    assert Decimal(str(position["quantity"])) == Decimal("0")
    last_order = _last_order(db_path)
    assert last_order["strategy_id"] == strategy_id
    assert last_order["side"] == Side.SELL.value


def test_execution_rejects_blocked_token_strategy_policy(tmp_path: Path):
    db_path = tmp_path / "execution-token-policy-blocked.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "momentum"
    _seed_bucket(
        db_path,
        user_id,
        tenant_id,
        strategy_id,
        TradingMode.PAPER.value,
        available_cash_cents=3_100_000,
    )
    _insert_token_policy(
        db_path,
        strategy_id=strategy_id,
        recommendation_status="blocked",
        recommendation_reason="blocked for execution",
    )
    service, _ = _service(db_path, redis)

    result = service.process_request(
        _request(user_id, tenant_id, strategy_id, TradingMode.PAPER)
    )

    assert result.state == ExecutionOrderStatus.FAILED
    order = _last_order(db_path)
    assert order["failure_code"] == "token_strategy_policy"
    assert "blocked" in str(order["failure_detail"])
    assert _count(db_path, "execution_fills") == 0


def test_execution_reduces_discouraged_token_strategy_size(tmp_path: Path):
    db_path = tmp_path / "execution-token-policy-discouraged.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "momentum"
    _seed_bucket(
        db_path,
        user_id,
        tenant_id,
        strategy_id,
        TradingMode.PAPER.value,
        available_cash_cents=3_100_000,
    )
    _insert_token_policy(
        db_path,
        strategy_id=strategy_id,
        recommendation_status="discouraged",
        recommendation_reason="reduced for execution",
    )
    service, _ = _service(db_path, redis)

    result = service.process_request(
        _request(
            user_id, tenant_id, strategy_id, TradingMode.PAPER, quantity=Decimal("1")
        )
    )

    assert result.state == ExecutionOrderStatus.FILLED
    order = _last_order(db_path)
    assert Decimal(str(order["quantity"])) == Decimal("0.60000000")
    position = _position(db_path)
    assert position is not None
    assert Decimal(str(position["quantity"])) == Decimal("0.60000000")


def test_execution_applies_token_strategy_position_cap(tmp_path: Path):
    db_path = tmp_path / "execution-token-policy-cap.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "momentum"
    _seed_bucket(db_path, user_id, tenant_id, strategy_id, TradingMode.PAPER.value)
    _insert_token_policy(
        db_path,
        strategy_id=strategy_id,
        max_position_pct_override=0.10,
    )
    service, _ = _service(db_path, redis)

    result = service.process_request(
        _request(
            user_id, tenant_id, strategy_id, TradingMode.PAPER, quantity=Decimal("1")
        )
    )

    assert result.state == ExecutionOrderStatus.FILLED
    order = _last_order(db_path)
    assert Decimal(str(order["quantity"])) == Decimal("0.06000000")

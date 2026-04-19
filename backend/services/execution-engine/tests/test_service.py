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
        self.balance_calls = 0
        self.convert_quote_calls: list[dict] = []
        self.convert_commit_calls: list[dict] = []
        self.balances = [
            {
                "uuid": "usd-account",
                "currency": "USD",
                "available_balance": {"currency": "USD", "value": "100000.00"},
            },
            {
                "uuid": "usdc-account",
                "currency": "USDC",
                "available_balance": {"currency": "USDC", "value": "100000.00"},
            },
        ]

    def place_order(
        self, request: ExecutionRequest, *, api_key_name: str, private_key_pem: str
    ) -> ExecutionSubmission:
        self.place_calls += 1
        return self._submission

    def list_balances(self, *, api_key_name: str, private_key_pem: str) -> list[dict]:
        self.balance_calls += 1
        return list(self.balances)

    def create_convert_quote(
        self,
        *,
        from_account: str,
        to_account: str,
        amount: str,
        api_key_name: str,
        private_key_pem: str,
    ) -> dict:
        payload = {
            "quote_id": f"quote-{len(self.convert_quote_calls) + 1}",
            "from_account": from_account,
            "to_account": to_account,
            "amount": amount,
        }
        self.convert_quote_calls.append(payload)
        return payload

    def commit_convert_trade(
        self,
        trade_id: str,
        *,
        from_account: str,
        to_account: str,
        api_key_name: str,
        private_key_pem: str,
    ) -> dict:
        payload = {
            "trade_id": trade_id,
            "from_account": from_account,
            "to_account": to_account,
        }
        self.convert_commit_calls.append(payload)
        return payload

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
                "expected_gross_edge_bps INTEGER NOT NULL, estimated_fee_bps INTEGER NOT NULL, estimated_slippage_bps INTEGER NOT NULL,"
                "estimated_total_cost_bps INTEGER NOT NULL, expected_net_edge_bps INTEGER NOT NULL,"
                "execution_preference TEXT NOT NULL, fallback_behavior TEXT NOT NULL, maker_timeout_seconds INTEGER NOT NULL,"
                "limit_price_offset_bps INTEGER NOT NULL, actual_fill_type TEXT, fallback_triggered BOOLEAN NOT NULL,"
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
                "CREATE TABLE strategy_decision_audits ("
                "id TEXT PRIMARY KEY, signal_snapshot_id TEXT, stage TEXT, decision TEXT, reason_code TEXT, reason_detail TEXT,"
                "size_before TEXT, size_after TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE trade_outcome_features ("
                "id TEXT PRIMARY KEY, trade_id TEXT, signal_snapshot_id TEXT, trading_mode TEXT, strategy_name TEXT, token_symbol TEXT,"
                "entry_price TEXT, exit_price TEXT, filled_size TEXT, fee_paid TEXT, slippage_realized TEXT, hold_seconds INTEGER,"
                "realized_pnl TEXT, realized_return_pct TEXT, max_favorable_excursion_pct TEXT, max_adverse_excursion_pct TEXT,"
                "exit_reason TEXT, win_loss_label TEXT, profitable_after_fees_label TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE market_data_candles ("
                "id TEXT PRIMARY KEY, source TEXT, product_id TEXT, granularity_sec INTEGER, bucket_start TEXT,"
                "open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT, event_time TEXT, ingest_time TEXT)"
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
    recommendation_status_override: str | None = None,
    recommendation_reason_override: str | None = None,
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
                ":recommendation_status_override, :recommendation_reason_override, :max_position_pct_override, NULL, :computed_at, :updated_at)"
            ),
            {
                "id": str(uuid4()),
                "token_id": token_id,
                "strategy_id": strategy_id,
                "admin_enabled": 1 if admin_enabled else 0,
                "recommendation_status": recommendation_status,
                "recommendation_reason": recommendation_reason,
                "recommendation_status_override": recommendation_status_override,
                "recommendation_reason_override": recommendation_reason_override,
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
        execution_preference="maker_preferred",
        fallback_behavior="convert_to_taker",
        maker_timeout_seconds=15,
        limit_price_offset_bps=2,
        expected_gross_edge_bps=150,
        estimated_fee_bps=100,
        estimated_slippage_bps=8,
        estimated_total_cost_bps=115,
        expected_net_edge_bps=35,
        fee_profile={
            "maker_fee_bps": 40,
            "taker_fee_bps": 60,
            "estimated_slippage_bps": 8,
            "execution_preference": "maker_preferred",
            "fallback_behavior": "convert_to_taker",
            "maker_timeout_seconds": 15,
            "limit_price_offset_bps": 2,
        },
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
            "metadata": {
                "fee_economics": {
                    "execution_preference": "maker_preferred",
                    "fallback_behavior": "convert_to_taker",
                    "maker_timeout_seconds": 15,
                    "limit_price_offset_bps": 2,
                    "expected_gross_edge_bps": 150,
                    "estimated_fee_bps": 100,
                    "estimated_slippage_bps": 8,
                    "estimated_total_cost_bps": 115,
                    "expected_net_edge_bps": 35,
                },
                "intelligence": {"signal_snapshot_id": "snapshot-1"},
                "reason": "strategy signal",
            },
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


def test_paper_maker_preferred_can_fill_mixed_with_fallback(tmp_path: Path):
    db_path = tmp_path / "execution-mixed.sqlite"
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
    base_request = _request(user_id, tenant_id, strategy_id, TradingMode.PAPER)
    request = base_request.model_copy(
        update={
            "order_type": OrderType.LIMIT,
            "intent_payload": {
                **base_request.intent_payload,
                "order_type": OrderType.LIMIT.value,
            },
        }
    )

    result = service.process_request(request)

    assert result.state == ExecutionOrderStatus.FILLED
    order = _last_order(db_path)
    assert order["actual_fill_type"] == "mixed"
    assert int(order["fallback_triggered"]) == 1
    assert _count(db_path, "execution_fills") == 2


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


def test_live_buy_auto_converts_usdc_when_usd_short(tmp_path: Path):
    db_path = tmp_path / "execution-live-usdc-funding.sqlite"
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
    live_client.balances = [
        {
            "uuid": "usd-account",
            "currency": "USD",
            "available_balance": {"currency": "USD", "value": "50.00"},
        },
        {
            "uuid": "usdc-account",
            "currency": "USDC",
            "available_balance": {"currency": "USDC", "value": "600.00"},
        },
    ]
    request = _request(
        user_id,
        tenant_id,
        strategy_id,
        TradingMode.LIVE,
        quantity=Decimal("0.01"),
        price_hint=Decimal("50000"),
    )

    result = service.process_request(request)

    assert result.state == ExecutionOrderStatus.PENDING
    assert live_client.place_calls == 1
    assert live_client.convert_quote_calls[0]["amount"] == "450.00"
    assert live_client.convert_commit_calls[0]["trade_id"] == "quote-1"
    order = _last_order(db_path)
    assert '"status": "converted"' in str(order["adapter_payload"])
    assert '"converted_amount": "450.00"' in str(order["adapter_payload"])


def test_live_buy_rejects_when_usd_and_usdc_are_insufficient(tmp_path: Path):
    db_path = tmp_path / "execution-live-insufficient-quote.sqlite"
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
    live_client.balances = [
        {
            "uuid": "usd-account",
            "currency": "USD",
            "available_balance": {"currency": "USD", "value": "50.00"},
        },
        {
            "uuid": "usdc-account",
            "currency": "USDC",
            "available_balance": {"currency": "USDC", "value": "25.00"},
        },
    ]
    request = _request(
        user_id,
        tenant_id,
        strategy_id,
        TradingMode.LIVE,
        quantity=Decimal("0.01"),
        price_hint=Decimal("50000"),
    )

    result = service.process_request(request)

    assert result.state == ExecutionOrderStatus.FAILED
    assert live_client.place_calls == 0
    order = _last_order(db_path)
    assert order["failure_code"] == "insufficient_quote_balance"
    assert "USDC was available to convert" in str(order["failure_detail"])


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
    assert int(buy_order["locked_cash_cents"]) == 2_517_012

    assert sell.state == ExecutionOrderStatus.FILLED
    assert _count(db_path, "execution_fills") == 2
    assert _count(db_path, "execution_trades") == 2
    position = _position(db_path)
    assert position is not None
    assert Decimal(str(position["quantity"])) == Decimal("0")
    last_order = _last_order(db_path)
    assert last_order["side"] == Side.SELL.value
    assert Decimal(str(last_order["filled_quantity"])) == Decimal("0.5")
    assert Decimal(str(last_order["avg_fill_price"])) == Decimal("50959.20000000")
    last_trade = _last_trade(db_path)
    assert last_trade["side"] == Side.SELL.value
    assert int(last_trade["realized_pnl_cents"]) == 15_660


def test_execution_records_trade_outcome_and_decision_audits(tmp_path: Path):
    db_path = tmp_path / "execution-intelligence.sqlite"
    _setup_db(db_path)
    redis = FakeRedis()
    user_id = str(uuid4())
    tenant_id = str(uuid4())
    strategy_id = "momentum"
    _seed_bucket(db_path, user_id, tenant_id, strategy_id, TradingMode.PAPER.value)
    service, _ = _service(db_path, redis)

    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"49990","best_ask_price":"50000"}'
    )
    service.process_request(
        _request(user_id, tenant_id, strategy_id, TradingMode.PAPER)
    )

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    now = datetime.now(UTC).isoformat()
    with eng.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO market_data_candles (id, source, product_id, granularity_sec, bucket_start, open, high, low, close, volume, event_time, ingest_time) "
                "VALUES (:id, 'coinbase', 'BTC-USD', 60, :bucket_start, '50000', '51200', '49800', '51000', '100', :event_time, :ingest_time)"
            ),
            {
                "id": str(uuid4()),
                "bucket_start": now,
                "event_time": now,
                "ingest_time": now,
            },
        )

    redis.set(
        "oziebot:md:bbo:BTC-USD", '{"best_bid_price":"51000","best_ask_price":"51010"}'
    )
    service.process_request(
        _request(
            user_id,
            tenant_id,
            strategy_id,
            TradingMode.PAPER,
            side=Side.SELL,
            price_hint=Decimal("51000"),
        )
    )

    with eng.begin() as conn:
        outcome = conn.execute(
            text(
                "SELECT signal_snapshot_id, trading_mode, win_loss_label, profitable_after_fees_label, hold_seconds FROM trade_outcome_features LIMIT 1"
            )
        ).first()
        decisions = conn.execute(
            text(
                "SELECT decision FROM strategy_decision_audits WHERE stage = 'execution' ORDER BY created_at"
            )
        ).all()
    assert outcome is not None
    assert outcome[0] == "snapshot-1"
    assert outcome[1] == "paper"
    assert outcome[2] == "win"
    assert outcome[3] == "profitable"
    assert outcome[4] is not None
    assert {row[0] for row in decisions} >= {"emitted", "executed"}


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
        recommendation_status_override="blocked",
        recommendation_reason_override="blocked for execution",
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

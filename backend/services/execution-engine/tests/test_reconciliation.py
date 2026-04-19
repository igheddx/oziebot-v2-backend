from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text

from oziebot_domain.execution import ExecutionOrderStatus, ExecutionSubmission
from oziebot_domain.trading_mode import TradingMode
from oziebot_execution_engine.adapters import (
    LiveCoinbaseExecutionAdapter,
    PaperExecutionAdapter,
)
from oziebot_execution_engine.config import Settings
from oziebot_execution_engine.reconciliation import ReconciliationService
from oziebot_execution_engine.service import ExecutionService

from test_service import FakeLiveClient, FakeRedis, _request, _setup_db, _seed_bucket


class FakeReconClient(FakeLiveClient):
    def __init__(self, submission: ExecutionSubmission | None = None) -> None:
        super().__init__(
            submission
            or ExecutionSubmission(
                status=ExecutionOrderStatus.PENDING,
                venue="coinbase",
                venue_order_id="venue-1",
            )
        )
        self.balance_calls = 0
        self.order_calls = 0
        self.fill_calls = 0
        self.orders: list[dict] = []
        self.fills: list[dict] = []
        self.raise_error: str | None = None

    def list_balances(self, *, api_key_name: str, private_key_pem: str) -> list[dict]:
        self.balance_calls += 1
        if self.raise_error:
            raise RuntimeError(self.raise_error)
        return self.balances

    def list_open_orders(
        self, *, api_key_name: str, private_key_pem: str
    ) -> list[dict]:
        self.order_calls += 1
        if self.raise_error:
            raise RuntimeError(self.raise_error)
        return self.orders

    def list_fills(
        self, *, api_key_name: str, private_key_pem: str, product_id: str | None = None
    ) -> list[dict]:
        self.fill_calls += 1
        if self.raise_error:
            raise RuntimeError(self.raise_error)
        return self.fills


def _service_with_reconciler(
    db_path: Path, redis: FakeRedis, client: FakeReconClient
) -> tuple[ExecutionService, ReconciliationService]:
    settings = Settings(
        database_url=f"sqlite+pysqlite:///{db_path}",
        reconciliation_health_failure_threshold=2,
        reconciliation_balance_drift_tolerance_cents=0,
    )
    paper = PaperExecutionAdapter(redis, fee_bps=10, slippage_bps=0)
    live = LiveCoinbaseExecutionAdapter(
        client, credential_loader=lambda tenant_id: ("key", "secret")
    )
    execution = ExecutionService(
        settings, redis, paper_adapter=paper, live_adapter=live
    )
    return execution, ReconciliationService(
        settings,
        execution,
        client,
        credential_loader=lambda tenant_id: ("key", "secret"),
    )


def _setup_recon_tables(db_path: Path, tenant_id: str) -> None:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    now = datetime.now(UTC).isoformat()
    with eng.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE exchange_connections (tenant_id TEXT, provider TEXT, api_key_name TEXT, encrypted_secret BLOB, validation_status TEXT, can_trade BOOLEAN, can_read_balances BOOLEAN, health_status TEXT, last_health_check_at TEXT, last_error TEXT, updated_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE tenant_integrations (tenant_id TEXT PRIMARY KEY, coinbase_connected BOOLEAN, coinbase_last_check_at TEXT, coinbase_health_status TEXT, coinbase_last_error TEXT, updated_at TEXT)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE execution_reconciliation_events (id TEXT PRIMARY KEY, tenant_id TEXT, order_id TEXT, trading_mode TEXT, scope TEXT, status TEXT, detail TEXT, internal_snapshot TEXT, external_snapshot TEXT, repair_applied BOOLEAN, metadata TEXT, created_at TEXT)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO exchange_connections (tenant_id, provider, api_key_name, encrypted_secret, validation_status, can_trade, can_read_balances, health_status, updated_at) VALUES (:tenant_id, 'coinbase', 'key', 'secret', 'valid', 1, 1, 'healthy', :now)"
            ),
            {"tenant_id": tenant_id, "now": now},
        )
        conn.execute(
            text(
                "INSERT INTO tenant_integrations (tenant_id, coinbase_connected, coinbase_health_status, updated_at) VALUES (:tenant_id, 1, 'healthy', :now)"
            ),
            {"tenant_id": tenant_id, "now": now},
        )


def _count(db_path: Path, table: str) -> int:
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar_one())


def test_reconciliation_ignores_paper_mode(tmp_path: Path):
    db_path = tmp_path / "recon1.sqlite"
    _setup_db(db_path)
    tenant_id = str(uuid4())
    _setup_recon_tables(db_path, tenant_id)
    redis = FakeRedis()
    client = FakeReconClient()
    execution, reconciler = _service_with_reconciler(db_path, redis, client)

    summary = reconciler.reconcile_tenant(uuid4(), TradingMode.PAPER)

    assert summary.skipped is True
    assert client.balance_calls == 0
    assert client.order_calls == 0
    assert client.fill_calls == 0
    assert _count(db_path, "execution_reconciliation_events") == 1


def test_reconciliation_repairs_partial_fill_and_position(tmp_path: Path):
    db_path = tmp_path / "recon2.sqlite"
    _setup_db(db_path)
    tenant_id = str(uuid4())
    user_id = str(uuid4())
    strategy_id = "momentum"
    _setup_recon_tables(db_path, tenant_id)
    _seed_bucket(db_path, user_id, tenant_id, strategy_id, TradingMode.LIVE.value)
    redis = FakeRedis()
    client = FakeReconClient()
    execution, reconciler = _service_with_reconciler(db_path, redis, client)

    request = _request(user_id, tenant_id, strategy_id, TradingMode.LIVE)
    result = execution.process_request(request)
    assert result.state == ExecutionOrderStatus.PENDING

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        order = (
            conn.execute(text("SELECT * FROM execution_orders LIMIT 1"))
            .mappings()
            .first()
        )
    venue_order_id = str(order["venue_order_id"])
    client.orders = [{"order_id": venue_order_id, "status": "OPEN"}]
    client.fills = [
        {
            "order_id": venue_order_id,
            "trade_id": "fill-1",
            "size": "0.2",
            "price": "50000",
            "commission": "1.00",
            "trade_time": datetime.now(UTC).isoformat(),
        }
    ]
    client.balances = [
        {
            "available_balance": {"currency": "USD", "value": "30000.00"},
            "hold": {"value": "20000.00"},
        }
    ]

    summary = reconciler.reconcile_tenant(uuid.UUID(tenant_id), TradingMode.LIVE)

    assert summary.repaired_orders >= 1
    assert summary.repaired_fills == 1
    with eng.begin() as conn:
        repaired_order = conn.execute(
            text("SELECT state, filled_quantity FROM execution_orders LIMIT 1")
        ).first()
        position = conn.execute(
            text("SELECT quantity FROM execution_positions LIMIT 1")
        ).first()
    assert repaired_order.state == ExecutionOrderStatus.PARTIALLY_FILLED.value
    assert repaired_order.filled_quantity == "0.2"
    assert position.quantity == "0.2"
    assert _count(db_path, "execution_fills") == 1
    assert _count(db_path, "execution_trades") == 1


def test_reconciliation_marks_connection_unhealthy_after_repeated_failures(
    tmp_path: Path,
):
    db_path = tmp_path / "recon3.sqlite"
    _setup_db(db_path)
    tenant_id = str(uuid4())
    _setup_recon_tables(db_path, tenant_id)
    redis = FakeRedis()
    client = FakeReconClient()
    client.raise_error = "coinbase timeout"
    execution, reconciler = _service_with_reconciler(db_path, redis, client)

    reconciler.reconcile_tenant(uuid.UUID(tenant_id), TradingMode.LIVE)
    reconciler.reconcile_tenant(uuid.UUID(tenant_id), TradingMode.LIVE)

    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        row = conn.execute(
            text(
                "SELECT health_status, last_error FROM exchange_connections WHERE tenant_id = :tenant_id"
            ),
            {"tenant_id": tenant_id},
        ).first()
        ti = conn.execute(
            text(
                "SELECT coinbase_connected, coinbase_health_status FROM tenant_integrations WHERE tenant_id = :tenant_id"
            ),
            {"tenant_id": tenant_id},
        ).first()
    assert row.health_status == "unhealthy"
    assert "coinbase timeout" in row.last_error
    assert ti.coinbase_connected == 0
    assert ti.coinbase_health_status == "unhealthy"


def test_reconciliation_treats_usdc_as_cash_equivalent(tmp_path: Path):
    db_path = tmp_path / "recon-usdc-cash.sqlite"
    _setup_db(db_path)
    tenant_id = str(uuid4())
    user_id = str(uuid4())
    strategy_id = "momentum"
    _setup_recon_tables(db_path, tenant_id)
    _seed_bucket(
        db_path,
        user_id,
        tenant_id,
        strategy_id,
        TradingMode.LIVE.value,
        available_cash_cents=50_000,
    )
    redis = FakeRedis()
    client = FakeReconClient()
    client.balances = [
        {
            "available_balance": {"currency": "USDC", "value": "500.00"},
            "hold": {"value": "0.00"},
        }
    ]
    _, reconciler = _service_with_reconciler(db_path, redis, client)

    summary = reconciler.reconcile_tenant(uuid.UUID(tenant_id), TradingMode.LIVE)

    assert summary.balance_drifts == 0


def test_reconciliation_recovers_full_fill_after_interruption(tmp_path: Path):
    db_path = tmp_path / "recon4.sqlite"
    _setup_db(db_path)
    tenant_id = str(uuid4())
    user_id = str(uuid4())
    strategy_id = "momentum"
    _setup_recon_tables(db_path, tenant_id)
    _seed_bucket(db_path, user_id, tenant_id, strategy_id, TradingMode.LIVE.value)
    redis = FakeRedis()
    client = FakeReconClient()
    execution, reconciler = _service_with_reconciler(db_path, redis, client)

    request = _request(user_id, tenant_id, strategy_id, TradingMode.LIVE)
    execution.process_request(request)
    eng = create_engine(f"sqlite+pysqlite:///{db_path}")
    with eng.begin() as conn:
        order = (
            conn.execute(text("SELECT * FROM execution_orders LIMIT 1"))
            .mappings()
            .first()
        )
    venue_order_id = str(order["venue_order_id"])
    client.orders = []
    client.fills = [
        {
            "order_id": venue_order_id,
            "trade_id": "fill-full",
            "size": "0.5",
            "price": "50000",
            "commission": "1.25",
            "trade_time": datetime.now(UTC).isoformat(),
        }
    ]
    client.balances = [
        {
            "available_balance": {"currency": "USD", "value": "50000.00"},
            "hold": {"value": "0"},
        }
    ]

    summary = reconciler.reconcile_tenant(uuid.UUID(tenant_id), TradingMode.LIVE)

    assert summary.repaired_fills == 1
    with eng.begin() as conn:
        repaired_order = conn.execute(
            text("SELECT state FROM execution_orders LIMIT 1")
        ).first()
    assert repaired_order.state == ExecutionOrderStatus.FILLED.value

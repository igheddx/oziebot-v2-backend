from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from collections.abc import Callable
from typing import Any

from sqlalchemy import create_engine, text

from oziebot_domain.execution import (
    ExecutionFill,
    ExecutionOrderStatus,
    ExecutionRequest,
    ExecutionSubmission,
)
from oziebot_domain.risk import RiskDecision
from oziebot_domain.trading import OrderType, Side, Venue
from oziebot_domain.trading_mode import TradingMode

from oziebot_execution_engine.coinbase_client import CoinbaseExecutionClient
from oziebot_execution_engine.service import ExecutionService


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    return Decimal(str(value))


@dataclass(frozen=True)
class ReconciliationSummary:
    tenant_id: str
    trading_mode: TradingMode
    scanned_orders: int
    repaired_orders: int
    repaired_fills: int
    repaired_positions: int
    balance_drifts: int
    skipped: bool = False


class ReconciliationService:
    def __init__(
        self,
        settings,
        execution_service: ExecutionService,
        coinbase_client: CoinbaseExecutionClient,
        *,
        credential_loader=None,
    ) -> None:
        self._settings = settings
        self._execution = execution_service
        self._engine = (
            create_engine(settings.database_url) if settings.database_url else None
        )
        self._coinbase = coinbase_client
        self._credential_loader = (
            credential_loader or execution_service.load_live_credentials
        )
        self._heartbeat: Callable[[], None] | None = None

    def set_heartbeat(self, callback: Callable[[], None] | None) -> None:
        self._heartbeat = callback

    def _touch(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat()

    def reconcile_all_live(self) -> list[ReconciliationSummary]:
        if self._engine is None:
            raise RuntimeError("DATABASE_URL is required")
        stmt = text(
            """
            SELECT DISTINCT tenant_id
            FROM execution_orders
            WHERE trading_mode = 'live'
            UNION
            SELECT tenant_id
            FROM exchange_connections
            WHERE provider = 'coinbase'
            """
        )
        with self._engine.begin() as conn:
            tenant_ids = [
                str(row.tenant_id) for row in conn.execute(stmt).mappings().all()
            ]
        results: list[ReconciliationSummary] = []
        for tenant_id in tenant_ids:
            self._touch()
            results.append(
                self.reconcile_tenant(uuid.UUID(tenant_id), TradingMode.LIVE)
            )
            self._touch()
        return results

    def reconcile_tenant(
        self, tenant_id: uuid.UUID, trading_mode: TradingMode
    ) -> ReconciliationSummary:
        if trading_mode != TradingMode.LIVE:
            self._audit(
                tenant_id=tenant_id,
                trading_mode=trading_mode,
                scope="service",
                status="skipped",
                detail="Reconciliation applies only to LIVE trading mode",
                internal_snapshot={"reason": "paper_mode_ignored"},
                external_snapshot=None,
                repair_applied=False,
            )
            return ReconciliationSummary(
                str(tenant_id), trading_mode, 0, 0, 0, 0, 0, skipped=True
            )

        if self._engine is None:
            raise RuntimeError("DATABASE_URL is required")

        try:
            api_key_name, private_key_pem = self._credential_loader(tenant_id)
            self._touch()
            balances = self._coinbase.list_balances(
                api_key_name=api_key_name, private_key_pem=private_key_pem
            )
            self._touch()
            open_orders = self._coinbase.list_open_orders(
                api_key_name=api_key_name, private_key_pem=private_key_pem
            )
            self._touch()
            fills = self._coinbase.list_fills(
                api_key_name=api_key_name, private_key_pem=private_key_pem
            )
            self._touch()
            self._mark_connection_healthy(tenant_id)
        except Exception as exc:
            self._mark_connection_failure(tenant_id, str(exc))
            self._audit(
                tenant_id=tenant_id,
                trading_mode=TradingMode.LIVE,
                scope="connection_health",
                status="error",
                detail=str(exc)[:512],
                internal_snapshot=None,
                external_snapshot=None,
                repair_applied=False,
            )
            return ReconciliationSummary(
                str(tenant_id), TradingMode.LIVE, 0, 0, 0, 0, 0
            )

        open_by_id = {
            str(item.get("order_id") or item.get("id")): item for item in open_orders
        }
        fills_by_order: dict[str, list[dict[str, Any]]] = {}
        for row in fills:
            order_id = str(row.get("order_id") or row.get("entry_id") or "")
            if not order_id:
                continue
            fills_by_order.setdefault(order_id, []).append(row)

        repaired_orders = 0
        repaired_fills = 0
        with self._engine.begin() as conn:
            orders = (
                conn.execute(
                    text(
                        "SELECT * FROM execution_orders WHERE tenant_id = :tenant_id AND trading_mode = 'live'"
                    ),
                    {"tenant_id": str(tenant_id)},
                )
                .mappings()
                .all()
            )

        for order in orders:
            if order["trading_mode"] != TradingMode.LIVE.value:
                continue
            repaired, fill_count = self._reconcile_order(
                order,
                open_by_id,
                fills_by_order.get(str(order.get("venue_order_id") or ""), []),
            )
            repaired_orders += int(repaired)
            repaired_fills += fill_count
            self._touch()

        repaired_positions = self._rebuild_positions(tenant_id)
        self._touch()
        balance_drifts = self._reconcile_balances(tenant_id, balances)
        self._touch()
        return ReconciliationSummary(
            str(tenant_id),
            TradingMode.LIVE,
            len(orders),
            repaired_orders,
            repaired_fills,
            repaired_positions,
            balance_drifts,
        )

    def _reconcile_order(
        self,
        order: dict[str, Any],
        open_by_id: dict[str, dict[str, Any]],
        external_fills: list[dict[str, Any]],
    ) -> tuple[bool, int]:
        venue_order_id = str(order.get("venue_order_id") or "")
        internal_state = ExecutionOrderStatus(str(order["state"]))
        request = self._request_from_order(order)
        external_order = open_by_id.get(venue_order_id)
        new_fills = self._missing_external_fills(str(order["id"]), external_fills)
        repaired = False
        repaired_fill_count = len(new_fills)

        if new_fills:
            target_state = internal_state
            if external_order is None:
                target_state = ExecutionOrderStatus.FILLED
            else:
                target_state = ExecutionOrderStatus.PARTIALLY_FILLED
            submission = ExecutionSubmission(
                status=target_state,
                venue=Venue.COINBASE,
                venue_order_id=venue_order_id or None,
                fills=new_fills,
                raw_payload={
                    "reconciliation": True,
                    "order": external_order,
                    "fills": external_fills,
                },
            )
            self._execution._persist_fills_and_positions(
                str(order["id"]), request, submission
            )
            repaired = True

        current = self._execution._get_order(str(order["id"]))
        current_state = ExecutionOrderStatus(str(current["state"]))

        target_state = current_state
        if external_order is not None:
            external_status = str(
                external_order.get("status")
                or external_order.get("order_status")
                or "OPEN"
            ).lower()
            if (
                external_status in {"open", "pending"}
                and current_state == ExecutionOrderStatus.SUBMITTED
            ):
                target_state = ExecutionOrderStatus.PENDING
            elif external_status == "open" and repaired_fill_count > 0:
                target_state = ExecutionOrderStatus.PARTIALLY_FILLED
            elif external_status in {"filled", "done"}:
                target_state = ExecutionOrderStatus.FILLED
            elif external_status in {"cancelled", "canceled"}:
                target_state = ExecutionOrderStatus.CANCELLED
        else:
            if repaired_fill_count > 0:
                target_state = ExecutionOrderStatus.FILLED
            elif current_state in {
                ExecutionOrderStatus.SUBMITTED,
                ExecutionOrderStatus.PENDING,
                ExecutionOrderStatus.PARTIALLY_FILLED,
            }:
                target_state = ExecutionOrderStatus.CANCELLED

        if target_state != current_state:
            self._repair_terminal_or_state(request, current, target_state)
            repaired = True

        if repaired:
            self._audit(
                tenant_id=request.tenant_id,
                trading_mode=TradingMode.LIVE,
                scope="orders",
                status="repaired",
                detail=f"Reconciled order {order['id']} to {target_state.value}",
                internal_snapshot={
                    "before_state": internal_state.value,
                    "after_state": target_state.value,
                },
                external_snapshot={"order": external_order, "fills": external_fills},
                repair_applied=True,
                order_id=str(order["id"]),
            )
        return repaired, repaired_fill_count

    def _repair_terminal_or_state(
        self,
        request: ExecutionRequest,
        order: dict[str, Any],
        target_state: ExecutionOrderStatus,
    ) -> None:
        order_id = str(order["id"])
        current_state = ExecutionOrderStatus(str(order["state"]))
        if target_state == current_state:
            return
        if target_state == ExecutionOrderStatus.CANCELLED:
            reserved = int(order.get("reserved_cash_cents") or 0)
            locked = int(order.get("locked_cash_cents") or 0)
            if reserved > 0:
                self._execution._release_reserved_capital(request, reserved, order_id)
            current_cost = (
                self._current_buy_cost_cents(order_id)
                if request.side == Side.BUY
                else 0
            )
            releasable_locked = max(0, locked - current_cost)
            if releasable_locked > 0:
                self._execution._settle_capital(request, releasable_locked, 0, order_id)
            self._execution._set_order_reserved_locked(
                order_id,
                reserved_cash_cents=0,
                locked_cash_cents=max(0, locked - releasable_locked),
            )
            self._execution._set_order_state(
                order_id, ExecutionOrderStatus.CANCELLED, cancelled_at=_utcnow()
            )
            return
        if target_state == ExecutionOrderStatus.FILLED:
            current_cost = (
                self._current_buy_cost_cents(order_id)
                if request.side == Side.BUY
                else 0
            )
            locked = int(order.get("locked_cash_cents") or 0)
            if request.side == Side.BUY and locked > current_cost:
                self._execution._settle_capital(
                    request, locked - current_cost, 0, order_id
                )
                self._execution._set_order_reserved_locked(
                    order_id, reserved_cash_cents=0, locked_cash_cents=current_cost
                )
            self._execution._set_order_state(
                order_id, ExecutionOrderStatus.FILLED, completed_at=_utcnow()
            )
            return
        self._execution._set_order_state(order_id, target_state)

    def _missing_external_fills(
        self, order_id: str, external_fills: list[dict[str, Any]]
    ) -> list[ExecutionFill]:
        known: set[str] = set()
        with self._engine.begin() as conn:
            rows = conn.execute(
                text(
                    "SELECT venue_fill_id FROM execution_fills WHERE order_id = :order_id"
                ),
                {"order_id": order_id},
            ).all()
            known = {str(row[0]) for row in rows}
        out: list[ExecutionFill] = []
        for row in external_fills:
            fill_id = str(
                row.get("trade_id") or row.get("entry_id") or row.get("fill_id") or ""
            )
            if not fill_id or fill_id in known:
                continue
            out.append(
                ExecutionFill(
                    fill_id=fill_id,
                    quantity=_to_decimal(row.get("size") or row.get("filled_size")),
                    price=_to_decimal(row.get("price")),
                    fee=_to_decimal(row.get("commission") or row.get("fee"), "0"),
                    liquidity=str(row.get("liquidity_indicator") or "exchange"),
                    occurred_at=datetime.fromisoformat(
                        str(
                            row.get("trade_time")
                            or row.get("created_time")
                            or _utcnow().isoformat()
                        ).replace("Z", "+00:00")
                    ),
                    raw_payload=row,
                )
            )
        return out

    def _request_from_order(self, order: dict[str, Any]) -> ExecutionRequest:
        risk_payload = order["risk_payload"]
        if isinstance(risk_payload, str):
            risk_payload = json.loads(risk_payload)
        intent_payload = order["intent_payload"]
        if isinstance(intent_payload, str):
            intent_payload = json.loads(intent_payload)
        risk = RiskDecision.model_validate(risk_payload)
        return ExecutionRequest(
            intent_id=order["intent_id"],
            trace_id=str(order["trace_id"]),
            user_id=order["user_id"],
            risk=risk,
            tenant_id=order["tenant_id"],
            trading_mode=TradingMode(str(order["trading_mode"])),
            strategy_id=str(order["strategy_id"]),
            symbol=str(order["symbol"]),
            side=Side(str(order["side"])),
            order_type=OrderType(str(order["order_type"])),
            quantity=_to_decimal(order["quantity"]),
            venue=Venue(str(order["venue"])),
            price_hint=_to_decimal(
                order["avg_fill_price"]
                or Decimal(str(order.get("requested_notional_cents") or 0))
                / max(_to_decimal(order["quantity"]), Decimal("1")),
                "0",
            ),
            idempotency_key=str(order["idempotency_key"]),
            client_order_id=str(order["client_order_id"]),
            intent_payload=intent_payload,
        )

    def _rebuild_positions(self, tenant_id: uuid.UUID) -> int:
        with self._engine.begin() as conn:
            trades = (
                conn.execute(
                    text(
                        "SELECT user_id, strategy_id, symbol, trading_mode, side, quantity, price, realized_pnl_cents FROM execution_trades WHERE tenant_id = :tenant_id AND trading_mode = 'live' ORDER BY executed_at ASC"
                    ),
                    {"tenant_id": str(tenant_id)},
                )
                .mappings()
                .all()
            )
            existing = (
                conn.execute(
                    text(
                        "SELECT * FROM execution_positions WHERE tenant_id = :tenant_id AND trading_mode = 'live'"
                    ),
                    {"tenant_id": str(tenant_id)},
                )
                .mappings()
                .all()
            )

        grouped: dict[tuple[str, str, str, str], dict[str, Decimal | int]] = {}
        for trade in trades:
            key = (
                str(trade["user_id"]),
                str(trade["strategy_id"]),
                str(trade["symbol"]),
                str(trade["trading_mode"]),
            )
            item = grouped.setdefault(
                key,
                {
                    "quantity": Decimal("0"),
                    "avg_entry_price": Decimal("0"),
                    "realized_pnl_cents": 0,
                },
            )
            qty = _to_decimal(trade["quantity"])
            price = _to_decimal(trade["price"])
            if str(trade["side"]) == Side.BUY.value:
                new_qty = item["quantity"] + qty  # type: ignore[operator]
                total_cost = (item["quantity"] * item["avg_entry_price"]) + (
                    qty * price
                )  # type: ignore[operator]
                item["quantity"] = new_qty
                item["avg_entry_price"] = (
                    total_cost / new_qty if new_qty > 0 else Decimal("0")
                )
            else:
                item["quantity"] = max(Decimal("0"), item["quantity"] - qty)  # type: ignore[operator]
                if item["quantity"] == 0:
                    item["avg_entry_price"] = Decimal("0")
            item["realized_pnl_cents"] = int(item["realized_pnl_cents"]) + int(
                trade["realized_pnl_cents"]
            )

        repaired = 0
        now = _utcnow()
        existing_map = {
            (
                str(row["user_id"]),
                str(row["strategy_id"]),
                str(row["symbol"]),
                str(row["trading_mode"]),
            ): row
            for row in existing
        }
        with self._engine.begin() as conn:
            for key, rebuilt in grouped.items():
                row = existing_map.get(key)
                if row is None:
                    conn.execute(
                        text(
                            "INSERT INTO execution_positions (id, tenant_id, user_id, strategy_id, symbol, trading_mode, quantity, avg_entry_price, realized_pnl_cents, created_at, updated_at, last_trade_at) VALUES (:id, :tenant_id, :user_id, :strategy_id, :symbol, :trading_mode, :quantity, :avg_entry_price, :realized_pnl_cents, :created_at, :updated_at, :last_trade_at)"
                        ),
                        {
                            "id": str(uuid.uuid4()),
                            "tenant_id": str(tenant_id),
                            "user_id": key[0],
                            "strategy_id": key[1],
                            "symbol": key[2],
                            "trading_mode": key[3],
                            "quantity": str(rebuilt["quantity"]),
                            "avg_entry_price": str(
                                Decimal(str(rebuilt["avg_entry_price"])).quantize(
                                    Decimal("0.00000001")
                                )
                            ),
                            "realized_pnl_cents": int(rebuilt["realized_pnl_cents"]),
                            "created_at": now,
                            "updated_at": now,
                            "last_trade_at": now,
                        },
                    )
                    repaired += 1
                    continue
                if (
                    str(row["quantity"]) != str(rebuilt["quantity"])
                    or str(row["avg_entry_price"])
                    != str(
                        Decimal(str(rebuilt["avg_entry_price"])).quantize(
                            Decimal("0.00000001")
                        )
                    )
                    or int(row["realized_pnl_cents"] or 0)
                    != int(rebuilt["realized_pnl_cents"])
                ):
                    conn.execute(
                        text(
                            "UPDATE execution_positions SET quantity = :quantity, avg_entry_price = :avg_entry_price, realized_pnl_cents = :realized_pnl_cents, updated_at = :updated_at WHERE id = :id"
                        ),
                        {
                            "id": str(row["id"]),
                            "quantity": str(rebuilt["quantity"]),
                            "avg_entry_price": str(
                                Decimal(str(rebuilt["avg_entry_price"])).quantize(
                                    Decimal("0.00000001")
                                )
                            ),
                            "realized_pnl_cents": int(rebuilt["realized_pnl_cents"]),
                            "updated_at": now,
                        },
                    )
                    repaired += 1
        if repaired:
            self._audit(
                tenant_id=tenant_id,
                trading_mode=TradingMode.LIVE,
                scope="positions",
                status="repaired",
                detail=f"Rebuilt {repaired} live position rows",
                internal_snapshot={"rebuilt_rows": repaired},
                external_snapshot=None,
                repair_applied=True,
            )
        return repaired

    def _reconcile_balances(
        self, tenant_id: uuid.UUID, balances: list[dict[str, Any]]
    ) -> int:
        usd_total_cents = 0
        for row in balances:
            currency = str(
                (row.get("available_balance") or {}).get("currency")
                or row.get("currency")
                or ""
            )
            if currency != "USD":
                continue
            available = _to_decimal(
                (row.get("available_balance") or {}).get("value")
                or row.get("available")
                or "0"
            )
            hold = _to_decimal(
                (row.get("hold") or {}).get("value") or row.get("hold_value") or "0"
            )
            usd_total_cents += int(
                ((available + hold) * Decimal("100")).quantize(Decimal("1"))
            )

        with self._engine.begin() as conn:
            internal = conn.execute(
                text(
                    "SELECT COALESCE(SUM(available_cash_cents + reserved_cash_cents + locked_capital_cents), 0) AS total FROM strategy_capital_buckets WHERE trading_mode = 'live'"
                )
            ).first()
        internal_total = int(internal.total if internal else 0)
        if (
            abs(internal_total - usd_total_cents)
            <= self._settings.reconciliation_balance_drift_tolerance_cents
        ):
            self._audit(
                tenant_id=tenant_id,
                trading_mode=TradingMode.LIVE,
                scope="balances",
                status="ok",
                detail="Balance reconciliation within tolerance",
                internal_snapshot={"internal_total_cents": internal_total},
                external_snapshot={"coinbase_total_cents": usd_total_cents},
                repair_applied=False,
            )
            return 0
        self._audit(
            tenant_id=tenant_id,
            trading_mode=TradingMode.LIVE,
            scope="balances",
            status="drift_detected",
            detail=f"Coinbase USD total differs by {usd_total_cents - internal_total} cents",
            internal_snapshot={"internal_total_cents": internal_total},
            external_snapshot={"coinbase_total_cents": usd_total_cents},
            repair_applied=False,
        )
        return 1

    def _current_buy_cost_cents(self, order_id: str) -> int:
        with self._engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT COALESCE(SUM(gross_notional_cents + fee_cents), 0) AS total FROM execution_fills WHERE order_id = :order_id"
                ),
                {"order_id": order_id},
            ).first()
        return int(row.total if row else 0)

    def _mark_connection_failure(
        self, tenant_id: uuid.UUID, error_message: str
    ) -> None:
        with self._engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT COUNT(*) AS total FROM execution_reconciliation_events WHERE tenant_id = :tenant_id AND scope = 'connection_health' AND status = 'error'"
                ),
                {"tenant_id": str(tenant_id)},
            ).first()
            failure_count = int(row.total if row else 0) + 1
            status = "degraded"
            connected = True
            if failure_count >= self._settings.reconciliation_health_failure_threshold:
                status = "unhealthy"
                connected = False
            conn.execute(
                text(
                    "UPDATE exchange_connections SET health_status = :status, last_health_check_at = :now, last_error = :last_error, updated_at = :now WHERE tenant_id = :tenant_id AND provider = 'coinbase'"
                ),
                {
                    "status": status,
                    "now": _utcnow(),
                    "last_error": error_message[:512],
                    "tenant_id": str(tenant_id),
                },
            )
            conn.execute(
                text(
                    "UPDATE tenant_integrations SET coinbase_connected = :connected, coinbase_health_status = :status, coinbase_last_check_at = :now, coinbase_last_error = :err, updated_at = :now WHERE tenant_id = :tenant_id"
                ),
                {
                    "connected": connected,
                    "status": status,
                    "now": _utcnow(),
                    "err": error_message[:512],
                    "tenant_id": str(tenant_id),
                },
            )
        self._audit(
            tenant_id=tenant_id,
            trading_mode=TradingMode.LIVE,
            scope="connection_health",
            status="error",
            detail=error_message[:512],
            internal_snapshot={"failure_count": failure_count},
            external_snapshot=None,
            repair_applied=status == "unhealthy",
        )

    def _mark_connection_healthy(self, tenant_id: uuid.UUID) -> None:
        now = _utcnow()
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE exchange_connections SET health_status = 'healthy', last_health_check_at = :now, last_error = NULL, updated_at = :now WHERE tenant_id = :tenant_id AND provider = 'coinbase'"
                ),
                {"tenant_id": str(tenant_id), "now": now},
            )
            conn.execute(
                text(
                    "UPDATE tenant_integrations SET coinbase_connected = true, coinbase_health_status = 'healthy', coinbase_last_check_at = :now, coinbase_last_error = NULL, updated_at = :now WHERE tenant_id = :tenant_id"
                ),
                {"tenant_id": str(tenant_id), "now": now},
            )

    def _audit(
        self,
        *,
        tenant_id: uuid.UUID,
        trading_mode: TradingMode,
        scope: str,
        status: str,
        detail: str | None,
        internal_snapshot: dict[str, Any] | None,
        external_snapshot: dict[str, Any] | None,
        repair_applied: bool,
        order_id: str | None = None,
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO execution_reconciliation_events (
                      id, tenant_id, order_id, trading_mode, scope, status, detail,
                      internal_snapshot, external_snapshot, repair_applied, metadata, created_at
                    ) VALUES (
                      :id, :tenant_id, :order_id, :trading_mode, :scope, :status, :detail,
                      :internal_snapshot, :external_snapshot, :repair_applied, :metadata, :created_at
                    )
                    """
                ),
                {
                    "id": str(uuid.uuid4()),
                    "tenant_id": str(tenant_id),
                    "order_id": order_id,
                    "trading_mode": trading_mode.value,
                    "scope": scope,
                    "status": status,
                    "detail": detail,
                    "internal_snapshot": json.dumps(
                        internal_snapshot or {}, default=str
                    ),
                    "external_snapshot": json.dumps(
                        external_snapshot or {}, default=str
                    ),
                    "repair_applied": repair_applied,
                    "metadata": json.dumps(internal_snapshot or {}, default=str),
                    "created_at": _utcnow(),
                },
            )

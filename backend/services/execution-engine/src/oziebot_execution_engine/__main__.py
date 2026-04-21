from __future__ import annotations

import logging
from datetime import UTC, datetime

from oziebot_common.health import install_shutdown_handlers, start_health_server
from oziebot_common.queues import (
    QueueNames,
    disconnect_redis,
    risk_decision_from_json,
    trade_intent_from_json,
)
from oziebot_common.worker_runtime import (
    DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    redis_client_for_worker,
    run_redis_queue_worker,
)

from oziebot_execution_engine.adapters import (
    LiveCoinbaseExecutionAdapter,
    PaperExecutionAdapter,
)
from oziebot_execution_engine.coinbase_client import HttpCoinbaseExecutionClient
from oziebot_execution_engine.config import get_settings
from oziebot_execution_engine.reconciliation import ReconciliationService
from oziebot_execution_engine.service import ExecutionService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("execution-engine")


def main() -> None:
    settings = get_settings()
    r = redis_client_for_worker(
        settings.redis_url,
        queue_pop_timeout_seconds=DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    )
    coinbase_client = HttpCoinbaseExecutionClient(settings.coinbase_api_base_url)
    service = ExecutionService(
        settings,
        r,
        paper_adapter=PaperExecutionAdapter(
            r,
            fee_bps=settings.paper_default_fee_bps,
            slippage_bps=settings.paper_default_slippage_bps,
        ),
        live_adapter=LiveCoinbaseExecutionAdapter(
            coinbase_client,
            credential_loader=lambda tenant_id: service.load_live_credentials(
                tenant_id
            ),
        ),
    )
    reconciler = ReconciliationService(settings, service, coinbase_client)
    health = start_health_server("execution-engine")
    reconciler.set_heartbeat(health.touch)
    stop_event = install_shutdown_handlers(
        "execution-engine",
        health_state=health,
        on_shutdown=lambda: disconnect_redis(r),
    )
    keys = QueueNames.all_intent_approved_keys()
    log.info("execution-engine listening on %s", keys)
    last_reconcile = datetime.now(UTC)

    def _reconcile_if_due() -> None:
        nonlocal last_reconcile
        now = datetime.now(UTC)
        if (
            now - last_reconcile
        ).total_seconds() >= settings.reconciliation_interval_seconds:
            enforced = service.enforce_runtime_controls()
            if enforced:
                log.info("runtime_controls_enforced count=%s", enforced)
            summaries = reconciler.reconcile_all_live()
            for summary in summaries:
                log.info(
                    "reconcile_live tenant_id=%s scanned=%s repaired_orders=%s repaired_fills=%s repaired_positions=%s balance_drifts=%s skipped=%s",
                    summary.tenant_id,
                    summary.scanned_orders,
                    summary.repaired_orders,
                    summary.repaired_fills,
                    summary.repaired_positions,
                    summary.balance_drifts,
                    summary.skipped,
                )
            last_reconcile = now
            health.touch()

    def _handle_message(_queue_key: str, raw: dict[str, object]) -> None:
        intent = trade_intent_from_json(raw["intent"])
        risk = risk_decision_from_json(raw["risk"])
        result = service.process_queue_message(raw)
        log.info(
            "execution_%s intent_id=%s mode=%s duplicated=%s",
            result.state.value,
            intent.intent_id,
            risk.trading_mode.value,
            result.duplicated,
        )

    run_redis_queue_worker(
        worker_name="execution-engine",
        redis_client=r,
        queue_keys=keys,
        stop_event=stop_event,
        health=health,
        handle_message=_handle_message,
        logger=log,
        on_iteration=_reconcile_if_due,
        queue_pop_timeout_seconds=DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    )
    log.info("execution-engine shutdown complete")


if __name__ == "__main__":
    main()

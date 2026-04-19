from __future__ import annotations

import logging
from datetime import UTC, datetime

from oziebot_common.health import start_health_server
from oziebot_common.queues import (
    QueueNames,
    brpop_json_any,
    redis_from_url,
    risk_decision_from_json,
    trade_intent_from_json,
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
    r = redis_from_url(
        settings.redis_url, probe=True, socket_connect_timeout=3, socket_timeout=3
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
    keys = QueueNames.all_intent_approved_keys()
    log.info("execution-engine listening on %s", keys)
    last_reconcile = datetime.now(UTC)
    health.mark_ready()
    while True:
        got = brpop_json_any(r, keys, timeout=30)
        now = datetime.now(UTC)
        health.touch()
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
        if got is None:
            continue
        _queue_key, raw = got
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
        health.touch()


if __name__ == "__main__":
    main()

from __future__ import annotations

import logging
import uuid

from sqlalchemy import create_engine

from oziebot_common.health import install_shutdown_handlers, start_health_server
from oziebot_common.postgres_runtime_kv import PostgresRuntimeKV
from oziebot_common.queues import (
    QueueNames,
    strategy_signal_from_json,
    trade_intent_to_json,
)
from oziebot_common.worker_outbox import enqueue_worker_payload
from oziebot_common.worker_runtime import (
    DEFAULT_POLL_IDLE_SECONDS,
    run_postgres_queue_worker,
)
from oziebot_domain.risk import RiskOutcome
from oziebot_risk_engine.config import get_settings
from oziebot_risk_engine.service import RiskEngineService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("risk-engine")


def main() -> None:
    settings = get_settings()
    engine = create_engine(settings.database_url)
    kv = PostgresRuntimeKV(engine)
    service = RiskEngineService(settings, engine, kv)
    health = start_health_server("risk-engine")
    stop_event = install_shutdown_handlers(
        "risk-engine",
        health_state=health,
    )
    keys = QueueNames.all_signal_generated_keys()
    log.info("risk-engine listening on %s", keys)

    def _handle_message(_queue_key: str, raw: dict[str, object]) -> None:
        trace_id = str(raw.get("trace_id") or uuid.uuid4())
        signal = strategy_signal_from_json(raw["signal"])

        decision, intent = service.evaluate(signal, trace_id)
        if decision.outcome == RiskOutcome.REJECT or intent is None:
            enqueue_worker_payload(
                engine,
                QueueNames.intent_rejected(signal.trading_mode),
                {
                    "signal": raw["signal"],
                    "risk": decision.model_dump(mode="json"),
                    "trace_id": trace_id,
                },
            )
            log.info(
                "risk_reject signal_id=%s mode=%s reason=%s",
                signal.signal_id,
                signal.trading_mode.value,
                decision.reason.value if decision.reason else None,
            )
            return

        approved_queue = (
            QueueNames.intent_approved_strategy(
                signal.trading_mode, signal.strategy_name
            )
            if signal.strategy_name in QueueNames.DEDICATED_INTENT_STRATEGIES
            else QueueNames.intent_approved(signal.trading_mode)
        )
        enqueue_worker_payload(
            engine,
            approved_queue,
            {
                "intent": trade_intent_to_json(intent),
                "risk": decision.model_dump(mode="json"),
                "trace_id": trace_id,
            },
        )
        log.info(
            "risk_%s signal_id=%s mode=%s final_size=%s",
            decision.outcome.value,
            signal.signal_id,
            signal.trading_mode.value,
            decision.final_size,
        )

    run_postgres_queue_worker(
        worker_name="risk-engine",
        engine=engine,
        queue_names=keys,
        stop_event=stop_event,
        health=health,
        handle_message=_handle_message,
        logger=log,
        poll_idle_seconds=DEFAULT_POLL_IDLE_SECONDS,
    )
    log.info("risk-engine shutdown complete")


if __name__ == "__main__":
    main()

from __future__ import annotations

import logging
import uuid

from oziebot_common.health import install_shutdown_handlers, start_health_server
from oziebot_common.queues import (
    QueueNames,
    disconnect_redis,
    push_json,
    strategy_signal_from_json,
    trade_intent_to_json,
)
from oziebot_common.worker_runtime import (
    DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    redis_client_for_worker,
    run_redis_queue_worker,
)
from oziebot_domain.risk import RiskOutcome
from oziebot_risk_engine.config import get_settings
from oziebot_risk_engine.service import RiskEngineService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("risk-engine")


def main() -> None:
    settings = get_settings()
    r = redis_client_for_worker(
        settings.redis_url,
        queue_pop_timeout_seconds=DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    )
    service = RiskEngineService(settings, r)
    health = start_health_server("risk-engine")
    stop_event = install_shutdown_handlers(
        "risk-engine",
        health_state=health,
        on_shutdown=lambda: disconnect_redis(r),
    )
    keys = QueueNames.all_signal_generated_keys()
    log.info("risk-engine listening on %s", keys)

    def _handle_message(_queue_key: str, raw: dict[str, object]) -> None:
        trace_id = str(raw.get("trace_id") or uuid.uuid4())
        signal = strategy_signal_from_json(raw["signal"])

        decision, intent = service.evaluate(signal, trace_id)
        if decision.outcome == RiskOutcome.REJECT or intent is None:
            push_json(
                r,
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

        push_json(
            r,
            QueueNames.intent_approved(signal.trading_mode),
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

    run_redis_queue_worker(
        worker_name="risk-engine",
        redis_client=r,
        queue_keys=keys,
        stop_event=stop_event,
        health=health,
        handle_message=_handle_message,
        logger=log,
        queue_pop_timeout_seconds=DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    )
    log.info("risk-engine shutdown complete")


if __name__ == "__main__":
    main()

from __future__ import annotations

import logging
import uuid

from oziebot_common.health import start_health_server
from oziebot_common.queues import (
    QueueNames,
    brpop_json_any,
    push_json,
    redis_from_url,
    strategy_signal_from_json,
    trade_intent_to_json,
)
from oziebot_domain.risk import RiskOutcome
from oziebot_risk_engine.config import get_settings
from oziebot_risk_engine.service import RiskEngineService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("risk-engine")


def main() -> None:
    settings = get_settings()
    r = redis_from_url(settings.redis_url)
    service = RiskEngineService(settings, r)
    health = start_health_server("risk-engine")
    keys = QueueNames.all_signal_generated_keys()
    log.info("risk-engine listening on %s", keys)
    health.mark_ready()
    while True:
        got = brpop_json_any(r, keys, timeout=30)
        health.touch()
        if got is None:
            continue
        _queue_key, raw = got
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
            continue

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
        health.touch()


if __name__ == "__main__":
    main()

from __future__ import annotations

from oziebot_common.queues import BOUNDED_QUEUE_MAX_LENGTH, QueueNames
from oziebot_common.worker_outbox import bounded_queue_cap
from oziebot_domain.trading_mode import TradingMode


def test_queue_names_stable() -> None:
    assert QueueNames.intent_submitted(TradingMode.PAPER).startswith(
        "oziebot:queue:intent_submitted:"
    )


def test_bounded_queue_cap_for_alerts_ops() -> None:
    assert bounded_queue_cap(QueueNames.ops_alerts()) == BOUNDED_QUEUE_MAX_LENGTH
    assert (
        bounded_queue_cap(QueueNames.alerts(TradingMode.LIVE))
        == BOUNDED_QUEUE_MAX_LENGTH
    )


def test_bounded_queue_cap_unbounded_signals() -> None:
    assert bounded_queue_cap(QueueNames.signal_generated(TradingMode.PAPER)) is None

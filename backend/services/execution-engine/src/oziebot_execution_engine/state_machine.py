from __future__ import annotations

from oziebot_domain.execution import ExecutionOrderStatus


_ALLOWED_TRANSITIONS: dict[ExecutionOrderStatus, set[ExecutionOrderStatus]] = {
    ExecutionOrderStatus.CREATED: {
        ExecutionOrderStatus.CAPITAL_RESERVED,
        ExecutionOrderStatus.SUBMITTED,
        ExecutionOrderStatus.FAILED,
    },
    ExecutionOrderStatus.CAPITAL_RESERVED: {
        ExecutionOrderStatus.SUBMITTED,
        ExecutionOrderStatus.CANCELLED,
        ExecutionOrderStatus.FAILED,
    },
    ExecutionOrderStatus.SUBMITTED: {
        ExecutionOrderStatus.PENDING,
        ExecutionOrderStatus.PARTIALLY_FILLED,
        ExecutionOrderStatus.FILLED,
        ExecutionOrderStatus.CANCELLED,
        ExecutionOrderStatus.FAILED,
    },
    ExecutionOrderStatus.PENDING: {
        ExecutionOrderStatus.PARTIALLY_FILLED,
        ExecutionOrderStatus.FILLED,
        ExecutionOrderStatus.CANCELLED,
        ExecutionOrderStatus.FAILED,
    },
    ExecutionOrderStatus.PARTIALLY_FILLED: {
        ExecutionOrderStatus.PARTIALLY_FILLED,
        ExecutionOrderStatus.FILLED,
        ExecutionOrderStatus.CANCELLED,
        ExecutionOrderStatus.FAILED,
    },
    ExecutionOrderStatus.FILLED: set(),
    ExecutionOrderStatus.CANCELLED: set(),
    ExecutionOrderStatus.FAILED: set(),
}


def ensure_transition(
    current: ExecutionOrderStatus, target: ExecutionOrderStatus
) -> None:
    if target == current:
        return
    if target not in _ALLOWED_TRANSITIONS[current]:
        raise ValueError(
            f"Invalid execution state transition: {current.value} -> {target.value}"
        )

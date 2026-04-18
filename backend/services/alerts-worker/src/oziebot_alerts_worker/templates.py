from __future__ import annotations

from oziebot_domain.events import NotificationEvent


def render_message(event: NotificationEvent) -> str:
    mode = event.trading_mode.value.upper()
    symbol = str(event.payload.get("symbol") or "")
    strategy = str(event.payload.get("strategy_id") or "")

    if event.event_type.value == "trade_opened":
        return f"[{mode}] Trade opened: {symbol} via {strategy}".strip()
    if event.event_type.value == "trade_closed":
        return f"[{mode}] Trade closed: {symbol} via {strategy}".strip()
    if event.event_type.value == "stop_loss_hit":
        return f"[{mode}] Stop loss hit for {symbol}".strip()
    if event.event_type.value == "take_profit_hit":
        return f"[{mode}] Take profit hit for {symbol}".strip()
    if event.event_type.value == "strategy_paused":
        return f"[{mode}] Strategy paused: {strategy}".strip()
    if event.event_type.value == "coinbase_connection_issue":
        return f"[{mode}] Coinbase connection issue detected".strip()
    if event.event_type.value == "insufficient_balance":
        return f"[{mode}] Insufficient balance for {symbol}".strip()
    if event.event_type.value == "daily_summary":
        return f"[{mode}] Daily summary ready".strip()
    return event.message or f"[{mode}] {event.event_type.value}"
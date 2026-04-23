from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from oziebot_common.trade_log import append_trade_log_event
from oziebot_common.trade_log_intelligence import (
    append_trade_log_sample,
    build_market_signal_snapshot,
    read_trade_log_samples,
    write_trade_log_summary,
)
from oziebot_domain.market_data import NormalizedBestBidAsk, NormalizedTrade


@dataclass
class SymbolPanelState:
    latest_bbo: NormalizedBestBidAsk | None = None
    latest_trade_time: datetime | None = None
    latest_trade_price: Decimal = Decimal("0")
    latest_trade_side: str = "neutral"
    price_high: Decimal = Decimal("0")
    price_low: Decimal = Decimal("0")
    trade_count: int = 0
    trade_volume: Decimal = Decimal("0")
    trade_notional_usd: Decimal = Decimal("0")
    buy_volume: Decimal = Decimal("0")
    sell_volume: Decimal = Decimal("0")
    last_sample_at: datetime | None = None
    last_snapshot_event_at: datetime | None = None
    last_derived_signature: tuple[str, str, str, str, str] | None = None

    def record_trade(self, trade: NormalizedTrade) -> None:
        self.latest_trade_time = trade.event_time
        self.latest_trade_price = trade.price
        self.latest_trade_side = trade.side
        self.trade_count += 1
        self.trade_volume += trade.size
        self.trade_notional_usd += trade.price * trade.size
        if trade.side == "buy":
            self.buy_volume += trade.size
        else:
            self.sell_volume += trade.size
        if self.price_low <= 0 or trade.price < self.price_low:
            self.price_low = trade.price
        if trade.price > self.price_high:
            self.price_high = trade.price

    def record_bbo(self, bbo: NormalizedBestBidAsk) -> None:
        self.latest_bbo = bbo

    def sample_due(self, now: datetime, *, interval_seconds: int) -> bool:
        if self.last_sample_at is None:
            return True
        return (now - self.last_sample_at).total_seconds() >= interval_seconds

    def snapshot_event_due(self, now: datetime, *, interval_seconds: int) -> bool:
        if self.last_snapshot_event_at is None:
            return True
        return (now - self.last_snapshot_event_at).total_seconds() >= interval_seconds

    def reset_interval(self) -> None:
        self.trade_count = 0
        self.trade_volume = Decimal("0")
        self.trade_notional_usd = Decimal("0")
        self.buy_volume = Decimal("0")
        self.sell_volume = Decimal("0")
        self.price_high = self.latest_trade_price
        self.price_low = self.latest_trade_price


@dataclass
class SignalPanelEmitter:
    client: Any
    retention_seconds: int = 60
    sample_interval_seconds: int = 5
    snapshot_event_interval_seconds: int = 15
    _states: dict[str, SymbolPanelState] = field(default_factory=dict)

    def observe_trade(self, trade: NormalizedTrade) -> dict[str, Any] | None:
        symbol = trade.product_id.upper()
        state = self._states.setdefault(symbol, SymbolPanelState())
        state.record_trade(trade)
        return self._maybe_emit(symbol, trade.ingest_time)

    def observe_bbo(self, bbo: NormalizedBestBidAsk) -> dict[str, Any] | None:
        symbol = bbo.product_id.upper()
        state = self._states.setdefault(symbol, SymbolPanelState())
        state.record_bbo(bbo)
        return self._maybe_emit(symbol, bbo.ingest_time)

    def force_emit(
        self, symbol: str, *, now: datetime | None = None
    ) -> dict[str, Any] | None:
        return self._maybe_emit(
            str(symbol).upper(), now or datetime.now(UTC), force_event=True
        )

    def _maybe_emit(
        self, symbol: str, now: datetime, *, force_event: bool = False
    ) -> dict[str, Any] | None:
        state = self._states.get(symbol)
        if state is None or state.latest_bbo is None:
            return None
        current = now.astimezone(UTC)
        if not force_event and not state.sample_due(
            current, interval_seconds=self.sample_interval_seconds
        ):
            return None

        sample = self._build_sample(symbol, state, current)
        append_trade_log_sample(
            self.client,
            symbol=symbol,
            sample=sample,
            timestamp=current,
            retention_seconds=self.retention_seconds,
        )
        samples = read_trade_log_samples(
            self.client,
            symbol=symbol,
            window_seconds=min(60, self.retention_seconds),
            now=current,
        )
        snapshot = build_market_signal_snapshot(symbol=symbol, samples=samples)
        state.last_sample_at = current
        state.reset_interval()
        if snapshot is None:
            return None

        write_trade_log_summary(
            self.client,
            symbol=symbol,
            summary=snapshot,
            retention_seconds=self.retention_seconds,
        )

        signature = self._signature(snapshot)
        state_changed = signature != state.last_derived_signature
        if (
            force_event
            or state.snapshot_event_due(
                current, interval_seconds=self.snapshot_event_interval_seconds
            )
            or state_changed
        ):
            append_trade_log_event(
                self.client,
                symbol=symbol,
                event_type="market_snapshot",
                message=self._snapshot_message(snapshot),
                timestamp=current,
                details=self._snapshot_details(snapshot),
            )
            state.last_snapshot_event_at = current
        if state_changed and state.last_derived_signature is not None:
            append_trade_log_event(
                self.client,
                symbol=symbol,
                event_type="derived_signal",
                message=self._derived_signal_message(snapshot),
                timestamp=current,
                details=self._derived_signal_details(snapshot),
            )
        state.last_derived_signature = signature
        return snapshot

    def _build_sample(
        self, symbol: str, state: SymbolPanelState, timestamp: datetime
    ) -> dict[str, Any]:
        latest_bbo = state.latest_bbo
        assert latest_bbo is not None
        mid_price = (latest_bbo.best_bid_price + latest_bbo.best_ask_price) / Decimal(
            "2"
        )
        spread_pct = Decimal("0")
        if mid_price > 0:
            spread_pct = (
                (latest_bbo.best_ask_price - latest_bbo.best_bid_price) / mid_price
            ) * Decimal("100")
        return {
            "timestamp": timestamp.isoformat(),
            "symbol": symbol,
            "best_bid": latest_bbo.best_bid_price,
            "best_ask": latest_bbo.best_ask_price,
            "bid_size": latest_bbo.best_bid_size,
            "ask_size": latest_bbo.best_ask_size,
            "mid_price": mid_price,
            "spread_pct": spread_pct,
            "last_price": state.latest_trade_price or mid_price,
            "last_side": state.latest_trade_side,
            "trade_count": state.trade_count,
            "trade_volume": state.trade_volume,
            "trade_notional_usd": state.trade_notional_usd,
            "buy_volume": state.buy_volume,
            "sell_volume": state.sell_volume,
            "price_high": state.price_high or state.latest_trade_price or mid_price,
            "price_low": state.price_low or state.latest_trade_price or mid_price,
        }

    def _signature(self, snapshot: dict[str, Any]) -> tuple[str, str, str, str, str]:
        market_state = snapshot.get("market_state") or {}
        return (
            str(market_state.get("trend") or ""),
            str(market_state.get("volatility") or ""),
            str(market_state.get("liquidity") or ""),
            str(market_state.get("trade_bias") or ""),
            str(snapshot.get("signal_quality_label") or ""),
        )

    def _snapshot_message(self, snapshot: dict[str, Any]) -> str:
        raw_metrics = snapshot.get("raw_metrics") or {}
        return (
            f"{snapshot['symbol']} | MARKET SNAPSHOT | "
            f"{snapshot['summary_line']} | "
            f"Spread {raw_metrics.get('spread_pct', '0')}% | "
            f"Vol(10s) ${raw_metrics.get('rolling_volume_10s_usd', '0')} | "
            f"Delta {raw_metrics.get('short_term_price_change_pct_10s', '0')}%"
        )

    def _snapshot_details(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            "summary_line": snapshot["summary_line"],
            "market_state": snapshot["market_state"],
            "signal_quality_score": snapshot["signal_quality_score"],
            "signal_quality_label": snapshot["signal_quality_label"],
            "derived_metrics": snapshot["raw_metrics"],
        }

    def _derived_signal_message(self, snapshot: dict[str, Any]) -> str:
        market_state = snapshot["market_state"]
        return (
            f"{snapshot['symbol']} derived signal | "
            f"score {snapshot['signal_quality_score']} ({snapshot['signal_quality_label']}) | "
            f"{market_state['trend']} / {market_state['trade_bias']}"
        )

    def _derived_signal_details(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return {
            "summary_line": snapshot["summary_line"],
            "market_state": snapshot["market_state"],
            "signal_quality_score": snapshot["signal_quality_score"],
            "signal_quality_label": snapshot["signal_quality_label"],
        }

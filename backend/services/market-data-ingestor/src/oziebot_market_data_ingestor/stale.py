from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class StaleThresholds:
    trade: int = 15
    bbo: int = 10
    candle: int = 120


@dataclass
class StaleDataDetector:
    thresholds: StaleThresholds
    _last_trade: dict[str, datetime] = field(default_factory=dict)
    _last_bbo: dict[str, datetime] = field(default_factory=dict)
    _last_candle: dict[str, datetime] = field(default_factory=dict)
    _candle_unavailable: set[str] = field(default_factory=set)

    def mark_trade(self, product_id: str, at: datetime) -> None:
        self._last_trade[product_id] = at

    def mark_bbo(self, product_id: str, at: datetime) -> None:
        self._last_bbo[product_id] = at

    def mark_candle(self, product_id: str, at: datetime) -> None:
        self._last_candle[product_id] = at
        self._candle_unavailable.discard(product_id)

    def mark_candle_unavailable(self, product_id: str) -> None:
        self._candle_unavailable.add(product_id)

    def prune(self, products: list[str]) -> None:
        active_products = {str(product_id) for product_id in products}
        for timestamps in (self._last_trade, self._last_bbo, self._last_candle):
            for product_id in list(timestamps):
                if product_id not in active_products:
                    del timestamps[product_id]
        self._candle_unavailable.intersection_update(active_products)

    def stale_products(
        self, now: datetime, products: list[str]
    ) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {"trade": [], "bbo": [], "candle": []}
        for p in products:
            if self._is_stale(now, self._last_trade.get(p), self.thresholds.trade):
                out["trade"].append(p)
            if self._is_stale(now, self._last_bbo.get(p), self.thresholds.bbo):
                out["bbo"].append(p)
            if p in self._candle_unavailable:
                continue
            if self._is_stale(now, self._last_candle.get(p), self.thresholds.candle):
                out["candle"].append(p)
        return out

    @staticmethod
    def _is_stale(now: datetime, last: datetime | None, threshold_seconds: int) -> bool:
        if last is None:
            return True
        return now - last > timedelta(seconds=threshold_seconds)

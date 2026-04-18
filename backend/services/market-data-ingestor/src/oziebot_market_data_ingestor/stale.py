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

    def mark_trade(self, product_id: str, at: datetime) -> None:
        self._last_trade[product_id] = at

    def mark_bbo(self, product_id: str, at: datetime) -> None:
        self._last_bbo[product_id] = at

    def mark_candle(self, product_id: str, at: datetime) -> None:
        self._last_candle[product_id] = at

    def stale_products(self, now: datetime, products: list[str]) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {"trade": [], "bbo": [], "candle": []}
        for p in products:
            if self._is_stale(now, self._last_trade.get(p), self.thresholds.trade):
                out["trade"].append(p)
            if self._is_stale(now, self._last_bbo.get(p), self.thresholds.bbo):
                out["bbo"].append(p)
            if self._is_stale(now, self._last_candle.get(p), self.thresholds.candle):
                out["candle"].append(p)
        return out

    @staticmethod
    def _is_stale(now: datetime, last: datetime | None, threshold_seconds: int) -> bool:
        if last is None:
            return True
        return now - last > timedelta(seconds=threshold_seconds)

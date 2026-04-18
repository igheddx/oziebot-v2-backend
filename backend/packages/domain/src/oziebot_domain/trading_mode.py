from enum import StrEnum


class TradingMode(StrEnum):
    """Partitions all trading data and execution paths; never mix PAPER with LIVE."""

    PAPER = "paper"
    LIVE = "live"

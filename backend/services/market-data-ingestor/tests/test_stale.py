from datetime import UTC, datetime, timedelta

from oziebot_market_data_ingestor.stale import StaleDataDetector, StaleThresholds


def test_stale_detector_flags_missing_and_old_data():
    detector = StaleDataDetector(StaleThresholds(trade=10, bbo=10, candle=30))
    now = datetime.now(UTC)

    detector.mark_trade("BTC-USD", now - timedelta(seconds=5))
    detector.mark_bbo("BTC-USD", now - timedelta(seconds=5))
    detector.mark_candle("BTC-USD", now - timedelta(seconds=20))

    detector.mark_trade("ETH-USD", now - timedelta(seconds=20))
    detector.mark_bbo("ETH-USD", now - timedelta(seconds=20))
    detector.mark_candle("ETH-USD", now - timedelta(seconds=40))

    stale = detector.stale_products(now, ["BTC-USD", "ETH-USD", "SOL-USD"])

    assert "ETH-USD" in stale["trade"]
    assert "ETH-USD" in stale["bbo"]
    assert "ETH-USD" in stale["candle"]
    assert "SOL-USD" in stale["trade"]
    assert "SOL-USD" in stale["bbo"]
    assert "SOL-USD" in stale["candle"]
    assert "BTC-USD" not in stale["trade"]


def test_stale_detector_prunes_removed_products():
    detector = StaleDataDetector(StaleThresholds(trade=10, bbo=10, candle=30))
    now = datetime.now(UTC)
    detector.mark_trade("BTC-USD", now)
    detector.mark_bbo("ETH-USD", now)
    detector.mark_candle("SOL-USD", now)

    detector.prune(["BTC-USD"])

    stale = detector.stale_products(now, ["BTC-USD"])
    assert stale == {"trade": [], "bbo": ["BTC-USD"], "candle": ["BTC-USD"]}


def test_stale_detector_waives_candles_when_market_has_none_yet():
    detector = StaleDataDetector(StaleThresholds(trade=10, bbo=10, candle=30))
    now = datetime.now(UTC)
    detector.mark_trade("IOTX-USD", now)
    detector.mark_bbo("IOTX-USD", now)
    detector.mark_candle_unavailable("IOTX-USD")

    stale = detector.stale_products(now + timedelta(seconds=120), ["IOTX-USD"])

    assert stale == {"trade": ["IOTX-USD"], "bbo": ["IOTX-USD"], "candle": []}

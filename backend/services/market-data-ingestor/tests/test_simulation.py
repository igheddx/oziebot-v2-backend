from oziebot_market_data_ingestor.simulation import simulated_coinbase_messages


def test_simulation_helper_emits_required_message_types():
    rows = simulated_coinbase_messages("BTC-USD")
    types = [r["type"] for r in rows]
    assert "match" in types
    assert "ticker" in types
    assert "snapshot" in types


def test_simulation_helper_product_id_consistent():
    rows = simulated_coinbase_messages("ETH-USD")
    assert all(r["product_id"] == "ETH-USD" for r in rows)

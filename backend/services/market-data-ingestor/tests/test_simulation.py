from oziebot_market_data_ingestor.simulation import simulated_coinbase_messages


def test_simulation_helper_emits_required_message_types():
    rows = simulated_coinbase_messages("BTC-USD")
    channels = [r["channel"] for r in rows]
    assert "market_trades" in channels
    assert "ticker" in channels
    assert "level2" in channels


def test_simulation_helper_product_id_consistent():
    rows = simulated_coinbase_messages("ETH-USD")
    product_ids = []
    for row in rows:
        for event in row["events"]:
            if "trades" in event:
                product_ids.extend(item["product_id"] for item in event["trades"])
            elif "tickers" in event:
                product_ids.extend(item["product_id"] for item in event["tickers"])
            elif "product_id" in event:
                product_ids.append(event["product_id"])
    assert all(product_id == "ETH-USD" for product_id in product_ids)

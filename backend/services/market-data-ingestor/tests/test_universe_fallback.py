from sqlalchemy import create_engine, text

from oziebot_market_data_ingestor.universe import SymbolUniverseProvider


def test_universe_falls_back_to_platform_tokens_when_no_user_permissions():
    eng = create_engine("sqlite+pysqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE platform_token_allowlist (id TEXT PRIMARY KEY, symbol TEXT, quote_currency TEXT, is_enabled BOOLEAN NOT NULL)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO platform_token_allowlist (id, symbol, quote_currency, is_enabled) VALUES "
                "('t1','BTC-USD','USD',1), ('t2','ETH-USD','USD',1), ('t3','SOL-USD','USD',0)"
            )
        )

    provider = SymbolUniverseProvider(eng)

    assert provider.list_active_product_ids() == ["BTC-USD", "ETH-USD"]

from sqlalchemy import create_engine, text

from oziebot_market_data_ingestor.universe import SymbolUniverseProvider


def test_universe_filters_platform_and_user_enabled_tokens():
    eng = create_engine("sqlite+pysqlite:///:memory:")
    with eng.begin() as conn:
        conn.execute(
            text("CREATE TABLE users (id TEXT PRIMARY KEY, is_active BOOLEAN NOT NULL)")
        )
        conn.execute(
            text(
                "CREATE TABLE platform_token_allowlist (id TEXT PRIMARY KEY, symbol TEXT, quote_currency TEXT, is_enabled BOOLEAN NOT NULL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE user_token_permissions (user_id TEXT, platform_token_id TEXT, is_enabled BOOLEAN NOT NULL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE user_strategies (user_id TEXT, strategy_id TEXT, is_enabled BOOLEAN NOT NULL, config TEXT NOT NULL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE execution_positions (user_id TEXT, symbol TEXT, quantity TEXT NOT NULL)"
            )
        )

        conn.execute(
            text("INSERT INTO users (id, is_active) VALUES ('u1', 1), ('u2', 0)")
        )
        conn.execute(
            text(
                "INSERT INTO platform_token_allowlist (id, symbol, quote_currency, is_enabled) VALUES "
                "('t1','BTC-USD','USD',1), ('t2','ETH-USD','USD',1), ('t3','SOL-USD','USD',0)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO user_token_permissions (user_id, platform_token_id, is_enabled) VALUES "
                "('u1','t1',1), ('u1','t2',0), ('u1','t3',1), ('u2','t1',1)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO execution_positions (user_id, symbol, quantity) VALUES "
                "('u1','ETH-USD','2'), ('u1','SOL-USD','0')"
            )
        )
        conn.execute(
            text(
                "INSERT INTO user_strategies (user_id, strategy_id, is_enabled, config) VALUES "
                "('u1','demo.momentum',1,'{\"symbol\":\"BTC-USD\"}')"
            )
        )

    provider = SymbolUniverseProvider(eng)
    out = provider.list_active_product_ids()
    assert out == ["BTC-USD", "ETH-USD"]

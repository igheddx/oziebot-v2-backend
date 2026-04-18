"""One-time restore script: migrates data from dev.db (SQLite) into PostgreSQL.

Uses raw psycopg (bypasses SQLAlchemy parameter parsing) so JSON strings with
colon-number patterns are not misinterpreted as bind parameters.
"""

from __future__ import annotations

from oziebot_api.config import get_settings
from oziebot_api.db.session import make_engine

# Capital was USD float in SQLite -> cents in Postgres (*100).
# allocation_pct in SQLite was already BPS.
STEPS = [
    # ---------------------------------------------------------------- users
    # Delete seed-created rows first (different UUIDs); restore SQLite originals.
    (
        "users – clear seed rows",
        """
        DELETE FROM users
        WHERE email IN ('dominic@oziebot.com', 'trader@example.com',
                        'dev@oziebot-example.com')
    """,
    ),
    (
        "users",
        """
        INSERT INTO users
          (id, email, password_hash, is_active, is_root_admin, created_at, updated_at)
        VALUES
          ('899e8cb8-4937-40cb-aa4b-8f2e51af58b9',
           'trader@example.com',
           '$pbkdf2-sha256$390000$FKIUQkiplRKidI6RsnbOmQ$P7tTVAQnUjOc03bZt9yYMtEeKw3nUww6E5Yn1W/FKx8',
           true, false,
           '2026-04-13 01:43:57.254761', '2026-04-13 01:43:57.254761'),
          ('a6953388-1b93-4c39-bace-e750f5f97431',
           'dominic@oziebot.com',
           '$pbkdf2-sha256$390000$ybn33htjrPU.55yTco5RCg$Vx4NasZK6EUT3/5UCoHlAHfOpNGdl0A8PCUxl0aYYXk',
           true, true,
           '2026-04-13 01:46:05.552225', '2026-04-13 03:03:22.462462')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # -------------------------------------------------------------- tenants
    (
        "tenants",
        """
        INSERT INTO tenants
          (id, name, created_at, default_trading_mode, trial_ends_at)
        VALUES
          ('80d464c9-cddf-4cfc-91a0-405a475ad355', 'My Trading',
           '2026-04-13 01:43:57.254761', 'paper', '2026-05-13 01:43:57.707100'),
          ('1cafada4-d71b-41ce-ab8f-e9146ed1c841', 'Oziebot Dev',
           '2026-04-13 01:46:05.552225', 'paper', '2026-05-13 01:46:06.029286')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # ---------------------------------------------------- tenant_memberships
    (
        "tenant_memberships",
        """
        INSERT INTO tenant_memberships
          (id, user_id, tenant_id, role, created_at)
        VALUES
          ('e30be35e-185e-4610-9f08-5cecce2fc870',
           '899e8cb8-4937-40cb-aa4b-8f2e51af58b9',
           '80d464c9-cddf-4cfc-91a0-405a475ad355',
           'user', '2026-04-13 01:43:57.254761'),
          ('2dc8aac7-5774-49b3-a49e-e2b9e186d1f5',
           'a6953388-1b93-4c39-bace-e750f5f97431',
           '1cafada4-d71b-41ce-ab8f-e9146ed1c841',
           'user', '2026-04-13 01:46:05.552225')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # --------------------------------------------------- tenant_entitlements
    (
        "tenant_entitlements",
        """
        INSERT INTO tenant_entitlements
          (id, tenant_id, platform_strategy_id, source, valid_from, valid_until,
           stripe_subscription_id, stripe_subscription_item_row_id, is_active, created_at, updated_at)
        VALUES
          ('d4c72547-7862-45af-8210-85cb6ad57127',
           '1cafada4-d71b-41ce-ab8f-e9146ed1c841',
           NULL,
           'root_admin',
           '2026-04-13 01:46:05.552225',
           NULL,
           NULL,
           NULL,
           true,
           '2026-04-13 01:46:05.552225',
           '2026-04-13 01:46:05.552225')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # ------------------------------------------ platform_token_allowlist
    (
        "platform_token_allowlist",
        """
        INSERT INTO platform_token_allowlist
          (id, symbol, quote_currency, network, display_name, is_enabled, sort_order,
           created_at, updated_at)
        VALUES
          ('49d4f3cc-ca6a-48e2-8ffd-a379d4252e28',
           'BTC-USD', 'USD', 'mainnet', 'Bitcoin / USD', true, 10,
           '2026-04-13 03:03:26.675552', '2026-04-13 09:24:32.947166'),
          ('cfc1afb6-6529-4df3-9f80-f5019d3fe67e',
           'ETH-USD', 'USD', 'mainnet', 'Ethereum / USD', true, 20,
           '2026-04-13 03:03:26.675552', '2026-04-13 09:23:10.609419'),
          ('6757f170-6fea-4284-a70c-e311c74491c0',
           'SOL-USD', 'USD', 'mainnet', 'SOL / USD - Volatility medium', true, 0,
           '2026-04-13 10:51:28.002289', '2026-04-13 10:51:28.002289')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # ------------------------------------------------- platform_strategies
    (
        "platform_strategies",
        r"""
        INSERT INTO platform_strategies
          (id, slug, display_name, description, is_enabled, entry_point,
           config_schema, sort_order, created_at, updated_at)
        VALUES
          ('39935cea-e3e7-428f-b32e-21c68bb219b4',
           'demo.momentum', 'Demo Momentum',
           'Example strategy entry for catalog',
           true, 'oziebot.strategies',
           '{"strategy_params":{"short_window":10,"long_window":40,"strength_threshold":0.02,"position_size":0.05},"risk_caps":{"max_position_usd":120,"max_daily_loss_pct":1.5,"max_open_positions":1},"signal_rules":{"min_confidence":0.75,"cooldown_seconds":180,"max_signals_per_day":12,"paper_only":true}}',
           10, '2026-04-13 03:03:26.675552', '2026-04-13 09:43:31.235429'),
          ('2609c9f7-8aec-4fe7-95f8-96e5d6200706',
           'momentum', 'Momentum', '',
           true, 'oziebot.strategies',
           '{"strategy_params":{"short_window":8,"long_window":34,"strength_threshold":0.012,"position_size_fraction":0.12,"stop_loss_pct":0.035,"take_profit_pct":0.08,"trailing_stop_pct":0.03,"max_hold_minutes":300},"risk_caps":{"max_position_usd":120},"signal_rules":{"min_confidence":0.6,"only_during_liquid_hours":false,"cooldown_seconds":45,"max_signals_per_day":150}}',
           0, '2026-04-13 11:08:17.107397', '2026-04-13 11:08:39.406554'),
          ('4f8af20f-5f87-43fe-ae0a-fcf82161db69',
           'day_trading', 'Day Trading',
           'Trades based on price momentum with moving averages.',
           true, 'oziebot.strategies',
           '{"strategy_params":{"entry_threshold":0.007,"exit_threshold":0.015,"stop_loss_pct":0.008,"position_size_fraction":0.08,"min_volume_multiplier":1.3,"min_volatility_pct":0.005,"require_trend_alignment":true,"min_entry_confirmations":1,"max_position_age_hours":3,"breakout_lookback_candles":5},"risk_caps":{"max_position_usd":80},"signal_rules":{"min_confidence":0.55,"only_during_liquid_hours":false,"cooldown_seconds":20,"max_signals_per_day":150}}',
           0, '2026-04-13 11:11:13.472270', '2026-04-13 11:11:13.472270'),
          ('b4c43bc8-5d72-aa9d-f233-179dc9d95e5e',
           'dca', 'Dollar Cost Averaging',
           'Regular fixed-amount purchases to build position over time',
           true, 'oziebot.strategies',
           '{"strategy_params":{"buy_amount_usd":50,"buy_interval_hours":24,"only_on_green_days":false},"risk_caps":{},"signal_rules":{"min_confidence":0.9,"only_during_liquid_hours":false,"cooldown_seconds":30,"max_signals_per_day":150}}',
           30, '2026-04-13 12:54:50+00', '2026-04-13 12:54:50+00'),
          ('bfc9ced9-7bdc-c393-4cb9-c935706ec233',
           'reversion', 'Mean Reversion',
           'Contrarian entries on oversold/overbought moves with RSI and optional fear-index filters',
           true, 'oziebot.strategies',
           '{"strategy_params":{"band_window":20,"rsi_period":14,"zscore_entry":1.6,"zscore_exit":0.4,"rsi_buy":30,"rsi_exit":50,"rsi_sell":65,"position_size_fraction":0.05,"stop_loss_pct":0.025,"take_profit_pct":0.04,"min_bandwidth":0.012,"max_hold_minutes":120,"use_fear_index_filter":false,"fear_index_buy_max":35,"fear_index_sell_min":60,"use_trend_filter":true,"ema_long_window":200},"risk_caps":{"max_position_usd":50},"signal_rules":{"min_confidence":0.6,"only_during_liquid_hours":false,"cooldown_seconds":60,"max_signals_per_day":150}}',
           40, '2026-04-13 12:54:50+00', '2026-04-13 12:58:56.593107')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # -------------------------------------------------- user_strategies
    (
        "user_strategies",
        """
        INSERT INTO user_strategies
          (id, user_id, strategy_id, is_enabled, config, metadata, created_at, updated_at)
        VALUES
          ('fd3eafe7-bc0e-455c-8e5f-77ee2de65f97',
           'a6953388-1b93-4c39-bace-e750f5f97431',
           'momentum', true, '{}', NULL,
           '2026-04-13 11:08:44.617105', '2026-04-13 11:10:02.665944'),
          ('4811dbcb-e225-34f8-7768-4736273fb191',
           'a6953388-1b93-4c39-bace-e750f5f97431',
           'day_trading', true, '{}', NULL,
           '2026-04-16 12:52:35+00', '2026-04-16 12:52:35+00'),
          ('a67a72d2-99bd-42ae-b6e1-c502532a799f',
           'a6953388-1b93-4c39-bace-e750f5f97431',
           'dca', true, '{}', '{"bootstrap":"root_admin"}',
           '2026-04-18 15:58:00+00', '2026-04-18 15:58:00+00'),
          ('f4bd6b9f-7d1d-4b17-97fc-4c2ef9695f0f',
           'a6953388-1b93-4c39-bace-e750f5f97431',
           'reversion', true, '{}', '{"bootstrap":"root_admin"}',
           '2026-04-18 15:58:00+00', '2026-04-18 15:58:00+00')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # ---------------------------------------------- user_token_permissions
    (
        "user_token_permissions",
        """
        INSERT INTO user_token_permissions
          (id, user_id, platform_token_id, is_enabled, created_at, updated_at)
        VALUES
          ('47c504c6-2f6c-4fe4-954d-b1d8e028da3c',
           'a6953388-1b93-4c39-bace-e750f5f97431',
           '49d4f3cc-ca6a-48e2-8ffd-a379d4252e28',
           true,
           '2026-04-13 10:50:47.220346', '2026-04-13 10:50:49.374279'),
          ('964bc66c-6e90-4603-a3ac-7fbec6b9c432',
           'a6953388-1b93-4c39-bace-e750f5f97431',
           'cfc1afb6-6529-4df3-9f80-f5019d3fe67e',
           true,
           '2026-04-13 10:50:50.274139', '2026-04-13 10:50:50.274144')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # ----------------------------------------- strategy_allocation_plans
    (
        "strategy_allocation_plans",
        """
        INSERT INTO strategy_allocation_plans
          (id, user_id, trading_mode, allocation_mode, preset_name,
           total_capital_cents, created_at, updated_at)
        VALUES
          ('9839f498-2583-4162-9651-bf9971932224',
           'a6953388-1b93-4c39-bace-e750f5f97431',
           'paper', 'manual', NULL, 10000000,
           '2026-04-13 12:19:19.339933', '2026-04-13 12:56:37.819213')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # ----------------------------------------- strategy_allocation_items
    (
        "strategy_allocation_items",
        """
        INSERT INTO strategy_allocation_items
          (id, plan_id, strategy_id, allocation_bps, assigned_capital_cents,
           created_at, updated_at)
        VALUES
          ('58ccf61a-28c6-4308-96e2-cdd5c7e12ad8',
           '9839f498-2583-4162-9651-bf9971932224',
           'momentum', 2900, 2900000,
           '2026-04-13 12:19:19.339933', '2026-04-13 12:56:37.819213'),
          ('be9fa338-4d26-4bcc-9b08-a12ebdec1beb',
           '9839f498-2583-4162-9651-bf9971932224',
           'day_trading', 2500, 2500000,
           '2026-04-13 12:25:02.452235', '2026-04-13 12:56:37.819213'),
          ('3aad9f37-5fe2-45cc-9f97-c03ba9c550da',
           '9839f498-2583-4162-9651-bf9971932224',
           'demo.momentum', 2900, 2900000,
           '2026-04-13 12:25:02.452235', '2026-04-13 12:56:37.819213'),
          ('0e668f4f-251d-4ede-babb-afe5ae876f10',
           '9839f498-2583-4162-9651-bf9971932224',
           'dca', 1700, 1700000,
           '2026-04-13 12:56:37.819213', '2026-04-13 12:56:37.819213'),
          ('a9339c4d-4a4f-4bd5-8d12-f0e8caf971e8',
           '9839f498-2583-4162-9651-bf9971932224',
           'reversion', 0, 0,
           '2026-04-13 12:56:37.819213', '2026-04-13 12:56:37.819213')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
    # ------------------------------------------------ tenant_integrations
    (
        "tenant_integrations",
        """
        INSERT INTO tenant_integrations
          (tenant_id, coinbase_connected, updated_at,
           coinbase_last_check_at, coinbase_health_status, coinbase_last_error)
        VALUES
          ('80d464c9-cddf-4cfc-91a0-405a475ad355',
           false, '2026-04-13 01:43:57.254761', NULL, NULL, NULL),
          ('1cafada4-d71b-41ce-ab8f-e9146ed1c841',
           true, '2026-04-13 02:58:05.755812',
           '2026-04-13 02:58:05.755812', 'healthy', NULL)
        ON CONFLICT (tenant_id) DO NOTHING
    """,
    ),
    # ----------------------------------------------- exchange_connections
    (
        "exchange_connections",
        """
        INSERT INTO exchange_connections
          (id, tenant_id, provider, api_key_name, encrypted_secret,
           secret_ciphertext_version,
           validation_status, last_validated_at,
           health_status, last_health_check_at, last_error,
           can_trade, can_read_balances, created_at, updated_at)
        VALUES
          ('f684ac46-e85e-44e8-8e6d-837f194b45af',
           '1cafada4-d71b-41ce-ab8f-e9146ed1c841',
           'coinbase',
           'organizations/cc3528dd-d17d-4ade-b27c-44094c7c28b2/apiKeys/fe0291ae-7e80-42b0-91d4-9d924f867af3',
           convert_to(
             'gAAAAABp3Fu92THo0A_OqB6WBk3f9xk8Us5hC1LUsS1ilx9hNEA6aXWAT-6uL7Xx53Jy1ek9Rr-et9FO01FS-o2bJDk2ZvPDNZfPr-VepKHS6RiCRkv0xWT-seqsKdSz0wO_iOa8oirAUvZGJ42562hRDnJWsSiT5N3I02Lb83kdNjAiDKy2Z7BVmazlwQB4HSqBm-5RqMrUf3xxM3Xg9ItevyhE9sW3wO8Uf5Ysv10MAsWAZ7euaRf8yCT7q-CzBFNKbeoLCfICEbyWCKRooR6a2cyO_HvmoBjNdIQHN2JeX3ZRih-nyn_7ai7yiGNiGynYzRX6fuQjKsGWpVAvZOHZTBlkza2GeOxukvVHjqYD6jtp_263dWvZcKi2rk7OBHHF_EDVj9MO',
             'UTF8'),
           1,
           'valid', '2026-04-13 02:58:05.755812',
           'healthy', '2026-04-13 02:58:05.755812', NULL,
           true, true,
           '2026-04-13 02:58:05.755812', '2026-04-13 02:58:05.755812')
        ON CONFLICT (id) DO NOTHING
    """,
    ),
]


def run() -> None:
    settings = get_settings()
    engine = make_engine(settings)
    if engine is None:
        raise SystemExit("Could not create database engine")

    # Use raw psycopg connection to bypass SQLAlchemy colon-parameter parsing.
    raw_conn = engine.raw_connection()
    try:
        for label, sql in STEPS:
            cursor = raw_conn.cursor()
            try:
                cursor.execute(sql)
                raw_conn.commit()
                print(f"OK  {label}")
            except Exception as exc:
                raw_conn.rollback()
                print(f"ERR {label} — {exc}")
            finally:
                cursor.close()
    finally:
        raw_conn.close()

    print("\nRestore complete.")


if __name__ == "__main__":
    run()

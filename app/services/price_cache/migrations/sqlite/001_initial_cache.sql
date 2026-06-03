CREATE TABLE IF NOT EXISTS price_cache_schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_cache_versions (
    cache_version_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    refresh_type TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    activated_at TEXT,
    triggered_by TEXT,
    source_host TEXT,
    source_env TEXT,
    rows_prices INTEGER NOT NULL DEFAULT 0,
    rows_commodities INTEGER NOT NULL DEFAULT 0,
    rows_markets INTEGER NOT NULL DEFAULT 0,
    rows_countries INTEGER NOT NULL DEFAULT 0,
    validation_summary_json TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS price_cache_active_version (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    activated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cached_countries (
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 TEXT NOT NULL,
    country_name TEXT NOT NULL,
    currency_code TEXT,
    currency_name TEXT,
    latest_price_date TEXT,
    PRIMARY KEY (cache_version_id, country_iso3)
);

CREATE TABLE IF NOT EXISTS cached_units (
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    commodity_unit_id INTEGER NOT NULL,
    commodity_unit_name TEXT NOT NULL,
    conversion_to_kg_l NUMERIC,
    active INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (cache_version_id, commodity_unit_id)
);

CREATE TABLE IF NOT EXISTS cached_commodities (
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 TEXT NOT NULL,
    commodity_id INTEGER NOT NULL,
    commodity_name TEXT NOT NULL,
    commodity_unit_id INTEGER,
    commodity_unit_name TEXT,
    category_name TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (cache_version_id, country_iso3, commodity_id)
);

CREATE TABLE IF NOT EXISTS cached_markets (
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 TEXT NOT NULL,
    market_id INTEGER NOT NULL,
    market_name TEXT NOT NULL,
    admin1_name TEXT,
    admin2_name TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    active INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (cache_version_id, country_iso3, market_id)
);

CREATE TABLE IF NOT EXISTS cached_currencies (
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    currency_id INTEGER NOT NULL,
    currency_code TEXT,
    currency_name TEXT NOT NULL,
    PRIMARY KEY (cache_version_id, currency_id)
);

CREATE TABLE IF NOT EXISTS cached_price_monthly (
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 TEXT NOT NULL,
    commodity_id INTEGER NOT NULL,
    market_id INTEGER NOT NULL,
    price_date TEXT NOT NULL,
    price NUMERIC NOT NULL,
    currency_id INTEGER,
    currency_code TEXT,
    currency_name TEXT,
    commodity_unit_id INTEGER,
    commodity_unit_name TEXT,
    price_type_id INTEGER,
    price_type_name TEXT NOT NULL DEFAULT '',
    price_flag TEXT NOT NULL DEFAULT '',
    original_frequency TEXT,
    observations INTEGER,
    source_payload_hash TEXT,
    admin1_name TEXT,
    admin2_name TEXT,
    market_name TEXT,
    commodity_name TEXT,
    data_source TEXT,
    PRIMARY KEY (
        cache_version_id,
        country_iso3,
        commodity_id,
        market_id,
        price_date,
        price_type_name,
        price_flag
    )
);

CREATE INDEX IF NOT EXISTS idx_cached_price_country_date
    ON cached_price_monthly(cache_version_id, country_iso3, price_date);

CREATE INDEX IF NOT EXISTS idx_cached_price_country_commodity_date
    ON cached_price_monthly(cache_version_id, country_iso3, commodity_id, price_date);

CREATE INDEX IF NOT EXISTS idx_cached_price_country_market_date
    ON cached_price_monthly(cache_version_id, country_iso3, market_id, price_date);

CREATE INDEX IF NOT EXISTS idx_cached_commodities_country_name
    ON cached_commodities(cache_version_id, country_iso3, commodity_name);

CREATE INDEX IF NOT EXISTS idx_cached_markets_country_admin
    ON cached_markets(cache_version_id, country_iso3, admin1_name);

CREATE TABLE IF NOT EXISTS price_cache_country_active_versions (
    country_iso3 TEXT PRIMARY KEY,
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    activated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_price_cache_country_active_version
    ON price_cache_country_active_versions(cache_version_id);

CREATE TABLE IF NOT EXISTS price_cache_version_countries (
    cache_version_id TEXT NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_prices INTEGER NOT NULL DEFAULT 0,
    rows_commodities INTEGER NOT NULL DEFAULT 0,
    rows_markets INTEGER NOT NULL DEFAULT 0,
    latest_price_date TEXT,
    validation_summary_json TEXT,
    error_message TEXT,
    started_at TEXT,
    completed_at TEXT,
    PRIMARY KEY (cache_version_id, country_iso3)
);

CREATE INDEX IF NOT EXISTS idx_price_cache_version_countries_country
    ON price_cache_version_countries(country_iso3, completed_at);

CREATE TABLE IF NOT EXISTS price_cache_refresh_locks (
    lock_name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS price_cache_country_active_versions (
    country_iso3 text PRIMARY KEY,
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    activated_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_price_cache_country_active_version
    ON price_cache_country_active_versions(cache_version_id);

CREATE TABLE IF NOT EXISTS price_cache_version_countries (
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 text NOT NULL,
    status text NOT NULL,
    rows_prices integer NOT NULL DEFAULT 0,
    rows_commodities integer NOT NULL DEFAULT 0,
    rows_markets integer NOT NULL DEFAULT 0,
    latest_price_date date,
    validation_summary_json jsonb,
    error_message text,
    started_at timestamptz,
    completed_at timestamptz,
    PRIMARY KEY (cache_version_id, country_iso3)
);

CREATE INDEX IF NOT EXISTS idx_price_cache_version_countries_country
    ON price_cache_version_countries(country_iso3, completed_at);

CREATE TABLE IF NOT EXISTS price_cache_refresh_locks (
    lock_name text PRIMARY KEY,
    owner text NOT NULL,
    acquired_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL
);

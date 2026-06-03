CREATE TABLE IF NOT EXISTS price_cache_schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS price_cache_versions (
    cache_version_id uuid PRIMARY KEY,
    status text NOT NULL,
    refresh_type text NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    activated_at timestamptz,
    triggered_by text,
    source_host text,
    source_env text,
    rows_prices integer NOT NULL DEFAULT 0,
    rows_commodities integer NOT NULL DEFAULT 0,
    rows_markets integer NOT NULL DEFAULT 0,
    rows_countries integer NOT NULL DEFAULT 0,
    validation_summary_json jsonb,
    error_message text
);

CREATE TABLE IF NOT EXISTS price_cache_active_version (
    singleton_id integer PRIMARY KEY CHECK (singleton_id = 1),
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    activated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS cached_countries (
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 text NOT NULL,
    country_name text NOT NULL,
    currency_code text,
    currency_name text,
    latest_price_date date,
    PRIMARY KEY (cache_version_id, country_iso3)
);

CREATE TABLE IF NOT EXISTS cached_units (
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    commodity_unit_id integer NOT NULL,
    commodity_unit_name text NOT NULL,
    conversion_to_kg_l numeric,
    active boolean NOT NULL DEFAULT true,
    PRIMARY KEY (cache_version_id, commodity_unit_id)
);

CREATE TABLE IF NOT EXISTS cached_commodities (
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 text NOT NULL,
    commodity_id integer NOT NULL,
    commodity_name text NOT NULL,
    commodity_unit_id integer,
    commodity_unit_name text,
    category_name text,
    active boolean NOT NULL DEFAULT true,
    PRIMARY KEY (cache_version_id, country_iso3, commodity_id)
);

CREATE TABLE IF NOT EXISTS cached_markets (
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 text NOT NULL,
    market_id integer NOT NULL,
    market_name text NOT NULL,
    admin1_name text,
    admin2_name text,
    latitude numeric,
    longitude numeric,
    active boolean NOT NULL DEFAULT true,
    PRIMARY KEY (cache_version_id, country_iso3, market_id)
);

CREATE TABLE IF NOT EXISTS cached_currencies (
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    currency_id integer NOT NULL,
    currency_code text,
    currency_name text NOT NULL,
    PRIMARY KEY (cache_version_id, currency_id)
);

CREATE TABLE IF NOT EXISTS cached_price_monthly (
    cache_version_id uuid NOT NULL REFERENCES price_cache_versions(cache_version_id),
    country_iso3 text NOT NULL,
    commodity_id integer NOT NULL,
    market_id integer NOT NULL,
    price_date date NOT NULL,
    price numeric NOT NULL,
    currency_id integer,
    currency_code text,
    currency_name text,
    commodity_unit_id integer,
    commodity_unit_name text,
    price_type_id integer,
    price_type_name text NOT NULL DEFAULT '',
    price_flag text NOT NULL DEFAULT '',
    original_frequency text,
    observations integer,
    source_payload_hash text,
    admin1_name text,
    admin2_name text,
    market_name text,
    commodity_name text,
    data_source text,
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

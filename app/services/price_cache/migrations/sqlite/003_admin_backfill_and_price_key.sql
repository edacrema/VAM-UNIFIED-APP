-- F1: backfill admin/market metadata onto price rows from cached markets.
UPDATE cached_price_monthly
SET admin1_name = (
        SELECT m.admin1_name FROM cached_markets m
        WHERE m.cache_version_id = cached_price_monthly.cache_version_id
          AND m.country_iso3 = cached_price_monthly.country_iso3
          AND m.market_id = cached_price_monthly.market_id
    )
WHERE (admin1_name IS NULL OR admin1_name = '');

UPDATE cached_price_monthly
SET admin2_name = (
        SELECT m.admin2_name FROM cached_markets m
        WHERE m.cache_version_id = cached_price_monthly.cache_version_id
          AND m.country_iso3 = cached_price_monthly.country_iso3
          AND m.market_id = cached_price_monthly.market_id
    )
WHERE (admin2_name IS NULL OR admin2_name = '');

UPDATE cached_price_monthly
SET market_name = (
        SELECT m.market_name FROM cached_markets m
        WHERE m.cache_version_id = cached_price_monthly.cache_version_id
          AND m.country_iso3 = cached_price_monthly.country_iso3
          AND m.market_id = cached_price_monthly.market_id
    )
WHERE (market_name IS NULL OR market_name = '');

-- F2: rebuild the table without the too-coarse primary key. SQLite cannot drop a
-- primary key in place, so rename, recreate, copy, and re-index.
ALTER TABLE cached_price_monthly RENAME TO cached_price_monthly_v002;

CREATE TABLE cached_price_monthly (
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
    data_source TEXT
);

INSERT INTO cached_price_monthly (
    cache_version_id, country_iso3, commodity_id, market_id, price_date, price,
    currency_id, currency_code, currency_name, commodity_unit_id, commodity_unit_name,
    price_type_id, price_type_name, price_flag, original_frequency, observations,
    source_payload_hash, admin1_name, admin2_name, market_name, commodity_name, data_source
)
SELECT
    cache_version_id, country_iso3, commodity_id, market_id, price_date, price,
    currency_id, currency_code, currency_name, commodity_unit_id, commodity_unit_name,
    price_type_id, price_type_name, price_flag, original_frequency, observations,
    source_payload_hash, admin1_name, admin2_name, market_name, commodity_name, data_source
FROM cached_price_monthly_v002;

DROP TABLE cached_price_monthly_v002;

CREATE INDEX IF NOT EXISTS idx_cached_price_country_date
    ON cached_price_monthly(cache_version_id, country_iso3, price_date);

CREATE INDEX IF NOT EXISTS idx_cached_price_country_commodity_date
    ON cached_price_monthly(cache_version_id, country_iso3, commodity_id, price_date);

CREATE INDEX IF NOT EXISTS idx_cached_price_country_market_date
    ON cached_price_monthly(cache_version_id, country_iso3, market_id, price_date);

CREATE UNIQUE INDEX IF NOT EXISTS ux_cached_price_monthly_canonical
    ON cached_price_monthly(
        cache_version_id,
        country_iso3,
        commodity_id,
        market_id,
        price_date,
        price_type_name,
        price_flag,
        COALESCE(price_type_id, -1),
        COALESCE(currency_id, -1),
        COALESCE(commodity_unit_id, -1)
    );

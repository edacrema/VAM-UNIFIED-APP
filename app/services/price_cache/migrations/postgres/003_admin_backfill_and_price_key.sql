-- F1: the Databridges PriceMonthly DTO carries no admin fields, so price rows
-- cached by earlier worker versions hold NULL admin1/admin2. Backfill them from
-- the market metadata captured in the same cache version.
UPDATE cached_price_monthly p
SET admin1_name = m.admin1_name,
    admin2_name = COALESCE(NULLIF(p.admin2_name, ''), m.admin2_name),
    market_name = COALESCE(NULLIF(p.market_name, ''), m.market_name)
FROM cached_markets m
WHERE m.cache_version_id = p.cache_version_id
  AND m.country_iso3 = p.country_iso3
  AND m.market_id = p.market_id
  AND (p.admin1_name IS NULL OR p.admin1_name = '');

-- F2: the original primary key omitted currency/unit/price-type identifiers, so
-- distinct series (e.g. dual-currency quotes) collided. Replace it with a unique
-- index over the extended canonical key.
ALTER TABLE cached_price_monthly DROP CONSTRAINT IF EXISTS cached_price_monthly_pkey;

CREATE UNIQUE INDEX IF NOT EXISTS ux_cached_price_monthly_canonical
    ON cached_price_monthly (
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

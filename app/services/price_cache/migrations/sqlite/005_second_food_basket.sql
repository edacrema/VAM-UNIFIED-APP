ALTER TABLE country_food_basket_versions
    ADD COLUMN basket_role TEXT NOT NULL DEFAULT 'primary'
        CHECK (basket_role IN ('primary', 'secondary'));

ALTER TABLE country_food_basket_versions
    ADD COLUMN basket_name TEXT NOT NULL DEFAULT 'MEB'
        CHECK (TRIM(basket_name) <> '');

ALTER TABLE country_food_basket_versions
    ADD COLUMN short_description TEXT DEFAULT 'Primary MEB reference basket configured by the Country Office.';

ALTER TABLE country_food_basket_versions
    ADD COLUMN scope_type TEXT NOT NULL DEFAULT 'national'
        CHECK (scope_type IN ('national', 'selected_regions'));

CREATE UNIQUE INDEX IF NOT EXISTS uq_country_food_basket_versions_role_id
    ON country_food_basket_versions(country_iso3, basket_role, basket_version_id);

ALTER TABLE country_food_basket_current
    RENAME TO country_food_basket_current_legacy;

CREATE TABLE country_food_basket_current (
    country_iso3 TEXT NOT NULL,
    basket_role TEXT NOT NULL CHECK (basket_role IN ('primary', 'secondary')),
    active_basket_version_id TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    updated_by_user_id TEXT NOT NULL DEFAULT 'unknown',
    PRIMARY KEY (country_iso3, basket_role),
    FOREIGN KEY (country_iso3, basket_role, active_basket_version_id)
        REFERENCES country_food_basket_versions(country_iso3, basket_role, basket_version_id)
);

INSERT INTO country_food_basket_current (
    country_iso3,
    basket_role,
    active_basket_version_id,
    updated_at,
    updated_by_user_id
)
SELECT
    country_iso3,
    'primary',
    active_basket_version_id,
    updated_at,
    updated_by_user_id
FROM country_food_basket_current_legacy;

DROP TABLE country_food_basket_current_legacy;

CREATE TABLE country_food_basket_regions (
    basket_region_id TEXT PRIMARY KEY,
    basket_version_id TEXT NOT NULL REFERENCES country_food_basket_versions(basket_version_id),
    region_name TEXT NOT NULL CHECK (TRIM(region_name) <> ''),
    sort_order INTEGER NOT NULL CHECK (sort_order > 0),
    UNIQUE (basket_version_id, region_name)
);

CREATE INDEX IF NOT EXISTS idx_country_food_basket_versions_role_history
    ON country_food_basket_versions(country_iso3, basket_role, status, version_number);

CREATE INDEX IF NOT EXISTS idx_country_food_basket_regions_version
    ON country_food_basket_regions(basket_version_id, sort_order);

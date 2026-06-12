CREATE TABLE IF NOT EXISTS country_food_basket_versions (
    basket_version_id TEXT PRIMARY KEY,
    country_iso3 TEXT NOT NULL,
    version_number INTEGER NOT NULL CHECK (version_number > 0),
    status TEXT NOT NULL CHECK (status IN ('active', 'superseded', 'archived')),
    created_at TEXT NOT NULL,
    created_by_user_id TEXT NOT NULL DEFAULT 'unknown',
    cache_version_id_at_creation TEXT,
    change_note TEXT,
    UNIQUE (country_iso3, basket_version_id),
    UNIQUE (country_iso3, version_number)
);

CREATE TABLE IF NOT EXISTS country_food_basket_items (
    basket_item_id TEXT PRIMARY KEY,
    basket_version_id TEXT NOT NULL REFERENCES country_food_basket_versions(basket_version_id),
    commodity_id INTEGER NOT NULL,
    commodity_name_snapshot TEXT NOT NULL,
    databridges_unit_id INTEGER,
    databridges_unit TEXT NOT NULL,
    weight_quantity NUMERIC NOT NULL CHECK (weight_quantity > 0),
    sort_order INTEGER NOT NULL,
    item_note TEXT,
    UNIQUE (basket_version_id, commodity_id)
);

CREATE TABLE IF NOT EXISTS country_food_basket_current (
    country_iso3 TEXT PRIMARY KEY,
    active_basket_version_id TEXT NOT NULL REFERENCES country_food_basket_versions(basket_version_id),
    updated_at TEXT NOT NULL,
    updated_by_user_id TEXT NOT NULL DEFAULT 'unknown',
    FOREIGN KEY (country_iso3, active_basket_version_id)
        REFERENCES country_food_basket_versions(country_iso3, basket_version_id)
);

CREATE INDEX IF NOT EXISTS idx_country_food_basket_versions_country
    ON country_food_basket_versions(country_iso3, status, version_number);

CREATE INDEX IF NOT EXISTS idx_country_food_basket_items_version
    ON country_food_basket_items(basket_version_id, sort_order);

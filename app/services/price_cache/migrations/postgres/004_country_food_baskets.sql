CREATE TABLE IF NOT EXISTS country_food_basket_versions (
    basket_version_id uuid PRIMARY KEY,
    country_iso3 text NOT NULL,
    version_number integer NOT NULL CHECK (version_number > 0),
    status text NOT NULL CHECK (status IN ('active', 'superseded', 'archived')),
    created_at timestamptz NOT NULL,
    created_by_user_id text NOT NULL DEFAULT 'unknown',
    cache_version_id_at_creation uuid,
    change_note text,
    UNIQUE (country_iso3, basket_version_id),
    UNIQUE (country_iso3, version_number)
);

CREATE TABLE IF NOT EXISTS country_food_basket_items (
    basket_item_id uuid PRIMARY KEY,
    basket_version_id uuid NOT NULL REFERENCES country_food_basket_versions(basket_version_id),
    commodity_id integer NOT NULL,
    commodity_name_snapshot text NOT NULL,
    databridges_unit_id integer,
    databridges_unit text NOT NULL,
    weight_quantity numeric NOT NULL CHECK (weight_quantity > 0),
    sort_order integer NOT NULL,
    item_note text,
    UNIQUE (basket_version_id, commodity_id)
);

CREATE TABLE IF NOT EXISTS country_food_basket_current (
    country_iso3 text PRIMARY KEY,
    active_basket_version_id uuid NOT NULL REFERENCES country_food_basket_versions(basket_version_id),
    updated_at timestamptz NOT NULL,
    updated_by_user_id text NOT NULL DEFAULT 'unknown',
    FOREIGN KEY (country_iso3, active_basket_version_id)
        REFERENCES country_food_basket_versions(country_iso3, basket_version_id)
);

CREATE INDEX IF NOT EXISTS idx_country_food_basket_versions_country
    ON country_food_basket_versions(country_iso3, status, version_number);

CREATE INDEX IF NOT EXISTS idx_country_food_basket_items_version
    ON country_food_basket_items(basket_version_id, sort_order);

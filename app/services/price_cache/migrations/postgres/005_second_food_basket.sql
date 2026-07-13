ALTER TABLE country_food_basket_versions
    ADD COLUMN basket_role text NOT NULL DEFAULT 'primary';

ALTER TABLE country_food_basket_versions
    ADD COLUMN basket_name text NOT NULL DEFAULT 'MEB';

ALTER TABLE country_food_basket_versions
    ADD COLUMN short_description text DEFAULT 'Primary MEB reference basket configured by the Country Office.';

ALTER TABLE country_food_basket_versions
    ADD COLUMN scope_type text NOT NULL DEFAULT 'national';

ALTER TABLE country_food_basket_versions
    ADD CONSTRAINT ck_country_food_basket_versions_role
        CHECK (basket_role IN ('primary', 'secondary'));

ALTER TABLE country_food_basket_versions
    ADD CONSTRAINT ck_country_food_basket_versions_name
        CHECK (btrim(basket_name) <> '');

ALTER TABLE country_food_basket_versions
    ADD CONSTRAINT ck_country_food_basket_versions_scope
        CHECK (scope_type IN ('national', 'selected_regions'));

CREATE UNIQUE INDEX uq_country_food_basket_versions_role_id
    ON country_food_basket_versions(country_iso3, basket_role, basket_version_id);

ALTER TABLE country_food_basket_current
    ADD COLUMN basket_role text NOT NULL DEFAULT 'primary';

ALTER TABLE country_food_basket_current
    DROP CONSTRAINT country_food_basket_current_pkey;

ALTER TABLE country_food_basket_current
    ADD CONSTRAINT pk_country_food_basket_current
        PRIMARY KEY (country_iso3, basket_role);

ALTER TABLE country_food_basket_current
    ADD CONSTRAINT ck_country_food_basket_current_role
        CHECK (basket_role IN ('primary', 'secondary'));

ALTER TABLE country_food_basket_current
    ADD CONSTRAINT fk_country_food_basket_current_role_version
        FOREIGN KEY (country_iso3, basket_role, active_basket_version_id)
        REFERENCES country_food_basket_versions(country_iso3, basket_role, basket_version_id);

CREATE TABLE country_food_basket_regions (
    basket_region_id uuid PRIMARY KEY,
    basket_version_id uuid NOT NULL REFERENCES country_food_basket_versions(basket_version_id),
    region_name text NOT NULL CHECK (btrim(region_name) <> ''),
    sort_order integer NOT NULL CHECK (sort_order > 0),
    UNIQUE (basket_version_id, region_name)
);

CREATE INDEX idx_country_food_basket_versions_role_history
    ON country_food_basket_versions(country_iso3, basket_role, status, version_number);

CREATE INDEX idx_country_food_basket_regions_version
    ON country_food_basket_regions(basket_version_id, sort_order);

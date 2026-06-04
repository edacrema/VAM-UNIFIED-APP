from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text

from .sql_repository import SqlPriceCacheRepository


def seed_cache_snapshot(
    repository: SqlPriceCacheRepository,
    *,
    cache_version_id: str = "11111111-1111-1111-1111-111111111111",
    activate: bool = True,
    price_offset: float = 0.0,
    country_iso3: str = "SSD",
    country_name: str = "South Sudan",
) -> str:
    now = datetime.now(timezone.utc).isoformat()
    with repository.engine.begin() as conn:
        for table in (
            "cached_price_monthly",
            "cached_currencies",
            "cached_markets",
            "cached_commodities",
            "cached_units",
            "cached_countries",
        ):
            conn.execute(
                text(f"DELETE FROM {table} WHERE cache_version_id = :cache_version_id"),
                {"cache_version_id": cache_version_id},
            )
        conn.execute(
            text("DELETE FROM price_cache_versions WHERE cache_version_id = :cache_version_id"),
            {"cache_version_id": cache_version_id},
        )
        if activate:
            conn.execute(text("UPDATE price_cache_versions SET status = 'superseded' WHERE status = 'active'"))

        conn.execute(
            text(
                """
                INSERT INTO price_cache_versions (
                    cache_version_id,
                    status,
                    refresh_type,
                    started_at,
                    completed_at,
                    activated_at,
                    triggered_by,
                    source_host,
                    source_env,
                    rows_prices,
                    rows_commodities,
                    rows_markets,
                    rows_countries,
                    validation_summary_json
                ) VALUES (
                    :cache_version_id,
                    :status,
                    'fixture',
                    :now,
                    :now,
                    :activated_at,
                    'pytest',
                    'fixture',
                    'test',
                    4,
                    2,
                    2,
                    1,
                    :validation_summary_json
                )
                """
            ),
            {
                "cache_version_id": cache_version_id,
                "status": "active" if activate else "building",
                "now": now,
                "activated_at": now if activate else None,
                "validation_summary_json": json.dumps({"fixture": True}),
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO cached_countries (
                    cache_version_id,
                    country_iso3,
                    country_name,
                    currency_code,
                    currency_name,
                    latest_price_date
                ) VALUES (
                    :cache_version_id,
                    :country_iso3,
                    :country_name,
                    'SSP',
                    'South Sudanese Pound',
                    '2025-02-01'
                )
                """
            ),
            {
                "cache_version_id": cache_version_id,
                "country_iso3": country_iso3,
                "country_name": country_name,
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO cached_units (
                    cache_version_id,
                    commodity_unit_id,
                    commodity_unit_name,
                    conversion_to_kg_l,
                    active
                ) VALUES (
                    :cache_version_id,
                    100,
                    'kg',
                    1,
                    :active
                )
                """
            ),
            {"cache_version_id": cache_version_id, "active": _active_value(conn)},
        )
        conn.execute(
            text(
                """
                INSERT INTO cached_currencies (
                    cache_version_id,
                    currency_id,
                    currency_code,
                    currency_name
                ) VALUES (
                    :cache_version_id,
                    200,
                    'SSP',
                    'South Sudanese Pound'
                )
                """
            ),
            {"cache_version_id": cache_version_id},
        )
        conn.execute(
            text(
                """
                INSERT INTO cached_commodities (
                    cache_version_id,
                    country_iso3,
                    commodity_id,
                    commodity_name,
                    commodity_unit_id,
                    commodity_unit_name,
                    category_name,
                    active
                ) VALUES
                    (:cache_version_id, :country_iso3, 1, 'Maize', 100, 'kg', 'Cereals', :active),
                    (:cache_version_id, :country_iso3, 2, 'Beans', 100, 'kg', 'Pulses', :active)
                """
            ),
            {
                "cache_version_id": cache_version_id,
                "country_iso3": country_iso3,
                "active": _active_value(conn),
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO cached_markets (
                    cache_version_id,
                    country_iso3,
                    market_id,
                    market_name,
                    admin1_name,
                    admin2_name,
                    latitude,
                    longitude,
                    active
                ) VALUES
                    (:cache_version_id, :country_iso3, 10, 'Juba', 'Central Equatoria', '', 4.85, 31.6, :active),
                    (:cache_version_id, :country_iso3, 11, 'Wau', 'Western Bahr el Ghazal', '', 7.7, 27.98, :active)
                """
            ),
            {
                "cache_version_id": cache_version_id,
                "country_iso3": country_iso3,
                "active": _active_value(conn),
            },
        )
        price_rows = [
            (1, 10, "2025-01-01", 10.0 + price_offset, "Juba", "Central Equatoria", "Maize"),
            (1, 10, "2025-02-01", 12.0 + price_offset, "Juba", "Central Equatoria", "Maize"),
            (2, 11, "2025-01-01", 5.0 + price_offset, "Wau", "Western Bahr el Ghazal", "Beans"),
            (2, 11, "2025-02-01", 6.0 + price_offset, "Wau", "Western Bahr el Ghazal", "Beans"),
        ]
        for commodity_id, market_id, price_date, price, market_name, admin1_name, commodity_name in price_rows:
            conn.execute(
                text(
                    """
                    INSERT INTO cached_price_monthly (
                        cache_version_id,
                        country_iso3,
                        commodity_id,
                        market_id,
                        price_date,
                        price,
                        currency_id,
                        currency_code,
                        currency_name,
                        commodity_unit_id,
                        commodity_unit_name,
                        price_type_id,
                        price_type_name,
                        price_flag,
                        original_frequency,
                        observations,
                        source_payload_hash,
                        admin1_name,
                        admin2_name,
                        market_name,
                        commodity_name,
                        data_source
                    ) VALUES (
                        :cache_version_id,
                        :country_iso3,
                        :commodity_id,
                        :market_id,
                        :price_date,
                        :price,
                        200,
                        'SSP',
                        'South Sudanese Pound',
                        100,
                        'kg',
                        1,
                        'Retail',
                        'actual',
                        'monthly',
                        1,
                        :source_payload_hash,
                        :admin1_name,
                        '',
                        :market_name,
                        :commodity_name,
                        'fixture'
                    )
                    """
                ),
                {
                    "cache_version_id": cache_version_id,
                    "country_iso3": country_iso3,
                    "commodity_id": commodity_id,
                    "market_id": market_id,
                    "price_date": price_date,
                    "price": price,
                    "source_payload_hash": f"{cache_version_id}-{commodity_id}-{market_id}-{price_date}",
                    "admin1_name": admin1_name,
                    "market_name": market_name,
                    "commodity_name": commodity_name,
                },
            )

        if activate:
            conn.execute(text("DELETE FROM price_cache_active_version WHERE singleton_id = 1"))
            conn.execute(
                text(
                    """
                    INSERT INTO price_cache_active_version (
                        singleton_id,
                        cache_version_id,
                        activated_at
                    ) VALUES (1, :cache_version_id, :now)
                    """
                ),
                {"cache_version_id": cache_version_id, "now": now},
            )
            if conn.dialect.name == "sqlite":
                table_rows = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type = 'table'")
                ).mappings().all()
            else:
                table_rows = conn.execute(
                    text("SELECT tablename AS name FROM pg_catalog.pg_tables WHERE schemaname = 'public'")
                ).mappings().all()
            table_names = {str(row["name"]) for row in table_rows}
            if "price_cache_country_active_versions" in table_names:
                conn.execute(
                    text(
                        """
                        DELETE FROM price_cache_country_active_versions
                        WHERE country_iso3 = :country_iso3
                        """
                    ),
                    {"country_iso3": country_iso3},
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO price_cache_country_active_versions (
                            country_iso3,
                            cache_version_id,
                            activated_at
                        ) VALUES (
                            :country_iso3,
                            :cache_version_id,
                            :now
                        )
                        """
                    ),
                    {"country_iso3": country_iso3, "cache_version_id": cache_version_id, "now": now},
                )
    return cache_version_id


def _active_value(conn) -> Optional[bool | int]:
    return True if conn.dialect.name != "sqlite" else 1

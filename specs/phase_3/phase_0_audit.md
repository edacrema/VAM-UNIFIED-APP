# Phase 0 Audit - DataBridges Baseline And Price Cache Assumptions

Date: 2026-06-02

## Purpose

Phase 0 locks the current Price Bulletin behavior and the DataBridges API/cache assumptions needed before implementing the app-owned cache in later Phase 3 work.

This audit is documentation-only. It does not change runtime behavior, public API contracts, dependencies, database schema, or deployment configuration.

Primary sources:

- Local repo, especially `pages/3_Price_Bulletin_Drafter.py`, `app/services/market_monitor/`, `app/shared/databridges.py`, `.env.example`, and existing tests.
- WFP DataBridges resource page: <https://vamresources.manuals.wfp.org/docs/data-bridges>
- WFP-VAM generated DataBridges client repository: <https://github.com/WFP-VAM/DataBridgesAPIClient>

## Current Price Bulletin Flow

### Streamlit form

Entry point: `pages/3_Price_Bulletin_Drafter.py`

- Loads country options with `GET /market-monitor/countries` and caches the response in `st.session_state["mm_countries_resp"]`.
- Shows only countries whose response item has `has_data`.
- Loads selected-country metadata with `GET /market-monitor/countries/{country}/metadata` and caches it per country in `st.session_state["mm_country_metadata"]`.
- Builds the month selector from metadata `date_range`; if metadata is missing or incomplete, falls back to the last 36 calendar months.
- Builds commodity and Admin1 multiselects from metadata.
- Uses `metadata.default_commodities` as the default commodity selection; if no defaults match, selects all valid commodities.
- On submit, posts to `POST /market-monitor/generate-async` and polls `/market-monitor/status/{run_id}` and `/market-monitor/result/{run_id}`.
- Renders report blocks, sections, visualizations, data statistics, and DOCX export after completion.
- DOCX export calls `POST /market-monitor/export-docx/{run_id}` and downloads bytes returned by the backend.

### FastAPI endpoints

Entry point: `app/services/market_monitor/router.py`

- `GET /market-monitor/countries`
  - Returns configured country options from `data_loader.get_supported_countries()`.
  - Current country list is static/local, based on `app/shared/countries.py`, not discovered from DataBridges.
- `GET /market-monitor/countries/{country}/metadata`
  - Calls `data_loader.get_country_metadata(country)`.
  - Returns commodities, commodity categories, default commodities, regions, markets, date range, metadata window, warnings, and source.
  - Can trigger live DataBridges calls during normal country selection.
- `POST /market-monitor/generate-async`
  - Creates an async run, then runs `graph.run_report_generation(...)` in a background task.
  - During `data_agent`, writes live DataBridges row artifacts for the run if rows were fetched.
- `GET /market-monitor/status/{run_id}`
  - Returns async status, progress, warnings, metadata, error, and traceback.
- `GET /market-monitor/result/{run_id}`
  - Returns completed report output and rebuilds report blocks from the stored result.
- `POST /market-monitor/export-docx/{run_id}`
  - Builds report blocks from the stored result and renders them with `build_docx_bytes_from_report_blocks(...)`.

### Loader and statistics behavior

Entry point: `app/services/market_monitor/data_loader.py`

- Maintains in-memory TTL caches for commodities, markets, prices, and metadata.
- `_CACHE_TTL_SECONDS` is 15 minutes.
- `get_supported_countries()` returns local static country options from `app/shared/countries.py`.
- `get_country_metadata(country)`:
  - Resolves the country to canonical name and ISO3.
  - Fetches commodities with `_get_commodities(...)`.
  - Fetches recent price rows for a 36-month metadata window with `_get_country_price_df(...)`.
  - Falls back to unbounded country price fetch if the recent window is empty.
  - Derives priced commodities, regions, markets, and date range from live DataBridges-backed data.
- `extract_time_series_from_csv(...)` is a compatibility wrapper whose active behavior is DataBridges-backed.
  - Resolves requested commodity names to DataBridges commodity IDs.
  - Fetches a 13-month reporting window by country, date range, and commodity IDs.
  - Produces a national pivot table by month and commodity.
  - Produces regional food basket values by month and Admin1.
  - Can return raw normalized rows for live run artifacts.
- `calculate_statistics_from_csv(...)` computes current price, MoM, YoY, and coverage metadata from the national time series.

### Graph, prompt, report, and DOCX dependencies

Relevant files:

- `app/services/market_monitor/graph.py`
- `app/shared/report_blocks.py`
- `app/shared/docx_export.py`

Current dependencies:

- `graph.node_data_agent(...)` loads DataBridges data unless `use_mock_data=True`.
- `graph.node_graph_designer(...)` expects a `FoodBasket` column for:
  - `visualizations["food_basket_trend"]`
  - `visualizations["regional_comparison"]`
- Commodity chart IDs are generated as `commodity_trends_{category_slug}_p{page}`.
- `graph.node_highlights_drafter(...)` passes `data_statistics.food_basket` and per-commodity statistics into the LLM prompt.
- `report_blocks.build_market_monitor_report_blocks(...)` always adds a `food_basket_trend` figure after Highlights when Highlights exists.
- `docx_export.build_docx_bytes_from_report_blocks(...)` renders report blocks and base64 visualizations. It has no direct knowledge of cache or basket versions yet.

## Fixed Food Basket Inventory

Current behavior is not a saved country basket.

- The user-selected commodity list is treated as the basket composition for that run.
- `extract_time_series_from_csv(...)` computes `FoodBasket` as the sum of selected commodity monthly average prices.
- Regional basket values also sum selected commodity prices by Admin1.
- Missing target-month components are surfaced through coverage fields:
  - `selected_component_count`
  - `historical_component_count`
  - `latest_component_count`
  - `latest_component_names`
  - `missing_latest_component_names`
- Default basket-like commodity selection is heuristic:
  - `data_loader._select_default_commodities(...)` prioritizes maize, wheat, rice, sorghum, millet, beans, lentil, oil, salt, and sugar.
  - `graph._select_default_commodities(...)` has a similar priority list and is used when no commodity list is supplied.
  - `router._get_food_basket_commodities(...)` is currently present as a helper but is not part of the active endpoint path.

Later phases must replace this selected-commodity-sum behavior with a shared, versioned country basket and a weighted basket calculator.

## Current DataBridges Integration

### Current custom requests wrapper

Entry point: `app/shared/databridges.py`

The app currently uses a custom requests-based wrapper, not the generated `data_bridges_client` package.

Current methods and endpoints:

| Current method | HTTP endpoint | Current use |
| --- | --- | --- |
| `list_commodities(country_code, ...)` | `/Commodities/List` | Country commodity metadata and commodity ID resolution |
| `list_markets(country_code)` | `/Markets/List` | Market names and Admin1/Admin2 lookup |
| `list_monthly_prices(country_code, ...)` | `/MarketPrices/PriceMonthly` | Metadata date ranges, available priced commodities, and report time series |
| `list_mfi_surveys(...)` | `/MFI/Surveys` | MFI drafter, outside Phase 3 price cache scope |
| `list_mfi_processed_data(...)` | `/MFI/Surveys/ProcessedData` | MFI drafter, outside Phase 3 price cache scope |

The wrapper paginates with `page`, reads items from `items`, `Items`, `data`, or direct-list responses, and uses first-page total values when present.

### Canonical gateway settings

Treat these v2 gateway settings as canonical for Phase 3:

```text
WFP_V2_API_BASE_URL=https://gateway.api.wfp.org/vam-data-bridges/v2
WFP_V2_TOKEN_URL=https://login.microsoftonline.com/462ad9ae-d7d9-4206-b874-71b1e079776f/oauth2/v2.0/token
WFP_V2_API_SCOPE=api://wfp-api-mediation-service/.default
WFP_V2_API_ENV=prod
```

`WFP_V2_*` values take precedence in code. `DATA_BRIDGES_*` variables remain compatibility aliases:

```text
DATA_BRIDGES_KEY
DATA_BRIDGES_SECRET
DATA_BRIDGES_API_BASE_URL
DATA_BRIDGES_TOKEN_URL
DATA_BRIDGES_SCOPE
DATA_BRIDGES_TIMEOUT
DATA_BRIDGES_MAX_RETRIES
DATA_BRIDGES_ENV
```

The older spec-style `DATABRIDGES_HOST=https://api.wfp.org/vam-data-bridges/7.0.0` should be treated as stale for this repo unless the official client or deployment owners explicitly require it.

The local `.env` was not inspected for secret values. `.env.example` and code were used for variable inventory.

### Official client endpoint map

The WFP-VAM generated client README lists all URIs relative to `https://gateway.api.wfp.org/vam-data-bridges/v2` and confirms the OAuth application scope `api://wfp-api-mediation-service/.default`.

Phase 1/2 should add and wrap the official package before replacing the custom connector:

- `data_bridges_client` is not installed in the current project venv.
- `requirements.txt` currently includes `requests`, but not `data_bridges_client`.

Required generated client methods for Phase 3:

| Purpose | Official generated client method | Endpoint |
| --- | --- | --- |
| Monthly prices | `MarketPricesApi.market_prices_price_monthly_get` | `GET /MarketPrices/PriceMonthly` |
| Commodities | `CommoditiesApi.commodities_list_get` | `GET /Commodities/List` |
| Commodity units | `CommodityUnitsApi.commodity_units_list_get` | `GET /CommodityUnits/List` |
| Unit conversions | `CommodityUnitsApi.commodity_units_conversion_list_get` | `GET /CommodityUnits/Conversion/List` |
| Markets | `MarketsApi.markets_list_get` | `GET /Markets/List` |
| Market GeoJSON | `MarketsApi.markets_geo_json_list_get` | `GET /Markets/GeoJSONList` |
| Currencies | `CurrencyApi.currency_list_get` | `GET /Currency/List` |

Current app code does not fetch units or currencies as separate metadata collections. It only carries unit and currency names from monthly price rows, plus static country currency defaults from `app/shared/countries.py`.

## Cache Schema Assumptions For Phase 1

### Starting monthly price uniqueness key

Use this as the Phase 1 starting key:

```text
cache_version_id
country_iso3
commodity_id
market_id
price_date
price_type_name
price_flag
```

Collision risk to revisit:

- Official DTOs may expose stable `price_type_id`, `currency_id`, `commodity_unit_id`, or other identifiers that should be added to the canonical key.
- Current normalized rows include `Currency`, `Unit`, and `Price Type` as names. Name-only fields are useful for display but may not be sufficient for durable uniqueness.
- Current loader drops exact duplicate normalized rows after sorting. The refresh worker should instead detect duplicates by canonical key and either reject or deterministically collapse them before promotion.

### Required normalized fields to preserve

The future cache repository must be able to reconstruct the current reporting data contract:

- `Country`
- `Country ISO3`
- `Commodity`
- `Commodity ID`
- `Price Type`
- `Price Date`
- `Price`
- `Admin 1`
- `Admin 2`
- `Market Name`
- `Market ID`
- `Unit`
- `Currency`
- `Data Type`
- `Price Flag`
- `Observations`
- `Data Source`

Phase 1 should also add stable storage for cache version metadata, active version pointer, countries, commodities, markets, units, currencies, and monthly prices.

## GCP Beta Defaults

Recommended defaults for implementation planning:

| Area | Default |
| --- | --- |
| Database | Cloud SQL for PostgreSQL |
| Scheduler | Cloud Scheduler |
| Worker | Cloud Run Job |
| Secrets | Secret Manager |
| Region | `europe-west1` unless beta project standards say otherwise |
| Cloud SQL instance placeholder | `vam-llm-price-cache-beta` |
| Database name placeholder | `price_cache` |
| Worker job placeholder | `price-cache-refresh-weekly` |
| Scheduler placeholder | `price-cache-refresh-sunday` |
| Service account placeholder | `price-cache-refresh@<project>.iam.gserviceaccount.com` |

Use Secret Manager for:

- DataBridges key and secret.
- Price cache database URL or Cloud SQL connection settings.
- Any worker-only credentials not already managed by platform identity.

## Exact Files And Endpoints To Change In Later Phases

Likely files or modules to add:

- `app/services/price_cache/repository.py`
- `app/services/price_cache/schemas.py`
- `app/services/price_cache/postgres_repository.py`
- `app/services/price_cache/sqlite_repository.py` or fixture adapter for tests
- `app/services/price_cache/migrations/`
- `app/services/price_cache/databridges_adapter.py`
- `app/services/price_cache/refresh_worker.py`
- `app/services/price_cache/validation.py`
- `app/services/price_bulletin/food_basket.py`
- `app/services/price_bulletin/basket_calculator.py`
- `app/services/price_bulletin/latest_overlay.py`

Likely files to modify:

- `requirements.txt`
- `.env.example`
- `app/services/market_monitor/data_loader.py`
- `app/services/market_monitor/graph.py`
- `app/services/market_monitor/router.py`
- `app/services/market_monitor/schemas.py`
- `pages/3_Price_Bulletin_Drafter.py`
- `app/shared/report_blocks.py`
- `app/shared/docx_export.py`
- `Dockerfile` and/or worker entrypoint scripts for Cloud Run Job packaging

Endpoints to keep but move to cache-first behavior:

- `GET /market-monitor/countries`
- `GET /market-monitor/countries/{country}/metadata`
- `POST /market-monitor/generate-async`
- `GET /market-monitor/result/{run_id}`
- `POST /market-monitor/export-docx/{run_id}`

Endpoints or admin surfaces likely to add later:

- Cache status endpoint or panel.
- Manual refresh trigger for admins.
- Refresh history/status endpoint or panel.
- Basket read/save endpoints if the Streamlit page does not call service functions directly.
- Manual latest overlay action for a run.

## Phase 0 Acceptance Checklist

- Current Price Bulletin country selection, metadata loading, row loading, stats, charting, and DOCX code paths are mapped.
- Fixed food basket constants, heuristics, prompts, and graph dependencies are inventoried.
- Current DataBridges credential variables and host configuration are documented, with v2 gateway settings as canonical.
- DataBridges client calls needed for monthly prices, commodities, units, markets, and currencies are mapped.
- Required OAuth scope is documented as `api://wfp-api-mediation-service/.default`.
- Initial canonical monthly price uniqueness key is defined with explicit collision risks.
- GCP beta resource choices are documented with concrete recommended defaults and placeholder names.

## Baseline Tests

The relevant baseline test command was run with the project venv:

```powershell
venv\Scripts\python.exe -m pytest tests\test_market_monitor_databridges_loader.py tests\test_databridges_client.py -q -p no:cacheprovider
```

Result:

```text
16 passed, 1 skipped
```

The same command with global Python failed before collection because the global environment auto-loaded a pytest plugin that required `pydantic`, which was missing. Use the project venv for Phase 3 baseline checks.

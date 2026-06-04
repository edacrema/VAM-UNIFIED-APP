# Phase 3 Price Cache Refresh On GCP

## Worker Command

Use the existing application image with a Cloud Run Job command override:

```text
python -m app.services.price_cache.refresh_worker run
```

Useful manual variants:

```text
python -m app.services.price_cache.refresh_worker run --json
python -m app.services.price_cache.refresh_worker run --country SSD --json
python -m app.services.price_cache.refresh_worker run --dry-run --country SSD --json
```

## Required Configuration

Set the cache backend to Cloud SQL PostgreSQL:

```text
PRICE_CACHE_BACKEND=cloud_sql_postgres
PRICE_CACHE_DATABASE_URL=<Secret Manager value>
PRICE_CACHE_GCP_PROJECT=<beta project>
PRICE_CACHE_GCP_REGION=europe-west1
PRICE_CACHE_GCP_CLOUD_SQL_INSTANCE=vam-llm-price-cache-beta
PRICE_CACHE_REFRESH_ENABLED=true
PRICE_CACHE_REFRESH_LOCK_TIMEOUT_MINUTES=30
PRICE_CACHE_VALIDATE_MAX_COUNTRY_DROP_RATIO=0.25
PRICE_CACHE_REFRESH_MAX_WORKERS=5
PRICE_CACHE_RETAIN_VERSIONS=3
```

Keep the canonical Databridges v2 variables in Secret Manager:

```text
WFP_V2_API_KEY
WFP_V2_API_SECRET
WFP_V2_API_BASE_URL=https://gateway.api.wfp.org/vam-data-bridges/v2
WFP_V2_TOKEN_URL=https://login.microsoftonline.com/462ad9ae-d7d9-4206-b874-71b1e079776f/oauth2/v2.0/token
WFP_V2_API_SCOPE=api://wfp-api-mediation-service/.default
WFP_V2_API_ENV=prod
```

## Scheduler Shape

Create a Cloud Scheduler job that triggers the Cloud Run Job weekly, for example Sunday off-hours in the beta region. The worker uses a database lock named `weekly_full_refresh`, so overlapping triggers fail fast instead of running concurrent full downloads.

The worker publishes successful countries independently. Countries that fail fetch or validation stay on their previous country-active cache version.

## Operator Checks

Use the backend endpoints after a run:

```text
GET /market-monitor/cache/status
GET /market-monitor/cache/refreshes
GET /market-monitor/cache/refreshes/{cache_version_id}
```

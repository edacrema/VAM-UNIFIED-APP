# Phase 2 Roadmap - Implementation Plan

This roadmap turns Phase 2 from a requirements sketch into an implementation sequence. It assumes the current alpha remains usable while new capabilities are added behind authentication, workspace, cache, and feature-flag boundaries.

## Guiding Principles

1. Keep the alpha working.
   Existing tools should remain available until their Phase 2 replacements are proven.

2. Build security foundations early.
   Authentication, authorization, workspace scoping, and audit logs must come before broader pilot use.

3. Make LLM selection a platform feature.
   Do not add per-tool model hacks. All tools should resolve models through one registry.

4. Make Price Bulletin cache-first.
   Country selection and metadata loading should not call Databridges for full data.

5. Treat methodology as product behavior.
   Food basket calculation, missing data handling, and cache freshness must be explicit and reviewable.

6. Feature-flag risky new tools.
   Data Explorer sandbox and Climate Report Drafter should be isolated until tested.

7. Log enough to reproduce.
   Every output should be traceable to user, inputs, model, data source, cache version, and generated artifacts.

## Phase Overview

| Phase | Theme | Main deliverables | Exit signal |
|---|---|---|---|
| 0 | Baseline audit and spec lock | Confirm current behavior, finalize open decisions needed for build | Updated implementation tickets and accepted Phase 2 spec |
| 1 | Auth and admin whitelist | Login gate, authorized email store, admin management | Only authorized users can enter |
| 2 | Workspace persistence | User workspace context, preferences, run history, file storage | Each user has isolated durable state |
| 3 | Vertex AI model registry | Config-driven model resolver and UI defaults | All LLM tools can switch models |
| 4 | AWS price cache foundation | Repository interface, AWS schema, sync job skeleton | Metadata and price reads can come from cache |
| 5 | Price Bulletin cache migration | Country selection, metadata, report data load use cache | Country selection is fast and avoids full Databridges pulls |
| 6 | Manual latest-data sync | User-triggered delta refresh | Latest missing rows can be fetched explicitly |
| 7 | Food basket customization | Basket editor, storage, weighted engine | Saved country baskets drive bulletin calculations |
| 8 | Workspace-aware artifacts | Generated files and histories saved per workspace | Outputs persist and remain user-isolated |
| 9 | Databridges Data Explorer | NL query planner, data executor, sandbox chart generation | User can ask for data and get inline chart/download |
| 10 | Climate Report Drafter | Climate data framework, analysis, draft/export pipeline | User can generate a climate report draft |
| 11 | Evals, security, and pilot hardening | Isolation tests, sandbox tests, model benchmarks, release checklist | Pilot-ready build with known risks documented |

Durations are intentionally omitted until database, auth provider, and climate-source details are confirmed. The likely critical path is auth/workspace -> model registry -> price cache -> basket customization -> new tools.

---

## Phase 0 - Baseline Audit And Spec Lock

**Goal:** Confirm exact current behavior and lock the implementation assumptions that would be expensive to reverse later.

### Work Items

- **0.1 - Confirm current app entry points.**
  List all Streamlit pages and FastAPI endpoints that need auth protection.

- **0.2 - Confirm LLM call sites.**
  Inventory all `get_model()` and direct Vertex AI usage across validators, drafters, and report generation.

- **0.3 - Confirm Price Bulletin data flow.**
  Document the current country selection -> metadata endpoint -> Databridges call chain, including calls to commodities, markets, price rows, and fallback behavior.

- **0.4 - Confirm current food basket behavior.**
  Record current default commodity selection, national basket calculation, regional basket calculation, stats fields, chart dependencies, and report prompt dependencies.

- **0.5 - Resolve blocking decisions.**
  Confirm identity provider, AWS database engine, workspace metadata store, manual delta persistence policy, and food basket methodology.

- **0.6 - Create implementation tickets.**
  Convert this roadmap into tickets grouped by service/module.

### Exit Criteria

- Open decisions required for Phases 1-7 are either resolved or marked with accepted temporary assumptions.
- Engineering has a code-level map of files/endpoints to change.

---

## Phase 1 - Authentication And Admin Whitelist

**Goal:** Require users to authenticate and allow access only to manually authorized emails.

### Work Items

- **1.1 - Add auth provider adapter.**
  Implement the chosen identity provider integration and extract verified email, provider subject, and display name.

- **1.2 - Add authorization repository.**
  Create the `authorized_users` store with normalized email, role, status, and audit fields.

- **1.3 - Add auth gate to Streamlit.**
  Block every app page until authentication and whitelist authorization succeed.

- **1.4 - Add backend auth dependency.**
  Ensure API routes receive authenticated user/workspace context or reject requests.

- **1.5 - Add admin management UI/API.**
  Admins can add, list, disable, and update authorized users.

- **1.6 - Add denial UX.**
  Unauthorized users see a clear message and cannot access tool pages.

- **1.7 - Add auth audit events.**
  Log successful login, denied login, disabled user attempts, and admin whitelist changes.

### Tests

- Whitelisted user can enter.
- Non-whitelisted user is denied.
- Disabled user is denied.
- Email matching is case-insensitive.
- Non-admin cannot manage authorized users.
- API endpoints fail closed without auth context.

### Exit Criteria

- No unauthenticated user can access tool pages or backend operations.
- Admin can manually authorize a new email without code changes.

---

## Phase 2 - Workspace Persistence And Isolation

**Goal:** Give each authorized user a durable, isolated workspace.

### Work Items

- **2.1 - Create workspace identity model.**
  Generate stable `user_id` and `workspace_id` for each authorized user.

- **2.2 - Implement workspace context.**
  Add `WorkspaceContext` available to Streamlit pages, API routes, service calls, and run creation.

- **2.3 - Implement workspace metadata repository.**
  Store user profile, preferences, model defaults, and tool settings.

- **2.4 - Implement operation history.**
  Persist run records for validators, Price Bulletin, MFI Drafter, and future tools.

- **2.5 - Implement workspace file storage.**
  Store uploaded inputs and generated outputs in user-scoped object paths.

- **2.6 - Add workspace settings page.**
  Show profile, default language, default model, tool preferences, saved baskets, file/history links.

- **2.7 - Migrate current generated artifacts.**
  Ensure generated DOCX, chart, CSV, and JSON artifacts are associated with workspace/run records.

### Tests

- User A cannot list User B runs.
- User A cannot download User B files.
- Storage paths use workspace IDs, not raw emails.
- Tool run creates operation history record.
- Generated file creates metadata record.

### Exit Criteria

- A returning user sees their own preferences, run history, and generated files.
- Workspace isolation tests pass.

---

## Phase 3 - Flexible Vertex AI Model Registry

**Goal:** Allow all LLM-using tools to switch among enabled Vertex AI models.

### Work Items

- **3.1 - Define model registry config.**
  Create a config file or metadata table listing enabled Vertex AI model keys, display names, model IDs, regions, capabilities, and defaults.

- **3.2 - Implement model resolver.**
  Resolve model by explicit run override, per-tool workspace default, workspace default, and system default.

- **3.3 - Refactor `app/shared/llm.py`.**
  Make `get_model()` delegate to the resolver while preserving compatibility for unmigrated call sites.

- **3.4 - Update LLM call sites.**
  Pass tool name and workspace context from MFI Validator, Price Validator, MFI Drafter, Price Bulletin, and new tools.

- **3.5 - Add UI model selection.**
  Add global workspace default and optional per-tool model selectors.

- **3.6 - Add model audit metadata.**
  Record resolved model on each LLM call and operation run.

- **3.7 - Add model availability checks.**
  Disabled/unavailable models produce clear errors and do not silently fall back unless configured.

### Tests

- Resolver follows correct fallback order.
- Changing workspace default changes tool execution model.
- Per-tool default overrides workspace default.
- Logs contain selected model key.
- Existing tools still run through compatibility wrapper during migration.

### Exit Criteria

- Every LLM-using tool can be run with at least two configured Vertex AI models without code changes.

---

## Phase 4 - AWS Databridges Price Cache Foundation

**Goal:** Establish the cache database and repository layer for Databridges price data.

### Work Items

- **4.1 - Confirm database engine and connection pattern.**
  Use provided AWS database details when available. Until then, implement against a repository interface and local/test adapter.

- **4.2 - Create schema migrations.**
  Add tables for countries, commodities, markets, monthly prices, sync runs, and country watermarks.

- **4.3 - Implement cache repository.**
  Add methods for countries, metadata, price rows, markets, commodities, and watermarks.

- **4.4 - Implement Databridges normalization reuse.**
  Reuse or extract the current row normalization logic so Databridges rows and cache rows produce the same dataframe contract.

- **4.5 - Implement initial sync job.**
  Backfill supported countries and write sync logs.

- **4.6 - Implement 14-day scheduled sync.**
  Add cron/EventBridge/Lambda/Celery path depending on deployment environment.

- **4.7 - Add admin sync status view.**
  Show latest successful sync, failed syncs, row counts, and country watermarks.

### Tests

- Repository returns expected metadata without Databridges calls.
- Upserts are idempotent.
- Watermark updates after successful sync.
- Failed sync preserves existing cache rows.
- Sync run logs are written.

### Exit Criteria

- Cache database can answer country metadata and price window queries required by Price Bulletin.
- Scheduled sync path exists and is observable.

---

## Phase 5 - Price Bulletin Cache Migration

**Goal:** Make the Price Bulletin form and generation path use the AWS cache by default.

### Work Items

- **5.1 - Replace country list source.**
  `/market-monitor/countries` reads from cache metadata.

- **5.2 - Replace country metadata source.**
  `/market-monitor/countries/{country}/metadata` reads commodities, regions, markets, date range, and watermarks from cache.

- **5.3 - Remove live Databridges metadata dependency from country selection.**
  Ensure selecting a country does not call `list_monthly_prices`.

- **5.4 - Replace report data loader source.**
  Update Price Bulletin data loading to read selected date windows from the cache repository.

- **5.5 - Preserve output contracts.**
  Keep downstream dataframe columns and stats structure stable until basket changes are introduced.

- **5.6 - Add cache freshness display.**
  Show latest cached price date and last successful sync timestamp in the form.

- **5.7 - Add cache fallback behavior.**
  If cache is missing, show clear message. Do not silently run a full Databridges fetch on country selection.

### Tests

- Country selection endpoint does not use Databridges client.
- Metadata loads in target time for seeded cache data.
- Report generation works from cache for a known fixture.
- Existing graphs and DOCX generation still work.
- Missing cache produces actionable error.

### Exit Criteria

- Selecting a country is cache-backed and fast.
- Price Bulletin can generate a report without live Databridges historical fetches.

---

## Phase 6 - Manual Latest-Data Sync

**Goal:** Let users explicitly fetch only missing latest data from Databridges when the cache is stale.

### Work Items

- **6.1 - Add manual sync endpoint.**
  Endpoint accepts selected country, date context, commodities, markets/regions where available, and workspace context.

- **6.2 - Compute delta range.**
  Determine start date from country watermark and end date from current date.

- **6.3 - Guard against full historical sync.**
  If no watermark exists, do not perform a user-triggered full sync unless admin policy allows it.

- **6.4 - Execute bounded Databridges call.**
  Call Databridges with `startDate`, `endDate`, and available filters.

- **6.5 - Merge delta into active run data.**
  Combine cache data and manual delta rows for the current report/session.

- **6.6 - Persist according to policy.**
  Implement chosen behavior: write to shared cache, save to workspace/run only, or queue for admin review.

- **6.7 - Show sync result.**
  UI displays rows fetched, latest available date, warnings, and whether shared cache was updated.

### Tests

- Manual sync call includes bounded dates.
- No-watermark case does not trigger full fetch.
- Inclusive date behavior does not duplicate rows.
- Delta rows are included in generated report data.
- Sync event is logged with user and run context.

### Exit Criteria

- User can refresh latest data explicitly without downloading all historical data.

---

## Phase 7 - Food Basket Customization

**Goal:** Replace auto-selected unweighted baskets with saved user-defined country compositions.

### Work Items

- **7.1 - Create basket storage.**
  Add `food_basket_compositions` and `food_basket_items` persistence.

- **7.2 - Add basket editor UI.**
  Users can select country, add commodities, set weights, validate, save, edit, duplicate, archive, and choose default basket.

- **7.3 - Add validation rules.**
  Enforce no duplicate commodities, positive weights, valid commodity IDs, and weight total policy.

- **7.4 - Update Price Bulletin form.**
  Show selected basket, allow choosing among saved baskets, and warn if no basket exists for selected country.

- **7.5 - Update national basket calculation.**
  Replace unweighted sum with weighted calculation.

- **7.6 - Update regional basket calculation.**
  Apply the same basket composition per region.

- **7.7 - Add missing-data coverage logic.**
  Warn when basket components are missing in latest month or historical months.

- **7.8 - Update charts and report text.**
  Ensure visualizations, statistics, prompts, and DOCX output reflect the saved basket and coverage warnings.

- **7.9 - Record basket in run metadata.**
  Save basket ID, item snapshot, weights, and calculation mode used.

### Tests

- Invalid weights are rejected.
- Basket persists after logout/login.
- Weighted calculation matches expected fixture.
- Missing latest component creates warning.
- Report metadata includes basket snapshot.
- User cannot use another user's private basket.

### Exit Criteria

- A user can create a Somalia basket, save it, generate a bulletin, and see weighted results reflected in graphs and report output.

---

## Phase 8 - Workspace-Aware Artifacts And History Polish

**Goal:** Make generated outputs discoverable, durable, and user-scoped across all tools.

### Work Items

- **8.1 - Standardize artifact creation.**
  Validators, MFI Drafter, Price Bulletin, Data Explorer, and Climate Drafter use one generated-file service.

- **8.2 - Add file browser.**
  Workspace page lists generated files by tool, date, country, run, and type.

- **8.3 - Add run history browser.**
  Users can inspect prior operations, inputs, warnings, model used, and generated outputs.

- **8.4 - Add rerun-from-history affordance where safe.**
  Allow reusing previous inputs for deterministic/report workflows.

- **8.5 - Add retention policy hooks.**
  Prepare for retention limits even if policy is not finalized.

### Tests

- Each tool writes file metadata.
- Download checks workspace ownership.
- Run history filters by current workspace.
- Artifact links remain valid after session reload.

### Exit Criteria

- Users can leave and return later to retrieve files and inspect previous runs.

---

## Phase 9 - Databridges Data Explorer And Graph Composer

**Goal:** Add a chatbot that turns natural language into Databridges data retrieval, rough analysis, and downloadable charts.

### Work Items

- **9.1 - Define supported query scope.**
  Start with monthly market prices, countries, commodities, markets, admin areas, date ranges, simple comparisons, and time-series charts.

- **9.2 - Build query planner.**
  LLM converts natural language to structured query spec. App validates spec before execution.

- **9.3 - Add clarification flow.**
  If country, commodity, market, or date range is ambiguous, ask user before calling data sources.

- **9.4 - Implement data executor.**
  Use cache for historical data where possible and Databridges direct calls where latest/uncached data is explicitly needed.

- **9.5 - Build analysis code generator.**
  Generate constrained pandas/matplotlib code against a known input dataframe contract.

- **9.6 - Implement sandbox runner.**
  Execute generated code in isolated environment with no network, no secrets, resource limits, timeout, and output allowlist.

- **9.7 - Render inline outputs.**
  Display chart images, tables/previews, generated explanation, and download links in chat.

- **9.8 - Save artifacts to workspace.**
  Store data extracts, chart PNGs, and optionally code snippets with run metadata.

- **9.9 - Add safety refusals.**
  Decline unsupported data domains, unsafe code requests, or requests requiring unauthorized data.

### Tests

- Known prompts produce expected query specs.
- Ambiguous prompts trigger clarification.
- Unsupported prompts are declined clearly.
- Sandbox blocks `os`, `sys`, `subprocess`, network, env reads, and path escapes.
- Chart artifact is generated and downloadable.
- User outputs are saved only to their workspace.

### Exit Criteria

- User can ask a natural language Databridges price question and receive a valid inline graph plus downloadable output.

---

## Phase 10 - Climate Report Drafter

**Goal:** Add a first structured climate report drafting tool, with pluggable data sources and reviewable evidence.

### Work Items

- **10.1 - Finalize first release scope.**
  Confirm required climate sources, geography, report structure, language, and output format.

- **10.2 - Add climate schemas.**
  Define request, source dataset, indicator summary, warning, draft section, and output artifact schemas.

- **10.3 - Implement data source adapters.**
  Add collectors for confirmed sources such as CHIRPS, NDVI, temperature anomalies, FEWS NET, or internal WFP sources.

- **10.4 - Implement indicator analysis.**
  Compute anomaly summaries and threshold labels using approved methodology.

- **10.5 - Build drafting prompts.**
  Generate narrative only from indicator summaries and source metadata.

- **10.6 - Add report page/API.**
  User selects area/period/indicators/model and runs the draft.

- **10.7 - Add DOCX export.**
  Save generated climate report draft and supporting tables/charts to workspace.

- **10.8 - Add grounding warnings.**
  If source coverage is incomplete, draft must include caveats and avoid unsupported claims.

### Tests

- Missing source data creates warning.
- Draft references only supplied facts.
- DOCX export succeeds.
- Model selected through registry.
- Output saved to workspace.

### Exit Criteria

- User can generate a first climate report draft for a supported country/period with evidence metadata and DOCX export.

---

## Phase 11 - Evals, Security, And Pilot Hardening

**Goal:** Verify Phase 2 is safe and measurable before a broader pilot.

### Work Items

- **11.1 - Workspace isolation test suite.**
  Automated tests for preferences, files, run history, baskets, and downloads.

- **11.2 - Auth test suite.**
  Unauthorized, disabled, malformed identity, non-admin admin access, and API bypass tests.

- **11.3 - Sandbox security test suite.**
  Attempt env reads, file reads outside sandbox, subprocess execution, network calls, import escapes, long-running jobs, memory-heavy jobs, and oversized outputs.

- **11.4 - Price cache correctness suite.**
  Compare cache-backed output against Databridges fixture/live equivalent where available.

- **11.5 - Food basket methodology suite.**
  Deterministic tests for weighted national/regional calculations and missing data behavior.

- **11.6 - Model comparison evals.**
  Run all LLM-using tools against configured Vertex AI models and record quality, latency, cost/token usage where available.

- **11.7 - Report grounding evals.**
  Verify Price Bulletin and Climate drafts do not invent unsupported metrics.

- **11.8 - Observability review.**
  Confirm logs answer: who ran what, with which model, using which data, producing which files.

- **11.9 - Pilot checklist.**
  Document known limitations, admin onboarding process, incident response, and rollback steps.

### Exit Criteria

- Security tests pass.
- Model selection works across tools.
- Price Bulletin is faster and cache-first.
- Food basket customization works.
- New tools are either pilot-ready or feature-flagged.
- Known risks and open decisions are documented.

---

## Cross-Cutting Workstreams

### Documentation

- Keep user-facing documentation updated for login, workspace settings, model selection, basket configuration, cache freshness, manual sync, and generated file history.

### Data Governance

- Record data source, cache watermark, and manual sync status on every output.
- Avoid storing unnecessary raw data when generated artifacts and reproducibility metadata are sufficient.

### Feature Flags

Use feature flags for:

- Auth bypass in local development only.
- New model selector UI.
- AWS cache-backed Price Bulletin.
- Manual latest sync.
- Basket editor.
- Data Explorer.
- Climate Report Drafter.

### Backward Compatibility

- Keep old tool pages operational while migrating internals.
- Preserve existing API response contracts where possible.
- Add adapters rather than broad rewrites until tests cover the new paths.

### Release Management

- Release auth/workspace to internal team first.
- Release cache-backed Price Bulletin to test users once cache data is validated.
- Release basket customization after methodology sign-off.
- Release Data Explorer and Climate Drafter to a small allowlist before general pilot.

## Open Decisions To Resolve

- Identity provider for the first deployment.
- AWS database engine and connection method.
- Workspace metadata store.
- Shared-cache persistence policy for manual delta syncs.
- Food basket weight methodology.
- Climate data sources and report template.
- Admin visibility into user workspaces.
- Retention policy for generated files and run history.
- Whether credit budgets from Phase 1 remain in Phase 2.

## Definition Of Done For Phase 2

Phase 2 is complete when:

- Every app user must authenticate.
- Only admin-authorized emails can enter.
- Every user has an isolated durable workspace.
- Tool preferences, generated files, operation history, and food basket compositions persist per user.
- All LLM-using tools can switch among configured Vertex AI models.
- Price Bulletin country selection and metadata loading are cache-backed and fast.
- Users can explicitly sync only latest missing Databridges data.
- Price Bulletin uses saved weighted food basket compositions.
- Data Explorer can retrieve Databridges data, run safe sandboxed analysis code, and produce downloadable graphs.
- Climate Report Drafter can generate a structured draft from approved climate data sources.
- Security and regression tests cover auth, workspace isolation, sandboxing, price cache, model selection, and weighted basket behavior.

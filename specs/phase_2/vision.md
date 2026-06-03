# Phase 2 Vision - Secure Personalized VAM Workspaces

## Purpose

Phase 2 turns the current VAM LLM alpha from a mostly single-user set of report and validation tools into a secure, multi-user workspace application for VAM officers.

The core purpose is to make the app usable in a real pilot environment where every officer:

- Authenticates before entering the app.
- Can enter only if an admin has manually authorized their email.
- Has a private workspace where their operations, generated files, and tool customizations persist.
- Can use the existing tools without re-entering the same preferences every session.
- Can choose among Vertex AI models so the team can compare speed, cost, and output quality across tools.
- Can generate Price Bulletins faster because historical Databridges price data is read from a periodically synchronized AWS database, not downloaded from Databridges on every country selection.
- Can configure the food basket composition used by the Price Bulletin tool.
- Can use two new tools: a Climate Report Drafter and a Databridges Data Explorer / Graph Composer.

Phase 2 should preserve the useful parts of Phase 1 and the alpha while removing the main blockers to pilot use: uncontrolled access, no persistent per-user state, hardcoded LLM selection, slow price data loading, hardcoded food basket behavior, and lack of flexible data exploration.

## Relationship To Phase 1

Phase 2 is not a rewrite of the product direction from Phase 1. It builds on Phase 1's goal of helping VAM officers reduce mechanical work in data retrieval, analysis, validation, and report drafting.

Unless explicitly changed here, the following Phase 1 principles still apply:

- DataBridges remains read-only from the application perspective.
- Final reports require human review before publication.
- The app handles aggregated operational data, not beneficiary-level PII.
- Existing alpha tools remain available during the transition.
- Generated narratives must be grounded in retrieved data and sourceable evidence.
- The system should be instrumented so tool calls, model choices, files, and outputs can be audited.

Phase 2 changes the product emphasis from "agent/tool capability" to "secure operational workspace capability." The app must now know who the user is, what they are authorized to do, what they configured, what they generated, and which model/tool settings were used.

## Current Alpha Baseline

The current app has four primary tool surfaces:

- MFI Dataset Validator.
- Price Data Validator.
- Price Bulletin Drafter / Market Monitor.
- MFI Report Generator.

Important current implementation facts that Phase 2 must account for:

- There is no end-user authentication gate.
- User state is mostly browser/session state, not durable workspace state.
- LLM access is centralized in `app/shared/llm.py`, but it is singleton-style and effectively resolves to one active Vertex AI chat model.
- Current LLM defaults are Gemini-oriented through Vertex AI.
- The Price Bulletin page calls country metadata after country selection. That metadata path currently reaches Databridges for commodities, markets, regions, date ranges, and price rows.
- The current food basket is selected automatically using commodity name heuristics, then calculated as an unweighted sum of selected commodity price averages.
- Generated files are available to the active session/run but are not organized as durable per-user workspace artifacts.

Phase 2 must specify and implement changes against this real baseline.

## North Star

A VAM officer opens the app, authenticates with an authorized email, lands in their own workspace, and sees their saved tool preferences, prior runs, generated files, and country-specific food basket configurations.

They select a country in the Price Bulletin tool and the UI loads quickly from the synchronized AWS cache. If the cache is missing the latest data, the officer can explicitly click a "Sync Latest Data" action that requests only the missing date range from Databridges. The officer uses their saved food basket composition, generates a bulletin, and the output is saved to their workspace with the model, inputs, data version, and generated files traceable.

For exploratory work, the officer opens the Databridges Data Explorer, asks a natural language question, and receives a clear query plan, downloaded or cached data, a rough analysis, and generated charts that can be viewed inline and downloaded. For climate work, they can generate a first draft climate report using agreed climate data sources and reviewable evidence.

## Primary Users

### VAM Officers In Country Offices

Primary day-to-day users. They use the app to validate datasets, prepare Price Bulletins, draft MFI reports, explore DataBridges data, generate charts, and draft climate reports. They need low-friction workflows and persistent preferences for the countries and tools they use repeatedly.

### VAM Officers At HQ

Cross-country users. They need to compare data across countries, inspect generated outputs, test model behavior, and potentially support country office users. Their permissions may need to include broader country access than a CO user.

### Admins

Operational administrators who manually authorize emails, disable access, adjust user metadata, and possibly review system-level logs. Admins do not necessarily need access to every user's generated content unless WFP policy explicitly allows it.

### Developers And Evaluators

Internal team members who configure available Vertex AI models, benchmark outputs, monitor traces, and run evals. Their access should be distinct from normal officer access.

## Access Model

Phase 2 access is whitelist-based:

- No spontaneous registration.
- A user can authenticate only after an admin adds their email to the authorized user store.
- Authentication proves identity; whitelist authorization grants application access.
- Email comparison must be case-insensitive and normalized.
- Unauthorized authenticated users must receive a clear access-denied message.
- Disabled users must be blocked even if their identity provider authentication succeeds.
- Admin users must be explicitly marked as admins in the authorization store.

Minimum roles:

- `user`: can access their own workspace and run tools.
- `admin`: can manage authorized users and inspect system-level status.
- `developer` or `evaluator`: optional role for model benchmarking and diagnostics.

Open policy decision:

- Whether admins can view user-generated files by default, only with explicit support mode, or never.

## Workspace Model

Every authorized user has one private workspace. A workspace contains:

- User profile metadata.
- Tool preferences.
- Default model selections.
- Country-specific food basket compositions.
- Operation/run history.
- Uploaded input files.
- Generated DOCX files.
- Generated CSV/XLSX outputs.
- Generated chart images.
- Data Explorer notebooks/snippets or generated analysis artifacts, if retained.
- Audit metadata for each run, including timestamp, tool name, selected model, input parameters, data source versions, warnings, and generated file references.

Workspace isolation must be enforced server-side. It is not enough to hide files in the UI. All APIs that read or write files, operation records, preferences, or generated artifacts must scope by authenticated user or by an explicitly permitted admin role.

## Core Product Changes

### 1. Authentication And Admin-Controlled Access

Phase 2 adds an authentication boundary before the user can access tool pages.

Required behavior:

- User logs in through the selected identity provider.
- App extracts verified email, display name when available, and identity provider subject.
- App checks the normalized email against the admin-managed authorization store.
- If allowed, the app creates or loads the user's workspace.
- If not allowed, the app blocks access.
- Admins can add, disable, and list authorized emails.

There is no public signup flow.

### 2. Persistent Per-User Workspaces

The app must stop treating all user preferences as temporary session state.

Required behavior:

- The app loads workspace preferences at login.
- Tool runs are written to operation history.
- Generated files are saved under a user-scoped storage namespace.
- User-specific configurations, especially food baskets and model defaults, survive logout/login.
- User A cannot read, download, overwrite, or list User B's files or run records.

### 3. Flexible Vertex AI LLM Model Selection

All tools currently using the central LLM accessor must be migrated to a model registry/resolver that can select from enabled Vertex AI models.

Required behavior:

- Available models are configuration-driven, not hardcoded in tool logic.
- The app can switch between Vertex AI models without changing validator, analyzer, drafter, or explorer code.
- The selected model is recorded on every LLM call and run.
- Model defaults can be global, per-workspace, and optionally per-tool.
- The old singleton path remains only as a temporary compatibility wrapper during migration.

The goal is to test multiple Vertex AI models across all tools, including future models once enabled in the Google Cloud project.

### 4. Price Bulletin Data Acceleration

Current country selection is slow because the app can trigger Databridges calls to assemble metadata and price availability. Phase 2 must make country selection and form population fast by reading from an AWS-hosted synchronized database.

Required behavior:

- Historical Databridges data is synchronized into an AWS database every two weeks.
- The Price Bulletin form loads country metadata, commodities, markets, regions, and date ranges from the AWS cache.
- Selecting a country must not trigger a full Databridges data pull.
- The UI shows cache freshness so users know the latest cached date.
- If latest Databridges data is missing from the AWS cache, the user can click a manual refresh button.
- The manual refresh fetches only the delta between the cache watermark and the present, not the full historical dataset.
- The manual refresh should be scoped as tightly as possible to the selected country, and where practical to selected commodities/markets.

Open implementation decision:

- Whether user-triggered delta data is persisted back to the shared AWS cache, saved only to the user's workspace/run, or persisted only by admins/background jobs.

### 5. Custom Food Basket Composition

The Price Bulletin food basket must become user-configurable.

Current behavior:

- The app auto-selects default basket commodities based on commodity name patterns.
- It calculates `FoodBasket` as the sum of selected commodity price columns.
- It does not persist a country-specific basket composition.
- It does not store user-defined weights.

Phase 2 behavior:

- Users manually define the basket composition they want to use.
- A composition includes a list of commodities and each commodity's weight.
- Compositions are saved in the user's workspace.
- Compositions are country-specific.
- The Price Bulletin uses the saved composition when generating statistics, graphs, and narratives.
- Users can review, edit, duplicate, and reset their basket configuration.
- The report records which composition was used.

Default assumption:

- Weights are normalized shares and should sum to 1.0 within a small tolerance.

Open methodology decision:

- If WFP wants food basket quantities instead of normalized shares, the schema must store a `calculation_mode` and support quantity-based costs.

### 6. Climate Report Drafter

Phase 2 adds a tool that drafts climate report narratives.

Required behavior:

- User selects a country/area and reporting period.
- Tool retrieves agreed climate indicators.
- Tool summarizes anomalies and trends.
- Tool drafts a structured climate report section or document.
- Tool saves generated report files to the user's workspace.
- Tool includes source metadata and caveats.

Known unresolved details:

- Exact report structure.
- Required climate data sources.
- Geographic granularity.
- Threshold methodology for "dry", "wet", "drought", "vegetation stress", or other anomaly language.
- Whether outputs are standalone reports, sections inside another report, or both.

Until clarified, the spec should treat this as a tool framework with pluggable data collectors and draft templates.

### 7. Databridges Data Explorer And Graph Composer

Phase 2 adds a natural-language data exploration chatbot.

Required behavior:

- User asks for a Databridges data retrieval, analysis, or chart in natural language.
- Agent converts the request into a structured Databridges query plan.
- Agent executes the data request using the Databridges connector and/or cached data layer when appropriate.
- Agent can generate limited Python analysis code against the downloaded data.
- Agent runs that code in a secure sandbox.
- Agent displays resulting charts inline in chat.
- User can download data, charts, and optionally generated code.
- Outputs are saved to the workspace when the user chooses to save them, or automatically if configured.

Important security principle:

- LLM-generated code must not run with access to app secrets, network access, arbitrary filesystem access, shell execution, or other users' files.

## Primary User Journeys

### Journey A - Admin Authorizes A User

1. Admin opens the admin page.
2. Admin enters an officer email.
3. App normalizes the email and checks for duplicates.
4. Admin selects role and optional metadata such as country access or notes.
5. App saves the authorized email record.
6. Officer can now authenticate and enter the app.
7. Admin can later disable the user without deleting historical audit records.

### Journey B - Officer First Login

1. Officer opens the app.
2. Officer authenticates through the identity provider.
3. App checks the email against the authorization store.
4. If authorized, app creates or loads the workspace.
5. Officer sees workspace status, saved preferences, and available tools.
6. If no preferences exist, app uses system defaults and prompts only when required.

### Journey C - Price Bulletin With Cached Data

1. Officer opens Price Bulletin Drafter.
2. App loads country options from the cache-backed metadata layer.
3. Officer selects a country.
4. App loads commodities, regions, markets, date range, cache watermark, and saved food basket from the AWS cache/workspace stores.
5. No full Databridges pull happens on selection.
6. Officer selects reporting period, regions, modules, and basket.
7. App generates the bulletin using cached rows.
8. Generated report, charts, warnings, and input parameters are saved to the workspace.

### Journey D - Manual Latest Data Sync

1. Officer sees that cache freshness is behind the desired reporting date.
2. Officer clicks "Sync Latest Data".
3. App determines the latest cached `price_date` for the selected country and relevant selection.
4. App calls Databridges only for the missing date range.
5. App merges delta rows with cached rows for the active run.
6. App records that a manual delta sync occurred.
7. Depending on the decided policy, delta rows are saved to the shared cache, saved to the run only, or queued for admin review.

### Journey E - Custom Food Basket Setup

1. Officer opens Workspace Settings or the Price Bulletin basket editor.
2. Officer selects country.
3. App shows available cached commodities for that country.
4. Officer adds commodities and weights.
5. App validates duplicates, missing commodities, invalid weights, and weight total.
6. Officer saves the composition.
7. App uses the saved composition for future bulletins for that country.

### Journey F - Databridges Data Explorer

1. Officer asks: "Compare local maize prices in Mogadishu and Hargeisa over the last 12 months."
2. Agent asks a clarification only if the request is ambiguous.
3. Agent creates a structured query plan.
4. App retrieves the data.
5. Agent generates a small Python analysis/plotting script.
6. Sandbox runs the script with the retrieved dataset mounted read-only.
7. Chat displays the line graph and a concise explanation.
8. User downloads or saves the chart and data.

### Journey G - Climate Report Draft

1. Officer selects a country/area and period.
2. Tool fetches available climate indicators.
3. Tool computes or retrieves anomaly signals.
4. LLM drafts a climate narrative using only supplied evidence.
5. App surfaces warnings when source coverage is incomplete.
6. Officer downloads or saves the draft for review.

## In Scope

- Email-based authentication with admin whitelist authorization.
- Admin page or API for manual authorized email management.
- Per-user workspace metadata.
- Per-user generated file storage.
- Per-user operation history.
- Per-user and optionally per-tool LLM model preferences.
- Vertex AI model registry and runtime model resolver.
- Migration of all LLM-using tools to the resolver.
- AWS database-backed cache for Databridges price data.
- Two-week automatic Databridges-to-AWS sync.
- Manual latest-data sync from Databridges for missing deltas.
- Price Bulletin form changes needed to expose cache freshness and manual sync.
- Custom country-specific food basket compositions with weights.
- Weighted food basket calculations in Price Bulletin statistics, charts, and report text.
- Climate Report Drafter framework.
- Databridges Data Explorer / Graph Composer with sandboxed code execution.
- Workspace-aware generated file downloads.
- Audit logging for authentication, tool runs, model selection, cache syncs, and generated artifacts.
- Security and regression tests for workspace isolation and sandbox escape attempts.

## Out Of Scope

- Public self-registration.
- Direct writes to Databridges source systems.
- Autonomous publication of reports.
- Beneficiary-level or household-level PII handling.
- Full enterprise document management workflow.
- Complete replacement of all Streamlit pages with a new frontend framework.
- Unlimited arbitrary code execution by users or LLMs.
- Guaranteeing support for every possible Vertex AI model shape without model capability metadata.
- Final climate methodology decisions before data sources and report format are clarified.

## Product Principles

1. Fail closed on access control.
   If identity or authorization cannot be verified, block access.

2. Keep workspaces isolated by design.
   User IDs and storage namespaces must be enforced in service code, not just UI code.

3. Make model use explicit and traceable.
   Every generated output should identify the model used.

4. Prefer cached Databridges data by default.
   Expensive live Databridges calls should be explicit user actions or background sync jobs.

5. Never hide cache freshness.
   Users must know whether they are using latest data, cached data, or manually refreshed data.

6. Preserve officer control.
   Food basket composition, report review, and manual data refresh are user-visible decisions.

7. Keep generated analysis reproducible.
   Save inputs, data version, code snippets where appropriate, model choice, and generated outputs.

8. Treat LLM-generated code as untrusted.
   Sandbox first; never execute generated code in the main app process.

## Success Metrics

### Access And Workspace

- 100 percent of app entry points require authentication.
- 100 percent of authenticated users are checked against the whitelist.
- Unauthorized emails are blocked.
- Workspace isolation tests pass for files, preferences, and run history.

### Price Bulletin Performance

- Country metadata loads from cache in 1-2 seconds for commonly used countries.
- Country selection does not call Databridges for full historical data.
- Manual latest sync fetches only rows after the cache watermark.
- Price Bulletin generation records the cache date and any manual refresh date.

### Food Basket

- Users can create and save a valid country-specific composition.
- Saved composition is reused on the next session.
- Reports include the composition used.
- Missing commodity coverage is surfaced as a warning.

### LLM Flexibility

- All LLM-using tools can run with a selected Vertex AI model.
- Logs record selected model, tool, user, run, and token/cost metadata when available.
- Model comparison evals can run without code changes.

### New Tools

- Data Explorer can produce a chart from a natural language Databridges request.
- Sandbox blocks known unsafe operations.
- Climate Report Drafter produces a structured draft with source/caveat metadata.

## Top Risks

1. Workspace data leakage.
   A user seeing another user's files or history is a release-blocking issue.

2. Unsafe code execution.
   The Data Explorer sandbox must be treated as a major security boundary.

3. Misleading Price Bulletin outputs.
   Cached data freshness, manual delta data, and missing food basket components must be visible.

4. Model selection without traceability.
   Flexible models are useful only if every output records which model produced it.

5. Climate report hallucinations.
   Climate narratives must be grounded in retrieved indicators and explicit methodology.

6. AWS cache ambiguity.
   The app must clearly define whether user-triggered deltas are shared, private, or queued.

## Open Decisions

- Which identity provider will be used in the first Phase 2 deployment: Google Workspace OAuth, Microsoft Entra ID, Cognito, or another WFP-standard provider?
- Which AWS database will be provided for the Databridges cache?
- Can the application write user-triggered Databridges delta refreshes back to the shared AWS cache?
- Are food basket weights normalized shares, real physical quantities, or both?
- Should admins be able to view user workspaces for support?
- Which climate data sources and report format are mandatory for the first Climate Report Drafter release?
- Should Data Explorer save all generated code and outputs automatically, or only after user confirmation?
- Are credit budgets from Phase 1 still part of Phase 2, or deferred?

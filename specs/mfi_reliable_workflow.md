# MFI reliable workflow

This revision implements the coordinated `mfi-reliable-v1` workflow for newly loaded processed CSVs. Keep `MFI_DRAFTER_ANALYSIS_VERSION=2`. Methodology remains `databridge-current`; analytical and narrative schema versions are 2.1. Existing 2.0 reports remain readable through the existing compatibility path. No SDK or model migration is included.

## Changes

- Stable source identities, aliases, duplicate detection, source-row/value lineage, explicit date parsing, count/coordinate validation, and separate parsing/applicability states.
- One analytical ledger and application-rendered facts, including threshold counts and ties; complete dimension evidence appears in the main report or analytical annex.
- Nine independent dimension drafts, market batches of at most five, semantic review batches of at most 40 claims, and homogeneous correction batches of at most four targets. Actual serialized outgoing messages and schemas are checked against 160,000 characters.
- Typed correction contracts, independent target validation, exact context-text normalization, one additional JSON/schema repair attempt per failed target, staged responses and field-hash checks.
- Immutable, checksummed input/artifact objects, deduplicated snapshot field references, transactional execution ownership and fenced commits. A heartbeat renews the 120-second lease every 30 seconds.
- Separate incomplete snapshot/export paths, manual Resume, browser run links, and spawned serial rendering workers. Each figure has a 120-second timeout and one retry.

Rendering launches a dedicated Python module, so it cannot re-execute Streamlit's page script when starting a worker. Correction batches are partitioned using the actual serialized messages and bound response schema, with the same complete regional mean/denominator comparisons used during drafting.

Stored scores, formula tolerances, unweighted means, the exact 15-market selection rule, Haiti's six MFIr exclusions, recommendation restrictions and the existing final unverified-number delivery policy remain in force.

## Storage and deployment compatibility

MFI uses the application's existing async-run backend selection on both local hosts and Cloud Run. Existing memory deployments need no new environment variables or services. The original reliable-workflow release introduced a cloud-only durable-storage requirement; that restriction has been removed to preserve the previous deployment configuration. No deployment resource, billing or shared-service backend behavior is changed.

With no run-storage configuration, or with `RUNS_BACKEND=memory`, checkpoints use memory. Status reports `recovery_storage=process_local` and a `recovery_limitation` displayed beside the run in Streamlit. Completed work and incomplete drafts can be reused while the same server process remains available; recovery after a process restart or on another instance is unavailable. A run URL identifies the run but does not make its storage persistent. An in-process model failure can still be resumed manually without repeating committed work.

Already configured Firestore/GCS deployments continue using their existing `RUNS_BACKEND`, `RUNS_GCS_URI`, `RUNS_FIRESTORE_DATABASE` and `RUNS_FIRESTORE_COLLECTION` settings. Backend aliases, URI autodetection and configuration normalization remain those of the shared async-run service. The execution identity needs read/write access to the existing run collection and GCS prefix. Checkpoints are stored under each run's `mfi/checkpoint` subdocument; immutable objects use the `mfi-recovery` prefix. Once the durable backend is selected, failed durable operations block progress rather than diverting that run to memory. Full-prompt capture is not enabled.

Background execution remains subject to the existing Cloud Run CPU and instance lifecycle settings. Google's [background activity guidance](https://docs.cloud.google.com/run/docs/tips/general#background_activity) describes CPU availability outside requests; this compatibility fix does not change billing or provision resources. Restart recovery requires an existing durable backend. Status polling never schedules work.

A run records the contract bundle, methodology, schemas, claim identity version, model/runtime configuration and dependency versions. Resume rejects incompatible bundles/configurations and changed inputs. Rollback affects new submissions; preserved checkpoints remain non-resumable where the recorded bundle is unsupported. Historical failures without checkpoints, including `mfi_3523f014`, cannot be reconstructed from diagnostic logs.

## Interfaces

Existing generation/status/result/artifact/final-DOCX interfaces remain. New routes:

- `POST /mfi-drafter/resume/{run_id}` with `expected_revision` and `idempotency_key`: 202 scheduled/replayed; 409 conflict, active/completed or non-resumable run; 404 unknown run; 503 unavailable recovery storage.
- `GET /mfi-drafter/draft/{run_id}?snapshot_revision=...`: an immutable incomplete narrative snapshot and QA findings.
- `POST /mfi-drafter/export-draft-docx/{run_id}` with optional `snapshot_revision`: a `DRAFT-` DOCX with the incomplete label in its title and page headers. Export does not publish, change QA or unlock final export.
- `GET /mfi-drafter/analysis/{run_id}`: validated analytical data as soon as analysis is saved.

Status adds workflow/run revisions, execution state, resumability/reason, recovery storage and its limitations, draft revision, analytical availability and evaluated QA counts. The original pending/running/completed/failed status vocabulary is preserved. Both API and in-process Streamlit use the same execution journal, recovery and export service.

## Verification and release gate

Run `python scripts/check_mfi_reliable.py` in the project environment. It selects the complete MFI suite plus affected shared-service tests on either Windows or Linux. The supplied source CSVs remain local/untracked in `MFI Test Databases`; tests that require them skip if absent. The committed baseline contains analytical regression expectations.

Validated local dependencies: Python 3.12.4; pandas 2.3.3; numpy 2.4.0; pydantic 2.13.4; langgraph 1.0.5; langchain-core 1.4.0; langchain-google-vertexai 3.2.3; matplotlib 3.10.8; python-docx 1.2.0; google-cloud-firestore 2.27.0; google-cloud-storage 3.10.1. Dependencies are recorded per run rather than silently changing a resumed run's contract.

Windows verification on 8 September 2026: the full MFI/shared sweep returned 840 passes, four existing expected failures and two historical-schema assertion failures. Those two expectations were corrected to preserve 2.0 inputs and passed in the subsequent 53-test compatibility/API/correction rerun. Additional focused runs passed 28 full-data input/package/recovery tests, 37 API/storage/shared-interface tests and 14 identity/applicability tests. The full-data checks cover both supplied CSVs, all nine dimension and correction packages, complete review comparator tables, unchanged analytical baselines, and the audited Benin counts. A Streamlit-followed-by-rendering run also verified worker isolation. No live model run was performed.

Before promotion, run the same tests inside the Python 3.11 Linux container, installing the project's test runner if necessary. The implementation host's Docker Linux engine was unavailable, so Linux execution has not been certified locally.

Storage compatibility verification on 9 September 2026: 87 targeted API, Streamlit-dispatch, recovery, delivery and shared-service tests passed, including Benin CSV submission under the previous Cloud Run memory configuration. Both Benin/Haiti analytical baseline checks also passed. The cloud-only 503 was reproduced before the fix. These checks use local execution and mocked model/cloud boundaries; no live generation or infrastructure change was performed.

The owner will run live validation after the commit/push:

1. Submit Benin and Haiti in a test deployment. Confirm 53 and 68 Full-MFI markets respectively, with six Haiti exclusions. Check unchanged priority ordering and scores against the baseline.
2. Inspect every dimension's main-body/annex coverage. Verify Service at/below median = 37/53, Infrastructure below 3 = 7, and Oueme Food Quality mean = 4.79. Inspect the DOCX and all chart labels.
3. Inject a generation failure after completed narrative batches; download the marked draft and Resume once in the same server process. Verify saved batches/figures and call IDs are reused. Where a durable backend is already configured, additionally stop the server, wait for lease expiry and verify recovery on a new process; that restart check does not apply to memory deployments.
4. Repeat an identical Resume idempotency key and send a stale revision. Confirm no duplicate execution and the documented conflict response. Confirm final endpoints remain locked for incomplete snapshots.
5. Confirm the reported storage capability, outgoing package sizes, retries, model latency, QA findings and rendering memory; verify durable writes only where durable storage is already configured. Keep this as a test deployment until these live and Linux checks pass.

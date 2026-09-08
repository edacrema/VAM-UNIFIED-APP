"""Immutable incomplete snapshots and exports, separate from final publication."""
from __future__ import annotations
import base64
import io
from datetime import datetime, timezone

from .execution import RecoveryError, recovery_store, load_snapshot
from .coverage import annex_blocks, evaluate_coverage

DRAFT_LABEL = "INCOMPLETE DRAFT — NOT VALIDATED"


def snapshot(run_id, revision=None, *, store=None):
    store = store or recovery_store()
    manifest = store.read(run_id)
    if not manifest or manifest.get("draft_revision") is None:
        raise RecoveryError("No structurally valid narrative snapshot is available", 409)
    revision = int(revision) if revision is not None else manifest["draft_revision"]
    ref = manifest.get("snapshots", {}).get(str(revision))
    if not ref:
        raise RecoveryError("Draft snapshot revision does not exist", 404)
    return revision, load_snapshot(store, ref)


def draft_payload(run_id, revision=None, *, store=None):
    from app.shared.report_blocks import ReportBlock, _apply_mfi_layout_contract
    revision, state = snapshot(run_id, revision, store=store)
    generated = datetime.now(timezone.utc).isoformat()
    flags = {str(flag.get("flag_id")): flag for flag in [*state.get("deterministic_flags", []),
        *state.get("red_team_flags", []), *(state.get("qa_review") or {}).get("flags", [])]}
    findings = list(flags.values())
    affected = {flag.get("claim_id") for flag in findings}
    reviewed = (state.get("generation_diagnostics") or {}).get("red_team_status") == "completed"
    blocks = [ReportBlock(type="heading", text=DRAFT_LABEL, level=1),
        ReportBlock(type="paragraph", text=f"Run: {run_id}. Snapshot revision: {revision}. Export generated: {generated}."),
        ReportBlock(type="limitation_box", text="This snapshot is incomplete and has not passed final delivery validation. It is not a final report."),
        ReportBlock(type="heading", text="Coverage and completion", level=2)]
    dimensions = state.get("assessment_profile", {}).get("dimensions", [])
    for dim in dimensions:
        name = dim["dimension"]
        complete = bool(state.get("dimension_narratives", {}).get(name))
        blocks.append(ReportBlock(type="paragraph", text=f"{name}: {'generated' if complete else 'not yet generated'}; {'review attempted — see findings' if reviewed else 'not yet reviewed'}."))
    def claims(value):
        if isinstance(value, dict):
            if "text" in value and ("claim_id" in value or "statement_id" in value):
                yield value
            else:
                for item in value.values():
                    yield from claims(item)
        elif isinstance(value, list):
            for item in value:
                yield from claims(item)
    for title, content in (("Context", state.get("context_evidence", [])), ("Executive summary", state.get("executive_summary_narrative", {})),
                           ("Dimension analysis", state.get("dimension_narratives", {})), ("Selected markets", state.get("market_narratives", {}))):
        blocks.append(ReportBlock(type="heading", text=title, level=2))
        if not content:
            blocks.append(ReportBlock(type="paragraph", text="Not yet generated."))
        for claim in claims(content):
            claim_id = claim.get("claim_id") or claim.get("statement_id")
            marker = "[UNRESOLVED FINDING] " if claim_id in affected else "[NOT YET REVIEWED] " if not reviewed else ""
            blocks.append(ReportBlock(type="paragraph", text=marker + str(claim.get("text", "")), meta={"claim_id": claim_id}))
    if state.get("assessment_profile"):
        blocks.extend(annex_blocks(state))
    charts = {}
    for figure_id, value in state.get("visualizations", {}).items():
        try:
            from PIL import Image
            raw = base64.b64decode(value.split(",", 1)[-1], validate=True)
            Image.open(io.BytesIO(raw)).verify()
            charts[figure_id] = value
            blocks.append(ReportBlock(type="figure", figure_id=figure_id, caption=figure_id))
        except Exception:
            blocks.append(ReportBlock(type="limitation_box", text=f"Chart omitted because its artifact is invalid: {figure_id}."))
    if not charts:
        blocks.append(ReportBlock(type="paragraph", text="Charts are not available in this snapshot."))
    blocks.append(ReportBlock(type="heading", text="Unresolved findings and incomplete work", level=2))
    if not findings:
        blocks.append(ReportBlock(type="paragraph", text="No findings have been recorded at this snapshot. This does not establish that review or final validation is complete."))
    for finding in findings:
        blocks.append(ReportBlock(type="qa_warning", text=f"{finding.get('severity', 'unknown').upper()} — {finding.get('claim_id') or finding.get('artifact_id')}: {finding.get('message')}", meta={"finding": finding}))
    blocks = _apply_mfi_layout_contract(blocks, country=state.get("country", ""), methodology_version="databridge-current")
    coverage = evaluate_coverage(state.get("assessment_profile", {}), blocks, state.get("dimension_narratives", {}))
    return {"run_id": run_id, "snapshot_revision": revision, "generated_at": generated, "label": DRAFT_LABEL,
            "is_final": False, "report_blocks": [b.model_dump(mode="json") for b in blocks], "coverage": coverage,
            "findings": findings, "visualizations": charts}


def export_draft(run_id, revision=None):
    from app.shared.docx_export import build_docx_bytes_from_report_blocks
    from app.shared.report_blocks import ReportBlock
    payload = draft_payload(run_id, revision)
    content = build_docx_bytes_from_report_blocks([ReportBlock.model_validate(b) for b in payload["report_blocks"]],
        visualizations=payload["visualizations"], draft_identity={"run_id": run_id, "revision": payload["snapshot_revision"]})
    return content, f"DRAFT-mfi-{run_id}-r{payload['snapshot_revision']}.docx"


def analysis_payload(run_id):
    store = recovery_store()
    manifest = store.read(run_id)
    if not manifest or not manifest.get("analysis_available"):
        raise RecoveryError("Validated analysis is not yet available", 409)
    state = load_snapshot(store, manifest["snapshot_ref"])
    return {"run_id": run_id, "assessment_profile": state["assessment_profile"],
            "input_findings": (state.get("csv_data") or {}).get("input_findings", []), "is_final_report": False}

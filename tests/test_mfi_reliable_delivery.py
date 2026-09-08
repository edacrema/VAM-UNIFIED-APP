import io
import zipfile
import pytest
from app.services.mfi_drafter.execution import MemoryRecoveryStore, Execution, create_checkpoint, reserve_execution
from app.services.mfi_drafter.drafts import draft_payload, DRAFT_LABEL
from app.services.mfi_drafter.coverage import body_projection, evaluate_coverage, table_block
from app.services.mfi_drafter.facts import subject_binding_problems
from app.services.mfi_drafter.review import reconcile_numerical_finding


def test_draft_export_is_labeled_and_does_not_publish():
    from app.shared.docx_export import build_docx_bytes_from_report_blocks
    from app.shared.report_blocks import ReportBlock
    store = MemoryRecoveryStore()
    create_checkpoint(store, "draft-test", {}, {})
    execution = Execution(store, "draft-test", reserve_execution(store, "draft-test"))
    execution.snapshot({"country": "Benin", "dimension_narratives": {"Service": {
        "summary": {"claim_id": "dimension.service.summary.1", "text": "Saved preliminary analysis."}}},
        "deterministic_flags": [{"flag_id": "f1", "claim_id": "dimension.service.summary.1", "severity": "high", "message": "Check this claim."}]})
    execution.finish(ValueError("interrupted"))
    before = store.read("draft-test")
    payload = draft_payload("draft-test", store=store)
    assert payload["is_final"] is False and payload["findings"]
    assert any("UNRESOLVED FINDING" in (block.get("text") or "") for block in payload["report_blocks"])
    content = build_docx_bytes_from_report_blocks([ReportBlock.model_validate(b) for b in payload["report_blocks"]], draft_identity={"run_id": "draft-test", "revision": payload["snapshot_revision"]})
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        headers = [archive.read(path).decode() for path in archive.namelist() if path.startswith("word/header")]
        assert headers and all(DRAFT_LABEL in header for header in headers)
        assert DRAFT_LABEL in archive.read("word/document.xml").decode()
    assert store.read("draft-test") == before


def test_body_projection_keeps_canonical_analysis_and_coverage_checks_visible_blocks():
    source = {"Price": {"key_findings": list(range(6)), "subdimension_analysis": list(range(4)), "geographic_patterns": list(range(3))}}
    body = body_projection(source)
    assert list(map(len, body["Price"].values())) == [3, 2, 2]
    assert len(source["Price"]["key_findings"]) == 6
    profile = {"coverage_manifest": [{"requirement_id": "r", "dimension": "Price"}]}
    assert not evaluate_coverage(profile, [])["complete"]
    table = table_block("Evidence", [{"value": "5.00"}], [("value", "Score")], requirements=["r"])
    assert evaluate_coverage(profile, [table])["complete"]


def test_swapped_named_market_scores_fail():
    entries = [{"market_name": "Market A", "unit": "score", "numeric_value": 3}, {"market_name": "Market B", "unit": "score", "numeric_value": 7}]
    assert len(subject_binding_problems("Market A scored 7/10 and Market B scored 3/10.", entries)) == 2
    assert not subject_binding_problems("Market A scored 3/10 and Market B scored 7/10.", entries)


def test_reviewer_cannot_replace_37_with_half_the_sample():
    review = {"package": {"evidence_by_metric_id": {"count": {"numeric_value": 37}}}}
    finding = {"issue_type": "data_mismatch", "fact_ids": ["count"], "claimed_value": 37, "expected_value": 27}
    assert not reconcile_numerical_finding(finding, review, {"text": "37 of 53 markets", "fact_ids": ["count"]})
    assert review["rejected_findings"]


def test_formatted_spelled_and_fractional_quantities_are_checked():
    from app.services.mfi_drafter.narrative import _numeric_tokens
    values = {(value, percent) for _, value, percent in _numeric_tokens(
        "1,234.5 points; thirty-seven of fifty-three assessed markets; "
        "one hundred and two traders; two thousand five markets; "
        "twenty point five percent; scores range from 2 to 8/10.")}
    assert {(1234.5, False), (37, False), (53, False), (102, False),
            (2005, False), (20.5, True), (2, False), (8, False)} <= values


def test_stale_field_hash_cannot_be_published():
    from app.services.mfi_drafter.simple_orchestration import apply_consolidated_patches
    with pytest.raises(ValueError, match="stale"):
        apply_consolidated_patches(targets=[{"task_id":"t", "artifact_type":"context",
            "artifact_id":"c", "field_name":"text", "expected_field_hash":"old"}],
            replacements={"t":"replacement"}, dimension_narratives={}, market_narratives={},
            executive_narrative={}, context_evidence=[{"statement_id":"c", "text":"changed"}], assessment_profile={})

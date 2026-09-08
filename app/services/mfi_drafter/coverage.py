"""Visible analytical coverage and lossless body/annex projection."""
from __future__ import annotations
from copy import deepcopy

BODY_LIMITS = {"key_findings": 3, "subdimension_analysis": 2, "geographic_patterns": 2}


def body_projection(narratives):
    result = deepcopy(narratives)
    for narrative in result.values():
        for field, limit in BODY_LIMITS.items():
            narrative[field] = narrative.get(field, [])[:limit]
    return result


def table_block(title, rows, columns, *, requirements=()):
    from app.shared.report_blocks import ReportBlock
    return ReportBlock(type="table", text=title, meta={"table_kind": "mfi_presentation",
        "spec_id": "mfi.analytical_annex.v1", "title": title, "columns": [key for key, _ in columns],
        "column_specs": [{"key": key, "label": label, "width_hint": 3 if index == 0 else 1,
                          "alignment": "left" if index == 0 else "right"} for index, (key, label) in enumerate(columns)],
        "rows": [{"values": row} for row in rows], "coverage_requirement_ids": list(requirements)})


def annex_blocks(result):
    from app.shared.report_blocks import ReportBlock, _append_mfi_claim, _mfi_qa_context
    profile, narratives = result["assessment_profile"], result.get("dimension_narratives", {})
    requirements = profile.get("coverage_manifest", [])
    blocks = [ReportBlock(type="heading", text="Analytical annex: complete dimension evidence", level=2)]
    def formatted(value):
        return "Unavailable" if value is None else f"{value:.2f}"
    for dimension in profile.get("dimensions", []):
        name = dimension["dimension"]
        blocks.append(ReportBlock(type="heading", text=name, level=3,
                                  meta={"section": "analytical_annex", "dimension": name}))
        for field, limit in BODY_LIMITS.items():
            for item in narratives.get(name, {}).get(field, [])[limit:]:
                claim = item.get("interpretation") if field == "subdimension_analysis" else item
                _append_mfi_claim(blocks, claim, catalog=result.get("claim_catalog", {}),
                    documents={doc["doc_id"]: doc for doc in result.get("contextual_documents", [])},
                    qa_context=_mfi_qa_context(result), include_evidence_notes=False)
        own = [r for r in requirements if r["dimension"] == name]
        by_kind = {r["kind"]: r["requirement_id"] for r in own}
        stats = dimension.get("statistics", {})
        rows = [{"statistic": key.replace("_", " ").title(), "value": formatted(stats.get(key))}
                for key in ("mean", "median", "minimum", "maximum", "q1", "q3", "iqr", "score_range")]
        blocks.append(table_block(f"{name}: unweighted score distribution (0–10)", rows,
            [("statistic", "Statistic"), ("value", "Score")], requirements=[by_kind["distribution"]]))
        local = dimension.get("localized_patterns", {}).get("ordered_markets", [])
        extrema = [{"market": item["name"], "score": formatted(item["value"]),
                    "extreme": "Minimum" if abs(item["value"] - stats["minimum"]) <= 1e-6 else "Maximum"}
                   for item in local if abs(item["value"] - stats["minimum"]) <= 1e-6 or abs(item["value"] - stats["maximum"]) <= 1e-6]
        if extrema and "local_extremes" in by_kind:
            blocks.append(table_block(f"{name}: local extremes, including all ties", extrema,
                [("market","Market"),("score","Score /10"),("extreme","Comparison")], requirements=[by_kind["local_extremes"]]))
        regions = dimension.get("regional_summaries", [])
        rows = [{"region": r["region"], "mean": formatted(r["statistics"]["mean"]),
                 "count": str(r["statistics"].get("coverage", {}).get("available_market_count", "Unavailable"))} for r in regions]
        if rows:
            blocks.append(table_block(f"{name}: complete regional comparisons", rows,
                [("region", "Region"), ("mean", "Mean /10"), ("count", "Markets")], requirements=[by_kind["geography"]]))
        else:
            blocks.append(ReportBlock(type="limitation_box", text=f"Regional comparison is unavailable for {name}.", meta={"coverage_requirement_ids": [by_kind["geography"]]}))
        for label, metrics in (("Official subsections and special scoring components", dimension.get("subsections", [])),
                               ("Fixed drivers and product indicators", dimension.get("drivers", []))):
            rows, covered = [], []
            for metric in metrics:
                count = metric.get("coverage", {}).get("available_market_count")
                total = metric.get("coverage", {}).get("total_assessed_market_count", profile.get("assessed_market_count"))
                unavailable = metric.get("mean_raw_value") is None and metric.get("mean_normalized_value") is None
                classification = (metric.get("availability") or {}).get("classification")
                status = "Unavailable; applicability unknown" if unavailable else "Observed"
                if classification == "unknown_applicability" and not unavailable:
                    status += "; applicability unknown elsewhere"
                elif classification == "not_applicable":
                    status = "Explicitly inapplicable outside the stated coverage"
                if metric.get("role") == "item_driver" and not metric.get("item_relevant"):
                    status += "; not selected by item eligibility rule"
                adverse = metric.get("unfavorable_rate")
                rows.append({"indicator": metric.get("display_name", metric["metric_id"]),
                    "raw": formatted(metric.get("mean_raw_value")), "score": formatted(metric.get("mean_normalized_value")),
                    "adverse": "Not directional" if adverse is None else f"{adverse * 100:.1f}%",
                    "coverage": f"{count if count is not None else 'Unavailable'}/{total}", "evidence": status})
                if metric["metric_id"] in by_kind:
                    covered.append(by_kind[metric["metric_id"]])
            if rows:
                blocks.append(table_block(f"{name}: {label}", rows,
                    [("indicator", "Indicator"), ("raw", "Raw mean"), ("score", "Normalized /10"),
                     ("adverse", "Adverse rate"), ("coverage", "Markets"), ("evidence", "Evidence / applicability")], requirements=covered))
        facts = dimension.get("analytical_facts", {})
        for fact_id, fact in facts.items():
            if fact_id.endswith("at_or_below_median") or ".region." in fact_id:
                blocks.append(ReportBlock(type="paragraph", text=fact["rendered_text"], meta={"fact_id": fact_id}))
    return blocks


def evaluate_coverage(profile, blocks, narratives=None):
    visible = {}
    for index, block in enumerate(blocks):
        value = block.model_dump() if hasattr(block, "model_dump") else block
        for requirement_id in (value.get("meta") or {}).get("coverage_requirement_ids", []):
            visible.setdefault(requirement_id, []).append(f"block:{index}")
    manifest = deepcopy(profile.get("coverage_manifest", []))
    for requirement in manifest:
        requirement["satisfied_by"] = visible.get(requirement["requirement_id"], [])
        requirement["status"] = "covered" if requirement["satisfied_by"] else "pending"
    missing_dimensions = [d["dimension"] for d in profile.get("dimensions", []) if narratives is not None and not narratives.get(d["dimension"], {}).get("summary")]
    return {"requirements": manifest, "complete": not missing_dimensions and all(r["status"] != "pending" for r in manifest),
            "missing_dimensions": missing_dimensions, "covered": sum(r["status"] == "covered" for r in manifest), "total": len(manifest)}

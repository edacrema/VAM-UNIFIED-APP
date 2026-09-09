"""Lossless relevant projections of the existing analytical engine; no new calculations."""
from __future__ import annotations

from copy import deepcopy
from .methodology import METRIC_DEFINITIONS_BY_ID

_LINEAGE = {"ledger_metric_ids", "source_metric_ids", "member_ids", "metric_ledger",
            "workflow_revision", "satisfied_by", "contributing_metric_ids"}


def compact(value):
    if isinstance(value, dict):
        return {k: compact(v) for k, v in value.items() if k not in _LINEAGE}
    if isinstance(value, list):
        return [compact(v) for v in value]
    return value


def table(rows, columns):
    return {"columns": columns, "rows": [[compact(row.get(c)) for c in columns] for row in rows]}


def selected_markets(profile):
    return sorted((m for m in profile["markets"] if m["is_priority_market"]),
                  key=lambda m: m["selection_order"])


def section_specs(profile, family):
    if family == "dimensions":
        return [{"section_id": d["dimension"], "title": d["dimension"]} for d in profile["dimensions"]]
    return [{"section_id": m.get("market_key") or m["market_name"], "title": m["market_name"]}
            for m in selected_markets(profile)]


def source_map(documents):
    docs = sorted(documents, key=lambda d: (str(d.get("doc_id", "")), str(d.get("url", ""))))
    seen, result = set(), {}
    for doc in docs:
        key = doc.get("url") or doc.get("doc_id")
        if not key or key in seen or not doc.get("content"):
            continue
        seen.add(key)
        result[f"S{len(result)+1}"] = {k: doc.get(k) for k in ("doc_id", "title", "source", "date", "url", "content")}
    return result


def metric_table(rows):
    prepared = []
    for row in rows:
        definition = METRIC_DEFINITIONS_BY_ID.get(row["metric_id"])
        prepared.append({**row, "parent_subsection_id": definition.parent_subsection_id if definition else None})
    return table(prepared, ["metric_id", "display_name", "role", "parent_subsection_id", "unit", "orientation",
        "evidence_scope", "mean_raw_value", "mean_normalized_value", "unfavorable_rate", "coverage",
        "availability", "product_group", "question_group", "item_name", "item_relevant", "relevance_reasons", "permitted_subject_phrase"])


def evidence(state, family, ids):
    profile = state["assessment_profile"]
    common = {"country": state["country"], "period": [state["data_collection_start"], state["data_collection_end"]],
        "methodology": state["methodology_version"], "survey_metadata": state["survey_metadata"],
        "overall": compact(profile["overall_statistics"]), "priority_dimensions": profile["priority_dimension_names"],
        "limitations": compact(profile["limitations"]), "sources": state.get("sources", {}),
        "context_limitation": state.get("context_limitation"),
        "market_scores": {"columns": ["market_key", "market_name", "region", "overall_mfi"] + [d["dimension"] for d in profile["dimensions"]],
            "rows": [[m.get("market_key"), m["market_name"], m.get("region"), m["overall_mfi"]] +
                     [next(x["score"] for x in m["dimension_profile"] if x["dimension"] == d["dimension"]) for d in profile["dimensions"]]
                     for m in profile["markets"]]}}
    if family == "dimensions":
        common["dimensions"] = [{"dimension": d["dimension"], "statistics": compact(d["statistics"]),
            "priority": d["is_priority"], "rank_among_dimensions": d["profile_rank"],
            "regions": compact(d["regional_summaries"]), "facts": compact(d["analytical_facts"]),
            "components_and_drivers": metric_table([*d["subsections"], *d["drivers"]])}
            for d in profile["dimensions"] if d["dimension"] in ids]
    elif family == "markets":
        from .simple_orchestration import build_market_prompt_projection
        selected = [m for m in selected_markets(profile) if (m.get("market_key") or m["market_name"]) in ids]
        common["selected_markets"] = table(selected, ["market_key", "market_name", "score_rank", "selection_order", "selection_reasons"])
        rows, definitions, ranks = [], {}, []
        for market in selected:
            projection = build_market_prompt_projection(profile, market)
            ranks.extend({"market_key": market["market_key"], "dimension": d["dimension"],
                "rank_within_market": d["rank"], "is_weak": d["is_weak"]} for d in market["dimension_profile"])
            for ledger_id in projection["selected_ledger_metric_ids"]:
                entry = profile["metric_ledger"][ledger_id]
                if entry["statistic"] not in {"market_explanatory_normalized_value", "market_explanatory_raw_value", "derived_market_unfavorable_rate"}:
                    continue  # Stored scores and ranks have their own single tables.
                metric_id = next((s for s in entry.get("source_metric_ids", []) if s in METRIC_DEFINITIONS_BY_ID), None)
                if not metric_id:  # Scores already appear once in the comparator matrix.
                    continue
                definition = METRIC_DEFINITIONS_BY_ID[metric_id]
                definitions[metric_id] = {"metric_id": metric_id, "name": definition.display_name,
                    "dimension": definition.dimension, "role": definition.role, "parent_subsection_id": definition.parent_subsection_id}
                rows.append({"market_key": market["market_key"], "metric_id": metric_id, "value": entry["value"],
                    "unit": entry["unit"], "statistic": entry["statistic"], "coverage": entry.get("coverage"),
                    "population_basis": entry.get("population_basis"), "subject": entry.get("permitted_subject_phrase")})
        common["within_market_dimension_ranks"] = table(ranks, ["market_key", "dimension", "rank_within_market", "is_weak"])
        common["indicator_definitions"] = table(list(definitions.values()), ["metric_id", "name", "dimension", "role", "parent_subsection_id"])
        common["local_evidence"] = table(rows, ["market_key", "metric_id", "value", "unit", "statistic", "coverage", "population_basis", "subject"])
    else:
        common["dimension_summary"] = table(profile["dimensions"], ["dimension", "statistics", "profile_rank", "is_priority"])
        common["selected_market_summary"] = table(selected_markets(profile), ["market_key", "market_name", "region", "overall_mfi", "score_rank", "selection_order"])
    return deepcopy(common)

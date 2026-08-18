"""Canonical reader-facing evidence-note composition.

The composer is intentionally independent of report blocks and Pydantic schemas.  Both
the narrative utilities and the report builder call this function, which prevents their
scope and representation wording from drifting apart.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Mapping


def _text(value: Any) -> str:
    return str(value or "").strip()


def _base_metric_label(entry: Mapping[str, Any]) -> str:
    label = _text(entry.get("label"))
    return label.split(":", 1)[0].strip() or label


def _joined_labels(labels: list[str]) -> str:
    unique = list(dict.fromkeys(label for label in labels if label))
    if not unique:
        return "metrics"
    if len(unique) == 1:
        return unique[0]
    if len(unique) == 2:
        return f"{unique[0]} and {unique[1]}"
    return f"{', '.join(unique[:-1])}, and {unique[-1]}"


def _claim_scope(
    claim: Mapping[str, Any], entries: list[Mapping[str, Any]]
) -> str:
    scope = _text(claim.get("scope"))
    if not scope and entries:
        scope = _text(entries[0].get("scope"))
    market_names = list(
        dict.fromkeys(_text(entry.get("market_name")) for entry in entries)
    )
    market_names = [name for name in market_names if name]
    region_names = list(
        dict.fromkeys(_text(entry.get("region")) for entry in entries)
    )
    region_names = [name for name in region_names if name]
    if scope == "surveyed_traders":
        location = ""
        if market_names:
            location = f" — market {_joined_labels(market_names)}"
        elif region_names:
            location = f" — region {_joined_labels(region_names)}"
        return "Claim scope: surveyed traders" + location
    if scope == "context":
        return "Claim scope: context"
    if scope == "market" or market_names:
        return "Claim scope: market" + (
            f" — {_joined_labels(market_names)}" if market_names else ""
        )
    if scope == "region" or region_names:
        return "Claim scope: region" + (
            f" — {_joined_labels(region_names)}" if region_names else ""
        )
    return "Claim scope: assessed-market profile"


def _representation_disclosures(
    entries: list[Mapping[str, Any]],
) -> list[str]:
    grouped: "OrderedDict[tuple[str, int, int], list[str]]" = OrderedDict()
    for entry in entries:
        if not bool(entry.get("representation_required")):
            continue
        kind = _text(entry.get("representation_kind")) or "none"
        available = entry.get("represented_market_count")
        total = entry.get("assessed_market_count")
        if kind == "none" or not isinstance(available, int) or not isinstance(total, int):
            continue
        key = (kind, available, total)
        grouped.setdefault(key, []).append(_base_metric_label(entry))

    disclosures: list[str] = []
    for (kind, available, total), labels in grouped.items():
        names = _joined_labels(labels)
        if kind == "item":
            disclosures.append(
                f"Item representation: {names} observed in {available}/{total} "
                "assessed markets"
            )
        elif kind == "applicability":
            disclosures.append(
                f"Applicability representation: {names} available in "
                f"{available}/{total} assessed markets where the evidence was applicable"
            )
        elif kind == "fixed_metric":
            noun = "metric" if len(set(labels)) == 1 else "metrics"
            disclosures.append(
                f"Assessment representation: {names} {noun} available in "
                f"{available}/{total} assessed markets"
            )
    return disclosures


def compose_evidence_note(
    claim: Mapping[str, Any],
    catalog: Mapping[str, Mapping[str, Any]],
    documents: Mapping[str, Mapping[str, Any]],
) -> str:
    """Compose one concise note in scope, value, representation, source order."""
    entries: list[Mapping[str, Any]] = []
    metric_parts: list[str] = []
    seen_metrics: set[str] = set()
    for metric_id in claim.get("metric_ids", []) or []:
        key = str(metric_id)
        if key in seen_metrics:
            continue
        seen_metrics.add(key)
        entry = catalog.get(key)
        if not isinstance(entry, Mapping):
            continue
        entries.append(entry)
        label = _text(entry.get("label")) or "Metric"
        formatted = _text(entry.get("formatted_value")) or "—"
        metric_parts.append(f"{label}: {formatted}")

    source_parts: list[str] = []
    seen_documents: set[str] = set()
    for document_id in claim.get("document_ids", []) or []:
        key = str(document_id)
        if key in seen_documents:
            continue
        seen_documents.add(key)
        document = documents.get(key)
        if not isinstance(document, Mapping):
            continue
        label = _text(document.get("title") or document.get("source"))
        if not label:
            continue
        date = _text(document.get("date"))
        source_parts.append(label + (f" ({date})" if date else ""))

    if not entries and not source_parts:
        return ""
    scope_claim = dict(claim)
    if not scope_claim.get("scope") and source_parts and not entries:
        scope_claim["scope"] = "context"
    clauses = [_claim_scope(scope_claim, entries)]
    clauses.extend(metric_parts)
    clauses.extend(_representation_disclosures(entries))
    if source_parts:
        clauses.append("Context sources: " + "; ".join(source_parts))
    return "; ".join(clauses) + "."

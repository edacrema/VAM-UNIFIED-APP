"""Pure contracts for the simplified live MFI narrative orchestration.

The application owns all artifact and claim identities.  Models draft prose and
return semantic findings, but they never provide authoritative routing metadata.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from .claim_identity import canonical_claim_index, normalized_slug
from .methodology import DISPLAY_DIMENSIONS, NARRATIVE_PROHIBITIONS
from .narrative import (
    compact_catalog,
    dimension_catalog_ids,
    executive_catalog_ids,
    market_catalog_ids,
    parse_dimension_narrative,
    parse_market_narrative,
)
from .qa_pipeline import (
    apply_field_patch,
    build_sequential_correction_tasks,
    correction_field_patch_contract,
    project_correction_transport,
    validate_field_patch_payload,
)
from .schemas import MFINarrativeQAFlag


NARRATIVE_ORCHESTRATION_VERSION = "mfi-narrative-simple-v1"
SEMANTIC_REVIEW_CONTRACT_VERSION = "mfi-semantic-review-v1"
SEMANTIC_REVIEW_MAX_CHARACTERS = 200_000
MAX_MARKETS_PER_DRAFT_BATCH = 5

DIMENSION_DRAFT_OPERATION = "mfi.dimension_batch_drafting.v1"
MARKET_DRAFT_OPERATION = "mfi.market_batch_drafting.v1"
EXECUTIVE_DRAFT_OPERATION = "mfi.executive_drafting.v3"
CONSOLIDATED_CORRECTION_OPERATION = "mfi.consolidated_correction.v1"
CORRECTED_CLAIM_VERIFICATION_OPERATION = (
    "mfi.corrected_claim_verification.v1"
)
SEMANTIC_REVIEW_OPERATIONS = {
    "overview": "mfi.semantic_review.overview.v1",
    "dimensions": "mfi.semantic_review.dimensions.v1",
    "markets": "mfi.semantic_review.markets.v1",
}

_DIMENSION_FIELDS = (
    "summary",
    "key_findings",
    "subdimension_analysis",
    "geographic_patterns",
    "data_limitations",
    "recommendations",
)
_MARKET_FIELDS = (
    "priority_issues",
    "recommended_interventions",
    "limitations",
)
_EXECUTIVE_FIELDS = (
    "motivation",
    "key_findings",
    "recommendations",
    "limitations",
    "scope_statement",
)
_ARTIFACT_ORDER = {
    "context": 0,
    "dimension": 1,
    "market": 2,
    "executive_summary": 3,
}
_FIELD_ORDER = {
    "summary": 0,
    "motivation": 0,
    "key_findings": 1,
    "priority_issues": 1,
    "subdimension_analysis": 2,
    "geographic_patterns": 3,
    "data_limitations": 4,
    "limitations": 4,
    "recommendations": 5,
    "recommended_interventions": 5,
    "scope_statement": 6,
    "text": 0,
}


def _serialized(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _normal_name(value: Any) -> str:
    return " ".join(str(value or "").casefold().split())


def ordered_dimension_profiles(
    assessment_profile: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    profiles = {
        str(item.get("dimension")): dict(item)
        for item in assessment_profile.get("dimensions", []) or []
        if isinstance(item, Mapping) and item.get("dimension")
    }
    order = [name for name in DISPLAY_DIMENSIONS if name in profiles]
    order.extend(
        sorted(
            (name for name in profiles if name not in order),
            key=lambda value: (_normal_name(value), value),
        )
    )
    return [profiles[name] for name in order]


def build_dimension_draft_batches(
    assessment_profile: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """Return one batch per priority dimension and one non-priority batch."""
    profiles = ordered_dimension_profiles(assessment_profile)
    priorities = [item for item in profiles if bool(item.get("is_priority"))]
    non_priorities = [item for item in profiles if not bool(item.get("is_priority"))]
    batches: List[Dict[str, Any]] = []
    for profile in priorities:
        dimension = str(profile["dimension"])
        batches.append(
            {
                "batch_id": f"dimension-priority-{normalized_slug(dimension)}",
                "batch_kind": "priority_dimension",
                "artifact_ids": [dimension],
                "profiles": [profile],
            }
        )
    if non_priorities:
        batches.append(
            {
                "batch_id": "dimension-non-priority",
                "batch_kind": "non_priority_dimensions",
                "artifact_ids": [str(item["dimension"]) for item in non_priorities],
                "profiles": non_priorities,
            }
        )
    return batches


def ordered_priority_market_profiles(
    assessment_profile: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    profiles = {
        str(item.get("market_name")): dict(item)
        for item in assessment_profile.get("markets", []) or []
        if isinstance(item, Mapping) and item.get("market_name")
    }
    ordered: List[Dict[str, Any]] = []
    for name in assessment_profile.get("priority_market_names", []) or []:
        if str(name) in profiles:
            ordered.append(profiles[str(name)])
    return ordered


def build_market_draft_batches(
    assessment_profile: Mapping[str, Any],
    *,
    maximum_batch_size: int = MAX_MARKETS_PER_DRAFT_BATCH,
) -> List[Dict[str, Any]]:
    if maximum_batch_size < 1:
        raise ValueError("maximum_batch_size must be positive")
    profiles = ordered_priority_market_profiles(assessment_profile)
    batches: List[Dict[str, Any]] = []
    for offset in range(0, len(profiles), maximum_batch_size):
        group = profiles[offset : offset + maximum_batch_size]
        sequence = len(batches) + 1
        batches.append(
            {
                "batch_id": f"market-draft-{sequence:02d}",
                "batch_kind": "selected_markets",
                "artifact_ids": [str(item["market_name"]) for item in group],
                "profiles": group,
            }
        )
    return batches


def validate_dimension_draft_batch(
    payload: Any,
    *,
    batch: Mapping[str, Any],
    assessment_profile: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    expected = [str(item) for item in batch.get("artifact_ids", [])]
    if (
        isinstance(payload, Mapping)
        and not isinstance(payload.get("dimensions"), list)
        and len(expected) == 1
        and "summary" in payload
    ):
        payload = {
            "dimensions": [
                {"dimension": expected[0], "narrative": dict(payload)}
            ]
        }
    if not isinstance(payload, Mapping) or not isinstance(payload.get("dimensions"), list):
        raise ValueError("Dimension batch must contain a dimensions array")
    rows = payload["dimensions"]
    actual = [
        str(item.get("dimension") or "") if isinstance(item, Mapping) else ""
        for item in rows
    ]
    if actual != expected:
        raise ValueError("Dimension batch identities or ordering are incomplete")
    profiles = {
        str(item["dimension"]): item for item in batch.get("profiles", []) or []
    }
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping) or not isinstance(
            row.get("narrative"), Mapping
        ):
            raise ValueError("Each dimension batch row requires a narrative object")
        dimension = str(row["dimension"])
        result[dimension] = parse_dimension_narrative(
            row["narrative"],
            dimension_profile=profiles[dimension],
            assessment_profile=assessment_profile,
            strict=True,
        )
    return result


def validate_market_draft_batch(
    payload: Any,
    *,
    batch: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    expected = [str(item) for item in batch.get("artifact_ids", [])]
    if (
        isinstance(payload, Mapping)
        and not isinstance(payload.get("markets"), list)
        and len(expected) == 1
        and "priority_issues" in payload
    ):
        payload = {
            "markets": [
                {"market_name": expected[0], "narrative": dict(payload)}
            ]
        }
    if not isinstance(payload, Mapping) or not isinstance(payload.get("markets"), list):
        raise ValueError("Market batch must contain a markets array")
    rows = payload["markets"]
    actual = [
        str(item.get("market_name") or "") if isinstance(item, Mapping) else ""
        for item in rows
    ]
    if actual != expected:
        raise ValueError("Market batch identities or ordering are incomplete")
    profiles = {
        str(item["market_name"]): item for item in batch.get("profiles", []) or []
    }
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping) or not isinstance(
            row.get("narrative"), Mapping
        ):
            raise ValueError("Each market batch row requires a narrative object")
        market = str(row["market_name"])
        result[market] = parse_market_narrative(
            row["narrative"],
            market_profile=profiles[market],
            strict=True,
        )
    return result


def dimension_batch_catalog(
    catalog: Mapping[str, Mapping[str, Any]],
    batch: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    profiles = dimension_batch_prompt_profiles(batch)
    ids: List[str] = []

    def collect(value: Any) -> None:
        if isinstance(value, Mapping):
            for nested in value.values():
                collect(nested)
        elif isinstance(value, list):
            for nested in value:
                collect(nested)
        elif isinstance(value, str) and value in catalog:
            ids.append(value)

    collect(profiles)
    return compact_catalog(catalog, list(dict.fromkeys(ids)))


def _compact_ranked_metric(metric: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: deepcopy(metric.get(key))
        for key in (
            "metric_id",
            "display_name",
            "role",
            "mean_raw_value",
            "mean_normalized_value",
            "unit",
            "orientation",
            "evidence_scope",
            "coverage",
            "availability",
            "unfavorable_rate",
            "weakness_rank",
            "group_rank",
            "product_group",
            "question_group",
            "item_name",
            "item_relevant",
            "relevance_reasons",
            "matching_category_metric_id",
            "ledger_metric_ids",
        )
        if metric.get(key) is not None
    }


def dimension_prompt_profile(profile: Mapping[str, Any]) -> Dict[str, Any]:
    """Keep only evidence that the bounded R8 prose can actually consume."""
    priority = bool(profile.get("is_priority"))
    result: Dict[str, Any] = {
        key: deepcopy(profile.get(key))
        for key in (
            "dimension",
            "statistics",
            "profile_rank",
            "selection_order",
            "is_priority",
            "priority_reasons",
            "localized_patterns",
            "ledger_metric_ids",
        )
        if profile.get(key) is not None
    }
    localized = dict(result.get("localized_patterns") or {})
    if not priority:
        localized.pop("ordered_markets", None)
    result["localized_patterns"] = localized
    if not priority:
        result["subsections"] = []
        result["drivers"] = []
        result["regional_summaries"] = []
        return result
    subsections = [
        _compact_ranked_metric(item)
        for item in list(profile.get("subsections", []) or [])[:2]
        if isinstance(item, Mapping)
    ]
    drivers = [
        item
        for item in profile.get("drivers", []) or []
        if isinstance(item, Mapping)
    ]
    fixed = [item for item in drivers if str(item.get("role")) != "item_driver"][:4]
    relevant_items = [
        item
        for item in drivers
        if str(item.get("role")) == "item_driver" and bool(item.get("item_relevant"))
    ][:12]
    result["subsections"] = subsections
    result["drivers"] = [
        _compact_ranked_metric(item) for item in [*fixed, *relevant_items]
    ]
    result["regional_summaries"] = deepcopy(
        list(profile.get("regional_summaries", []) or [])
    )
    return result


def dimension_batch_prompt_profiles(
    batch: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    return [
        dimension_prompt_profile(profile)
        for profile in batch.get("profiles", []) or []
        if isinstance(profile, Mapping)
    ]


def market_batch_catalog(
    catalog: Mapping[str, Mapping[str, Any]],
    batch: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    ids: List[str] = []
    for profile in batch.get("profiles", []) or []:
        ids.extend(market_catalog_ids(profile, catalog))
    return compact_catalog(catalog, list(dict.fromkeys(ids)))


def _add_claim(
    rows: List[Dict[str, Any]],
    claim: Mapping[str, Any],
    *,
    artifact_type: str,
    artifact_id: str,
    field_name: str,
    position: int,
) -> None:
    if not claim.get("claim_id"):
        return
    rows.append(
        {
            "claim_id": str(claim["claim_id"]),
            "artifact_type": artifact_type,
            "artifact_id": artifact_id,
            "field_name": field_name,
            "position": position,
            "text": str(claim.get("text") or ""),
            "scope": claim.get("scope"),
            "polarity": claim.get("polarity"),
            "metric_ids": [str(item) for item in claim.get("metric_ids", []) or []],
            "document_ids": [
                str(item) for item in claim.get("document_ids", []) or []
            ],
        }
    )


def _artifact_claim_rows(
    narrative: Mapping[str, Any],
    *,
    artifact_type: str,
    artifact_id: str,
    fields: Sequence[str],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for field_name in fields:
        value = narrative.get(field_name)
        if isinstance(value, Mapping):
            _add_claim(
                rows,
                value,
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                field_name=field_name,
                position=1,
            )
        elif isinstance(value, list):
            for position, claim in enumerate(value, start=1):
                if isinstance(claim, Mapping):
                    if field_name == "subdimension_analysis":
                        interpretation = claim.get("interpretation")
                        if isinstance(interpretation, Mapping):
                            _add_claim(
                                rows,
                                interpretation,
                                artifact_type=artifact_type,
                                artifact_id=artifact_id,
                                field_name=field_name,
                                position=position,
                            )
                    else:
                        _add_claim(
                            rows,
                            claim,
                            artifact_type=artifact_type,
                            artifact_id=artifact_id,
                            field_name=field_name,
                            position=position,
                        )
    return rows


def canonical_claim_rows(
    *,
    dimension_narratives: Mapping[str, Mapping[str, Any]],
    market_narratives: Mapping[str, Mapping[str, Any]],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]],
    assessment_profile: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for profile in ordered_dimension_profiles(assessment_profile):
        dimension = str(profile["dimension"])
        narrative = dimension_narratives.get(dimension)
        if isinstance(narrative, Mapping):
            rows.extend(
                _artifact_claim_rows(
                    narrative,
                    artifact_type="dimension",
                    artifact_id=dimension,
                    fields=_DIMENSION_FIELDS,
                )
            )
    for profile in ordered_priority_market_profiles(assessment_profile):
        market = str(profile["market_name"])
        narrative = market_narratives.get(market)
        if isinstance(narrative, Mapping):
            rows.extend(
                _artifact_claim_rows(
                    narrative,
                    artifact_type="market",
                    artifact_id=market,
                    fields=_MARKET_FIELDS,
                )
            )
    if isinstance(executive_narrative, Mapping):
        rows.extend(
            _artifact_claim_rows(
                executive_narrative,
                artifact_type="executive_summary",
                artifact_id="executive_summary",
                fields=_EXECUTIVE_FIELDS,
            )
        )
    for position, statement in enumerate(context_evidence, start=1):
        if not isinstance(statement, Mapping) or not statement.get("statement_id"):
            continue
        rows.append(
            {
                "claim_id": str(statement["statement_id"]),
                "artifact_type": "context",
                "artifact_id": str(statement["statement_id"]),
                "field_name": "text",
                "position": position,
                "text": str(statement.get("text") or ""),
                "scope": "context",
                "polarity": "descriptive",
                "metric_ids": [],
                "document_ids": [
                    str(item) for item in statement.get("document_ids", []) or []
                ],
            }
        )
    claim_ids = [row["claim_id"] for row in rows]
    if not all(claim_ids) or len(claim_ids) != len(set(claim_ids)):
        raise ValueError("Review input requires globally unique canonical claim IDs")
    return rows


def _review_evidence(
    catalog: Mapping[str, Mapping[str, Any]],
    claims: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    cited = sorted(
        {
            str(metric_id)
            for claim in claims
            for metric_id in claim.get("metric_ids", []) or []
            if metric_id
        }
    )
    return {
        metric_id: {
            key: catalog[metric_id].get(key)
            for key in (
                "label",
                "formatted_value",
                "unit",
                "orientation",
                "scope",
                "coverage_label",
                "dimension",
                "market_name",
                "region",
                "representation_kind",
                "representation_complete",
                "representation_required",
                "source_metric_ids",
            )
            if catalog[metric_id].get(key) is not None
        }
        for metric_id in cited
        if metric_id in catalog
    }


def _review_documents(
    documents: Sequence[Mapping[str, Any]],
    claims: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    cited = {
        str(document_id)
        for claim in claims
        for document_id in claim.get("document_ids", []) or []
        if document_id
    }
    return [
        {
            "document_id": str(item.get("doc_id")),
            "source": item.get("source"),
            "title": item.get("title"),
            "date": item.get("date"),
        }
        for item in documents
        if isinstance(item, Mapping) and str(item.get("doc_id")) in cited
    ]


def _dimension_review_facts(
    assessment_profile: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    facts: List[Dict[str, Any]] = []
    for profile in ordered_dimension_profiles(assessment_profile):
        facts.append(dimension_prompt_profile(profile))
    return facts


def _market_review_facts(
    assessment_profile: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    return [
        {
            key: deepcopy(profile.get(key))
            for key in (
                "market_name",
                "region",
                "overall_mfi",
                "score_rank",
                "selection_order",
                "weak_dimensions",
            )
            if profile.get(key) is not None
        }
        for profile in ordered_priority_market_profiles(assessment_profile)
    ]


def build_semantic_review_packages(
    *,
    dimension_narratives: Mapping[str, Mapping[str, Any]],
    market_narratives: Mapping[str, Mapping[str, Any]],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]],
    assessment_profile: Mapping[str, Any],
    claim_catalog: Mapping[str, Mapping[str, Any]],
    documents: Sequence[Mapping[str, Any]],
    deterministic_flags: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows = canonical_claim_rows(
        dimension_narratives=dimension_narratives,
        market_narratives=market_narratives,
        executive_narrative=executive_narrative,
        context_evidence=context_evidence,
        assessment_profile=assessment_profile,
    )
    by_section = {
        "overview": [
            row
            for row in rows
            if row["artifact_type"] in {"context", "executive_summary"}
        ],
        "dimensions": [row for row in rows if row["artifact_type"] == "dimension"],
        "markets": [row for row in rows if row["artifact_type"] == "market"],
    }
    packages: List[Dict[str, Any]] = []
    for sequence, section in enumerate(("overview", "dimensions", "markets"), start=1):
        claims = by_section[section]
        if section == "overview":
            facts: Any = {
                "assessed_market_count": assessment_profile.get(
                    "assessed_market_count"
                ),
                "mean_mfi_across_assessed_markets": assessment_profile.get(
                    "mean_mfi_across_assessed_markets"
                ),
                "priority_dimension_names": assessment_profile.get(
                    "priority_dimension_names", []
                ),
                "priority_market_names": assessment_profile.get(
                    "priority_market_names", []
                ),
                "limitations": assessment_profile.get("limitations", []),
            }
        elif section == "dimensions":
            facts = _dimension_review_facts(assessment_profile)
        else:
            facts = _market_review_facts(assessment_profile)
        claim_ids = {row["claim_id"] for row in claims}
        relevant_deterministic_flags = [
            {
                key: flag.get(key)
                for key in ("code", "severity", "claim_id", "message")
                if flag.get(key) is not None
            }
            for flag in deterministic_flags
            if isinstance(flag, Mapping)
            and (
                not flag.get("claim_id")
                or str(flag.get("claim_id")) in claim_ids
            )
        ]
        package = {
            "contract_version": SEMANTIC_REVIEW_CONTRACT_VERSION,
            "section": section,
            "claims": claims,
            "deterministic_facts": facts,
            "evidence_by_metric_id": _review_evidence(claim_catalog, claims),
            "cited_documents": _review_documents(documents, claims),
            "deterministic_flags": relevant_deterministic_flags,
            "prohibitions": list(NARRATIVE_PROHIBITIONS),
            "severity_policy": {
                "high": (
                    "Concrete factual contradiction, fabricated or unsupported value "
                    "or source, materially reversed interpretation, or unsupported "
                    "conclusion that changes the assessment meaning."
                ),
                "medium": (
                    "Overstatement, ambiguous scope, weak recommendation linkage, "
                    "repetition, or material interpretive weakness."
                ),
                "low": "Style, clarity, or minor wording improvement.",
            },
        }
        character_count = len(_serialized(package))
        packages.append(
            {
                "review_id": f"semantic-review-{section}",
                "section": section,
                "sequence": sequence,
                "claim_ids": [row["claim_id"] for row in claims],
                "character_count": character_count,
                "package": package,
            }
        )
    return packages


def validate_semantic_review_response(
    payload: Any,
    *,
    review: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """Whitelist the semantic response and resolve routing from canonical claims."""
    if not isinstance(payload, Mapping) or not isinstance(payload.get("flags"), list):
        raise ValueError("Semantic review response requires a flags array")
    claim_rows = {
        str(row["claim_id"]): row
        for row in review.get("package", {}).get("claims", []) or []
        if isinstance(row, Mapping) and row.get("claim_id")
    }
    result: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for index, raw in enumerate(payload["flags"]):
        if not isinstance(raw, Mapping):
            raise ValueError(f"flags[{index}] must be an object")
        code = str(raw.get("code") or "").strip()
        severity = str(raw.get("severity") or "").strip().lower()
        message = str(raw.get("message") or "").strip()
        claim_id = str(raw.get("claim_id") or "").strip() or None
        if not code or severity not in {"high", "medium", "low"} or not message:
            raise ValueError(f"flags[{index}] is missing required semantic fields")
        if claim_id is not None and claim_id not in claim_rows:
            raise ValueError("Semantic review references a claim outside its section")
        row = claim_rows.get(claim_id or "")
        artifact_type = str(row["artifact_type"]) if row else "global"
        artifact_id = str(row["artifact_id"]) if row else None
        field_name = str(row["field_name"]) if row else None
        key = (code, claim_id or "global")
        if key in seen:
            continue
        seen.add(key)
        identity = "|".join(
            [
                str(review.get("section")),
                code,
                claim_id or "global",
            ]
        )
        result.append(
            MFINarrativeQAFlag(
                flag_id=f"semantic-{normalized_slug(code)}-{_short_hash(identity)}",
                source="red_team",
                code=code,
                severity=severity,
                artifact_type=artifact_type,
                artifact_id=artifact_id,
                field_name=field_name,
                claim_id=claim_id,
                message=message,
                recommendation=str(raw.get("recommendation") or "").strip(),
                metric_ids=list(row.get("metric_ids", [])) if row else [],
                document_ids=list(row.get("document_ids", [])) if row else [],
                repairable=row is not None,
                review_batch_id=str(review.get("review_id")),
                review_batch_ids=[str(review.get("review_id"))],
            ).model_dump()
        )
    return result


def material_local_flags(
    flags: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    return [
        dict(flag)
        for flag in flags
        if str(flag.get("severity")) in {"high", "medium"}
        and bool(flag.get("repairable", True))
        and str(flag.get("artifact_type") or "") != "global"
        and flag.get("artifact_id")
        and flag.get("field_name")
    ]


def unresolved_high_flags(
    flags: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    return [dict(flag) for flag in flags if str(flag.get("severity")) == "high"]


def build_consolidated_correction_targets(
    flags: Sequence[Mapping[str, Any]],
    *,
    assessment_profile: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    return build_sequential_correction_tasks(
        material_local_flags(flags),
        attempt_number=1,
        assessment_profile=assessment_profile,
    )


def validate_consolidated_correction_response(
    payload: Any,
    *,
    targets: Sequence[Mapping[str, Any]],
    ignored_metadata_fields: List[str] | None = None,
) -> Dict[str, Any]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("patches"), list):
        raise ValueError("Consolidated correction requires a patches array")
    expected = {str(target["task_id"]): target for target in targets}
    patches = payload["patches"]
    actual_ids: List[str] = []
    result: Dict[str, Any] = {}
    for index, patch in enumerate(patches):
        if not isinstance(patch, Mapping):
            raise ValueError(f"patches[{index}] must be an object")
        target_id = str(patch.get("target_id") or "")
        if not target_id or target_id not in expected:
            raise ValueError("Correction response contains an unknown target_id")
        actual_ids.append(target_id)
        result[target_id] = validate_field_patch_payload(
            {"replacement": patch.get("replacement")},
            task=expected[target_id],
            ignored_metadata_fields=ignored_metadata_fields,
        )
    if len(actual_ids) != len(set(actual_ids)) or set(actual_ids) != set(expected):
        raise ValueError("Every correction target must appear exactly once")
    return result


def consolidated_correction_prompt_payload(
    *,
    targets: Sequence[Mapping[str, Any]],
    flags: Sequence[Mapping[str, Any]],
    dimension_narratives: Mapping[str, Mapping[str, Any]],
    market_narratives: Mapping[str, Mapping[str, Any]],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]],
    assessment_profile: Mapping[str, Any],
    claim_catalog: Mapping[str, Mapping[str, Any]],
    documents: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    flags_by_id = {
        str(flag.get("flag_id")): flag
        for flag in flags
        if isinstance(flag, Mapping) and flag.get("flag_id")
    }
    document_by_id = {
        str(item.get("doc_id")): item
        for item in documents
        if isinstance(item, Mapping) and item.get("doc_id")
    }
    entries: List[Dict[str, Any]] = []
    for target in targets:
        artifact_type = str(target["artifact_type"])
        artifact_id = str(target["artifact_id"])
        field_name = str(target["field_name"])
        if artifact_type == "dimension":
            artifact = dimension_narratives[artifact_id]
            profile = next(
                item
                for item in assessment_profile.get("dimensions", []) or []
                if str(item.get("dimension")) == artifact_id
            )
            catalog = compact_catalog(
                claim_catalog, dimension_catalog_ids(profile)
            )
        elif artifact_type == "market":
            artifact = market_narratives[artifact_id]
            profile = next(
                item
                for item in assessment_profile.get("markets", []) or []
                if str(item.get("market_name")) == artifact_id
            )
            catalog = compact_catalog(
                claim_catalog, market_catalog_ids(profile, claim_catalog)
            )
        elif artifact_type == "executive_summary":
            artifact = executive_narrative
            catalog = compact_catalog(
                claim_catalog, executive_catalog_ids(assessment_profile)
            )
        else:
            artifact = next(
                item
                for item in context_evidence
                if str(item.get("statement_id")) == artifact_id
            )
            catalog = {}
        target_flags = [
            flags_by_id[flag_id]
            for flag_id in target.get("flag_ids", []) or []
            if flag_id in flags_by_id
        ]
        cited_document_ids = {
            str(document_id)
            for flag in target_flags
            for document_id in flag.get("document_ids", []) or []
        }
        entries.append(
            {
                "target_id": str(target["task_id"]),
                "artifact_type": artifact_type,
                "artifact_id": artifact_id,
                "field_name": field_name,
                "findings": [
                    {
                        key: flag.get(key)
                        for key in (
                            "flag_id",
                            "code",
                            "severity",
                            "claim_id",
                            "message",
                            "recommendation",
                        )
                    }
                    for flag in target_flags
                ],
                "current_field": project_correction_transport(
                    artifact.get(field_name)
                ),
                "read_only_context": project_correction_transport(
                    {key: value for key, value in artifact.items() if key != field_name}
                ),
                "patch_contract": correction_field_patch_contract(
                    task=target,
                    artifact=artifact,
                    assessment_profile=assessment_profile,
                ),
                "authorized_claim_catalog": catalog,
                "authorized_documents": [
                    {
                        "document_id": document_id,
                        "source": document_by_id[document_id].get("source"),
                        "date": document_by_id[document_id].get("date"),
                        "title": document_by_id[document_id].get("title"),
                    }
                    for document_id in sorted(cited_document_ids)
                    if document_id in document_by_id
                ],
            }
        )
    return {
        "contract_version": "mfi-consolidated-correction-v1",
        "targets": entries,
        "prohibitions": list(NARRATIVE_PROHIBITIONS),
    }


def apply_consolidated_patches(
    *,
    targets: Sequence[Mapping[str, Any]],
    replacements: Mapping[str, Any],
    dimension_narratives: Mapping[str, Mapping[str, Any]],
    market_narratives: Mapping[str, Mapping[str, Any]],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]],
    assessment_profile: Mapping[str, Any],
) -> Dict[str, Any]:
    state = {
        "dimension_narratives": deepcopy(dict(dimension_narratives)),
        "market_narratives": deepcopy(dict(market_narratives)),
        "executive_summary_narrative": deepcopy(dict(executive_narrative)),
        "context_evidence": deepcopy(list(context_evidence)),
    }
    for target in targets:
        state = apply_field_patch(
            task=target,
            replacement=replacements[str(target["task_id"])],
            dimension_narratives=state["dimension_narratives"],
            market_narratives=state["market_narratives"],
            executive_narrative=state["executive_summary_narrative"],
            context_evidence=state["context_evidence"],
            assessment_profile=assessment_profile,
        )
    return state


def target_keys(targets: Sequence[Mapping[str, Any]]) -> set[tuple[str, str, str]]:
    return {
        (
            str(target.get("artifact_type")),
            str(target.get("artifact_id")),
            str(target.get("field_name")),
        )
        for target in targets
    }


def flags_outside_targets(
    flags: Sequence[Mapping[str, Any]],
    targets: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    keys = target_keys(targets)
    return [
        dict(flag)
        for flag in flags
        if (
            str(flag.get("artifact_type")),
            str(flag.get("artifact_id")),
            str(flag.get("field_name")),
        )
        not in keys
    ]


def build_corrected_claim_verification_package(
    *,
    targets: Sequence[Mapping[str, Any]],
    dimension_narratives: Mapping[str, Mapping[str, Any]],
    market_narratives: Mapping[str, Mapping[str, Any]],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]],
    assessment_profile: Mapping[str, Any],
    claim_catalog: Mapping[str, Mapping[str, Any]],
    documents: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    keys = target_keys(targets)
    claims = [
        row
        for row in canonical_claim_rows(
            dimension_narratives=dimension_narratives,
            market_narratives=market_narratives,
            executive_narrative=executive_narrative,
            context_evidence=context_evidence,
            assessment_profile=assessment_profile,
        )
        if (
            row["artifact_type"],
            row["artifact_id"],
            row["field_name"],
        )
        in keys
    ]
    package = {
        "contract_version": SEMANTIC_REVIEW_CONTRACT_VERSION,
        "section": "corrected_claims",
        "claims": claims,
        "evidence_by_metric_id": _review_evidence(claim_catalog, claims),
        "cited_documents": _review_documents(documents, claims),
        "prohibitions": list(NARRATIVE_PROHIBITIONS),
    }
    return {
        "review_id": "corrected-claim-verification",
        "section": "corrected_claims",
        "sequence": 4,
        "claim_ids": [row["claim_id"] for row in claims],
        "character_count": len(_serialized(package)),
        "package": package,
    }

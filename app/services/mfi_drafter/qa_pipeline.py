"""Fail-closed correction and Red-Team contracts for live MFI generation."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any, Dict, List, Mapping, Sequence

from pydantic import ValidationError

from .claim_identity import (
    MFIClaimIdentityError,
    assert_claim_identity_contract,
    canonicalize_narrative_identities,
)
from .errors import MFIGenerationBlockedError, claim_identity_blocked
from .methodology import DISPLAY_DIMENSIONS
from .narrative import NARRATIVE_DENSITY_POLICY, apply_narrative_density_policy
from .schemas import (
    MFIClaimPatchValue,
    MFIContextEvidenceStatement,
    MFIContextClassificationFieldPatch,
    MFIContextDocumentsFieldPatch,
    MFIContextTextFieldPatch,
    MFICorrectionTask,
    MFIDimensionNarrative,
    MFIExecutiveNarrative,
    MFIFieldPatch,
    MFIMarketNarrative,
    MFIRedTeamReviewBatch,
    MFISubdimensionPatchValue,
)

MFI_RED_TEAM_BATCH_TARGET_CHARACTERS = 45_000
MFI_RED_TEAM_BATCH_CONTRACT_VERSION = "mfi-red-team-batches-v1"

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
    "classification": 1,
    "document_ids": 2,
}
_CLAIM_FIELDS = {
    "dimension": {
        "summary",
        "key_findings",
        "geographic_patterns",
        "data_limitations",
        "recommendations",
    },
    "market": {"priority_issues", "recommended_interventions", "limitations"},
    "executive_summary": {
        "motivation",
        "key_findings",
        "recommendations",
        "limitations",
        "scope_statement",
    },
}
_LIST_CLAIM_FIELDS = {
    "key_findings",
    "geographic_patterns",
    "data_limitations",
    "recommendations",
    "priority_issues",
    "recommended_interventions",
    "limitations",
}
_SINGLE_CLAIM_FIELDS = {"summary", "motivation", "scope_statement"}

APPLICATION_OWNED_CLAIM_FIELDS = frozenset(
    {
        "claim_id",
        "validation_status",
        "validation_flags",
        "validation_flag_ids",
        "substituted",
    }
)


def project_correction_transport(value: Any) -> Any:
    """Return the LLM-visible projection without application-owned claim state."""
    if isinstance(value, Mapping):
        return {
            str(key): project_correction_transport(item)
            for key, item in value.items()
            if str(key) not in APPLICATION_OWNED_CLAIM_FIELDS
        }
    if isinstance(value, list):
        return [project_correction_transport(item) for item in value]
    if isinstance(value, tuple):
        return [project_correction_transport(item) for item in value]
    return deepcopy(value)


def _strip_application_owned_claim_fields(
    value: Any,
    *,
    path: str,
    ignored_metadata_fields: List[str],
) -> Any:
    """Strip only known application fields while preserving unknown extras."""
    if isinstance(value, Mapping):
        projected: Dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            item_path = f"{path}.{key}" if path else key
            if key in APPLICATION_OWNED_CLAIM_FIELDS:
                ignored_metadata_fields.append(item_path)
                continue
            projected[key] = _strip_application_owned_claim_fields(
                item,
                path=item_path,
                ignored_metadata_fields=ignored_metadata_fields,
            )
        return projected
    if isinstance(value, list):
        return [
            _strip_application_owned_claim_fields(
                item,
                path=f"{path}[{index}]",
                ignored_metadata_fields=ignored_metadata_fields,
            )
            for index, item in enumerate(value)
        ]
    if isinstance(value, tuple):
        return [
            _strip_application_owned_claim_fields(
                item,
                path=f"{path}[{index}]",
                ignored_metadata_fields=ignored_metadata_fields,
            )
            for index, item in enumerate(value)
        ]
    return value


def _claim_patch_example(*, claim_kind: str) -> Dict[str, Any]:
    return {
        "text": "Revised claim supported by the cited evidence.",
        "claim_kind": claim_kind,
        "metric_ids": ["exact.authorized.metric.id"],
        "document_ids": [],
        "scope": "assessment",
        "polarity": "neutral",
    }


def correction_field_patch_contract(
    *,
    task: Mapping[str, Any],
    artifact: Mapping[str, Any],
    assessment_profile: Mapping[str, Any],
) -> Dict[str, Any]:
    """Describe the exact field-only response contract and R8 cardinality."""
    artifact_type = str(task["artifact_type"])
    field_name = str(task["field_name"])
    if artifact_type == "context":
        examples: Dict[str, Any] = {
            "text": {"replacement": "Revised source-supported statement."},
            "classification": {"replacement": "corroborating"},
            "document_ids": {"replacement": ["supplied-document-id"]},
        }
        shapes = {
            "text": "non-empty string",
            "classification": (
                "one of: corroborating, potentially_explanatory, unrelated"
            ),
            "document_ids": "array of supplied document IDs",
        }
        return {
            "field": field_name,
            "replacement_shape": shapes[field_name],
            "nullable": False,
            "example": examples[field_name],
        }

    claim_kind = _claim_kind_for_field(field_name)
    claim_example = _claim_patch_example(claim_kind=claim_kind)
    if field_name in _SINGLE_CLAIM_FIELDS:
        nullable = field_name in {"motivation", "scope_statement"}
        return {
            "field": field_name,
            "replacement_shape": "one claim object",
            "allowed_claim_fields": list(claim_example),
            "nullable": nullable,
            "example": {"replacement": claim_example},
        }

    if field_name == "subdimension_analysis":
        maximum_items = (
            NARRATIVE_DENSITY_POLICY.priority_subdimensions
            if bool(artifact.get("is_priority"))
            else 0
        )
        return {
            "field": field_name,
            "replacement_shape": "array of subdimension objects",
            "allowed_subdimension_fields": [
                "name",
                "subsection_metric_id",
                "score_0_10",
                "interpretation",
                "driver_metric_ids",
            ],
            "allowed_interpretation_fields": list(claim_example),
            "minimum_items": 0,
            "maximum_items": maximum_items,
            "nullable": False,
            "example": {
                "replacement": [
                    {
                        "name": "Authorized subsection label",
                        "subsection_metric_id": "exact.authorized.metric.id",
                        "score_0_10": 5.0,
                        "interpretation": claim_example,
                        "driver_metric_ids": [],
                    }
                ]
            },
        }

    if field_name not in _LIST_CLAIM_FIELDS:
        raise ValueError(f"Unsupported correction field: {field_name}")
    if artifact_type == "dimension":
        priority = bool(artifact.get("is_priority"))
        maximum_items = {
            "key_findings": (
                NARRATIVE_DENSITY_POLICY.priority_findings
                if priority
                else NARRATIVE_DENSITY_POLICY.non_priority_findings
            ),
            "geographic_patterns": (
                NARRATIVE_DENSITY_POLICY.priority_geographic_patterns
                if priority
                else NARRATIVE_DENSITY_POLICY.non_priority_geographic_patterns
            ),
            "data_limitations": (
                NARRATIVE_DENSITY_POLICY.priority_limitations
                if priority
                else NARRATIVE_DENSITY_POLICY.non_priority_limitations
            ),
            "recommendations": (
                NARRATIVE_DENSITY_POLICY.priority_recommendations
                if priority
                else NARRATIVE_DENSITY_POLICY.non_priority_recommendations
            ),
        }[field_name]
    elif artifact_type == "market":
        maximum_items = {
            "priority_issues": NARRATIVE_DENSITY_POLICY.market_priority_issues,
            "recommended_interventions": (
                NARRATIVE_DENSITY_POLICY.market_recommendations
            ),
            "limitations": NARRATIVE_DENSITY_POLICY.market_limitations,
        }[field_name]
    else:
        maximum_items = {
            "key_findings": len(
                assessment_profile.get("priority_dimension_names", []) or []
            ),
            "recommendations": NARRATIVE_DENSITY_POLICY.executive_recommendations,
            "limitations": NARRATIVE_DENSITY_POLICY.executive_limitations,
        }[field_name]
    return {
        "field": field_name,
        "replacement_shape": "array of claim objects",
        "allowed_claim_fields": list(claim_example),
        "minimum_items": 0,
        "maximum_items": maximum_items,
        "nullable": False,
        "example": {"replacement": [claim_example]},
    }


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]


def _normalized_name(value: Any) -> str:
    return " ".join(str(value or "").casefold().split())


def _material_flags(flags: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [
        dict(flag)
        for flag in flags
        if str(flag.get("severity")) in {"high", "medium"}
    ]


def build_sequential_correction_tasks(
    flags: Sequence[Mapping[str, Any]],
    *,
    attempt_number: int,
    assessment_profile: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    """Group material findings by exact artifact field or fail if unsafe to target."""
    material = _material_flags(flags)
    if not material:
        return []
    for flag in material:
        artifact_type = str(flag.get("artifact_type") or "")
        if (
            not bool(flag.get("repairable", True))
            or artifact_type == "global"
            or artifact_type not in _ARTIFACT_ORDER
            or not str(flag.get("artifact_id") or "").strip()
            or not str(flag.get("field_name") or "").strip()
        ):
            raise MFIGenerationBlockedError(
                "mfi_narrative_qa_unresolved",
                "A material QA finding cannot be mapped to a safe field correction.",
                stage="targeted_correction",
                status_code=502,
                artifact_type=artifact_type or None,
                artifact_id=str(flag.get("artifact_id") or "") or None,
                field_name=str(flag.get("field_name") or "") or None,
                attempt=attempt_number,
            )

    dimension_order = {
        name: index for index, name in enumerate(DISPLAY_DIMENSIONS)
    }
    market_order = {
        str(name): index
        for index, name in enumerate(
            assessment_profile.get("priority_market_names", []) or []
        )
    }

    grouped: Dict[tuple[str, str, str], List[Dict[str, Any]]] = {}
    for flag in material:
        key = (
            str(flag["artifact_type"]),
            str(flag["artifact_id"]),
            str(flag["field_name"]),
        )
        grouped.setdefault(key, []).append(flag)

    def sort_key(key: tuple[str, str, str]) -> tuple[Any, ...]:
        artifact_type, artifact_id, field_name = key
        if artifact_type == "dimension":
            artifact_position = dimension_order.get(artifact_id, 999)
        elif artifact_type == "market":
            artifact_position = market_order.get(artifact_id, 999)
        else:
            artifact_position = 0
        return (
            _ARTIFACT_ORDER[artifact_type],
            artifact_position,
            _normalized_name(artifact_id),
            _FIELD_ORDER.get(field_name, 999),
            field_name,
        )

    tasks: List[Dict[str, Any]] = []
    for key in sorted(grouped, key=sort_key):
        artifact_type, artifact_id, field_name = key
        target_flags = grouped[key]
        identity = f"{attempt_number}|{artifact_type}|{artifact_id}|{field_name}"
        task = MFICorrectionTask(
            task_id=f"mfi-correction-{attempt_number}-{_short_hash(identity)}",
            attempt_number=attempt_number,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            field_name=field_name,
            claim_ids=sorted(
                {str(flag.get("claim_id")) for flag in target_flags if flag.get("claim_id")}
            ),
            flag_ids=sorted(
                {str(flag.get("flag_id")) for flag in target_flags if flag.get("flag_id")}
            ),
            flag_codes=sorted(
                {str(flag.get("code")) for flag in target_flags if flag.get("code")}
            ),
        )
        tasks.append(task.model_dump())
    return tasks


def _validate_claim_transport(
    value: Any,
    *,
    field_name: str,
    ignored_metadata_fields: List[str],
) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} replacement must be a claim object")
    claim = _strip_application_owned_claim_fields(
        value,
        path=field_name,
        ignored_metadata_fields=ignored_metadata_fields,
    )
    return MFIClaimPatchValue.model_validate(claim).model_dump()


def _claim_kind_for_field(field_name: str) -> str:
    root = field_name.split("[", 1)[0]
    return {
        "summary": "summary",
        "key_findings": "finding",
        "priority_issues": "finding",
        "subdimension_analysis": "finding",
        "geographic_patterns": "geographic_pattern",
        "data_limitations": "limitation",
        "limitations": "limitation",
        "recommendations": "recommendation",
        "recommended_interventions": "recommendation",
        "motivation": "summary",
        "scope_statement": "limitation",
    }.get(root, "finding")


def validate_field_patch_payload(
    payload: Any,
    *,
    task: Mapping[str, Any],
    ignored_metadata_fields: List[str] | None = None,
) -> Any:
    """Validate only the field requested by a correction task."""
    ignored_fields = (
        ignored_metadata_fields if ignored_metadata_fields is not None else []
    )
    artifact_type = str(task["artifact_type"])
    field_name = str(task["field_name"])
    if artifact_type == "context":
        if field_name == "text":
            return MFIContextTextFieldPatch.model_validate(payload).replacement.strip()
        if field_name == "classification":
            return MFIContextClassificationFieldPatch.model_validate(
                payload
            ).replacement
        if field_name == "document_ids":
            return MFIContextDocumentsFieldPatch.model_validate(
                payload
            ).replacement
        raise ValueError(f"Unsupported context correction field: {field_name}")

    allowed = _CLAIM_FIELDS.get(artifact_type, set())
    if field_name not in allowed and field_name != "subdimension_analysis":
        raise ValueError(
            f"Unsupported {artifact_type} correction field: {field_name}"
        )
    replacement = MFIFieldPatch.model_validate(payload).replacement
    if field_name in _SINGLE_CLAIM_FIELDS:
        if replacement is None and field_name in {"motivation", "scope_statement"}:
            return None
        claim = _validate_claim_transport(
            replacement,
            field_name=field_name,
            ignored_metadata_fields=ignored_fields,
        )
        claim["claim_kind"] = _claim_kind_for_field(field_name)
        return claim
    if field_name in _LIST_CLAIM_FIELDS:
        if not isinstance(replacement, list):
            raise ValueError(f"{field_name} replacement must be a list")
        claims = []
        for index, item in enumerate(replacement):
            claim = _validate_claim_transport(
                item,
                field_name=f"{field_name}[{index}]",
                ignored_metadata_fields=ignored_fields,
            )
            claim["claim_kind"] = _claim_kind_for_field(field_name)
            claims.append(claim)
        return claims
    if field_name == "subdimension_analysis":
        if not isinstance(replacement, list):
            raise ValueError("subdimension_analysis replacement must be a list")
        result: List[Dict[str, Any]] = []
        for index, item in enumerate(replacement):
            if not isinstance(item, Mapping):
                raise ValueError(f"subdimension_analysis[{index}] must be an object")
            normalized = _strip_application_owned_claim_fields(
                item,
                path=f"subdimension_analysis[{index}]",
                ignored_metadata_fields=ignored_fields,
            )
            normalized = MFISubdimensionPatchValue.model_validate(
                normalized
            ).model_dump()
            normalized["interpretation"]["claim_kind"] = "finding"
            result.append(normalized)
        return result
    raise ValueError(f"Unsupported correction field: {field_name}")


def apply_field_patch(
    *,
    task: Mapping[str, Any],
    replacement: Any,
    dimension_narratives: Mapping[str, Mapping[str, Any]],
    market_narratives: Mapping[str, Mapping[str, Any]],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]],
    assessment_profile: Mapping[str, Any],
) -> Dict[str, Any]:
    """Merge one patch, reapply identities, and validate the complete artifact set."""
    dimensions = deepcopy(dict(dimension_narratives))
    markets = deepcopy(dict(market_narratives))
    executive = deepcopy(dict(executive_narrative))
    context = deepcopy(list(context_evidence))
    artifact_type = str(task["artifact_type"])
    artifact_id = str(task["artifact_id"])
    field_name = str(task["field_name"])

    if artifact_type == "dimension":
        if artifact_id not in dimensions:
            raise ValueError(f"Unknown dimension correction artifact: {artifact_id}")
        dimensions[artifact_id][field_name] = replacement
    elif artifact_type == "market":
        if artifact_id not in markets:
            raise ValueError(f"Unknown market correction artifact: {artifact_id}")
        markets[artifact_id][field_name] = replacement
    elif artifact_type == "executive_summary":
        executive[field_name] = replacement
    elif artifact_type == "context":
        matched = False
        for statement in context:
            if str(statement.get("statement_id") or "") == artifact_id:
                statement[field_name] = replacement
                matched = True
                break
        if not matched:
            raise ValueError(f"Unknown context correction artifact: {artifact_id}")
    else:
        raise ValueError(f"Unsupported correction artifact: {artifact_type}")

    dimensions, markets, executive = apply_narrative_density_policy(
        dimension_narratives=dimensions,
        market_narratives=markets,
        executive_narrative=executive,
        assessment_profile=assessment_profile,
    )
    try:
        dimensions, markets, executive, context = canonicalize_narrative_identities(
            dimension_narratives=dimensions,
            market_narratives=markets,
            executive_narrative=executive,
            context_evidence=context,
        )
        assert_claim_identity_contract(dimensions, markets, executive, context)
    except MFIClaimIdentityError as exc:
        raise claim_identity_blocked(
            str(exc),
            stage="targeted_correction",
            artifact_type=artifact_type,
            artifact_id=artifact_id,
        ) from exc

    try:
        for value in dimensions.values():
            MFIDimensionNarrative.model_validate(value)
        for value in markets.values():
            MFIMarketNarrative.model_validate(value)
        MFIExecutiveNarrative.model_validate(executive)
        for value in context:
            MFIContextEvidenceStatement.model_validate(value)
    except ValidationError as exc:
        raise ValueError(f"Merged correction violates the narrative schema: {exc}") from exc
    return {
        "dimension_narratives": dimensions,
        "market_narratives": markets,
        "executive_summary_narrative": executive,
        "context_evidence": context,
    }


def _serialized(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _evidence_for_rows(
    evidence: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> Dict[str, Any]:
    ids = sorted(
        {
            str(metric_id)
            for row in rows
            for metric_id in row.get("m", []) or []
            if metric_id
        }
    )
    return {metric_id: evidence[metric_id] for metric_id in ids if metric_id in evidence}


def _batch_package(
    source: Mapping[str, Any],
    *,
    rows: Sequence[Mapping[str, Any]],
    kind: str,
) -> Dict[str, Any]:
    document_ids = sorted(
        {
            str(document_id)
            for row in rows
            for document_id in row.get("d", []) or []
            if document_id
        }
    )
    documents = [
        item
        for item in source.get("cited_documents", []) or []
        if str(item.get("id") or "") in document_ids
    ]
    artifact_dimensions = {
        str(row.get("aid")) for row in rows if row.get("a") == "dimension"
    }
    artifact_markets = {
        str(row.get("aid")) for row in rows if row.get("a") == "market"
    }
    priority_context = source.get("priority_context", {}) or {}
    dimensions = [
        item
        for item in priority_context.get("dimensions", []) or []
        if kind != "local" or str(item.get("id")) in artifact_dimensions
    ]
    markets = [
        item
        for item in priority_context.get("markets", []) or []
        if kind != "local" or str(item.get("id")) in artifact_markets
    ]
    limitations = [
        item
        for item in source.get("limitations", []) or []
        if (
            (not item.get("dimension") and not item.get("market"))
            or str(item.get("dimension") or "") in artifact_dimensions
            or str(item.get("market") or "") in artifact_markets
        )
    ]
    claim_ids = {
        str(row.get("id")) for row in rows if row.get("id")
    }
    artifact_fields = {
        (
            str(row.get("a") or ""),
            str(row.get("aid") or ""),
            str(row.get("f") or ""),
        )
        for row in rows
    }
    deterministic_flags = [
        item
        for item in source.get("deterministic_flags", []) or []
        if (
            str(item.get("claim_id") or "") in claim_ids
            or (
                str(item.get("artifact_type") or ""),
                str(item.get("artifact_id") or ""),
                str(item.get("field_name") or ""),
            )
            in artifact_fields
        )
    ]
    return {
        "contract_version": MFI_RED_TEAM_BATCH_CONTRACT_VERSION,
        "batch_kind": kind,
        "claims": list(rows),
        "priority_context": {"dimensions": dimensions, "markets": markets},
        "evidence_by_metric_id": _evidence_for_rows(
            source.get("evidence_by_metric_id", {}) or {}, rows
        ),
        "cited_documents": documents,
        "limitations": limitations,
        "deterministic_flags": deterministic_flags,
        "prohibitions": list(source.get("prohibitions", []) or []),
    }


def build_red_team_batches(
    source: Mapping[str, Any],
    *,
    target_characters: int = MFI_RED_TEAM_BATCH_TARGET_CHARACTERS,
) -> List[MFIRedTeamReviewBatch]:
    """Partition Red-Team review deterministically without truncating a field."""
    rows = [dict(row) for row in source.get("claims", []) or []]
    context_rows = [
        {
            "a": "context",
            "aid": item.get("id"),
            "f": "text",
            "p": index,
            "id": item.get("id"),
            "text": item.get("text"),
            "class": item.get("class"),
            "s": "context",
            "o": "descriptive",
            "d": list(item.get("docs", []) or []),
        }
        for index, item in enumerate(source.get("context_statements", []) or [], start=1)
    ]
    rows = [*context_rows, *rows]
    grouped: List[List[Dict[str, Any]]] = []
    for row in rows:
        key = (str(row.get("a")), str(row.get("aid")), str(row.get("f")))
        if grouped:
            prior = grouped[-1][0]
            prior_key = (
                str(prior.get("a")), str(prior.get("aid")), str(prior.get("f"))
            )
            if key == prior_key:
                grouped[-1].append(row)
                continue
        grouped.append([row])

    local_groups: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    for atom in grouped:
        atom_package = _batch_package(source, rows=atom, kind="local")
        if len(_serialized(atom_package)) > target_characters:
            raise MFIGenerationBlockedError(
                "mfi_red_team_batch_contract_failed",
                "A Red-Team artifact field exceeds the configured batch size.",
                stage="red_team",
                status_code=500,
                artifact_type=str(atom[0].get("a") or "") or None,
                artifact_id=str(atom[0].get("aid") or "") or None,
                field_name=str(atom[0].get("f") or "") or None,
            )
        candidate = [*current, *atom]
        if current and len(_serialized(_batch_package(source, rows=candidate, kind="local"))) > target_characters:
            local_groups.append(current)
            current = list(atom)
        else:
            current = candidate
    if current:
        local_groups.append(current)

    packages: List[tuple[str, List[Dict[str, Any]]]] = [
        ("local", group) for group in local_groups
    ]
    priority_dimensions = {
        str(item.get("id"))
        for item in (source.get("priority_context", {}) or {}).get("dimensions", []) or []
    }
    selected_markets = {
        str(item.get("id"))
        for item in (source.get("priority_context", {}) or {}).get("markets", []) or []
    }
    executive_rows = [row for row in rows if row.get("a") == "executive_summary"]
    dimension_rows = [
        row
        for row in rows
        if row.get("a") == "dimension"
        and str(row.get("aid")) in priority_dimensions
        and row.get("f") in {"summary", "key_findings", "recommendations"}
    ]
    market_rows = [
        row
        for row in rows
        if row.get("a") == "market"
        and str(row.get("aid")) in selected_markets
        and row.get("f") in {"priority_issues", "recommended_interventions"}
    ]
    if dimension_rows or executive_rows:
        packages.append(("dimension_coherence", [*dimension_rows, *executive_rows]))
    if market_rows or executive_rows:
        packages.append(("market_coherence", [*market_rows, *executive_rows]))

    batches: List[MFIRedTeamReviewBatch] = []
    for sequence, (kind, batch_rows) in enumerate(packages, start=1):
        package = _batch_package(source, rows=batch_rows, kind=kind)
        serialized = _serialized(package)
        if len(serialized) > target_characters:
            raise MFIGenerationBlockedError(
                "mfi_red_team_batch_contract_failed",
                f"The {kind} Red-Team package exceeds the configured batch size.",
                stage="red_team",
                status_code=500,
            )
        signature = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        membership = "|".join(
            f"{row.get('a')}:{row.get('aid')}:{row.get('f')}"
            for row in batch_rows
        )
        batch_id = (
            f"red-team-{kind}-{sequence:02d}-"
            f"{_short_hash(f'{kind}|{sequence}|{membership}')}"
        )
        batches.append(
            MFIRedTeamReviewBatch(
                batch_id=batch_id,
                signature=signature,
                batch_kind=kind,
                sequence=sequence,
                package=package,
                claim_ids=[str(row.get("id")) for row in batch_rows if row.get("id")],
                artifact_refs=sorted(
                    {
                        f"{row.get('a')}:{row.get('aid')}"
                        for row in batch_rows
                        if row.get("a") and row.get("aid")
                    }
                ),
                character_count=len(serialized),
            )
        )
    return batches

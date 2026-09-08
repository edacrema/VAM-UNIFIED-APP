"""Application-owned identities for MFI 2.0 narrative artifacts.

LLM output is content, never identity.  This module is the single authority for
claim and context-statement identifiers used by validation, correction, QA, and
delivery.  The functions are deliberately pure so the same contract can be
reapplied at every graph boundary without changing narrative content.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from hashlib import sha256
import re
import unicodedata
from typing import Any, Mapping, MutableMapping, Sequence


CLAIM_IDENTITY_AUTHORITY = "application"
CLAIM_IDENTITY_VERSION = "mfi-claim-id-v2"


class MFIClaimIdentityError(ValueError):
    """Raised when canonical narrative identity cannot be established safely."""

    def __init__(self, message: str, *, artifacts: Sequence[str] = ()) -> None:
        super().__init__(message)
        self.artifacts = tuple(dict.fromkeys(str(value) for value in artifacts if value))


@dataclass(frozen=True)
class MFIClaimLocation:
    """One structural claim location, independent of model-provided identifiers."""

    artifact_type: str
    artifact_id: str
    field_name: str
    position: int
    claim_id: str


def normalized_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii").casefold()
    slug = re.sub(r"[^a-z0-9]+", "_", ascii_value).strip("_")
    return slug or "unnamed"


def context_token(value: str) -> str:
    """Return the Phase 2 collision-resistant normalized context token."""
    normalized = unicodedata.normalize("NFKC", str(value)).strip()
    digest = sha256(normalized.encode("utf-8")).hexdigest()[:8]
    return f"{normalized_slug(normalized)}_{digest}"


def dimension_claim_id(dimension: str, field_name: str, position: int = 1) -> str:
    return (
        f"dimension.{normalized_slug(dimension)}."
        f"{normalized_slug(field_name)}.{_positive_position(position)}"
    )


def subdimension_claim_id(dimension: str, position: int) -> str:
    return (
        f"dimension.{normalized_slug(dimension)}.subdimension."
        f"{_positive_position(position)}.interpretation"
    )


def market_claim_id(market_name: str, field_name: str, position: int = 1) -> str:
    return (
        f"market.{context_token(market_name)}."
        f"{normalized_slug(field_name)}.{_positive_position(position)}"
    )


def executive_claim_id(field_name: str, position: int = 1) -> str:
    return f"executive.{normalized_slug(field_name)}.{_positive_position(position)}"


def context_statement_id(position: int) -> str:
    return f"context.statement.{_positive_position(position)}"


def count_model_identifiers(payload: Any) -> int:
    """Count model-supplied identity fields that the application will ignore."""
    count = 0
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            if key in {"claim_id", "statement_id"} and value is not None:
                count += 1
            count += count_model_identifiers(value)
    elif isinstance(payload, Sequence) and not isinstance(
        payload, (str, bytes, bytearray)
    ):
        count += sum(count_model_identifiers(value) for value in payload)
    return count


def canonicalize_narrative_identities(
    *,
    dimension_narratives: Mapping[str, Any],
    market_narratives: Mapping[str, Any],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]] = (),
    preserve_context_ids: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    """Return detached artifacts with IDs derived only from structural location."""
    dimensions = deepcopy(dict(dimension_narratives))
    markets = deepcopy(dict(market_narratives))
    executive = deepcopy(dict(executive_narrative))
    context = deepcopy(list(context_evidence))

    for key, narrative in dimensions.items():
        if not isinstance(narrative, MutableMapping):
            raise MFIClaimIdentityError(
                "Dimension narrative must be an object.",
                artifacts=[f"dimension:{key}"],
            )
        dimension = str(narrative.get("dimension") or key)
        artifact = f"dimension:{dimension}"
        _assign_single(
            narrative,
            "summary",
            dimension_claim_id(dimension, "summary", 1),
            artifact=artifact,
            required=False,
        )
        _assign_list(
            narrative,
            "key_findings",
            lambda position: dimension_claim_id(dimension, "finding", position),
            artifact=artifact,
        )
        _assign_list(
            narrative,
            "geographic_patterns",
            lambda position: dimension_claim_id(dimension, "geography", position),
            artifact=artifact,
        )
        _assign_list(
            narrative,
            "data_limitations",
            lambda position: dimension_claim_id(dimension, "limitation", position),
            artifact=artifact,
        )
        _assign_list(
            narrative,
            "recommendations",
            lambda position: dimension_claim_id(
                dimension, "recommendation", position
            ),
            artifact=artifact,
        )
        for position, subdimension in enumerate(
            narrative.get("subdimension_analysis") or [], start=1
        ):
            if not isinstance(subdimension, MutableMapping):
                raise MFIClaimIdentityError(
                    "Subdimension narrative must be an object.",
                    artifacts=[artifact],
                )
            interpretation = subdimension.get("interpretation")
            if not isinstance(interpretation, MutableMapping):
                raise MFIClaimIdentityError(
                    "Subdimension interpretation must be a claim object.",
                    artifacts=[artifact],
                )
            interpretation["claim_id"] = subdimension_claim_id(dimension, position)

    for key, narrative in markets.items():
        if not isinstance(narrative, MutableMapping):
            raise MFIClaimIdentityError(
                "Market narrative must be an object.",
                artifacts=[f"market:{key}"],
            )
        market_name = str(narrative.get("market_name") or key)
        identity_name = str(narrative.get("market_key") or market_name)
        artifact = f"market:{market_name}"
        _assign_list(
            narrative,
            "priority_issues",
            lambda position: market_claim_id(identity_name, "issue", position),
            artifact=artifact,
        )
        _assign_list(
            narrative,
            "recommended_interventions",
            lambda position: market_claim_id(
                identity_name, "intervention", position
            ),
            artifact=artifact,
        )
        _assign_list(
            narrative,
            "limitations",
            lambda position: market_claim_id(identity_name, "limitation", position),
            artifact=artifact,
        )
        # Retained only in the API model for compatibility; R8 keeps it null. If an
        # older in-memory artifact supplies one, it still receives application identity.
        _assign_single(
            narrative,
            "modality_consideration",
            market_claim_id(identity_name, "modality_consideration", 1),
            artifact=artifact,
            required=False,
        )

    if not isinstance(executive, MutableMapping):
        raise MFIClaimIdentityError(
            "Executive narrative must be an object.", artifacts=["executive_summary"]
        )
    _assign_single(
        executive,
        "motivation",
        executive_claim_id("motivation", 1),
        artifact="executive_summary",
        required=False,
    )
    _assign_list(
        executive,
        "key_findings",
        lambda position: executive_claim_id("finding", position),
        artifact="executive_summary",
    )
    _assign_list(
        executive,
        "recommendations",
        lambda position: executive_claim_id("recommendation", position),
        artifact="executive_summary",
    )
    _assign_list(
        executive,
        "limitations",
        lambda position: executive_claim_id("limitation", position),
        artifact="executive_summary",
    )
    _assign_single(
        executive,
        "scope_statement",
        executive_claim_id("scope_statement", 1),
        artifact="executive_summary",
        required=False,
    )

    seen_context_ids: set[str] = set()
    for position, statement in enumerate(context, start=1):
        if not isinstance(statement, MutableMapping):
            raise MFIClaimIdentityError(
                "Context statement must be an object.", artifacts=["context"]
            )
        existing = str(statement.get("statement_id") or "")
        expected = context_statement_id(position)
        if preserve_context_ids and _is_canonical_context_id(existing):
            statement_id = existing
        else:
            statement_id = expected
        if statement_id in seen_context_ids:
            raise MFIClaimIdentityError(
                "Context statement identities must be unique.", artifacts=["context"]
            )
        seen_context_ids.add(statement_id)
        statement["statement_id"] = statement_id

    assert_claim_identity_contract(
        dimensions,
        markets,
        executive,
        context,
    )
    return dimensions, markets, executive, context


def claim_locations(
    dimension_narratives: Mapping[str, Any],
    market_narratives: Mapping[str, Any],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]] = (),
) -> list[MFIClaimLocation]:
    locations: list[MFIClaimLocation] = []
    for dimension, narrative in dimension_narratives.items():
        if not isinstance(narrative, Mapping):
            continue
        for field_name in (
            "summary",
            "key_findings",
            "geographic_patterns",
            "data_limitations",
            "recommendations",
        ):
            locations.extend(
                _field_locations("dimension", str(dimension), field_name, narrative)
            )
        for position, subdimension in enumerate(
            narrative.get("subdimension_analysis") or [], start=1
        ):
            if isinstance(subdimension, Mapping) and isinstance(
                subdimension.get("interpretation"), Mapping
            ):
                locations.append(
                    MFIClaimLocation(
                        "dimension",
                        str(dimension),
                        "subdimension_analysis",
                        position,
                        str(subdimension["interpretation"].get("claim_id") or ""),
                    )
                )
    for market, narrative in market_narratives.items():
        if not isinstance(narrative, Mapping):
            continue
        for field_name in (
            "priority_issues",
            "recommended_interventions",
            "limitations",
            "modality_consideration",
        ):
            locations.extend(
                _field_locations("market", str(market), field_name, narrative)
            )
    if isinstance(executive_narrative, Mapping):
        for field_name in (
            "motivation",
            "key_findings",
            "recommendations",
            "limitations",
            "scope_statement",
        ):
            locations.extend(
                _field_locations(
                    "executive_summary",
                    "executive_summary",
                    field_name,
                    executive_narrative,
                )
            )
    for position, statement in enumerate(context_evidence, start=1):
        if isinstance(statement, Mapping):
            locations.append(
                MFIClaimLocation(
                    "context",
                    str(statement.get("statement_id") or ""),
                    "statement",
                    position,
                    str(statement.get("statement_id") or ""),
                )
            )
    return locations


def canonical_claim_index(
    dimension_narratives: Mapping[str, Any],
    market_narratives: Mapping[str, Any],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]] = (),
) -> dict[str, MFIClaimLocation]:
    locations = claim_locations(
        dimension_narratives,
        market_narratives,
        executive_narrative,
        context_evidence,
    )
    index: dict[str, MFIClaimLocation] = {}
    duplicates: list[str] = []
    missing: list[str] = []
    for location in locations:
        if not location.claim_id:
            missing.append(f"{location.artifact_type}:{location.artifact_id}")
        elif location.claim_id in index:
            duplicates.append(location.claim_id)
        else:
            index[location.claim_id] = location
    if missing or duplicates:
        raise MFIClaimIdentityError(
            "Canonical claim IDs must be non-empty and globally unique.",
            artifacts=[*missing, *(["global"] if duplicates else [])],
        )
    return index


def assert_claim_identity_contract(
    dimension_narratives: Mapping[str, Any],
    market_narratives: Mapping[str, Any],
    executive_narrative: Mapping[str, Any],
    context_evidence: Sequence[Mapping[str, Any]] = (),
) -> None:
    index = canonical_claim_index(
        dimension_narratives,
        market_narratives,
        executive_narrative,
        context_evidence,
    )
    for claim_id, location in index.items():
        expected = _expected_id(location, dimension_narratives, market_narratives)
        if claim_id != expected:
            raise MFIClaimIdentityError(
                f"Claim identity does not match canonical location: {claim_id}.",
                artifacts=[f"{location.artifact_type}:{location.artifact_id}"],
            )


def _expected_id(
    location: MFIClaimLocation,
    dimension_narratives: Mapping[str, Any],
    market_narratives: Mapping[str, Any],
) -> str:
    if location.artifact_type == "context":
        return location.claim_id
    if location.artifact_type == "dimension":
        narrative = dimension_narratives.get(location.artifact_id) or {}
        dimension = str(narrative.get("dimension") or location.artifact_id)
        if location.field_name == "subdimension_analysis":
            return subdimension_claim_id(dimension, location.position)
        field = {
            "key_findings": "finding",
            "geographic_patterns": "geography",
            "data_limitations": "limitation",
            "recommendations": "recommendation",
        }.get(location.field_name, location.field_name)
        return dimension_claim_id(dimension, field, location.position)
    if location.artifact_type == "market":
        narrative = market_narratives.get(location.artifact_id) or {}
        market_name = str(narrative.get("market_key") or narrative.get("market_name") or location.artifact_id)
        field = {
            "priority_issues": "issue",
            "recommended_interventions": "intervention",
            "limitations": "limitation",
        }.get(location.field_name, location.field_name)
        return market_claim_id(market_name, field, location.position)
    field = {
        "key_findings": "finding",
        "recommendations": "recommendation",
        "limitations": "limitation",
    }.get(location.field_name, location.field_name)
    return executive_claim_id(field, location.position)


def _field_locations(
    artifact_type: str,
    artifact_id: str,
    field_name: str,
    artifact: Mapping[str, Any],
) -> list[MFIClaimLocation]:
    value = artifact.get(field_name)
    if isinstance(value, Mapping):
        return [
            MFIClaimLocation(
                artifact_type,
                artifact_id,
                field_name,
                1,
                str(value.get("claim_id") or ""),
            )
        ]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            MFIClaimLocation(
                artifact_type,
                artifact_id,
                field_name,
                position,
                str(item.get("claim_id") or ""),
            )
            for position, item in enumerate(value, start=1)
            if isinstance(item, Mapping)
        ]
    return []


def _assign_single(
    container: MutableMapping[str, Any],
    field_name: str,
    claim_id: str,
    *,
    artifact: str,
    required: bool = True,
) -> None:
    value = container.get(field_name)
    if value is None and not required:
        return
    if not isinstance(value, MutableMapping):
        raise MFIClaimIdentityError(
            f"{field_name} must be a claim object.", artifacts=[artifact]
        )
    value["claim_id"] = claim_id


def _assign_list(
    container: MutableMapping[str, Any],
    field_name: str,
    id_factory: Any,
    *,
    artifact: str,
) -> None:
    values = container.get(field_name) or []
    if not isinstance(values, list):
        raise MFIClaimIdentityError(
            f"{field_name} must be a list of claims.", artifacts=[artifact]
        )
    for position, claim in enumerate(values, start=1):
        if not isinstance(claim, MutableMapping):
            raise MFIClaimIdentityError(
                f"{field_name} must contain claim objects.", artifacts=[artifact]
            )
        claim["claim_id"] = id_factory(position)


def _positive_position(position: int) -> int:
    value = int(position)
    if value < 1:
        raise ValueError("Claim positions are one-based")
    return value


def _is_canonical_context_id(value: str) -> bool:
    return bool(re.fullmatch(r"context\.statement\.[1-9][0-9]*", value))

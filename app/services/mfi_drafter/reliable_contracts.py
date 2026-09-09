"""Versioned contracts shared by the reliable MFI workflow (no application I/O)."""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Literal

from pydantic import BaseModel, Field, ConfigDict

WORKFLOW_REVISION = "mfi-reliable-v1"
CONTRACT_BUNDLE = "mfi-reliable-contracts-v2"
MAX_PACKAGE_CHARACTERS = 160_000


def fingerprint(value: Any) -> str:
    def stable(item: Any) -> Any:
        if isinstance(item, float) and not math.isfinite(item):
            return {"nonfinite_source_value": str(item)}
        if isinstance(item, dict):
            return {str(k): stable(v) for k, v in item.items()}
        if isinstance(item, (tuple, list)):
            return [stable(v) for v in item]
        return item
    value = stable(value)
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class MarketIdentity(BaseModel):
    market_key: str
    country_identity: str
    survey_id: str | None = None
    source_market_id: str | None = None
    source_market_name: str
    aliases: list[str] = Field(default_factory=list)
    display_label: str
    identity_basis: Literal["source_ids", "legacy_geography_name"]


class InputFinding(BaseModel):
    code: str
    severity: Literal["info", "warning", "error"] = "warning"
    message: str
    row_references: list[int] = Field(default_factory=list)
    market_key: str | None = None
    fields: list[str] = Field(default_factory=list)


class ValidatedAssessment(BaseModel):
    workflow_revision: str = WORKFLOW_REVISION
    input_fingerprint: str
    country_identity: str
    survey_id: str | None = None
    market_identities: dict[str, MarketIdentity]
    included_market_keys: list[str] = Field(default_factory=list)
    excluded_market_keys: list[str] = Field(default_factory=list)
    official_scores: dict[str, Any] = Field(default_factory=dict)
    evidence_validity: dict[str, Any] = Field(default_factory=dict)
    findings: list[InputFinding] = Field(default_factory=list)
    metadata_provenance: dict[str, Any] = Field(default_factory=dict)


class AnalyticalFact(BaseModel):
    fact_id: str
    dimension: str | None = None
    subject: str
    population: str
    statistic: str
    value: float | None = None
    unit: str
    operator: Literal["lt", "le", "minimum", "maximum", "value"] = "value"
    threshold: float | None = None
    numerator: int | None = None
    denominator: int | None = None
    member_ids: list[str] = Field(default_factory=list)
    source_metric_ids: list[str] = Field(default_factory=list)
    rendered_text: str


class CoverageRequirement(BaseModel):
    requirement_id: str
    dimension: str
    kind: str
    metric_ids: list[str] = Field(default_factory=list)
    satisfied_by: list[str] = Field(default_factory=list)
    status: Literal["pending", "covered", "limitation"] = "pending"


class NarrativeSegment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["text", "fact", "source_passage"]
    text: str | None = None
    fact_id: str | None = None
    passage_id: str | None = None


class SourcePassage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    document_id: str
    text: str


class RunExecutionRecord(BaseModel):
    task_id: str
    kind: str
    input_fingerprint: str
    status: Literal["planned", "running", "succeeded", "failed"] = "planned"
    attempt: int = 0
    epoch: int = 1
    call_ids: list[str] = Field(default_factory=list)
    output_ref: str | None = None
    error: dict[str, Any] | None = None


class RunCheckpoint(BaseModel):
    workflow_revision: str = WORKFLOW_REVISION
    contract_bundle: str = CONTRACT_BUNDLE
    run_id: str
    run_revision: int = 0
    input_fingerprint: str
    runtime_fingerprint: str
    epoch: int = 1
    owner: str | None = None
    fence: int = 0
    lease_until: float = 0
    execution_state: str = "pending"
    resumable: bool = False
    resume_block_reason: str | None = None
    tasks: dict[str, RunExecutionRecord] = Field(default_factory=dict)
    input_ref: str | None = None
    snapshot_ref: str | None = None
    draft_revision: int | None = None
    requests: dict[str, Any] = Field(default_factory=dict)

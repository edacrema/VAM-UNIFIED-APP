"""Reader-facing projections for canonical MFI deterministic tables.

Phase 2 tables are deliberately presentation-neutral and remain part of the public
assessment profile.  This module is the only place where those rows are selected,
formatted, labelled, and prepared for report renderers.  It never mutates the profile.
"""
from __future__ import annotations

import csv
import io
import json
import math
import unicodedata
from types import MappingProxyType
from typing import Any, Literal, Mapping, Optional, Sequence

from pydantic import BaseModel, ConfigDict, field_validator, model_validator


MFIReportCellFormat = Literal[
    "text",
    "score_statistic",
    "percentage",
    "percentage_point",
    "integer",
    "list",
    "boolean",
    "coverage",
]
MFIReportAlignment = Literal["left", "center", "right"]
MFILedgerLinkagePolicy = Literal["required", "not_applicable"]

MISSING_DISPLAY_VALUE = "—"


class MFIReportTableProjectionError(ValueError):
    """Raised when a canonical MFI table cannot be projected safely."""


class MFIReportTableColumn(BaseModel):
    """One immutable, explicitly rendered report-table column."""

    model_config = ConfigDict(frozen=True)

    key: str
    label: str
    format: MFIReportCellFormat = "text"
    alignment: MFIReportAlignment = "left"
    width_hint: float = 1.0
    ledger_linkage_policy: MFILedgerLinkagePolicy = "required"

    @field_validator("key", "label")
    @classmethod
    def _nonempty_text(cls, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("table column key and label must be non-empty")
        return cleaned

    @field_validator("width_hint")
    @classmethod
    def _positive_width(cls, value: float) -> float:
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            raise ValueError("table column width_hint must be finite and positive")
        return number


class MFIReportTableSpec(BaseModel):
    """Immutable projection contract for one canonical deterministic table."""

    model_config = ConfigDict(frozen=True)

    spec_id: str
    canonical_source_table: str
    columns: tuple[MFIReportTableColumn, ...]
    maximum_rows: Optional[int] = None

    @field_validator("spec_id", "canonical_source_table")
    @classmethod
    def _nonempty_identity(cls, value: str) -> str:
        cleaned = str(value).strip()
        if not cleaned:
            raise ValueError("table spec identity fields must be non-empty")
        return cleaned

    @model_validator(mode="after")
    def _validate_contract(self) -> "MFIReportTableSpec":
        if not self.columns:
            raise ValueError("table specs require at least one column")
        if len(self.columns) > 8:
            raise ValueError("reader-facing MFI tables may not exceed eight columns")
        keys = [column.key for column in self.columns]
        if len(keys) != len(set(keys)):
            raise ValueError("table spec column keys must be unique")
        if self.maximum_rows is not None and self.maximum_rows < 1:
            raise ValueError("maximum_rows must be positive when provided")
        return self


def _column(
    key: str,
    label: str,
    format: MFIReportCellFormat = "text",
    alignment: MFIReportAlignment = "left",
    width: float = 1.0,
    linkage: MFILedgerLinkagePolicy = "required",
) -> MFIReportTableColumn:
    return MFIReportTableColumn(
        key=key,
        label=label,
        format=format,
        alignment=alignment,
        width_hint=width,
        ledger_linkage_policy=linkage,
    )


_REPORT_TABLE_SPECS = (
    MFIReportTableSpec(
        spec_id="mfi.dimension_summary.v1",
        canonical_source_table="dimension_rows",
        maximum_rows=9,
        columns=(
            _column("dimension", "Dimension", width=1.7),
            _column("mean", "Mean", "score_statistic", "right"),
            _column("median", "Median", "score_statistic", "right"),
            _column("minimum", "Minimum", "score_statistic", "right"),
            _column("maximum", "Maximum", "score_statistic", "right"),
            _column("iqr", "IQR", "score_statistic", "right"),
            _column("rank", "Rank", "integer", "right", 0.7),
            _column("is_priority", "Priority", "boolean", "center", 0.8),
        ),
    ),
    MFIReportTableSpec(
        spec_id="mfi.regional_summary.v1",
        canonical_source_table="regional_rows",
        columns=(
            _column("region", "Region", width=1.5),
            _column("dimension", "Dimension", width=1.6),
            _column("mean", "Mean", "score_statistic", "right"),
            _column("market_count", "Assessed markets", "integer", "right"),
            _column("coverage", "Coverage", "coverage", "right", 1.6),
        ),
    ),
    MFIReportTableSpec(
        spec_id="mfi.official_subsection.v1",
        canonical_source_table="subsection_rows",
        maximum_rows=2,
        columns=(
            _column("display_name", "Official subsection", width=2.4),
            _column(
                "mean_normalized_value",
                "Normalized score",
                "score_statistic",
                "right",
                1.3,
            ),
            _column("weakness_rank", "Weakness rank", "integer", "right", 1.0),
            _column("coverage", "Coverage", "coverage", "right", 1.6),
        ),
    ),
    MFIReportTableSpec(
        spec_id="mfi.ranked_driver.v1",
        canonical_source_table="driver_rows",
        maximum_rows=4,
        columns=(
            _column("display_name", "Driver or question", width=2.5),
            _column(
                "unfavorable_rate",
                "Unfavorable rate",
                "percentage",
                "right",
                1.2,
            ),
            _column("weakness_rank", "Weakness rank", "integer", "right", 1.0),
            _column("evidence_scope", "Evidence scope", width=1.7),
            _column("coverage", "Coverage", "coverage", "right", 1.5),
        ),
    ),
    MFIReportTableSpec(
        spec_id="mfi.relevant_item.v1",
        canonical_source_table="relevant_item_rows",
        columns=(
            _column("item_name", "Relevant item", width=1.8),
            _column("question_group", "Question group", width=1.8),
            _column(
                "unfavorable_rate",
                "Unfavorable rate",
                "percentage",
                "right",
                1.2,
            ),
            _column(
                "category_contrast",
                "Category contrast",
                "percentage_point",
                "right",
                1.2,
            ),
            _column(
                "represented_markets",
                "Represented markets",
                "integer",
                "right",
                1.1,
            ),
        ),
    ),
    MFIReportTableSpec(
        spec_id="mfi.priority_market.v1",
        canonical_source_table="priority_market_rows",
        maximum_rows=15,
        columns=(
            _column("market_name", "Assessed market", width=2.0),
            _column("region", "Region", width=1.5),
            _column("overall_mfi", "MFI score", "score_statistic", "right"),
            _column("score_rank", "Score rank", "integer", "right", 0.8),
            _column("weak_dimensions", "Weak dimensions", "list", width=2.5),
        ),
    ),
)

if len({spec.spec_id for spec in _REPORT_TABLE_SPECS}) != len(_REPORT_TABLE_SPECS):
    raise RuntimeError("MFI report-table spec IDs must be unique")
if len({spec.canonical_source_table for spec in _REPORT_TABLE_SPECS}) != len(
    _REPORT_TABLE_SPECS
):
    raise RuntimeError("Each canonical MFI table must have exactly one projection")

MFI_REPORT_TABLE_SPECS: Mapping[str, MFIReportTableSpec] = MappingProxyType(
    {spec.spec_id: spec for spec in _REPORT_TABLE_SPECS}
)
MFI_REPORT_TABLE_SPEC_BY_SOURCE: Mapping[str, MFIReportTableSpec] = MappingProxyType(
    {spec.canonical_source_table: spec for spec in _REPORT_TABLE_SPECS}
)

MFI_QA_FINDINGS_TABLE_SPEC = MFIReportTableSpec(
    spec_id="mfi.qa_findings.v1",
    canonical_source_table="qa_review.flags",
    columns=(
        _column("severity", "Severity", linkage="not_applicable"),
        _column("source", "Source", linkage="not_applicable"),
        _column(
            "artifact_location",
            "Artifact / location",
            width=1.5,
            linkage="not_applicable",
        ),
        _column("field", "Field", linkage="not_applicable"),
        _column("claim_code", "Claim / code", width=1.5, linkage="not_applicable"),
        _column("message", "Message", width=3.0, linkage="not_applicable"),
        _column(
            "attempts_outcome",
            "Attempts / outcome",
            width=1.8,
            linkage="not_applicable",
        ),
        _column("disposition", "Disposition", width=1.8, linkage="not_applicable"),
    ),
)


def get_mfi_report_table_spec(spec_id: str) -> MFIReportTableSpec:
    try:
        return MFI_REPORT_TABLE_SPECS[str(spec_id)]
    except KeyError as exc:
        raise MFIReportTableProjectionError(
            f"No registered MFI presentation projection for {spec_id!r}"
        ) from exc


def format_mfi_report_value(value: Any, format_kind: MFIReportCellFormat) -> str:
    """Apply the single R6 formatting policy used by every renderer."""
    if value is None:
        return MISSING_DISPLAY_VALUE
    if format_kind == "text":
        cleaned = str(value).strip()
        return cleaned or MISSING_DISPLAY_VALUE
    if format_kind == "list":
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            rendered = ", ".join(str(item) for item in value if str(item).strip())
            return rendered or MISSING_DISPLAY_VALUE
        return str(value)
    if format_kind == "boolean":
        if not isinstance(value, bool):
            raise MFIReportTableProjectionError(
                f"Boolean report cell received {type(value).__name__}"
            )
        return "Yes" if value else "No"
    if format_kind == "coverage":
        if not isinstance(value, Mapping):
            raise MFIReportTableProjectionError("Coverage cells require a mapping")
        available = _strict_int(value.get("available_market_count"), "available")
        total = _strict_int(value.get("total_assessed_market_count"), "total")
        ratio = _strict_number(value.get("coverage_ratio"), "coverage ratio")
        if available < 0 or total < 0 or available > total:
            raise MFIReportTableProjectionError("Coverage cell contains invalid counts")
        expected = (available / total) if total else 0.0
        if abs(ratio - expected) > 1e-9:
            raise MFIReportTableProjectionError("Coverage cell ratio is inconsistent")
        return f"{available}/{total} markets ({ratio * 100:.1f}%)"
    number = _strict_number(value, format_kind)
    if format_kind == "score_statistic":
        return f"{number:.2f}"
    if format_kind == "percentage":
        return f"{number * 100:.1f}%"
    if format_kind == "percentage_point":
        return f"{number * 100:.1f} pp"
    if format_kind == "integer":
        if abs(number - round(number)) > 1e-9:
            raise MFIReportTableProjectionError(
                f"Integer report cell received non-integral value {number!r}"
            )
        return str(int(round(number)))
    raise MFIReportTableProjectionError(f"Unsupported report-cell format {format_kind!r}")


def build_mfi_presentation_table(
    profile: Mapping[str, Any],
    *,
    spec_id: str,
    title: str,
    dimension: Optional[str] = None,
) -> dict[str, Any]:
    """Project one canonical table into renderer-complete metadata."""
    spec = get_mfi_report_table_spec(spec_id)
    tables = _mapping(profile.get("tables"), "assessment_profile.tables")
    source_rows = tables.get(spec.canonical_source_table)
    if not isinstance(source_rows, list):
        raise MFIReportTableProjectionError(
            f"Canonical table {spec.canonical_source_table!r} is missing or malformed"
        )
    selected = _select_rows(
        spec,
        source_rows,
        tables=tables,
        profile=profile,
        dimension=dimension,
    )
    ledger_value = profile.get("metric_ledger")
    ledger = _mapping(
        ledger_value if selected or ledger_value is not None else {},
        "assessment_profile.metric_ledger",
    )
    projected_rows = [
        _project_row(spec, row, ledger=ledger, tables=tables)
        for row in selected
    ]
    return _presentation_metadata(spec, title=title, rows=projected_rows)


def build_mfi_qa_presentation_table(
    *,
    title: str,
    rows: Sequence[Mapping[str, Any]],
    qa_flag_ids: Sequence[str],
) -> dict[str, Any]:
    """Build the explicit eight-column R4 QA-table projection."""
    spec = MFI_QA_FINDINGS_TABLE_SPEC
    projected = []
    for row in rows:
        raw = _mapping(row.get("raw_values"), "QA raw_values")
        values = _mapping(row.get("values"), "QA values")
        display = {
            column.key: format_mfi_report_value(values.get(column.key), column.format)
            for column in spec.columns
        }
        projected.append(
            {
                "row_id": str(row.get("row_id") or ""),
                "values": display,
                "ledger_metric_ids": [],
                "cell_ledger_metric_ids": {},
                "raw_values": dict(raw),
            }
        )
    meta = _presentation_metadata(spec, title=title, rows=projected)
    meta["qa_flag_ids"] = [str(flag_id) for flag_id in qa_flag_ids]
    return meta


def build_mfi_raw_table_downloads(
    profile: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Return one canonical JSON bundle and one CSV for each Phase 2 table."""
    tables = _mapping(profile.get("tables"), "assessment_profile.tables")
    missing = [
        source for source in MFI_REPORT_TABLE_SPEC_BY_SOURCE if source not in tables
    ]
    if missing:
        raise MFIReportTableProjectionError(
            f"Canonical table bundle is missing: {', '.join(missing)}"
        )
    canonical = {
        source: tables[source]
        for source in MFI_REPORT_TABLE_SPEC_BY_SOURCE
    }
    downloads = [
        {
            "label": "Complete raw-table JSON",
            "file_name": "mfi_canonical_tables.json",
            "mime": "application/json",
            "data": json.dumps(
                canonical,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ).encode("utf-8"),
        }
    ]
    for source, rows in canonical.items():
        downloads.append(
            {
                "label": f"{source.replace('_rows', '').replace('_', ' ').title()} CSV",
                "file_name": f"mfi_{source.removesuffix('_rows')}_raw.csv",
                "mime": "text/csv",
                "data": _canonical_rows_csv(rows),
            }
        )
    return downloads


def _presentation_metadata(
    spec: MFIReportTableSpec,
    *,
    title: str,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "table_kind": "mfi_presentation",
        "spec_id": spec.spec_id,
        "canonical_source_table": spec.canonical_source_table,
        "title": str(title),
        "columns": [column.key for column in spec.columns],
        "column_specs": [column.model_dump(mode="json") for column in spec.columns],
        "rows": [dict(row) for row in rows],
    }


def _select_rows(
    spec: MFIReportTableSpec,
    rows: Sequence[Any],
    *,
    tables: Mapping[str, Any],
    profile: Mapping[str, Any],
    dimension: Optional[str],
) -> list[Mapping[str, Any]]:
    prepared = [_canonical_row(row) for row in rows]
    source = spec.canonical_source_table
    priority_dimensions = {
        str(value) for value in profile.get("priority_dimension_names", []) or []
    }

    if source == "dimension_rows":
        selected = prepared
    elif source == "regional_rows":
        selected = prepared
    elif source == "subsection_rows":
        _require_priority_dimension(dimension, priority_dimensions)
        if dimension == "Food Quality":
            selected = []
        else:
            selected = [
                row
                for row in prepared
                if row["values"].get("dimension") == dimension
                and row["values"].get("role") == "official_subsection"
                and row["values"].get("mean_normalized_value") is not None
            ]
            selected.sort(key=_metric_weakness_order)
    elif source == "driver_rows":
        _require_priority_dimension(dimension, priority_dimensions)
        selected = [
            row
            for row in prepared
            if row["values"].get("dimension") == dimension
            and row["values"].get("role") in {"category_driver", "question_driver"}
            and row["values"].get("unfavorable_rate") is not None
            and row["values"].get("weakness_rank") is not None
        ]
        selected.sort(key=_metric_weakness_order)
    elif source == "relevant_item_rows":
        _require_priority_dimension(dimension, priority_dimensions)
        selected = [
            row
            for row in prepared
            if row["values"].get("dimension") == dimension
            and row["values"].get("role") == "item_driver"
            and row["values"].get("item_relevant") is True
        ]
        selected.sort(
            key=lambda row: (
                _normalized(str(row["values"].get("question_group") or "")),
                int(row["values"].get("group_rank") or 10**9),
                str(row["values"].get("metric_id") or ""),
            )
        )
    elif source == "priority_market_rows":
        selected = sorted(
            prepared,
            key=lambda row: (
                int(row["values"].get("selection_order") or 10**9),
                _normalized(str(row["values"].get("market_name") or "")),
            ),
        )
    else:  # guarded by the registry, kept as a fail-closed invariant
        raise MFIReportTableProjectionError(
            f"No selection policy for canonical MFI table {source!r}"
        )
    if spec.maximum_rows is not None:
        selected = selected[: spec.maximum_rows]
    return selected


def _project_row(
    spec: MFIReportTableSpec,
    row: Mapping[str, Any],
    *,
    ledger: Mapping[str, Any],
    tables: Mapping[str, Any],
) -> dict[str, Any]:
    values = _mapping(row.get("values"), "canonical row values")
    row_ids = [str(value) for value in row.get("ledger_metric_ids", []) or []]
    _validate_ledger_ids(row_ids, ledger, row_id=str(row.get("row_id") or ""))
    raw_projected = _raw_projected_values(spec, values, tables=tables, ledger=ledger)
    cell_links = {
        column.key: _cell_ledger_ids(
            spec,
            column.key,
            values=values,
            raw_projected=raw_projected,
            row_ids=row_ids,
            ledger=ledger,
            tables=tables,
        )
        for column in spec.columns
    }
    for column in spec.columns:
        links = cell_links[column.key]
        if column.ledger_linkage_policy == "required" and not links:
            raise MFIReportTableProjectionError(
                f"Visible cell {row.get('row_id')}.{column.key} has no ledger linkage"
            )
        _validate_ledger_ids(links, ledger, row_id=str(row.get("row_id") or ""))
    return {
        "row_id": str(row.get("row_id") or ""),
        "values": {
            column.key: format_mfi_report_value(
                raw_projected.get(column.key), column.format
            )
            for column in spec.columns
        },
        "ledger_metric_ids": row_ids,
        "cell_ledger_metric_ids": cell_links,
    }


def _raw_projected_values(
    spec: MFIReportTableSpec,
    values: Mapping[str, Any],
    *,
    tables: Mapping[str, Any],
    ledger: Mapping[str, Any],
) -> dict[str, Any]:
    projected = {column.key: values.get(column.key) for column in spec.columns}
    source = spec.canonical_source_table
    if source in {"regional_rows", "subsection_rows", "driver_rows"}:
        projected["coverage"] = {
            "available_market_count": (
                values.get("market_count")
                if source == "regional_rows"
                else values.get("available_market_count")
            ),
            "total_assessed_market_count": (
                _coverage_total_from_ledger(values, ledger)
                if source == "regional_rows"
                else values.get("total_assessed_market_count")
            ),
            "coverage_ratio": (
                _coverage_ratio_from_ledger(values, ledger)
                if source == "regional_rows"
                else values.get("coverage_ratio")
            ),
        }
    if source == "driver_rows" and projected.get("evidence_scope") is not None:
        projected["evidence_scope"] = _humanize_token(projected["evidence_scope"])
    if source == "relevant_item_rows":
        projected["represented_markets"] = values.get("available_market_count")
        category_row = _matching_category_row(values, tables)
        item_rate = values.get("unfavorable_rate")
        category_rate = category_row["values"].get("unfavorable_rate")
        projected["category_contrast"] = (
            None
            if item_rate is None or category_rate is None
            else float(item_rate) - float(category_rate)
        )
    return projected


def _cell_ledger_ids(
    spec: MFIReportTableSpec,
    key: str,
    *,
    values: Mapping[str, Any],
    raw_projected: Mapping[str, Any],
    row_ids: list[str],
    ledger: Mapping[str, Any],
    tables: Mapping[str, Any],
) -> list[str]:
    if not row_ids:
        return []
    statistics_by_key = {
        "mean": "mean",
        "median": "median",
        "minimum": "minimum",
        "maximum": "maximum",
        "iqr": "iqr",
        "rank": "rank_lowest_first",
        "market_count": "denominator",
        "mean_normalized_value": "mean_normalized_value",
        "unfavorable_rate": "derived_unfavorable_rate",
        "weakness_rank": "rank",
        "overall_mfi": "stored_level_1_score",
        "score_rank": "rank_lowest_first",
    }
    statistics = (
        {"coverage", "coverage_ratio"}
        if key == "coverage"
        else {statistics_by_key[key]}
        if key in statistics_by_key
        else set()
    )
    if statistics:
        matching = [
            ledger_id
            for ledger_id in row_ids
            if _ledger_statistic(ledger.get(ledger_id)) in statistics
        ]
        if matching:
            return matching
    if spec.canonical_source_table == "relevant_item_rows":
        if key == "represented_markets":
            return [
                ledger_id
                for ledger_id in row_ids
                if _ledger_statistic(ledger.get(ledger_id)) == "coverage_ratio"
            ]
        if key == "category_contrast":
            category = _matching_category_row(values, tables)
            category_ids = [
                str(value) for value in category.get("ledger_metric_ids", []) or []
            ]
            return [
                ledger_id
                for ledger_id in [*row_ids, *category_ids]
                if _ledger_statistic(ledger.get(ledger_id))
                == "derived_unfavorable_rate"
            ]
    # Labels, scopes, booleans, and lists are deterministic descriptions of the row.
    # Their lineage is the complete row-level source set.
    return list(row_ids)


def _matching_category_row(
    values: Mapping[str, Any], tables: Mapping[str, Any]
) -> Mapping[str, Any]:
    metric_id = str(values.get("matching_category_metric_id") or "")
    if not metric_id:
        raise MFIReportTableProjectionError(
            f"Relevant item {values.get('metric_id')!r} lacks a matching category"
        )
    candidates = tables.get("driver_rows")
    if not isinstance(candidates, list):
        raise MFIReportTableProjectionError("Canonical driver_rows table is malformed")
    for candidate in candidates:
        row = _canonical_row(candidate)
        if str(row["values"].get("metric_id") or "") == metric_id:
            if row["values"].get("role") != "category_driver":
                raise MFIReportTableProjectionError(
                    f"Matching metric {metric_id!r} is not a category driver"
                )
            return row
    raise MFIReportTableProjectionError(
        f"Relevant item references missing matching category {metric_id!r}"
    )


def _coverage_total_from_ledger(
    values: Mapping[str, Any], ledger: Mapping[str, Any]
) -> int:
    entry = _coverage_entry_for_region(values, ledger)
    coverage = _mapping(_entry_value(entry, "coverage"), "regional ledger coverage")
    return _strict_int(coverage.get("total_assessed_market_count"), "coverage total")


def _coverage_ratio_from_ledger(
    values: Mapping[str, Any], ledger: Mapping[str, Any]
) -> float:
    entry = _coverage_entry_for_region(values, ledger)
    return _strict_number(_entry_value(entry, "value"), "regional coverage ratio")


def _coverage_entry_for_region(
    values: Mapping[str, Any], ledger: Mapping[str, Any]
) -> Any:
    region = str(values.get("region") or "")
    dimension = str(values.get("dimension") or "")
    entries = [
        entry
        for entry in ledger.values()
        if _entry_value(entry, "region") == region
        and _entry_value(entry, "dimension") == dimension
        and _entry_value(entry, "statistic") == "coverage"
    ]
    if len(entries) != 1:
        raise MFIReportTableProjectionError(
            f"Regional row {region!r}/{dimension!r} has no unique coverage ledger entry"
        )
    return entries[0]


def _metric_weakness_order(row: Mapping[str, Any]) -> tuple[Any, ...]:
    values = row["values"]
    return (
        int(values.get("weakness_rank") or 10**9),
        -int(values.get("severity_weight") or 0),
        str(values.get("metric_id") or ""),
    )


def _require_priority_dimension(
    dimension: Optional[str], priority_dimensions: set[str]
) -> None:
    if not dimension or dimension not in priority_dimensions:
        raise MFIReportTableProjectionError(
            "Priority evidence projections require a selected priority dimension"
        )


def _canonical_row(value: Any) -> Mapping[str, Any]:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="python")
    row = _mapping(value, "canonical table row")
    _mapping(row.get("values"), "canonical row values")
    return row


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="python")
    if not isinstance(value, Mapping):
        raise MFIReportTableProjectionError(f"{label} must be an object")
    return value


def _entry_value(entry: Any, key: str) -> Any:
    if hasattr(entry, key):
        return getattr(entry, key)
    if isinstance(entry, Mapping):
        return entry.get(key)
    return None


def _ledger_statistic(entry: Any) -> str:
    return str(_entry_value(entry, "statistic") or "")


def _validate_ledger_ids(
    ledger_ids: Sequence[str], ledger: Mapping[str, Any], *, row_id: str
) -> None:
    missing = [ledger_id for ledger_id in ledger_ids if ledger_id not in ledger]
    if missing:
        raise MFIReportTableProjectionError(
            f"Projected row {row_id!r} references missing ledger IDs: {missing}"
        )


def _strict_number(value: Any, label: str) -> float:
    if isinstance(value, bool):
        raise MFIReportTableProjectionError(f"{label} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise MFIReportTableProjectionError(f"{label} must be numeric") from exc
    if not math.isfinite(number):
        raise MFIReportTableProjectionError(f"{label} must be finite")
    return number


def _strict_int(value: Any, label: str) -> int:
    number = _strict_number(value, label)
    if abs(number - round(number)) > 1e-9:
        raise MFIReportTableProjectionError(f"{label} must be an integer")
    return int(round(number))


def _normalized(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip().casefold()


def _humanize_token(value: Any) -> str:
    cleaned = str(value or "").replace("_", " ").strip()
    return cleaned[:1].upper() + cleaned[1:] if cleaned else ""


def _canonical_rows_csv(rows: Any) -> bytes:
    if not isinstance(rows, list):
        raise MFIReportTableProjectionError("Canonical table rows must be a list")
    prepared = [_canonical_row(row) for row in rows]
    value_keys = list(
        dict.fromkeys(
            key
            for row in prepared
            for key in _mapping(row.get("values"), "canonical row values")
        )
    )
    columns = ["row_id", *value_keys, "ledger_metric_ids"]
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    for row in prepared:
        values = _mapping(row.get("values"), "canonical row values")
        record = {"row_id": str(row.get("row_id") or "")}
        record.update({key: _csv_value(values.get(key)) for key in value_keys})
        record["ledger_metric_ids"] = _csv_value(
            row.get("ledger_metric_ids", []) or []
        )
        writer.writerow(record)
    return stream.getvalue().encode("utf-8-sig")


def _csv_value(value: Any) -> Any:
    if isinstance(value, (list, dict, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if value is None:
        return ""
    return value

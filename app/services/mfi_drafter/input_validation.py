"""Identity, source lineage and metadata checks before numerical MFI loading."""
from __future__ import annotations

from datetime import date, datetime
import math
from typing import Any
import unicodedata

import pandas as pd

from .reliable_contracts import InputFinding, MarketIdentity, fingerprint


class MFIInputError(ValueError):
    def __init__(self, message: str, findings: list[dict] | None = None, groups: list[str] | None = None):
        super().__init__(message)
        self.findings = findings or []
        self.groups = groups or []


def clean(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return unicodedata.normalize("NFKC", str(value)).strip()


def parse_collection_date(value: Any) -> str:
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.strftime("%Y-%m-%d") if not pd.isna(value) else "Unknown"
    text = clean(value)
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return "Unknown"


def output_state(value: Any, *, present: bool = True) -> str:
    if not present:
        return "absent"
    if value is None or value is pd.NA or (isinstance(value, float) and math.isnan(value)) or not str(value).strip():
        return "empty"
    try:
        number = float(value)
    except (ValueError, TypeError):
        return "nonnumeric"
    return "valid" if math.isfinite(number) else "nonfinite"


def prepare_identities(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df["_source_row"] = list(range(2, len(df) + 2))
    df["_raw_output"] = df["OutputValue"].map(lambda v: None if v is pd.NA or (isinstance(v, float) and math.isnan(v)) else v)
    df["_output_state"] = df["OutputValue"].map(output_state)
    df["_raw_traders"] = df["TradersSampleSize"]
    for field in ("MarketLatitude", "MarketLongitude"):
        if field in df:
            df["_raw_" + field] = df[field]
    findings: list[dict] = []
    countries = sorted({clean(v).casefold() for v in df["Adm0Name"] if clean(v)})
    codes = sorted({clean(v) for v in df.get("Adm0Code", []) if clean(v)})
    surveys = sorted({clean(v) for v in df.get("SurveyID", []) if clean(v)})
    if len(countries) > 1 or len(codes) > 1 or len(surveys) > 1:
        groups = countries if len(countries) > 1 else codes if len(codes) > 1 else surveys
        finding = InputFinding(code="mixed_assessments", severity="error", message="Separate countries and survey rounds before analysis.", fields=["Adm0Name", "Adm0Code", "SurveyID"], row_references=df["_source_row"].tolist()).model_dump()
        raise MFIInputError(f"MFI input contains multiple assessment identities: {groups}", [finding], groups)
    country = codes[0] if codes else countries[0] if countries else "unspecified"
    survey = surveys[0] if surveys else None
    keys, original_names = [], []
    for _, row in df.iterrows():
        name = clean(row["MarketName"])
        market_id = clean(row.get("MarketID"))
        identity = ["databridges", country, survey, market_id] if market_id and survey else ["legacy", country, survey, clean(row.get("Adm1Name")), clean(row.get("Adm2Name")), name]
        keys.append("mk_" + fingerprint(identity)[:24])
        original_names.append(name)
    df["_market_key"] = keys
    df["_source_name"] = original_names
    identities: dict[str, dict] = {}
    for key, group in df.groupby("_market_key", sort=True):
        names = sorted(set(group["_source_name"]), key=lambda x: (x.casefold(), x))
        source_id = clean(group.iloc[0].get("MarketID"))
        identities[key] = MarketIdentity(market_key=key, country_identity=country, survey_id=survey, source_market_id=source_id or None, source_market_name=names[0], aliases=names, display_label=names[0], identity_basis="source_ids" if source_id and survey else "legacy_geography_name").model_dump()
        if not source_id or not survey:
            findings.append(InputFinding(code="legacy_market_identity", severity="info", message="Market identity uses source geography and name; stable source identifiers were unavailable.", market_key=key, fields=["SurveyID", "MarketID"]).model_dump())
        for field in ("Adm1Name", "Adm2Name"):
            values = {clean(v) for v in group.get(field, []) if clean(v)}
            if len(values) > 1:
                raise MFIInputError(f"Conflicting {field} for market {names[0]}", [InputFinding(code="conflicting_market_geography", severity="error", message=f"Conflicting {field} within one market identity.", market_key=key, fields=[field], row_references=group["_source_row"].tolist()).model_dump()])
    name_counts: dict[str, int] = {}
    for item in identities.values():
        name_counts[item["display_label"]] = name_counts.get(item["display_label"], 0) + 1
    for key, item in identities.items():
        if name_counts[item["display_label"]] > 1:
            row = df[df["_market_key"] == key].iloc[0]
            geography = clean(row.get("Adm2Name")) or clean(row.get("Adm1Name"))
            item["display_label"] = f"{item['display_label']} [{geography}; {item['source_market_id'] or key}]"
    df["MarketName"] = df["_market_key"].map(lambda k: identities[k]["display_label"])
    intervals = df[[c for c in ("StartDate", "EndDate", "_source_row") if c in df]].to_dict("records")
    df["_source_rows"] = df["_source_row"].map(lambda x: [int(x)])
    # Deduplicate only source-identified identical semantic rows. Audit all lineage.
    df["_source_values"] = df["_raw_output"].map(lambda value: [None if value is None else str(value)])
    metadata_by_market = {key: market_metadata(group, key) for key, group in df.groupby("_market_key", sort=False)}
    df["_semantic_value"] = df["OutputValue"].map(lambda value: str(float(value)) if output_state(value) == "valid" else output_state(value) + ":" + clean(value))
    semantic = ["_market_key", "LevelID", "DimensionName", "VariableName", "_semantic_value"]
    if "MetricApplicability" in df:
        semantic.append("MetricApplicability")
    df["_source_rows"] = df["_source_row"].map(lambda x: [int(x)])
    identified = df["_market_key"].map(lambda k: identities[k]["identity_basis"] == "source_ids")
    if identified.any():
        with_ids = df[identified].copy()
        with_ids["_duplicate_group"] = with_ids.groupby(semantic, dropna=False, sort=False).ngroup()
        lineage = with_ids.groupby("_duplicate_group")["_source_row"].agg(list)
        source_values = with_ids.groupby("_duplicate_group")["_source_values"].agg(lambda groups: [value for group in groups for value in group])
        dedup = with_ids.drop_duplicates("_duplicate_group").copy()
        dedup["_source_rows"] = dedup["_duplicate_group"].map(lineage)
        dedup["_source_values"] = dedup["_duplicate_group"].map(source_values)
        dedup = dedup.drop(columns="_duplicate_group")
        df = pd.concat([dedup, df[~identified]], ignore_index=True)
    df.attrs.update(market_identities=identities, input_findings=findings, country_identity=country, survey_id=survey, market_metadata=metadata_by_market, source_intervals=intervals)
    return df


def market_metadata(group: pd.DataFrame, key: str) -> tuple[int | None, float | None, float | None, list[dict]]:
    if key in group.attrs.get("market_metadata", {}):
        return group.attrs["market_metadata"][key]
    findings: list[dict] = []
    def issue(code: str, field: str) -> None:
        findings.append(InputFinding(code=code, message=f"{field} is invalid or inconsistent; dependent outputs are unavailable.", market_key=key, fields=[field], row_references=sorted({int(r) for rows in group["_source_rows"] for r in rows})).model_dump())
    raw = group["_raw_traders"].tolist()
    present = [v for v in raw if clean(v)]
    valid = [float(v) for v in present if output_state(v) == "valid"]
    traders = None
    if present and len(valid) == len(present) and len(set(valid)) == 1 and valid[0] >= 0 and valid[0].is_integer():
        traders = int(valid[0])
    else:
        issue("invalid_trader_count" if present else "missing_trader_count", "TradersSampleSize")
    coords = []
    for field, bound in (("MarketLatitude", 90), ("MarketLongitude", 180)):
        values = [v for v in group.get("_raw_" + field, group.get(field, [])) if clean(v)]
        nums = [float(v) for v in values if output_state(v) == "valid"]
        value = None
        if values and len(nums) == len(values) and len(set(nums)) == 1 and abs(nums[0]) <= bound:
            value = nums[0]
        elif values:
            issue("invalid_coordinates", field)
        coords.append(value)
    if any(v is None for v in coords):
        coords = [None, None]
    return traders, coords[0], coords[1], findings

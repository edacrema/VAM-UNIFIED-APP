"""Baseline lock for the Phase R0 diagnostic regression.

The remediation phases must not change any authoritative value. This module records the
analytical identity of a diagnostic assessment once and compares every later run against
it, so a score, mean, or priority-set change fails automatically rather than relying on
someone noticing.

Identity values are stored as both ``repr()`` and ``float.hex()`` and compared bit-exactly
via ``float.fromhex``. No tolerance is applied: an authoritative score that differs in the
last bit is a regression, not a rounding artifact. The ``repr`` form exists purely so a
human can read the diff.

Structural measurements are recorded alongside the numeric identity but are advisory —
later phases are *expected* to change them, and the difference is that phase's evidence.

Assessment data is never committed. Inputs are resolved from the ignored
``MFI Test Databases`` directory, and snapshots are written under ``.tmp``.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .report_inspector import StructuralReport

BASELINE_SCHEMA_VERSION = "1.0"
DEFAULT_CASE_ID = "diagnostic"
DEFAULT_BASELINE_ROOT = Path(".tmp") / "mfi-r0"
BENCHMARK_DIRECTORY_NAME = "MFI Test Databases"
DIAGNOSTIC_CSV_GLOB = "*Gaza*.csv"

ENV_DIAGNOSTIC_CSV = "MFI_R0_DIAGNOSTIC_CSV"
ENV_ARTIFACT_DOCX = "MFI_R0_ARTIFACT_DOCX"
ENV_BASELINE_DIR = "MFI_R0_BASELINE_DIR"
ENV_UPDATE_BASELINE = "MFI_R0_UPDATE_BASELINE"
ENV_REQUIRE_DIAGNOSTIC = "MFI_R0_REQUIRE_DIAGNOSTIC"

REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class BaselineComparison:
    """Outcome of comparing a run against a recorded baseline."""

    source_changed: bool = False
    blocking_differences: Mapping[str, tuple[Any, Any]] = field(default_factory=dict)
    advisory_differences: Mapping[str, tuple[Any, Any]] = field(default_factory=dict)

    @property
    def is_clean(self) -> bool:
        return not self.source_changed and not self.blocking_differences

    def describe(self) -> str:
        lines: list[str] = []
        if self.source_changed:
            lines.append(
                "The diagnostic source file changed since the baseline was recorded. "
                f"Re-snapshot deliberately with {ENV_UPDATE_BASELINE}=1."
            )
        for field_name, (expected, actual) in sorted(self.blocking_differences.items()):
            lines.append(f"  {field_name}\n    expected: {expected!r}\n    actual:   {actual!r}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------


def benchmark_directory() -> Path:
    return REPO_ROOT / BENCHMARK_DIRECTORY_NAME


def resolve_diagnostic_csv() -> Optional[Path]:
    """Locate the diagnostic assessment, or return ``None`` when it is absent."""
    override = os.environ.get(ENV_DIAGNOSTIC_CSV)
    if override:
        candidate = Path(override)
        return candidate if candidate.is_file() else None
    matches = sorted(benchmark_directory().glob(DIAGNOSTIC_CSV_GLOB))
    return matches[0] if matches else None


def resolve_artifact_docx() -> Optional[Path]:
    """Locate the reference DOCX produced by a real run, or return ``None``."""
    override = os.environ.get(ENV_ARTIFACT_DOCX)
    if override:
        candidate = Path(override)
        return candidate if candidate.is_file() else None
    matches = sorted((REPO_ROOT / ".tmp").glob("mfi-review-*/*.docx"))
    return matches[0] if matches else None


def baseline_path(case_id: str = DEFAULT_CASE_ID) -> Path:
    """Return the snapshot path, enforcing that snapshots stay under ``.tmp``."""
    root = Path(os.environ.get(ENV_BASELINE_DIR) or (REPO_ROOT / DEFAULT_BASELINE_ROOT))
    resolved = root.resolve()
    tmp_root = (REPO_ROOT / ".tmp").resolve()
    if not str(resolved).startswith(str(tmp_root)):
        raise ValueError(
            f"R0 baselines must be written below {tmp_root}; got {resolved}."
        )
    return resolved / case_id / "baseline.json"


def update_requested() -> bool:
    return os.environ.get(ENV_UPDATE_BASELINE, "").strip().lower() in {"1", "true", "yes"}


def diagnostic_required() -> bool:
    return os.environ.get(ENV_REQUIRE_DIAGNOSTIC, "").strip().lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# Float identity
# ---------------------------------------------------------------------------


def encode_float(value: Any) -> Optional[dict[str, str]]:
    """Encode a float for bit-exact round-tripping plus human-readable diffing."""
    if value is None:
        return None
    number = float(value)
    return {"repr": repr(number), "hex": number.hex()}


def decode_float(payload: Any) -> Optional[float]:
    if not isinstance(payload, Mapping):
        return None
    hex_value = payload.get("hex")
    if isinstance(hex_value, str):
        return float.fromhex(hex_value)
    repr_value = payload.get("repr")
    return float(repr_value) if isinstance(repr_value, str) else None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Snapshot construction and comparison
# ---------------------------------------------------------------------------


def build_numeric_identity(
    loaded: Mapping[str, Any], profile: Mapping[str, Any]
) -> dict[str, Any]:
    """Capture every value the remediation must leave untouched."""
    markets: dict[str, Any] = {}
    for market in loaded.get("markets_data", []) or []:
        name = str(market.get("market_name"))
        markets[name] = {
            "overall": encode_float(market.get("overall_mfi")),
            "dimensions": {
                str(dimension): encode_float(value)
                for dimension, value in sorted(
                    (market.get("dimension_scores") or {}).items()
                )
            },
        }
    warnings = loaded.get("methodology_warnings") or []
    return {
        "included_market_count": len(loaded.get("markets_data", []) or []),
        "excluded_market_count": len(loaded.get("excluded_market_records", []) or []),
        "loader_warning_count": len(loaded.get("warnings", []) or []),
        "methodology_warning_codes": sorted(
            {
                str(
                    warning.get("code")
                    if isinstance(warning, Mapping)
                    else getattr(warning, "code", warning)
                )
                for warning in warnings
            }
        ),
        "priority_dimension_names": list(profile.get("priority_dimension_names") or []),
        "score_authority": str(loaded.get("score_authority") or ""),
        "mean_mfi_across_assessed_markets": encode_float(
            profile.get("mean_mfi_across_assessed_markets")
        ),
        "market_scores": dict(sorted(markets.items())),
    }


def build_baseline(
    *,
    loaded: Mapping[str, Any],
    profile: Mapping[str, Any],
    report: Optional[StructuralReport],
    source: Path,
    case_id: str = DEFAULT_CASE_ID,
    revision: str = "unknown",
) -> dict[str, Any]:
    return {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "case_id": case_id,
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "created_revision": revision,
        "source": {
            "name": source.name,
            "sha256": _sha256(source),
            "byte_count": source.stat().st_size,
        },
        "numeric": build_numeric_identity(loaded, profile),
        "structure": report.to_json_dict() if report is not None else {},
    }


def load_baseline(path: Path) -> Optional[dict[str, Any]]:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_baseline(baseline: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(baseline, indent=2, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _flatten_numeric(numeric: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten the numeric block so differences name an exact field."""
    flat: dict[str, Any] = {}
    for key, value in numeric.items():
        if key == "market_scores":
            for market, payload in (value or {}).items():
                flat[f"market_scores.{market}.overall"] = decode_float(payload.get("overall"))
                for dimension, encoded in (payload.get("dimensions") or {}).items():
                    flat[f"market_scores.{market}.{dimension}"] = decode_float(encoded)
            continue
        if isinstance(value, Mapping) and "hex" in value:
            flat[key] = decode_float(value)
            continue
        flat[key] = value
    return flat


def compare_baseline(
    baseline: Mapping[str, Any], current: Mapping[str, Any]
) -> BaselineComparison:
    """Compare a fresh snapshot against a recorded one."""
    source_changed = (
        str((baseline.get("source") or {}).get("sha256"))
        != str((current.get("source") or {}).get("sha256"))
    )

    expected = _flatten_numeric(baseline.get("numeric") or {})
    actual = _flatten_numeric(current.get("numeric") or {})
    blocking: dict[str, tuple[Any, Any]] = {}
    for key in sorted(set(expected) | set(actual)):
        before, after = expected.get(key), actual.get(key)
        if before != after:
            blocking[key] = (before, after)

    advisory: dict[str, tuple[Any, Any]] = {}
    before_structure = baseline.get("structure") or {}
    after_structure = current.get("structure") or {}
    for key in sorted(set(before_structure) | set(after_structure)):
        before, after = before_structure.get(key), after_structure.get(key)
        if before != after:
            advisory[key] = (before, after)

    return BaselineComparison(
        source_changed=source_changed,
        blocking_differences=blocking,
        advisory_differences=advisory,
    )


def assert_diagnostic_invariants(
    loaded: Mapping[str, Any],
    profile: Mapping[str, Any],
    *,
    expected_market_count: int,
    expected_mean_mfi: float,
    expected_priority_dimensions: Sequence[str],
) -> list[str]:
    """Return a list of invariant violations; empty means the baseline holds.

    These are the Section 3 invariants of the remediation specification, checked directly
    rather than through the snapshot, so that a first run on a machine with no recorded
    baseline still verifies something real.
    """
    problems: list[str] = []
    markets = loaded.get("markets_data") or []
    if len(markets) != expected_market_count:
        problems.append(
            f"included markets: expected {expected_market_count}, got {len(markets)}"
        )
    excluded = loaded.get("excluded_market_records") or []
    if excluded:
        problems.append(f"excluded records: expected 0, got {len(excluded)}")
    warnings = loaded.get("methodology_warnings") or []
    if warnings:
        problems.append(f"methodology warnings: expected 0, got {len(warnings)}")

    mean = profile.get("mean_mfi_across_assessed_markets")
    if mean is None or float(mean) != float(expected_mean_mfi):
        problems.append(f"mean MFI: expected {expected_mean_mfi!r}, got {mean!r}")

    priorities = list(profile.get("priority_dimension_names") or [])
    if priorities != list(expected_priority_dimensions):
        problems.append(
            f"priority dimensions: expected {list(expected_priority_dimensions)}, "
            f"got {priorities}"
        )
    return problems


def stored_scores_match_source(csv_path: Path, loaded: Mapping[str, Any]) -> list[str]:
    """Verify every parsed Level-1 value equals the stored value in the source file.

    Numeric parsing is the one place a remediation could silently alter an authoritative
    score, so the comparison is exact rather than tolerant.
    """
    import pandas as pd

    from .methodology import CSV_DIMENSION_TO_DISPLAY, OFFICIAL_SCORE_DEFINITIONS

    frame = pd.read_csv(csv_path, dtype=str)
    frame["OutputValue"] = pd.to_numeric(frame["OutputValue"], errors="coerce")
    level_one = frame[frame["LevelID"].astype(str).str.strip() == "1"]

    by_variable = {
        definition.variable_name: definition
        for definition in OFFICIAL_SCORE_DEFINITIONS
    }
    stored: dict[tuple[str, str], float] = {}
    for _, row in level_one.iterrows():
        definition = by_variable.get(str(row["VariableName"]).strip())
        if definition is None:
            continue
        dimension = CSV_DIMENSION_TO_DISPLAY.get(
            str(row["DimensionName"]).strip(), str(row["DimensionName"]).strip()
        )
        stored[(str(row["MarketName"]).strip(), dimension)] = float(row["OutputValue"])

    problems: list[str] = []
    for market in loaded.get("markets_data") or []:
        name = str(market.get("market_name")).strip()
        expected_overall = stored.get((name, "MFI"))
        if expected_overall is not None and float(market["overall_mfi"]) != expected_overall:
            problems.append(
                f"{name}/overall: source {expected_overall!r} != parsed "
                f"{market['overall_mfi']!r}"
            )
        for dimension, value in (market.get("dimension_scores") or {}).items():
            expected = stored.get((name, str(dimension)))
            if expected is not None and float(value) != expected:
                problems.append(
                    f"{name}/{dimension}: source {expected!r} != parsed {value!r}"
                )
    return problems

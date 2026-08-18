"""Structural inspection of MFI report blocks, DOCX exports, and analysis profiles.

This module is a pure measuring instrument for the Phase R0 regression harness. It
deliberately never calls an LLM, invokes a retriever, deploys anything, imports the
graph/LangChain stack, or mutates the artifacts it inspects. Generated evidence belongs
under the repository's ignored ``.tmp`` directory.

Import discipline is a contract, enforced by ``tests/test_mfi_r0_inspector.py``: runtime
imports are limited to the standard library, ``python-docx``, and the
``app.services.mfi_drafter`` modules ``methodology`` and ``wording``, both of which are
themselves cheap. Report blocks are duck-typed rather than imported so that inspecting a
DOCX never pays for the shared report-block package.

Three inspection modes exist because the defects under remediation are not all visible in
the same place:

``profile``
    Reads a deterministic assessment profile. Authoritative for evidence-availability
    limitations, which are decided in analysis long before anything is rendered.
``blocks`` / ``pipeline``
    Reads report blocks, optionally with captured chart titles. The only mode that can see
    chart coverage, because chart titles are rasterised into the figure images.
``docx``
    Reads an exported document. The only mode that can see artifacts of a real LLM run,
    such as Markdown leakage in narrative prose.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Sequence,
)

from docx import Document as _open_document
from docx.table import Table
from docx.text.paragraph import Paragraph

from . import wording
from .methodology import METRIC_DEFINITIONS_BY_ID

if TYPE_CHECKING:  # pragma: no cover - typing only, never imported at runtime
    from app.shared.report_blocks import ReportBlock


InspectionSource = Literal["blocks", "docx", "profile", "pipeline"]

# The exporter renders definition, limitation, methodology, and QA blocks as single-cell
# tables (``app/shared/docx_export.py``). They must be classified separately from data
# tables, otherwise they dominate table statistics and mask real width regressions.
NOTICE_BOX_LABELS: tuple[str, ...] = (
    "Definition:",
    "Data limitation:",
    "Methodology:",
    "QA warning:",
    "Claim warning:",
)


@dataclass(frozen=True)
class InspectorConfig:
    """Thresholds and patterns governing a structural inspection."""

    wide_table_column_threshold: int = 14
    repeated_phrase_min_count: int = 3
    repeated_phrase_min_words: int = 8
    top_repeated_phrases: int = 10
    coverage_note_pattern: str = (
        r"coverage:\s*(?P<available>\d+)\s*/\s*(?P<total>\d+)\s+assessed markets"
    )
    chart_coverage_pattern: str = (
        r"Coverage:\s*(?P<available>\d+)\s*/\s*(?P<total>\d+)\s+markets"
    )
    notice_box_labels: tuple[str, ...] = NOTICE_BOX_LABELS
    boilerplate_templates: tuple[str, ...] = (
        r"Use the cited (?P<slot>[^.]{1,60}?) evidence to target further market "
        r"assessment and proportionate operational follow-up\.",
    )
    claim_marker_patterns: tuple[str, ...] = (
        r"\[(?:unverified|verified|pending|do not use)[^\]]*\]",
        r"\((?:unverified|pending)\)",
        r"^\s*(?:Unverified|UNVERIFIED)\s*[—:-]",
    )
    qa_table_title_patterns: tuple[str, ...] = (
        r"\bQA findings?\b",
        r"\bunresolved (?:issues|findings)\b",
    )
    # Shared with the narrative validator so that what the report is measured for and what
    # drafting is rejected for are the same rule. If these diverged, the remediation's exit
    # gate would be either unsatisfiable or vacuous depending on the direction.
    modality_subject_pattern: str = wording.MODALITY_SUBJECT_PATTERN
    modality_verdict_pattern: str = wording.MODALITY_VERDICT_PATTERN
    modality_negation_pattern: str = wording.METHODOLOGICAL_NEGATION_PATTERN
    pooled_population_pattern: str = wording.POOLED_POPULATION_PATTERN


@dataclass(frozen=True)
class DocxText:
    """Text of a document, split by container so a zero is never ambiguous."""

    paragraphs: str = ""
    table_cells: str = ""

    @property
    def full(self) -> str:
        if self.paragraphs and self.table_cells:
            return f"{self.paragraphs}\n{self.table_cells}"
        return self.paragraphs or self.table_cells


@dataclass(frozen=True)
class TableShape:
    index: int
    kind: Literal["data", "notice", "unreadable"]
    title: Optional[str]
    declared_column_count: int
    grid_column_count: int
    header_cell_count: int
    effective_column_count: int
    row_count: int


@dataclass(frozen=True)
class HeadingRef:
    index: int
    level: int
    text: str


@dataclass(frozen=True)
class EmptySection:
    heading: HeadingRef
    next_heading: HeadingRef


@dataclass(frozen=True)
class ChartTitle:
    figure_id: Optional[str]
    title: str
    coverage_available: Optional[int] = None
    coverage_total: Optional[int] = None

    @property
    def has_coverage(self) -> bool:
        return self.coverage_available is not None and self.coverage_total is not None

    @property
    def is_zero_coverage(self) -> bool:
        return self.coverage_available == 0 and self.coverage_total == 0


@dataclass(frozen=True)
class RepeatedPhrase:
    phrase: str
    count: int
    template: Optional[str] = None


@dataclass(frozen=True)
class LimitationFinding:
    code: str
    dimension: Optional[str]
    message: str
    metric_ids: tuple[str, ...] = ()
    required_metric_count: int = 0
    optional_metric_count: int = 0
    unknown_metric_count: int = 0

    @property
    def is_optional_only(self) -> bool:
        return bool(self.metric_ids) and self.required_metric_count == 0


@dataclass(frozen=True)
class StructuralReport:
    """Every structural measurement the remediation phases are gated on."""

    source: InspectionSource

    # Text corpora sizes, so that a zero measurement can be distinguished from no input.
    paragraph_char_count: int = 0
    table_cell_char_count: int = 0

    # Markdown leakage (FIX-08).
    backtick_count: int = 0
    backtick_count_paragraphs: int = 0
    backtick_count_table_cells: int = 0
    code_fence_count: int = 0
    markdown_link_count: int = 0
    markdown_emphasis_count: int = 0
    markdown_heading_line_count: int = 0
    markdown_bullet_line_count: int = 0

    # Charts (FIX-04). Populated in live/pipeline mode only.
    chart_titles: tuple[ChartTitle, ...] = ()
    coverage_titled_chart_count: int = 0
    zero_coverage_chart_count: int = 0

    # Tables (FIX-05).
    tables: tuple[TableShape, ...] = ()
    data_table_count: int = 0
    notice_box_count: int = 0
    unreadable_table_count: int = 0
    max_table_column_count: int = 0
    max_table_row_count: int = 0
    wide_table_count: int = 0
    undeclared_column_table_count: int = 0

    # Sections (FIX-07).
    headings: tuple[HeadingRef, ...] = ()
    empty_sections: tuple[EmptySection, ...] = ()
    empty_section_titles: tuple[str, ...] = ()
    heading_level_jumps: tuple[EmptySection, ...] = ()

    # Claims and QA visibility (FIX-01).
    claim_paragraph_count: int = 0
    claim_status_counts: Mapping[str, int] = field(default_factory=dict)
    claim_status_marker_count: int = 0
    unverified_word_count: int = 0
    qa_warning_block_count: int = 0
    qa_table_row_count: int = 0

    # Narrative overreach (FIX-02, FIX-03).
    modality_conclusion_count: int = 0
    modality_conclusion_samples: tuple[str, ...] = ()
    pooled_population_phrase_count: int = 0
    pooled_population_samples: tuple[str, ...] = ()

    # Repetition (FIX-09, FIX-11).
    evidence_note_count: int = 0
    evidence_note_word_count: int = 0
    coverage_note_total: int = 0
    coverage_note_repetition: Mapping[str, int] = field(default_factory=dict)
    repeated_phrases: tuple[RepeatedPhrase, ...] = ()
    boilerplate_families: tuple[RepeatedPhrase, ...] = ()
    max_boilerplate_repetition: int = 0

    # Evidence availability (FIX-06). Populated in profile/pipeline mode only.
    limitations: tuple[LimitationFinding, ...] = ()
    limitation_code_counts: Mapping[str, int] = field(default_factory=dict)
    optional_only_limitation_count: int = 0
    optional_only_limitation_dimensions: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable view suitable for snapshots and diffing."""
        return {
            "source": self.source,
            "paragraph_char_count": self.paragraph_char_count,
            "table_cell_char_count": self.table_cell_char_count,
            "backtick_count": self.backtick_count,
            "backtick_count_paragraphs": self.backtick_count_paragraphs,
            "backtick_count_table_cells": self.backtick_count_table_cells,
            "code_fence_count": self.code_fence_count,
            "markdown_link_count": self.markdown_link_count,
            "markdown_emphasis_count": self.markdown_emphasis_count,
            "markdown_heading_line_count": self.markdown_heading_line_count,
            "markdown_bullet_line_count": self.markdown_bullet_line_count,
            "chart_title_count": len(self.chart_titles),
            "coverage_titled_chart_count": self.coverage_titled_chart_count,
            "zero_coverage_chart_count": self.zero_coverage_chart_count,
            "chart_titles": [
                {
                    "figure_id": item.figure_id,
                    "title": item.title,
                    "coverage_available": item.coverage_available,
                    "coverage_total": item.coverage_total,
                }
                for item in self.chart_titles
            ],
            "data_table_count": self.data_table_count,
            "notice_box_count": self.notice_box_count,
            "unreadable_table_count": self.unreadable_table_count,
            "max_table_column_count": self.max_table_column_count,
            "max_table_row_count": self.max_table_row_count,
            "wide_table_count": self.wide_table_count,
            "undeclared_column_table_count": self.undeclared_column_table_count,
            "heading_count": len(self.headings),
            "empty_section_titles": list(self.empty_section_titles),
            "heading_level_jump_count": len(self.heading_level_jumps),
            "claim_paragraph_count": self.claim_paragraph_count,
            "claim_status_counts": dict(self.claim_status_counts),
            "claim_status_marker_count": self.claim_status_marker_count,
            "unverified_word_count": self.unverified_word_count,
            "qa_warning_block_count": self.qa_warning_block_count,
            "qa_table_row_count": self.qa_table_row_count,
            "modality_conclusion_count": self.modality_conclusion_count,
            "modality_conclusion_samples": list(self.modality_conclusion_samples),
            "pooled_population_phrase_count": self.pooled_population_phrase_count,
            "pooled_population_samples": list(self.pooled_population_samples),
            "evidence_note_count": self.evidence_note_count,
            "evidence_note_word_count": self.evidence_note_word_count,
            "coverage_note_total": self.coverage_note_total,
            "coverage_note_repetition": dict(self.coverage_note_repetition),
            "max_boilerplate_repetition": self.max_boilerplate_repetition,
            "boilerplate_families": [
                {"phrase": item.phrase, "count": item.count, "template": item.template}
                for item in self.boilerplate_families
            ],
            "repeated_phrases": [
                {"phrase": item.phrase, "count": item.count}
                for item in self.repeated_phrases
            ],
            "limitation_code_counts": dict(self.limitation_code_counts),
            "optional_only_limitation_count": self.optional_only_limitation_count,
            "optional_only_limitation_dimensions": list(
                self.optional_only_limitation_dimensions
            ),
        }


# ---------------------------------------------------------------------------
# DOCX primitives
# ---------------------------------------------------------------------------


def iter_docx_body(document: Any) -> Iterator[tuple[Literal["paragraph", "table"], Any]]:
    """Yield paragraphs and tables in true document order.

    ``document.paragraphs`` and ``document.tables`` are separate flat lists, so neither
    preserves the interleaving. Document order is required to associate a table with the
    heading that introduces it and to detect a heading immediately followed by another.
    """
    body = document.element.body
    for child in body.iterchildren():
        tag = child.tag.rsplit("}", 1)[-1]
        if tag == "p":
            yield "paragraph", Paragraph(child, document)
        elif tag == "tbl":
            yield "table", Table(child, document)


def docx_heading_level(paragraph: Any) -> Optional[int]:
    """Return the heading level of a paragraph, or ``None`` when it is not a heading."""
    style = getattr(paragraph, "style", None)
    name = str(getattr(style, "name", "") or "")
    if name == "Title":
        return 1
    mfi_levels = {
        "MFI Title": 1,
        "MFI Major Heading": 2,
        "MFI Subsection Heading": 3,
        "MFI Minor Heading": 4,
    }
    if name in mfi_levels:
        return mfi_levels[name]
    match = re.fullmatch(r"Heading (\d+)", name)
    if match:
        return int(match.group(1))
    return None


def extract_docx_text(document: Any) -> DocxText:
    """Extract document text, keeping paragraph and table-cell corpora separate.

    The distinction is load-bearing: the exporter renders QA warnings inside single-cell
    tables, so a paragraphs-only extractor silently reports zero QA text.
    """
    paragraph_parts: list[str] = []
    cell_parts: list[str] = []
    for kind, item in iter_docx_body(document):
        if kind == "paragraph":
            paragraph_parts.append(item.text)
            continue
        try:
            for row in item.rows:
                for cell in row.cells:
                    cell_parts.append(cell.text)
        except Exception:  # pragma: no cover - defensive, counted by the caller
            continue
    return DocxText(
        paragraphs="\n".join(paragraph_parts),
        table_cells="\n".join(cell_parts),
    )


# ---------------------------------------------------------------------------
# Measurement helpers
# ---------------------------------------------------------------------------


def _count_markdown(text: str) -> dict[str, int]:
    lines = text.splitlines()
    return {
        "backtick_count": text.count("`"),
        "code_fence_count": len(re.findall(r"```", text)),
        "markdown_link_count": len(re.findall(r"\[[^\]\n]+\]\([^)\n]+\)", text)),
        "markdown_emphasis_count": len(re.findall(r"(?<!\*)\*\*(?!\s)[^*\n]+\*\*", text)),
        "markdown_heading_line_count": sum(
            1 for line in lines if re.match(r"^\s{0,3}#{1,6}\s+\S", line)
        ),
        "markdown_bullet_line_count": sum(
            1 for line in lines if re.match(r"^\s{0,3}[*+]\s+\S", line)
        ),
    }


def _parse_chart_title(
    title: str, figure_id: Optional[str], config: InspectorConfig
) -> ChartTitle:
    match = re.search(config.chart_coverage_pattern, title)
    if match is None:
        return ChartTitle(figure_id=figure_id, title=title)
    return ChartTitle(
        figure_id=figure_id,
        title=title,
        coverage_available=int(match.group("available")),
        coverage_total=int(match.group("total")),
    )


def _coverage_notes(text: str, config: InspectorConfig) -> tuple[int, dict[str, int]]:
    counter: Counter[str] = Counter()
    total = 0
    for match in re.finditer(config.coverage_note_pattern, text, flags=re.IGNORECASE):
        total += 1
        counter[f"{match.group('available')}/{match.group('total')}"] += 1
    return total, dict(counter)


def _boilerplate_families(
    text: str, config: InspectorConfig
) -> tuple[tuple[RepeatedPhrase, ...], int]:
    families: list[RepeatedPhrase] = []
    for template in config.boilerplate_templates:
        matches = re.findall(template, text)
        if not matches:
            continue
        count = len(matches)
        sample = re.search(template, text)
        families.append(
            RepeatedPhrase(
                phrase=sample.group(0) if sample else template,
                count=count,
                template=template,
            )
        )
    maximum = max((item.count for item in families), default=0)
    return tuple(families), maximum


def _repeated_phrases(
    sentences: Iterable[str], config: InspectorConfig
) -> tuple[RepeatedPhrase, ...]:
    counter: Counter[str] = Counter()
    for sentence in sentences:
        cleaned = " ".join(sentence.split())
        if len(cleaned.split()) < config.repeated_phrase_min_words:
            continue
        counter[cleaned] += 1
    repeated = [
        RepeatedPhrase(phrase=phrase, count=count)
        for phrase, count in counter.most_common(config.top_repeated_phrases)
        if count >= config.repeated_phrase_min_count
    ]
    return tuple(repeated)


def _split_sentences(text: str) -> list[str]:
    parts: list[str] = []
    for line in text.splitlines():
        for chunk in re.split(r"(?<=[.!?])\s+", line):
            chunk = chunk.strip()
            if chunk:
                parts.append(chunk)
    return parts


def _modality_conclusions(
    sentences: Sequence[str], config: InspectorConfig
) -> tuple[int, tuple[str, ...]]:
    """Find sentences that pass an operational verdict on a transfer modality.

    A sentence qualifies only when it names a modality and reaches a verdict about it.
    Sentences that explicitly deny such a conclusion — the methodology statement that MFI
    evidence does not determine modality — are excluded, since that wording is the
    remediation's own neutral replacement and must not be flagged as the defect.
    """
    matches: list[str] = []
    for sentence in sentences:
        if not re.search(config.modality_subject_pattern, sentence, flags=re.IGNORECASE):
            continue
        if not re.search(config.modality_verdict_pattern, sentence, flags=re.IGNORECASE):
            continue
        if re.search(config.modality_negation_pattern, sentence, flags=re.IGNORECASE):
            continue
        matches.append(sentence.strip())
    return len(matches), tuple(matches[:5])


def _pooled_population_phrases(
    text: str, config: InspectorConfig
) -> tuple[int, tuple[str, ...]]:
    """Find percentages attributed to a respondent population."""
    found = re.findall(config.pooled_population_pattern, text, flags=re.IGNORECASE)
    unique: list[str] = []
    for item in found:
        if item not in unique:
            unique.append(item)
    return len(found), tuple(unique[:5])


def _count_claim_markers(text: str, config: InspectorConfig) -> int:
    total = 0
    for pattern in config.claim_marker_patterns:
        total += len(
            re.findall(pattern, text, flags=re.MULTILINE | re.IGNORECASE)
        )
    return total


def _is_qa_findings_title(title: Optional[str], config: InspectorConfig) -> bool:
    """Return whether a table introduces itemised QA findings.

    A QA warning box states that findings exist; a findings table is what lets a reader
    locate them. Only the latter counts, so a notice box can never satisfy the measure.
    """
    if not title:
        return False
    return any(
        re.search(pattern, title, flags=re.IGNORECASE)
        for pattern in config.qa_table_title_patterns
    )


def _empty_sections(
    headings: Sequence[HeadingRef],
) -> tuple[tuple[EmptySection, ...], tuple[EmptySection, ...]]:
    """Return (empty sections, advisory level jumps).

    A section is empty when the very next body element is another heading of equal or
    higher rank. Requiring ``level <= own level`` is what separates a genuinely empty
    section from ordinary parent-to-child nesting, which is legitimate structure.
    """
    empty: list[EmptySection] = []
    jumps: list[EmptySection] = []
    for current, following in zip(headings, headings[1:]):
        if following.index != current.index + 1:
            continue
        if following.level <= current.level:
            empty.append(EmptySection(heading=current, next_heading=following))
        elif following.level - current.level >= 2:
            jumps.append(EmptySection(heading=current, next_heading=following))
    return tuple(empty), tuple(jumps)


def _classify_limitation_metrics(metric_ids: Sequence[str]) -> tuple[int, int, int]:
    required = optional = unknown = 0
    for metric_id in metric_ids:
        definition = METRIC_DEFINITIONS_BY_ID.get(str(metric_id))
        if definition is None:
            unknown += 1
        elif getattr(definition, "applicability_rule", "required") == "required":
            required += 1
        else:
            optional += 1
    return required, optional, unknown


def _limitation_findings(profile: Mapping[str, Any]) -> tuple[LimitationFinding, ...]:
    findings: list[LimitationFinding] = []
    for limitation in profile.get("limitations", []) or []:
        if not isinstance(limitation, Mapping):
            continue
        metric_ids = tuple(
            str(item) for item in (limitation.get("metric_ids") or []) if item
        )
        required, optional, unknown = _classify_limitation_metrics(metric_ids)
        findings.append(
            LimitationFinding(
                code=str(limitation.get("code") or ""),
                dimension=(
                    str(limitation["dimension"])
                    if limitation.get("dimension")
                    else None
                ),
                message=str(limitation.get("message") or ""),
                metric_ids=metric_ids,
                required_metric_count=required,
                optional_metric_count=optional,
                unknown_metric_count=unknown,
            )
        )
    return tuple(findings)


def _limitation_metrics(
    findings: Sequence[LimitationFinding],
) -> tuple[Mapping[str, int], int, tuple[str, ...]]:
    codes = Counter(item.code for item in findings)
    optional_only = [item for item in findings if item.is_optional_only]
    dimensions = tuple(
        sorted({item.dimension for item in optional_only if item.dimension})
    )
    return dict(codes), len(optional_only), dimensions


def _table_metrics(shapes: Sequence[TableShape], config: InspectorConfig) -> dict[str, Any]:
    data_tables = [shape for shape in shapes if shape.kind == "data"]
    return {
        "tables": tuple(shapes),
        "data_table_count": len(data_tables),
        "notice_box_count": sum(1 for shape in shapes if shape.kind == "notice"),
        "unreadable_table_count": sum(1 for shape in shapes if shape.kind == "unreadable"),
        "max_table_column_count": max(
            (shape.effective_column_count for shape in data_tables), default=0
        ),
        "max_table_row_count": max((shape.row_count for shape in data_tables), default=0),
        "wide_table_count": sum(
            1
            for shape in data_tables
            if shape.effective_column_count >= config.wide_table_column_threshold
        ),
        "undeclared_column_table_count": sum(
            1 for shape in data_tables if shape.declared_column_count == 0
        ),
    }


def _text_metrics(
    paragraph_text: str, cell_text: str, config: InspectorConfig
) -> dict[str, Any]:
    combined = DocxText(paragraphs=paragraph_text, table_cells=cell_text).full
    markdown = _count_markdown(combined)
    coverage_total, coverage_repetition = _coverage_notes(combined, config)
    families, max_boilerplate = _boilerplate_families(combined, config)
    sentences = _split_sentences(combined)
    modality_count, modality_samples = _modality_conclusions(sentences, config)
    pooled_count, pooled_samples = _pooled_population_phrases(combined, config)
    return {
        "modality_conclusion_count": modality_count,
        "modality_conclusion_samples": modality_samples,
        "pooled_population_phrase_count": pooled_count,
        "pooled_population_samples": pooled_samples,
        "paragraph_char_count": len(paragraph_text),
        "table_cell_char_count": len(cell_text),
        "backtick_count": markdown["backtick_count"],
        "backtick_count_paragraphs": paragraph_text.count("`"),
        "backtick_count_table_cells": cell_text.count("`"),
        "code_fence_count": markdown["code_fence_count"],
        "markdown_link_count": markdown["markdown_link_count"],
        "markdown_emphasis_count": markdown["markdown_emphasis_count"],
        "markdown_heading_line_count": markdown["markdown_heading_line_count"],
        "markdown_bullet_line_count": markdown["markdown_bullet_line_count"],
        "unverified_word_count": len(
            re.findall(r"\bunverified\b", combined, flags=re.IGNORECASE)
        ),
        "claim_status_marker_count": _count_claim_markers(combined, config),
        "coverage_note_total": coverage_total,
        "coverage_note_repetition": coverage_repetition,
        "boilerplate_families": families,
        "max_boilerplate_repetition": max_boilerplate,
        "repeated_phrases": _repeated_phrases(sentences, config),
    }


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def inspect_profile(
    profile: Mapping[str, Any], *, config: InspectorConfig = InspectorConfig()
) -> StructuralReport:
    """Measure evidence-availability limitations from a deterministic profile."""
    findings = _limitation_findings(profile)
    codes, optional_only, dimensions = _limitation_metrics(findings)
    return StructuralReport(
        source="profile",
        limitations=findings,
        limitation_code_counts=codes,
        optional_only_limitation_count=optional_only,
        optional_only_limitation_dimensions=dimensions,
    )


def inspect_report_blocks(
    blocks: Sequence[Any],
    *,
    chart_titles: Sequence[ChartTitle] = (),
    profile: Optional[Mapping[str, Any]] = None,
    config: InspectorConfig = InspectorConfig(),
) -> StructuralReport:
    """Measure a list of report blocks, optionally with captured chart titles."""
    paragraph_parts: list[str] = []
    cell_parts: list[str] = []
    headings: list[HeadingRef] = []
    shapes: list[TableShape] = []
    claim_status: Counter[str] = Counter()
    claim_paragraphs = 0
    evidence_notes = 0
    evidence_note_words = 0
    qa_warnings = 0
    qa_rows = 0

    for index, block in enumerate(blocks):
        block_type = str(getattr(block, "type", "") or "")
        text = str(getattr(block, "text", "") or "")
        meta = getattr(block, "meta", None) or {}
        if not isinstance(meta, Mapping):
            meta = {}

        if block_type == "heading":
            level = int(getattr(block, "level", None) or 1)
            headings.append(HeadingRef(index=index, level=level, text=text.strip()))
            paragraph_parts.append(text)
            continue

        if block_type == "paragraph":
            paragraph_parts.append(text)
            if meta.get("claim_id"):
                claim_paragraphs += 1
                claim_status[str(meta.get("validation_status") or "unset")] += 1
            continue

        if block_type == "evidence_note":
            evidence_notes += 1
            evidence_note_words += len(re.findall(r"\b[\w'-]+\b", text))
            paragraph_parts.append(text)
            continue

        if block_type in {"definition_box", "limitation_box", "methodology_note"}:
            cell_parts.append(text)
            continue

        if block_type == "qa_warning":
            qa_warnings += 1
            cell_parts.append(text)
            continue

        if block_type == "claim_warning":
            cell_parts.append(text)
            continue

        if block_type == "table":
            shape = _block_table_shape(index, meta, config)
            shapes.append(shape)
            if _is_qa_findings_title(shape.title, config):
                qa_rows += shape.row_count
            continue

        if text:
            paragraph_parts.append(text)

    paragraph_text = "\n".join(paragraph_parts)
    cell_text = "\n".join(cell_parts)
    empty, jumps = _empty_sections(headings)
    # Accept raw title strings and pre-built ChartTitle objects alike; a ChartTitle that
    # carries a title but no parsed coverage is re-parsed rather than trusted as absent.
    parsed_titles = tuple(
        item
        if isinstance(item, ChartTitle) and item.has_coverage
        else _parse_chart_title(
            item.title if isinstance(item, ChartTitle) else str(item),
            item.figure_id if isinstance(item, ChartTitle) else None,
            config,
        )
        for item in chart_titles
    )

    metrics: dict[str, Any] = {
        **_text_metrics(paragraph_text, cell_text, config),
        **_table_metrics(shapes, config),
        "source": "blocks",
        "chart_titles": parsed_titles,
        "coverage_titled_chart_count": sum(1 for item in parsed_titles if item.has_coverage),
        "zero_coverage_chart_count": sum(
            1 for item in parsed_titles if item.is_zero_coverage
        ),
        "headings": tuple(headings),
        "empty_sections": empty,
        "empty_section_titles": tuple(item.heading.text for item in empty),
        "heading_level_jumps": jumps,
        "claim_paragraph_count": claim_paragraphs,
        "claim_status_counts": dict(claim_status),
        "qa_warning_block_count": qa_warnings,
        "qa_table_row_count": qa_rows,
        "evidence_note_count": evidence_notes,
        "evidence_note_word_count": evidence_note_words,
    }
    report = StructuralReport(**metrics)
    if profile is not None:
        report = _attach_profile(report, profile)
    return report


def _block_table_shape(
    index: int, meta: Mapping[str, Any], config: InspectorConfig
) -> TableShape:
    rows = meta.get("rows")
    rows = rows if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)) else []
    declared = meta.get("columns")
    declared = (
        [str(item) for item in declared if str(item).strip()]
        if isinstance(declared, Sequence) and not isinstance(declared, (str, bytes))
        else []
    )
    keys: list[str] = []
    for row in rows:
        values = row.get("values") if isinstance(row, Mapping) else None
        if isinstance(values, Mapping):
            for key in values:
                if key not in keys:
                    keys.append(str(key))
    effective = len(declared) if declared else len(keys)
    return TableShape(
        index=index,
        kind="data",
        title=str(meta.get("title") or "") or None,
        declared_column_count=len(declared),
        grid_column_count=0,
        header_cell_count=effective,
        effective_column_count=effective,
        row_count=len(rows),
    )


def _attach_profile(
    report: StructuralReport, profile: Mapping[str, Any]
) -> StructuralReport:
    findings = _limitation_findings(profile)
    codes, optional_only, dimensions = _limitation_metrics(findings)
    return replace(
        report,
        limitations=findings,
        limitation_code_counts=codes,
        optional_only_limitation_count=optional_only,
        optional_only_limitation_dimensions=dimensions,
    )


def inspect_docx_bytes(
    data: bytes, *, config: InspectorConfig = InspectorConfig()
) -> StructuralReport:
    """Measure an exported DOCX supplied as bytes."""
    import io

    return _inspect_document(_open_document(io.BytesIO(data)), config=config)


def inspect_docx_path(
    path: Path | str, *, config: InspectorConfig = InspectorConfig()
) -> StructuralReport:
    """Measure an exported DOCX on disk."""
    return _inspect_document(_open_document(str(path)), config=config)


def _inspect_document(document: Any, *, config: InspectorConfig) -> StructuralReport:
    paragraph_parts: list[str] = []
    cell_parts: list[str] = []
    headings: list[HeadingRef] = []
    shapes: list[TableShape] = []
    qa_warnings = 0
    qa_rows = 0
    evidence_notes = 0
    evidence_note_words = 0
    body_index = 0
    last_heading: Optional[str] = None

    for kind, item in iter_docx_body(document):
        if kind == "paragraph":
            text = item.text
            level = docx_heading_level(item)
            if level is not None and text.strip():
                headings.append(
                    HeadingRef(index=body_index, level=level, text=text.strip())
                )
                last_heading = text.strip()
            elif text.strip().startswith("Evidence:"):
                evidence_notes += 1
                evidence_note_words += len(re.findall(r"\b[\w'-]+\b", text))
            paragraph_parts.append(text)
            body_index += 1
            continue

        shape, texts, is_qa_notice = _docx_table_shape(
            body_index, item, last_heading, config
        )
        shapes.append(shape)
        cell_parts.extend(texts)
        if is_qa_notice:
            qa_warnings += 1
        elif shape.kind == "data" and _is_qa_findings_title(shape.title, config):
            qa_rows += shape.row_count
        body_index += 1

    paragraph_text = "\n".join(paragraph_parts)
    cell_text = "\n".join(cell_parts)
    empty, jumps = _empty_sections(headings)

    metrics: dict[str, Any] = {
        **_text_metrics(paragraph_text, cell_text, config),
        **_table_metrics(shapes, config),
        "source": "docx",
        "headings": tuple(headings),
        "empty_sections": empty,
        "empty_section_titles": tuple(entry.heading.text for entry in empty),
        "heading_level_jumps": jumps,
        "qa_warning_block_count": qa_warnings,
        "qa_table_row_count": qa_rows,
        "evidence_note_count": evidence_notes,
        "evidence_note_word_count": evidence_note_words,
    }
    return StructuralReport(**metrics)


def _docx_table_shape(
    index: int,
    table: Any,
    heading: Optional[str],
    config: InspectorConfig,
) -> tuple[TableShape, list[str], bool]:
    """Measure one DOCX table.

    Column count is read from the raw ``tblGrid`` rather than ``len(table.columns)``,
    which is unreliable for irregular tables, and cross-checked against the header row.
    ``row.cells`` expands horizontal merges, so grid and header counts are recorded
    separately instead of being collapsed into a single number.
    """
    texts: list[str] = []
    try:
        grid = len(table._tbl.tblGrid.gridCol_lst)
        rows = list(table.rows)
        header_cells = list(rows[0].cells) if rows else []
        header_count = len(header_cells)
        for row in rows:
            for cell in row.cells:
                texts.append(cell.text)
    except Exception:
        return (
            TableShape(
                index=index,
                kind="unreadable",
                title=heading,
                declared_column_count=0,
                grid_column_count=0,
                header_cell_count=0,
                effective_column_count=0,
                row_count=0,
            ),
            texts,
            False,
        )

    first_text = texts[0].strip() if texts else ""
    is_notice = (
        grid == 1
        and len(rows) == 1
        and any(first_text.startswith(label) for label in config.notice_box_labels)
    )
    is_qa_notice = is_notice and first_text.startswith("QA warning:")
    kind: Literal["data", "notice", "unreadable"] = "notice" if is_notice else "data"
    effective = max(grid, header_count)
    # Header row is excluded from the data row count for data tables.
    row_count = len(rows) - 1 if kind == "data" and len(rows) > 1 else len(rows)
    return (
        TableShape(
            index=index,
            kind=kind,
            title=heading,
            # DOCX has no report-block metadata. A concrete header grid is therefore
            # the rendered declaration of its columns; block-mode inspection remains
            # responsible for detecting missing projection metadata before export.
            declared_column_count=header_count if kind == "data" else 0,
            grid_column_count=grid,
            header_cell_count=header_count,
            effective_column_count=effective,
            row_count=row_count,
        ),
        texts,
        is_qa_notice,
    )


def merge_reports(*reports: StructuralReport) -> StructuralReport:
    """Combine reports from different modes into one pipeline-level view.

    Later reports win for scalar fields they actually populated, so a blocks report
    carrying chart titles and limitations composes cleanly with a DOCX report carrying
    rendered text and true table geometry.
    """
    if not reports:
        raise ValueError("merge_reports requires at least one report")
    merged: dict[str, Any] = {}
    for report in reports:
        for key, value in vars(report).items():
            if key == "source":
                continue
            if _is_empty(value):
                continue
            merged[key] = value
    merged["source"] = "pipeline"
    return StructuralReport(**merged)


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value == 0
    if isinstance(value, (str, bytes, tuple, list, dict)):
        return len(value) == 0
    return False


def diff_reports(
    baseline: StructuralReport | Mapping[str, Any],
    current: StructuralReport | Mapping[str, Any],
    *,
    fields: Optional[Sequence[str]] = None,
) -> dict[str, tuple[Any, Any]]:
    """Return ``{field: (baseline, current)}`` for every differing measurement."""
    left = baseline.to_json_dict() if isinstance(baseline, StructuralReport) else dict(baseline)
    right = current.to_json_dict() if isinstance(current, StructuralReport) else dict(current)
    keys = list(fields) if fields else sorted(set(left) | set(right))
    differences: dict[str, tuple[Any, Any]] = {}
    for key in keys:
        before = left.get(key)
        after = right.get(key)
        if before != after:
            differences[key] = (before, after)
    return differences


# ---------------------------------------------------------------------------
# Command line interface
# ---------------------------------------------------------------------------


def _emit(report: StructuralReport, destination: Optional[str]) -> None:
    payload = json.dumps(report.to_json_dict(), indent=2, ensure_ascii=True, sort_keys=True)
    if destination:
        target = Path(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
        print(f"wrote {target}")
        return
    print(payload)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="report_inspector",
        description="Measure structural properties of MFI report artifacts.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    docx_parser = subparsers.add_parser("inspect-docx", help="inspect an exported DOCX")
    docx_parser.add_argument("--docx", required=True)
    docx_parser.add_argument("--json", dest="json_out")

    csv_parser = subparsers.add_parser(
        "inspect-csv", help="run the deterministic pipeline on a CSV and inspect it"
    )
    csv_parser.add_argument("--csv", required=True)
    csv_parser.add_argument("--json", dest="json_out")
    csv_parser.add_argument("--no-figures", action="store_true")

    diff_parser = subparsers.add_parser("diff", help="diff two inspection JSON files")
    diff_parser.add_argument("--baseline", required=True)
    diff_parser.add_argument("--current", required=True)

    args = parser.parse_args(argv)

    if args.command == "inspect-docx":
        _emit(inspect_docx_path(args.docx), args.json_out)
        return 0

    if args.command == "inspect-csv":
        # Imported lazily: the deterministic pipeline pulls in the graph module, which is
        # expensive and drags the LLM stack into the process.
        from .deterministic_report import run_deterministic_report_from_csv

        run = run_deterministic_report_from_csv(
            Path(args.csv), render_figures=not args.no_figures
        )
        report = merge_reports(
            inspect_report_blocks(
                run.blocks, chart_titles=run.chart_titles, profile=run.profile
            ),
            inspect_docx_bytes(run.docx),
        )
        _emit(report, args.json_out)
        return 0

    if args.command == "diff":
        left = json.loads(Path(args.baseline).read_text(encoding="utf-8"))
        right = json.loads(Path(args.current).read_text(encoding="utf-8"))
        differences = diff_reports(left, right)
        print(json.dumps(differences, indent=2, ensure_ascii=True, sort_keys=True))
        return 1 if differences else 0

    return 2


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main())

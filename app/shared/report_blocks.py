from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel


class ReportBlock(BaseModel):
    type: Literal["heading", "paragraph", "figure", "references"]
    text: Optional[str] = None
    level: Optional[int] = None
    figure_id: Optional[str] = None
    caption: Optional[str] = None
    alt_text: Optional[str] = None
    width: Optional[float] = None
    references: Optional[List[Dict[str, Any]]] = None


_MFI_DIMENSIONS = [
    "Assortment",
    "Availability",
    "Price",
    "Resilience",
    "Competition",
    "Infrastructure",
    "Service",
    "Food Quality",
    "Access & Protection",
]


_INSERT_FIGURE_RE = re.compile(r"\[INSERT GRAPH:\s*([A-Za-z0-9_\-]+)\s*\]", flags=re.IGNORECASE)


def _sanitize_text(text: str) -> str:
    text = text.replace("\r\n", "\n")
    text = text.replace("\r", "\n")
    text = text.replace("**", "")
    text = text.replace("__", "")
    return text.strip()


def _text_to_paragraph_blocks(text: str) -> List[ReportBlock]:
    cleaned = _sanitize_text(text)
    if not cleaned:
        return []

    blocks: List[ReportBlock] = []
    parts = re.split(r"\n\s*\n+", cleaned)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        blocks.append(ReportBlock(type="paragraph", text=part))
    return blocks


def _blocks_from_text_with_figures(text: str) -> List[ReportBlock]:
    cleaned = _sanitize_text(text)
    if not cleaned:
        return []

    blocks: List[ReportBlock] = []
    last = 0
    for m in _INSERT_FIGURE_RE.finditer(cleaned):
        before = cleaned[last : m.start()]
        blocks.extend(_text_to_paragraph_blocks(before))

        fig_id = m.group(1).strip()
        if fig_id:
            blocks.append(ReportBlock(type="figure", figure_id=fig_id))

        last = m.end()

    blocks.extend(_text_to_paragraph_blocks(cleaned[last:]))
    return blocks


def build_market_monitor_report_blocks(result: Dict[str, Any]) -> List[ReportBlock]:
    country = (result.get("country") or "").strip()
    time_period = (result.get("time_period") or "").strip()

    title = "Market Monitor"
    if country and time_period:
        title = f"Market Monitor - {country} - {time_period}"
    elif country:
        title = f"Market Monitor - {country}"

    sections = result.get("report_draft_sections") or result.get("report_sections") or {}
    module_sections = result.get("module_sections") or {}
    document_references = result.get("document_references") or []

    blocks: List[ReportBlock] = [ReportBlock(type="heading", text=title, level=1)]

    highlights = sections.get("HIGHLIGHTS")
    if isinstance(highlights, str) and highlights.strip():
        blocks.append(ReportBlock(type="heading", text="Highlights", level=2))
        blocks.extend(_text_to_paragraph_blocks(highlights))
        blocks.append(
            ReportBlock(
                type="figure",
                figure_id="food_basket_trend",
                caption="Food basket cost trend",
            )
        )

    overview = sections.get("MARKET_OVERVIEW")
    if isinstance(overview, str) and overview.strip():
        blocks.append(ReportBlock(type="heading", text="Market Overview", level=2))
        blocks.extend(_text_to_paragraph_blocks(overview))

    commodity = sections.get("COMMODITY_ANALYSIS")
    if isinstance(commodity, str) and commodity.strip():
        blocks.append(ReportBlock(type="heading", text="Commodity Analysis", level=2))
        blocks.extend(_blocks_from_text_with_figures(commodity))

    regional = sections.get("REGIONAL_HIGHLIGHTS")
    if isinstance(regional, str) and regional.strip():
        blocks.append(ReportBlock(type="heading", text="Regional Highlights", level=2))
        blocks.extend(_blocks_from_text_with_figures(regional))

    if isinstance(module_sections, dict):
        for module_id, section_text in module_sections.items():
            if not isinstance(section_text, str) or not section_text.strip():
                continue
            blocks.append(ReportBlock(type="heading", text=f"{module_id.upper()} Analysis", level=2))
            blocks.extend(_text_to_paragraph_blocks(section_text))

    if document_references:
        blocks.append(ReportBlock(type="references", references=document_references))

    return blocks


def build_mfi_report_blocks(result: Dict[str, Any]) -> List[ReportBlock]:
    country = (result.get("country") or "").strip()
    title = f"MFI Report - {country}" if country else "MFI Report"

    country_context = result.get("country_context")
    executive_summary = result.get("executive_summary")
    dimension_findings = result.get("dimension_findings") or {}
    market_recommendations = result.get("market_recommendations") or {}

    document_references = result.get("document_references") or []

    blocks: List[ReportBlock] = [ReportBlock(type="heading", text=title, level=1)]

    if isinstance(country_context, str) and country_context.strip():
        blocks.append(ReportBlock(type="heading", text="Context", level=2))
        blocks.extend(_text_to_paragraph_blocks(country_context))

    blocks.append(ReportBlock(type="figure", figure_id="mfi_radar", caption="MFI dimension scores"))

    blocks.append(
        ReportBlock(
            type="figure",
            figure_id="overview_table",
            caption="Market Functionality Index overview by market and dimension",
            width=7.0,
        )
    )

    if isinstance(executive_summary, str) and executive_summary.strip():
        blocks.append(ReportBlock(type="heading", text="Executive Summary", level=2))
        blocks.extend(_text_to_paragraph_blocks(executive_summary))

    blocks.append(
        ReportBlock(type="figure", figure_id="risk_distribution", caption="Market risk distribution")
    )

    blocks.append(
        ReportBlock(
            type="figure",
            figure_id="geographic_map",
            caption="MFI scores - geographic distribution",
            width=7.0,
        )
    )

    if isinstance(dimension_findings, dict) and dimension_findings:
        blocks.append(ReportBlock(type="heading", text="Dimension Findings", level=2))
        for dim in _MFI_DIMENSIONS:
            finding = dimension_findings.get(dim)
            if not isinstance(finding, dict):
                continue
            blocks.append(ReportBlock(type="heading", text=dim, level=3))

            safe_dim_name = dim.lower().replace(" ", "_").replace("&", "and")
            safe_dim_name = re.sub(r"[^a-z0-9_]+", "_", safe_dim_name).strip("_")
            blocks.append(
                ReportBlock(
                    type="figure",
                    figure_id=f"dim_{safe_dim_name}_bars",
                    caption=f"{dim} - score by market",
                )
            )

            key_findings = finding.get("key_findings")
            if isinstance(key_findings, str) and key_findings.strip():
                blocks.extend(_text_to_paragraph_blocks(f"Key findings\n{key_findings}"))

            score_interp = finding.get("score_interpretation")
            if isinstance(score_interp, str) and score_interp.strip():
                blocks.extend(_text_to_paragraph_blocks(f"Score interpretation\n{score_interp}"))

            recs = finding.get("recommendations")
            if isinstance(recs, str) and recs.strip():
                blocks.extend(_text_to_paragraph_blocks(f"Recommendations\n{recs}"))

    if isinstance(market_recommendations, dict) and market_recommendations:
        blocks.append(ReportBlock(type="heading", text="Recommendations by Market", level=2))

        items: List[tuple[str, Dict[str, Any]]] = []
        for market_name, payload in market_recommendations.items():
            if not isinstance(payload, dict):
                continue
            items.append((str(market_name), payload))

        items = sorted(items, key=lambda x: float(x[1].get("mfi_score", 0) or 0))

        for market_name, payload in items:
            region = str(payload.get("region", "") or "").strip()
            risk_level = str(payload.get("risk_level", "") or "").strip()
            try:
                mfi_score = float(payload.get("mfi_score", 0) or 0)
            except Exception:
                mfi_score = 0.0

            heading = market_name
            if region:
                heading = f"{heading} ({region})"
            if risk_level:
                heading = f"{heading} - {risk_level}"
            heading = f"{heading} (MFI: {mfi_score:.1f})"

            blocks.append(ReportBlock(type="heading", text=heading, level=3))

            priority_issues = payload.get("priority_issues") or []
            if isinstance(priority_issues, list) and priority_issues:
                issues_text = "\n".join([f"- {str(i).strip()}" for i in priority_issues if str(i).strip()])
                if issues_text.strip():
                    blocks.extend(_text_to_paragraph_blocks(f"Priority Issues\n{issues_text}"))

            interventions = payload.get("recommended_interventions") or []
            if isinstance(interventions, list) and interventions:
                int_text = "\n".join([f"- {str(i).strip()}" for i in interventions if str(i).strip()])
                if int_text.strip():
                    blocks.extend(_text_to_paragraph_blocks(f"Recommended Interventions\n{int_text}"))

            modality = payload.get("modality_considerations")
            if isinstance(modality, str) and modality.strip():
                blocks.extend(_text_to_paragraph_blocks(f"Modality Consideration\n{modality.strip()}"))

    if document_references:
        blocks.append(ReportBlock(type="references", references=document_references))

    return blocks

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel


class ReportBlock(BaseModel):
    type: Literal["heading", "paragraph", "figure", "references", "table", "definition_box"]
    text: Optional[str] = None
    level: Optional[int] = None
    figure_id: Optional[str] = None
    caption: Optional[str] = None
    alt_text: Optional[str] = None
    width: Optional[float] = None
    references: Optional[List[Dict[str, Any]]] = None
    meta: Optional[Dict[str, Any]] = None


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


_MFI_DIMENSION_DEFINITIONS: Dict[str, str] = {
    "Assortment": """The assortment of essential goods measures market breadth and depth.
It answers two key questions: (1) Can beneficiaries find all essential food and non-food items?
(2) Do they have a wide range of choices within each category?
Essential needs include cereals, pulses, oils, and basic NFIs. A high score indicates markets
can support diverse household needs; a low score suggests limited product variety.""",
    "Availability": """Availability measures consistent supply of essential commodities.
It answers: (1) Are essential goods consistently in stock? (2) How frequent are stockouts?
The dimension tracks scarcity reports and runout frequency across food and NFI categories.
High scores indicate reliable supply; low scores signal supply chain disruptions or
seasonal shortages requiring intervention.""",
    "Price": """Price stability measures affordability and predictability of essential goods.
It answers: (1) Have prices increased significantly? (2) Are prices stable over time?
This dimension tracks both price levels and volatility across commodity categories.
High scores indicate stable, accessible pricing; low scores suggest inflation pressures
or market manipulation affecting household purchasing power.""",
    "Resilience": """Resilience measures supply chain robustness and adaptive capacity.
It answers: (1) Can markets respond to demand shocks? (2) How vulnerable are supply networks?
The dimension evaluates node density, complexity, and criticality of supply chains.
High scores indicate robust, diversified supply networks; low scores suggest fragile
systems vulnerable to disruptions.""",
    "Competition": """Competition measures market structure and trader dynamics.
It answers: (1) Are there enough traders to ensure fair pricing? (2) Is there monopoly risk?
The dimension tracks market concentration and number of active competitors.
High scores indicate healthy competition; low scores suggest market power concentration
that may disadvantage consumers.""",
    "Infrastructure": """Infrastructure measures physical market conditions and facilities.
It answers: (1) What is the condition of market structures? (2) Are essential facilities available?
The dimension evaluates structural condition, sanitation, electricity, and water access.
High scores indicate well-maintained facilities; low scores suggest infrastructure
investments are needed.""",
    "Service": """Service quality measures the retail experience for consumers.
It answers: (1) How efficient is the checkout process? (2) Is the shopping experience positive?
The dimension tracks service speed, courtesy, and overall consumer satisfaction.
High scores indicate professional retail operations; low scores suggest service
improvements are needed.""",
    "Food Quality": """Food quality measures safety and handling standards.
It answers: (1) Are food items properly stored and handled? (2) Do products meet safety standards?
The dimension evaluates packaging integrity, storage conditions, and hygiene practices.
High scores indicate safe food handling; low scores suggest food safety risks
requiring monitoring.""",
    "Access & Protection": """Access and protection measures physical and social accessibility.
It answers: (1) Can all population groups access the market? (2) Are there safety concerns?
The dimension tracks geographic accessibility, operating hours, and protection issues.
High scores indicate inclusive, safe markets; low scores suggest access barriers
or protection concerns.""",
}


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
    markets_data = result.get("markets_data") or []

    document_references = result.get("document_references") or []

    blocks: List[ReportBlock] = [ReportBlock(type="heading", text=title, level=1)]

    if isinstance(country_context, str) and country_context.strip():
        context_text = country_context.strip()
        lc = context_text.lower()
        looks_like_disclaimer = (
            "cannot be extracted" in lc
            or "can not be extracted" in lc
            or "unable to extract" in lc
            or "unable to" in lc and "extract" in lc
            or "do not contain specific information" in lc
            or "does not contain specific information" in lc
            or ("do not contain" in lc and "specific information" in lc)
            or "not enough information" in lc
            or "insufficient information" in lc
        )
        if looks_like_disclaimer:
            context_text = ""

    if isinstance(country_context, str) and country_context.strip() and context_text:
        blocks.append(ReportBlock(type="heading", text="Context", level=2))
        blocks.extend(_text_to_paragraph_blocks(context_text))

    blocks.append(ReportBlock(type="figure", figure_id="mfi_radar", caption="MFI dimension scores"))

    blocks.append(
        ReportBlock(
            type="figure",
            figure_id="overview_table",
            caption="Market Functionality Index overview by market and dimension",
            width=7.0,
        )
    )

    if isinstance(markets_data, list) and markets_data:
        table_rows: List[Dict[str, Any]] = []
        for m in markets_data:
            if not isinstance(m, dict):
                continue
            dim_scores = m.get("dimension_scores")
            if not isinstance(dim_scores, dict):
                dim_scores = {}
            table_rows.append(
                {
                    "market_name": str(m.get("market_name", "") or "").strip(),
                    "region": str(m.get("region", m.get("admin1", "")) or "").strip(),
                    "overall_mfi": m.get("overall_mfi", 0),
                    "dimension_scores": dim_scores,
                }
            )

        blocks.append(ReportBlock(type="heading", text="Market Scores Table (Editable)", level=3))
        blocks.append(
            ReportBlock(
                type="table",
                meta={
                    "table_kind": "mfi_overview",
                    "dimensions": list(_MFI_DIMENSIONS),
                    "rows": table_rows,
                },
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

            definition = _MFI_DIMENSION_DEFINITIONS.get(dim)
            if isinstance(definition, str) and definition.strip():
                blocks.append(ReportBlock(type="definition_box", text=definition.strip()))

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

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel

from app.services.market_monitor.i18n import format_decimal_value, format_month_label, t


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


def _blocks_from_text_with_figures(
    text: str,
    *,
    visualizations: Optional[Dict[str, Any]] = None,
    figure_aliases: Optional[Dict[str, str]] = None,
) -> List[ReportBlock]:
    cleaned = _sanitize_text(text)
    if not cleaned:
        return []

    blocks: List[ReportBlock] = []
    last = 0
    for m in _INSERT_FIGURE_RE.finditer(cleaned):
        before = cleaned[last : m.start()]
        blocks.extend(_text_to_paragraph_blocks(before))

        fig_id = m.group(1).strip()
        if figure_aliases and fig_id in figure_aliases:
            fig_id = figure_aliases[fig_id]
        if fig_id and (visualizations is None or visualizations.get(fig_id)):
            blocks.append(ReportBlock(type="figure", figure_id=fig_id))

        last = m.end()

    blocks.extend(_text_to_paragraph_blocks(cleaned[last:]))
    return blocks


def _basket_snapshots_for_report(result: Dict[str, Any]) -> List[tuple[str, Dict[str, Any]]]:
    baskets = result.get("food_baskets") or {}
    primary = baskets.get("primary") if isinstance(baskets, dict) else None
    if not isinstance(primary, dict) or not primary:
        legacy = result.get("food_basket")
        primary = legacy if isinstance(legacy, dict) and legacy else None
    selected: List[tuple[str, Dict[str, Any]]] = []
    if isinstance(primary, dict) and primary:
        selected.append(("primary", dict(primary)))
    secondary = baskets.get("secondary") if isinstance(baskets, dict) else None
    if bool(result.get("secondary_basket_included")) and isinstance(secondary, dict) and secondary:
        selected.append(("secondary", dict(secondary)))
    return selected


def _basket_component_payload(item: Dict[str, Any], index: int) -> Dict[str, Any]:
    name = str(
        item.get("commodity_name_snapshot")
        or item.get("commodity_name")
        or item.get("name")
        or ""
    ).strip()
    quantity = item.get("weight_quantity")
    try:
        quantity_value = float(quantity)
    except (TypeError, ValueError):
        quantity_value = None
    return {
        "commodity_id": item.get("commodity_id"),
        "commodity_name": name,
        "unit_id": item.get("databridges_unit_id") or item.get("unit_id"),
        "unit": str(item.get("databridges_unit") or item.get("unit") or "").strip(),
        "quantity": quantity_value,
        "note": item.get("item_note") or item.get("note"),
        "sort_order": item.get("sort_order") or index,
    }


def _basket_component_label(component: Dict[str, Any], language: str) -> str:
    quantity = component.get("quantity")
    if quantity is None:
        quantity_label = ""
    elif language == "en":
        quantity_label = f"{float(quantity):g}"
    else:
        quantity_label = format_decimal_value(quantity, language, decimals=2).rstrip("0").rstrip(",.")
    parts = [quantity_label, str(component.get("unit") or "").strip(), str(component.get("commodity_name") or "").strip()]
    return " ".join(part for part in parts if part)


def _basket_definition_table_meta(result: Dict[str, Any], language: str) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for role, snapshot in _basket_snapshots_for_report(result):
        raw_items = [item for item in snapshot.get("items") or [] if isinstance(item, dict)]
        components = [_basket_component_payload(item, index) for index, item in enumerate(raw_items, start=1)]
        components.sort(key=lambda item: (int(item.get("sort_order") or 0), int(item.get("commodity_id") or 0)))
        regions = [str(region).strip() for region in snapshot.get("regions") or [] if str(region).strip()]
        scope_type = str(snapshot.get("scope_type") or "national").strip().lower() or "national"
        if scope_type == "national":
            scope_label = t(language, "basket.scope.national")
        elif regions:
            scope_label = t(language, "basket.scope.selected_regions_named", regions=", ".join(regions))
        else:
            scope_label = t(language, "basket.scope.selected_regions")
        name = str(snapshot.get("basket_name") or ("MEB" if role == "primary" else "")).strip()
        role_label = t(language, f"basket.role.{role}")
        rows.append(
            {
                "basket_role": role,
                "role_label": role_label,
                "basket_name": name,
                "short_description": str(snapshot.get("short_description") or "").strip(),
                "scope_type": scope_type,
                "scope_label": scope_label,
                "regions": regions,
                "components": components,
                "basket_version_id": snapshot.get("basket_version_id"),
                "version_number": snapshot.get("version_number"),
                "display": {
                    "basket": f"{name} ({role_label})",
                    "description": str(snapshot.get("short_description") or "").strip(),
                    "scope": scope_label,
                    "composition": "; ".join(
                        label for label in (_basket_component_label(item, language) for item in components) if label
                    ),
                },
            }
        )
    return {
        "table_kind": "basket_definitions",
        "language": language,
        "columns": ["basket", "description", "scope", "composition"],
        "headers": {
            "basket": t(language, "basket.table.header.basket"),
            "description": t(language, "basket.table.header.description"),
            "scope": t(language, "basket.table.header.scope"),
            "composition": t(language, "basket.table.header.composition"),
        },
        "rows": rows,
    }


def basket_definition_table_display(meta: Dict[str, Any]) -> tuple[List[str], List[List[str]]]:
    columns = [str(item) for item in meta.get("columns") or []]
    headers_by_key = meta.get("headers") or {}
    headers = [str(headers_by_key.get(key) or key) for key in columns]
    display_rows: List[List[str]] = []
    for row in meta.get("rows") or []:
        if not isinstance(row, dict):
            continue
        display = row.get("display") or {}
        display_rows.append([str(display.get(key) or "") for key in columns])
    return headers, display_rows


def _basket_row(meta: Dict[str, Any], role: str) -> Optional[Dict[str, Any]]:
    return next(
        (
            row
            for row in meta.get("rows") or []
            if isinstance(row, dict) and row.get("basket_role") == role
        ),
        None,
    )


def _basket_figure_caption(
    meta: Dict[str, Any],
    role: str,
    kind: str,
    *,
    language: str,
    time_period: str,
) -> str:
    row = _basket_row(meta, role)
    if row is None and role == "primary" and kind == "trend":
        return t(language, "figure.food_basket_trend")
    if row is None:
        return ""
    key = "figure.basket_trend_role" if kind == "trend" else "figure.basket_regional_role"
    kwargs = {
        "basket": row.get("basket_name") or ("MEB" if role == "primary" else ""),
        "scope": row.get("scope_label") or "",
        "period": format_month_label(time_period, language),
    }
    return t(language, key, **kwargs)


def _available_basket_figure(
    visualizations: Dict[str, Any],
    canonical_id: str,
    legacy_id: Optional[str] = None,
) -> Optional[str]:
    if visualizations.get(canonical_id):
        return canonical_id
    if legacy_id and visualizations.get(legacy_id):
        return legacy_id
    return None


def _module_section_title(module_id: Any, language: str = "en") -> str:
    module_key = str(module_id or "").strip()
    known_titles = {
        "exchange_rate": "module.exchange_rate",
        "fuel_energy": "module.fuel_energy",
        "livestock_animal_products": "module.livestock_animal_products",
        "labour_market": "module.labour_market",
    }
    if module_key in known_titles:
        return t(language, known_titles[module_key])
    words = [word for word in re.split(r"[_\-\s]+", module_key) if word]
    base = " ".join(word.capitalize() for word in words) if words else "Module"
    if base.lower().endswith(" analysis"):
        return base
    if language == "en":
        return f"{base} Analysis"
    return base


def build_market_monitor_report_blocks(result: Dict[str, Any]) -> List[ReportBlock]:
    country = (result.get("country") or "").strip()
    time_period = (result.get("time_period") or "").strip()
    language = str(result.get("language") or "en").strip().lower() or "en"
    display_period = time_period if language == "en" else format_month_label(time_period, language)

    title = t(language, "report.title")
    if country and display_period:
        title = f"{title} - {country} - {display_period}"
    elif country:
        title = f"{title} - {country}"

    sections = result.get("report_draft_sections") or result.get("report_sections") or {}
    module_sections = result.get("module_sections") or {}
    document_references = result.get("document_references") or []
    visualizations = result.get("visualizations") or {}
    visualizations = visualizations if isinstance(visualizations, dict) else {}
    basket_meta = _basket_definition_table_meta(result, language)

    blocks: List[ReportBlock] = [ReportBlock(type="heading", text=title, level=1)]

    highlights = sections.get("HIGHLIGHTS")
    if isinstance(highlights, str) and highlights.strip():
        blocks.append(ReportBlock(type="heading", text=t(language, "section.HIGHLIGHTS"), level=2))
        blocks.extend(_text_to_paragraph_blocks(highlights))

    if basket_meta.get("rows"):
        blocks.append(
            ReportBlock(
                type="heading",
                text=t(language, "section.BASKET_DEFINITIONS"),
                level=2,
            )
        )
        blocks.append(ReportBlock(type="table", meta=basket_meta))

    primary_trend = _available_basket_figure(
        visualizations,
        "food_basket_trend_primary",
        "food_basket_trend",
    )
    if primary_trend:
        blocks.append(
            ReportBlock(
                type="figure",
                figure_id=primary_trend,
                caption=_basket_figure_caption(
                    basket_meta,
                    "primary",
                    "trend",
                    language=language,
                    time_period=time_period,
                ),
            )
        )
    secondary_trend = _available_basket_figure(
        visualizations,
        "food_basket_trend_secondary",
    )
    if secondary_trend and bool(result.get("secondary_basket_included")):
        blocks.append(
            ReportBlock(
                type="figure",
                figure_id=secondary_trend,
                caption=_basket_figure_caption(
                    basket_meta,
                    "secondary",
                    "trend",
                    language=language,
                    time_period=time_period,
                ),
            )
        )

    overview = sections.get("MARKET_OVERVIEW")
    if isinstance(overview, str) and overview.strip():
        blocks.append(ReportBlock(type="heading", text=t(language, "section.MARKET_OVERVIEW"), level=2))
        blocks.extend(_text_to_paragraph_blocks(overview))

    commodity = sections.get("COMMODITY_ANALYSIS")
    if isinstance(commodity, str) and commodity.strip():
        blocks.append(ReportBlock(type="heading", text=t(language, "section.COMMODITY_ANALYSIS"), level=2))
        blocks.extend(_blocks_from_text_with_figures(commodity, visualizations=visualizations))

        has_inline_commodity_figs = False
        for m in _INSERT_FIGURE_RE.finditer(_sanitize_text(commodity)):
            fig_id = (m.group(1) or "").strip()
            if fig_id.lower().startswith("commodity_trends"):
                has_inline_commodity_figs = True
                break

        if not has_inline_commodity_figs and isinstance(visualizations, dict) and visualizations:
            ids = [
                k
                for k in visualizations.keys()
                if isinstance(k, str) and k.startswith("commodity_trends_")
            ]

            if ids:
                cat_order = [
                    "cereals",
                    "pulses",
                    "oil",
                    "sugar",
                    "condiments",
                    "vegetables",
                    "livestock",
                    "other",
                ]

                def sort_key(fig_id: str) -> tuple:
                    m = re.match(r"^commodity_trends_([a-z0-9_]+)_p(\d+)$", fig_id)
                    if not m:
                        return (99, fig_id, 0)
                    cat = m.group(1)
                    try:
                        page = int(m.group(2))
                    except Exception:
                        page = 0
                    try:
                        cat_rank = cat_order.index(cat)
                    except ValueError:
                        cat_rank = 50
                    return (cat_rank, cat, page)

                for fig_id in sorted(ids, key=sort_key):
                    blocks.append(ReportBlock(type="figure", figure_id=fig_id))

    regional = sections.get("REGIONAL_HIGHLIGHTS")
    primary_regional = _available_basket_figure(
        visualizations,
        "regional_comparison_primary",
        "regional_comparison",
    )
    secondary_regional = _available_basket_figure(
        visualizations,
        "regional_comparison_secondary",
    )
    has_regional_text = isinstance(regional, str) and bool(regional.strip())
    if has_regional_text or primary_regional or (secondary_regional and bool(result.get("secondary_basket_included"))):
        blocks.append(ReportBlock(type="heading", text=t(language, "section.REGIONAL_HIGHLIGHTS"), level=2))
        regional_blocks = (
            _blocks_from_text_with_figures(
                regional,
                visualizations=visualizations,
                figure_aliases={
                    "regional_comparison": primary_regional or "regional_comparison",
                },
            )
            if has_regional_text
            else []
        )
        blocks.extend(regional_blocks)
        inserted_ids = {
            block.figure_id
            for block in regional_blocks
            if block.type == "figure" and block.figure_id
        }
        if primary_regional and primary_regional not in inserted_ids:
            blocks.append(
                ReportBlock(
                    type="figure",
                    figure_id=primary_regional,
                    caption=_basket_figure_caption(
                        basket_meta,
                        "primary",
                        "regional",
                        language=language,
                        time_period=time_period,
                    ),
                )
            )
        if (
            secondary_regional
            and bool(result.get("secondary_basket_included"))
            and secondary_regional not in inserted_ids
        ):
            blocks.append(
                ReportBlock(
                    type="figure",
                    figure_id=secondary_regional,
                    caption=_basket_figure_caption(
                        basket_meta,
                        "secondary",
                        "regional",
                        language=language,
                        time_period=time_period,
                    ),
                )
            )

    if isinstance(module_sections, dict):
        for module_id, section_text in module_sections.items():
            if not isinstance(section_text, str) or not section_text.strip():
                continue
            blocks.append(ReportBlock(type="heading", text=_module_section_title(module_id, language), level=2))
            blocks.extend(_text_to_paragraph_blocks(section_text))
            if module_id == "fuel_energy" and isinstance(visualizations, dict) and visualizations.get("fuel_prices"):
                blocks.append(
                    ReportBlock(
                        type="figure",
                        figure_id="fuel_prices",
                        caption=t(language, "figure.fuel_prices"),
                    )
                )
            if (
                module_id == "livestock_animal_products"
                and isinstance(visualizations, dict)
                and visualizations.get("livestock_animal_products")
            ):
                blocks.append(
                    ReportBlock(
                        type="figure",
                        figure_id="livestock_animal_products",
                        caption=t(language, "figure.livestock_animal_products"),
                    )
                )
            if module_id == "labour_market" and isinstance(visualizations, dict) and visualizations.get("labour_market"):
                blocks.append(
                    ReportBlock(
                        type="figure",
                        figure_id="labour_market",
                        caption=t(language, "figure.labour_market"),
                    )
                )

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

from __future__ import annotations

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel

from app.services.market_monitor.i18n import format_decimal_value, format_month_label, t
from app.services.mfi_drafter.methodology import (
    DIMENSION_DESCRIPTIONS,
    METHODOLOGY_VERSION,
)
from app.services.mfi_drafter.evidence_notes import compose_evidence_note
from app.services.mfi_drafter.table_projection import (
    build_mfi_presentation_table,
    build_mfi_qa_presentation_table,
)
from app.services.mfi_drafter.wording import NEUTRAL_SCOPE_STATEMENT


class ReportBlock(BaseModel):
    type: Literal[
        "heading",
        "paragraph",
        "figure",
        "references",
        "table",
        "definition_box",
        "evidence_note",
        "limitation_box",
        "methodology_note",
        "qa_warning",
        "claim_warning",
    ]
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


_MFI_DIMENSION_DEFINITIONS: Dict[str, str] = dict(DIMENSION_DESCRIPTIONS)


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


_MFI_QA_SEVERITY_ORDER = {"high": 0, "medium": 1, "low": 2}
_MFI_QA_ARTIFACT_ORDER = {
    "context": 0,
    "executive_summary": 1,
    "dimension": 2,
    "market": 3,
    "global": 4,
}


def _mfi_material_qa_flags(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    qa_review = result.get("qa_review") or {}
    unique: Dict[str, Dict[str, Any]] = {}
    for index, flag in enumerate(qa_review.get("flags", []) or []):
        if not isinstance(flag, dict) or flag.get("severity") not in {"high", "medium"}:
            continue
        flag_id = str(flag.get("flag_id") or f"qa-flag-{index}")
        unique[flag_id] = {**flag, "flag_id": flag_id}
    return sorted(
        unique.values(),
        key=lambda flag: (
            _MFI_QA_SEVERITY_ORDER.get(str(flag.get("severity")), 9),
            _MFI_QA_ARTIFACT_ORDER.get(str(flag.get("artifact_type")), 9),
            str(flag.get("artifact_id") or "").casefold(),
            str(flag.get("field_name") or "").casefold(),
            str(flag.get("claim_id") or "").casefold(),
            str(flag.get("code") or "").casefold(),
            str(flag.get("flag_id") or ""),
        ),
    )


def _mfi_qa_context(result: Dict[str, Any]) -> Dict[str, Any]:
    qa_review = result.get("qa_review") or {}
    diagnostics = result.get("generation_diagnostics") or {}
    substitutions = result.get("claim_substitutions") or diagnostics.get(
        "claim_substitutions", []
    )
    return {
        "flags": _mfi_material_qa_flags(result),
        "history": [
            dict(record)
            for record in qa_review.get("correction_history", []) or []
            if isinstance(record, dict)
        ],
        "substitutions": {
            str(record.get("claim_id")): dict(record)
            for record in substitutions or []
            if isinstance(record, dict) and record.get("claim_id")
        },
        "rendered_flag_ids": set(),
        "marker_count_by_flag": {},
    }


def _mfi_flags_for_claim(
    qa_context: Dict[str, Any], claim_id: str
) -> List[Dict[str, Any]]:
    return [
        flag
        for flag in qa_context.get("flags", [])
        if str(flag.get("claim_id") or "") == claim_id
        or (
            str(flag.get("artifact_type") or "") == "context"
            and str(flag.get("artifact_id") or "") == claim_id
        )
    ]


def _mfi_history_for_flag(
    qa_context: Dict[str, Any], flag: Dict[str, Any]
) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for record in qa_context.get("history", []):
        record_codes = {str(value) for value in record.get("flag_codes", []) if value}
        if record_codes and str(flag.get("code") or "") not in record_codes:
            continue
        if flag.get("claim_id"):
            if str(record.get("claim_id") or "") != str(flag.get("claim_id")):
                continue
        else:
            if str(record.get("artifact_type") or "") != str(
                flag.get("artifact_type") or ""
            ):
                continue
            if str(record.get("artifact_id") or "") != str(
                flag.get("artifact_id") or ""
            ):
                continue
            if record.get("field_name") is not None and str(
                record.get("field_name") or ""
            ) != str(flag.get("field_name") or ""):
                continue
        matches.append(record)
    return sorted(matches, key=lambda item: int(item.get("attempt_number") or 0))


def _mfi_claim_warning(
    claim: Dict[str, Any], qa_context: Optional[Dict[str, Any]]
) -> Optional[ReportBlock]:
    if not qa_context:
        return None
    claim_id = str(claim.get("claim_id") or "").strip()
    if not claim_id:
        return None
    flags = _mfi_flags_for_claim(qa_context, claim_id)
    if not flags:
        return None
    severity = min(
        (str(flag.get("severity") or "medium") for flag in flags),
        key=lambda value: _MFI_QA_SEVERITY_ORDER.get(value, 9),
    )
    flag_ids = sorted({str(flag.get("flag_id")) for flag in flags})
    codes = sorted({str(flag.get("code")) for flag in flags if flag.get("code")})
    messages = list(
        dict.fromkeys(
            _mfi_safe_qa_text(flag.get("message"), qa_context)
            for flag in flags
            if flag.get("message")
        )
    )
    history = {
        (
            int(record.get("attempt_number") or 0),
            str(record.get("execution_outcome") or "pending"),
            str(record.get("validation_outcome") or "pending"),
        ): record
        for flag in flags
        for record in _mfi_history_for_flag(qa_context, flag)
    }
    attempted = [
        record
        for record in history.values()
        if record.get("execution_outcome") not in {"pending", "not_executed"}
    ]
    substitution = qa_context.get("substitutions", {}).get(claim_id)
    if substitution:
        disposition = "replaced_by_deterministic_fallback"
        lead = "[DO NOT USE ORIGINAL] Original draft withdrawn; deterministic fallback shown."
    elif severity == "medium":
        disposition = "retained_unverified_for_delivery"
        lead = "[UNVERIFIED] Unverified — review required."
    else:
        disposition = "unresolved_delivery_issue"
        lead = "[DO NOT USE] Unresolved delivery issue."
    outcomes = sorted(
        {
            f"{record.get('execution_outcome')} / {record.get('validation_outcome')}"
            for record in history.values()
        }
    )
    text = (
        f"{lead} Claim ID: {claim_id}. Severity: {severity.upper()}. "
        f"QA codes: {', '.join(codes) or 'not recorded'}. "
        f"Reason: {' | '.join(messages) or 'Material QA validation issue.'} "
        f"Repair attempted: {'yes' if attempted else 'no'}; attempts: {len(attempted)}; "
        f"outcome: {', '.join(outcomes) or 'not attempted'}."
    )
    for flag_id in flag_ids:
        qa_context["rendered_flag_ids"].add(flag_id)
        counts = qa_context["marker_count_by_flag"]
        counts[flag_id] = int(counts.get(flag_id, 0)) + 1
    primary = flags[0]
    return ReportBlock(
        type="claim_warning",
        text=text,
        meta={
            "claim_id": claim_id,
            "severity": severity,
            "flag_ids": flag_ids,
            "flag_codes": codes,
            "messages": messages,
            "artifact_type": primary.get("artifact_type"),
            "artifact_id": primary.get("artifact_id"),
            "field_name": primary.get("field_name"),
            "repair_attempted": bool(attempted),
            "attempt_count": len(attempted),
            "execution_outcomes": outcomes,
            "disposition": disposition,
        },
    )


def _append_mfi_claim(
    blocks: List[ReportBlock],
    claim: Any,
    *,
    catalog: Dict[str, Any],
    documents: Dict[str, Dict[str, Any]],
    qa_context: Optional[Dict[str, Any]] = None,
) -> None:
    if not isinstance(claim, dict) or not str(claim.get("text") or "").strip():
        return
    blocks.append(
        ReportBlock(
            type="paragraph",
            text=str(claim["text"]).strip(),
            meta={
                "claim_id": claim.get("claim_id"),
                "validation_status": claim.get("validation_status"),
                "substituted": bool(claim.get("substituted")),
                "metric_ids": list(claim.get("metric_ids", []) or []),
                "document_ids": list(claim.get("document_ids", []) or []),
            },
        )
    )
    note = compose_evidence_note(claim, catalog, documents)
    if note:
        blocks.append(
            ReportBlock(
                type="evidence_note",
                text=note,
                meta={
                    "claim_id": claim.get("claim_id"),
                    "metric_ids": list(claim.get("metric_ids", []) or []),
                    "document_ids": list(claim.get("document_ids", []) or []),
                },
            )
        )
    warning = _mfi_claim_warning(claim, qa_context)
    if warning is not None:
        blocks.append(warning)


def _mfi_table_block(
    title: str,
    profile: Dict[str, Any],
    *,
    spec_id: str,
    dimension: Optional[str] = None,
) -> Optional[ReportBlock]:
    meta = build_mfi_presentation_table(
        profile,
        spec_id=spec_id,
        title=title,
        dimension=dimension,
    )
    if not meta.get("rows"):
        return None
    return ReportBlock(type="table", meta=meta)


def _mfi_safe_qa_text(text: Any, qa_context: Dict[str, Any]) -> str:
    cleaned = str(text or "")
    for substitution in qa_context.get("substitutions", {}).values():
        rejected = str(substitution.get("rejected_text") or "").strip()
        if rejected:
            cleaned = cleaned.replace(rejected, "[withdrawn draft omitted]")
    return cleaned


def _mfi_flag_audit(
    flag: Dict[str, Any], qa_context: Dict[str, Any]
) -> Dict[str, Any]:
    history = _mfi_history_for_flag(qa_context, flag)
    attempted = [
        record
        for record in history
        if record.get("execution_outcome") not in {"pending", "not_executed"}
    ]
    outcomes = list(
        dict.fromkeys(
            f"{record.get('execution_outcome')} / {record.get('validation_outcome')}"
            for record in history
        )
    )
    claim_key = str(
        flag.get("claim_id")
        or (
            flag.get("artifact_id")
            if flag.get("artifact_type") == "context"
            else ""
        )
        or ""
    )
    if claim_key in qa_context.get("substitutions", {}):
        disposition = "replaced_by_deterministic_fallback"
    elif str(flag.get("flag_id")) in qa_context.get("rendered_flag_ids", set()):
        disposition = (
            "retained_unverified_for_delivery"
            if flag.get("severity") == "medium"
            else "unresolved_delivery_issue"
        )
    else:
        disposition = "global_unresolved"
    return {
        "attempted": attempted,
        "attempt_count": len(attempted),
        "outcome": ", ".join(outcomes) or "not attempted",
        "disposition": disposition,
    }


def _mfi_qa_findings_table(
    qa_context: Dict[str, Any]
) -> Optional[ReportBlock]:
    flags = qa_context.get("flags", [])
    if not flags:
        return None
    rows = []
    for flag in flags:
        audit = _mfi_flag_audit(flag, qa_context)
        raw_values = {
            "severity": str(flag.get("severity") or "").upper(),
            "source": flag.get("source") or "",
            "artifact": flag.get("artifact_type") or "",
            "location": flag.get("artifact_id") or "",
            "field": flag.get("field_name") or "",
            "claim_id": flag.get("claim_id") or "",
            "code": flag.get("code") or "",
            "message": _mfi_safe_qa_text(flag.get("message"), qa_context),
            "attempts": audit["attempt_count"],
            "outcome": audit["outcome"],
            "disposition": audit["disposition"],
        }
        rows.append(
            {
                "row_id": str(flag.get("flag_id")),
                "values": {
                    "severity": raw_values["severity"],
                    "source": raw_values["source"],
                    "artifact_location": " / ".join(
                        value
                        for value in (
                            str(raw_values["artifact"]),
                            str(raw_values["location"]),
                        )
                        if value
                    ),
                    "field": raw_values["field"],
                    "claim_code": " / ".join(
                        value
                        for value in (
                            str(raw_values["claim_id"]),
                            str(raw_values["code"]),
                        )
                        if value
                    ),
                    "message": raw_values["message"],
                    "attempts_outcome": (
                        f"{raw_values['attempts']} / {raw_values['outcome']}"
                    ),
                    "disposition": raw_values["disposition"],
                },
                "raw_values": raw_values,
            }
        )
    return ReportBlock(
        type="table",
        meta=build_mfi_qa_presentation_table(
            title="QA findings",
            rows=rows,
            qa_flag_ids=[str(flag.get("flag_id")) for flag in flags],
        ),
    )


def _assert_mfi_qa_traceability(
    blocks: List[ReportBlock], qa_context: Dict[str, Any]
) -> None:
    """Fail closed on a renderer contract that would hide a material finding."""
    material_ids = {
        str(flag.get("flag_id")) for flag in qa_context.get("flags", [])
    }
    marker_counts = qa_context.get("marker_count_by_flag", {})
    global_ids = {
        str(flag_id)
        for block in blocks
        if block.type == "qa_warning" and isinstance(block.meta, dict)
        for flag_id in block.meta.get("global_flag_ids", []) or []
    }
    table_ids = [
        str(row.get("row_id"))
        for block in blocks
        if block.type == "table"
        and isinstance(block.meta, dict)
        and block.meta.get("title") == "QA findings"
        for row in block.meta.get("rows", []) or []
        if isinstance(row, dict)
    ]
    if len(table_ids) != len(set(table_ids)) or set(table_ids) != material_ids:
        raise ValueError("Every material QA flag must appear exactly once in QA findings")
    for flag_id in material_ids:
        marker_count = int(marker_counts.get(flag_id, 0))
        global_count = 1 if flag_id in global_ids else 0
        if marker_count + global_count != 1:
            raise ValueError(
                f"Material QA flag {flag_id} must map to one visible marker or global notice"
            )
    visible_parts: List[str] = []
    for block in blocks:
        if block.text:
            visible_parts.append(block.text)
        if block.type == "table" and isinstance(block.meta, dict):
            for row in block.meta.get("rows", []) or []:
                if isinstance(row, dict):
                    visible_parts.extend(
                        str(value) for value in (row.get("values") or {}).values()
                    )
    visible = "\n".join(visible_parts)
    for substitution in qa_context.get("substitutions", {}).values():
        rejected = str(substitution.get("rejected_text") or "").strip()
        if rejected and rejected in visible:
            raise ValueError("Withdrawn narrative text reached reader-facing report blocks")


_MFI_CONTEXT_DISCLOSURES = {
    "no_results": (
        "No contextual documents were retrieved for the selected country and period."
    ),
    "retrieval_failed": (
        "Context retrieval was unavailable; interpretation relies only on the MFI "
        "assessment."
    ),
    "classification_failed": (
        "Context classification was unavailable after documents were retrieved; "
        "interpretation relies only on the MFI assessment."
    ),
    "no_accepted_statements": (
        "Documents were retrieved, but none met the evidence-classification "
        "requirements."
    ),
    "not_attempted": (
        "Context retrieval was not run for this report; interpretation relies only on "
        "the MFI assessment."
    ),
}


def _mfi_context_statement_is_accepted(
    statement: Dict[str, Any], documents: Dict[str, Dict[str, Any]]
) -> bool:
    if statement.get("classification") == "unrelated" or statement.get("substituted"):
        return False
    return any(
        str(document_id) in documents
        for document_id in statement.get("document_ids", []) or []
    )


def _mfi_context_limitation_text(code: str) -> str:
    return {
        "context_retrieval_unavailable": (
            "Context limitation: contextual-document retrieval was unavailable for all "
            "configured sources."
        ),
        "context_partial_retrieval_unavailable": (
            "Context limitation: one or more contextual-document sources were "
            "unavailable; available cited evidence is retained."
        ),
        "context_classification_unavailable": (
            "Context limitation: retrieved documents could not be classified against "
            "the context-evidence contract."
        ),
    }.get(code, "Context limitation: contextual evidence was unavailable.")


def build_mfi_report_blocks(result: Dict[str, Any]) -> List[ReportBlock]:
    """Build the Phase 3 report solely from canonical profile and narratives."""
    country = str(result.get("country") or "").strip()
    title = f"MFI Report - {country}" if country else "MFI Report"
    profile = result.get("assessment_profile") or {}
    catalog = result.get("claim_catalog") or {}
    context_evidence = result.get("context_evidence") or []
    dimension_narratives = result.get("dimension_narratives") or {}
    market_narratives = result.get("market_narratives") or {}
    executive = result.get("executive_summary_narrative") or {}
    context_status = result.get("context_status") or {}
    if hasattr(context_status, "model_dump"):
        context_status = context_status.model_dump()
    references = result.get("document_references") or []
    documents = {
        str(item.get("doc_id")): item
        for item in [
            *(result.get("contextual_documents") or []),
            *references,
        ]
        if isinstance(item, dict) and item.get("doc_id")
    }
    visualizations = result.get("visualizations") or {}
    qa_context = _mfi_qa_context(result)

    blocks: List[ReportBlock] = [
        ReportBlock(type="heading", text=title, level=1),
        ReportBlock(type="heading", text="Assessment metadata and coverage", level=2),
    ]
    collection_period = (
        f"{result.get('data_collection_start', '')} to "
        f"{result.get('data_collection_end', '')}"
    ).strip()
    blocks.append(
        ReportBlock(
            type="paragraph",
            text=f"Collection period: {collection_period}.",
        )
    )
    _append_mfi_claim(
        blocks,
        {
            "claim_id": "report.assessment.coverage",
            "text": "Included assessed-market count and score coverage are reported below.",
            "metric_ids": [
                "assessment.mfi.denominator",
                "assessment.mfi.coverage",
            ],
            "document_ids": [],
            "validation_status": "verified",
        },
        catalog=catalog,
        documents=documents,
        qa_context=qa_context,
    )
    methodology_warnings = result.get("methodology_warnings") or []
    excluded = result.get("excluded_market_records") or []
    if methodology_warnings or excluded:
        blocks.append(
            ReportBlock(
                type="limitation_box",
                text=(
                    f"Methodology warnings: {len(methodology_warnings)}. "
                    f"Excluded records: {len(excluded)}."
                ),
                meta={
                    "methodology_warnings": methodology_warnings,
                    "excluded_market_records": excluded,
                },
            )
        )

    blocks.append(ReportBlock(type="heading", text="Context and sources", level=2))
    accepted_context = [
        statement
        for statement in context_evidence
        if isinstance(statement, dict)
        and _mfi_context_statement_is_accepted(statement, documents)
    ]
    visible_context = [
        statement
        for statement in context_evidence
        if isinstance(statement, dict)
        and (
            statement in accepted_context
            or (
                statement.get("classification") != "unrelated"
                and bool(statement.get("substituted"))
            )
        )
    ]
    context_state = str(context_status.get("status") or "").strip()
    if context_state == "available" and not accepted_context:
        context_state = "no_accepted_statements" if documents else "no_results"
    elif context_state not in {"available", *_MFI_CONTEXT_DISCLOSURES}:
        context_state = "available" if accepted_context else "not_attempted"
    disclosure = _MFI_CONTEXT_DISCLOSURES.get(context_state)
    if disclosure:
        blocks.append(
            ReportBlock(
                type="paragraph",
                text=disclosure,
                meta={"context_status": context_state},
            )
        )
    limitation_code = str(context_status.get("limitation_code") or "").strip()
    if limitation_code:
        blocks.append(
            ReportBlock(
                type="limitation_box",
                text=_mfi_context_limitation_text(limitation_code),
                meta={"code": limitation_code},
            )
        )
    for statement in visible_context:
        if isinstance(statement, dict):
            _append_mfi_claim(
                blocks,
                {
                    "claim_id": statement.get("statement_id"),
                    "text": statement.get("text"),
                    "metric_ids": [],
                    "document_ids": statement.get("document_ids", []),
                    "validation_status": statement.get("validation_status"),
                    "substituted": statement.get("substituted", False),
                    "scope": "context",
                },
                catalog=catalog,
                documents=documents,
                qa_context=qa_context,
            )
    cited_document_ids = {
        str(document_id)
        for statement in accepted_context
        for document_id in statement.get("document_ids", []) or []
    }
    cited_references = [
        reference
        for reference in references
        if isinstance(reference, dict)
        and str(reference.get("doc_id") or "") in cited_document_ids
    ]
    if cited_references:
        blocks.append(ReportBlock(type="references", references=cited_references))

    blocks.append(
        ReportBlock(type="heading", text="Assessed-market MFI profile", level=2)
    )
    mean_entry = catalog.get("assessment.mfi.mean") or {}
    _append_mfi_claim(
        blocks,
        {
            "claim_id": "report.assessment.mean",
            "text": (
                "Mean MFI across assessed markets: "
                f"{mean_entry.get('formatted_value', 'not available')}."
            ),
            "metric_ids": ["assessment.mfi.mean"],
            "document_ids": [],
            "validation_status": "verified",
        },
        catalog=catalog,
        documents=documents,
        qa_context=qa_context,
    )
    if visualizations.get("mfi_radar"):
        blocks.append(
            ReportBlock(
                type="figure",
                figure_id="mfi_radar",
                caption="Average MFI dimension profile across assessed markets",
            )
        )
    if visualizations.get("market_score_ranking"):
        blocks.append(
            ReportBlock(
                type="figure",
                figure_id="market_score_ranking",
                caption="Ordered assessed-market MFI scores; selected markets highlighted",
            )
        )
    if visualizations.get("overview_table"):
        blocks.append(
            ReportBlock(
                type="figure",
                figure_id="overview_table",
                caption="Continuous 0-10 dimension profile by assessed market",
                width=7.0,
            )
        )
    for title_text, spec_id in (
        ("Dimension summary", "mfi.dimension_summary.v1"),
        ("Regional dimension summary", "mfi.regional_summary.v1"),
    ):
        table = _mfi_table_block(title_text, profile, spec_id=spec_id)
        if table:
            blocks.append(table)
    if visualizations.get("geographic_map"):
        blocks.append(
            ReportBlock(
                type="figure",
                figure_id="geographic_map",
                caption=(
                    "Stored assessed-market MFI scores by location; numbered callouts "
                    "identify selected review markets"
                ),
                width=7.0,
            )
        )

    blocks.append(ReportBlock(type="heading", text="Executive summary", level=2))
    _append_mfi_claim(
        blocks,
        executive.get("motivation"),
        catalog=catalog,
        documents=documents,
        qa_context=qa_context,
    )


    # Stated once, where the reader meets the report, rather than repeated per market.
    _append_mfi_claim(
        blocks,
        executive.get("scope_statement"),
        catalog=catalog,
        documents=documents,
        qa_context=qa_context,
    )
    for field in ("key_findings", "recommendations", "limitations"):
        if executive.get(field):
            blocks.append(
                ReportBlock(type="heading", text=field.replace("_", " ").title(), level=3)
            )
        for claim in executive.get(field, []) or []:
            _append_mfi_claim(
                blocks,
                claim,
                catalog=catalog,
                documents=documents,
                qa_context=qa_context,
            )

    blocks.append(ReportBlock(type="heading", text="MFI dimensions", level=2))
    for dimension in _MFI_DIMENSIONS:
        narrative = dimension_narratives.get(dimension)
        if not isinstance(narrative, dict):
            continue
        blocks.append(ReportBlock(type="heading", text=dimension, level=3))
        definition = _MFI_DIMENSION_DEFINITIONS.get(dimension)
        if definition:
            blocks.append(ReportBlock(type="definition_box", text=definition))
        safe_name = re.sub(
            r"[^a-z0-9_]+",
            "_",
            dimension.lower().replace(" ", "_").replace("&", "and"),
        ).strip("_")
        figure_id = f"dim_{safe_name}_bars"
        if visualizations.get(figure_id):
            blocks.append(
                ReportBlock(
                    type="figure",
                    figure_id=figure_id,
                    caption=f"{dimension} by assessed market with regional means",
                )
            )
        _append_mfi_claim(
            blocks,
            narrative.get("summary"),
            catalog=catalog,
            documents=documents,
            qa_context=qa_context,
        )
        for field in (
            "key_findings",
            "geographic_patterns",
            "data_limitations",
            "recommendations",
        ):
            for claim in narrative.get(field, []) or []:
                _append_mfi_claim(
                    blocks,
                    claim,
                    catalog=catalog,
                    documents=documents,
                    qa_context=qa_context,
                )

    blocks.append(
        ReportBlock(type="heading", text="Expanded priority-dimension evidence", level=2)
    )
    priority_dimensions = profile.get("priority_dimension_names", []) or []
    for dimension in priority_dimensions:
        narrative = dimension_narratives.get(dimension) or {}
        blocks.append(
            ReportBlock(
                type="heading",
                text=f"{dimension} expanded evidence",
                level=3,
            )
        )
        for subdimension in narrative.get("subdimension_analysis", []) or []:
            if not isinstance(subdimension, dict):
                continue
            blocks.append(
                ReportBlock(
                    type="heading",
                    text=str(subdimension.get("name") or "Evidence"),
                    level=4,
                )
            )
            _append_mfi_claim(
                blocks,
                subdimension.get("interpretation"),
                catalog=catalog,
                documents=documents,
                qa_context=qa_context,
            )
        for spec_id, label in (
            ("mfi.official_subsection.v1", "Official subsection evidence"),
            ("mfi.ranked_driver.v1", "Ranked explanatory evidence"),
            ("mfi.relevant_item.v1", "Relevant item evidence"),
        ):
            if dimension == "Food Quality" and spec_id == "mfi.official_subsection.v1":
                continue
            table = _mfi_table_block(
                f"{dimension}: {label}",
                profile,
                spec_id=spec_id,
                dimension=str(dimension),
            )
            if table:
                blocks.append(table)
        safe_name = re.sub(
            r"[^a-z0-9_]+",
            "_",
            str(dimension).lower().replace(" ", "_").replace("&", "and"),
        ).strip("_")
        for suffix, caption in (
            ("subsections", "Official subsection evidence"),
            ("drivers", "Ranked explanatory evidence"),
            ("items", "Relevant item evidence"),
        ):
            figure_id = f"priority_{safe_name}_{suffix}"
            if visualizations.get(figure_id):
                blocks.append(
                    ReportBlock(
                        type="figure",
                        figure_id=figure_id,
                        caption=f"{dimension}: {caption}",
                    )
                )

    blocks.append(
        ReportBlock(
            type="heading",
            text="Lowest-scoring assessed markets selected for review",
            level=2,
        )
    )
    priority_market_table = _mfi_table_block(
        "Selected assessed markets",
        profile,
        spec_id="mfi.priority_market.v1",
    )
    if priority_market_table:
        blocks.append(priority_market_table)
    market_order = {
        str(item.get("market_name")): int(item.get("selection_order", 10**9))
        for item in profile.get("markets", []) or []
        if isinstance(item, dict)
    }
    for market_name, narrative in sorted(
        market_narratives.items(),
        key=lambda item: (market_order.get(str(item[0]), 10**9), str(item[0]).casefold()),
    ):
        if not isinstance(narrative, dict):
            continue
        heading = str(market_name)
        if narrative.get("region"):
            heading += f" ({narrative['region']})"
        blocks.append(ReportBlock(type="heading", text=heading, level=3))
        for field in ("priority_issues", "recommended_interventions"):
            for claim in narrative.get(field, []) or []:
                _append_mfi_claim(
                    blocks,
                    claim,
                    catalog=catalog,
                    documents=documents,
                    qa_context=qa_context,
                )

    blocks.append(
        ReportBlock(
            type="heading",
            text="Methodology, limitations, and QA notices",
            level=2,
        )
    )
    blocks.append(
        ReportBlock(
            type="methodology_note",
            text=(
                f"Methodology version: {result.get('methodology_version') or METHODOLOGY_VERSION}. "
                f"Score authority: {result.get('score_authority', '')}. "
                "Stored DataBridge Level-1 scores remain authoritative. "
                f"{NEUTRAL_SCOPE_STATEMENT} "
                "Complete unprojected analytical tables remain available as JSON and "
                "CSV files in the report's Technical details downloads."
            ),
        )
    )
    for limitation in profile.get("limitations", []) or []:
        if isinstance(limitation, dict) and limitation.get("message"):
            blocks.append(
                ReportBlock(
                    type="limitation_box",
                    text=str(limitation["message"]),
                    meta={"code": limitation.get("code"), "metric_ids": limitation.get("metric_ids", [])},
                )
            )
    qa_review = result.get("qa_review") or {}
    material_flags = qa_context.get("flags", [])
    if material_flags:
        blocks.append(
            ReportBlock(type="heading", text="Final QA findings", level=3)
        )
        all_flags = [
            flag
            for flag in qa_review.get("flags", []) or []
            if isinstance(flag, dict)
        ]
        severity_counts = {
            severity: sum(flag.get("severity") == severity for flag in all_flags)
            for severity in ("high", "medium", "low")
        }
        rendered_ids = qa_context.get("rendered_flag_ids", set())
        global_flags = [
            flag
            for flag in material_flags
            if str(flag.get("flag_id")) not in rendered_ids
        ]
        substitutions = qa_context.get("substitutions", {})
        summary_parts = [
            "Narrative QA completed with unresolved material issues.",
            (
                f"High: {severity_counts['high']}; medium: "
                f"{severity_counts['medium']}; low: {severity_counts['low']}; "
                f"correction cycles: {int(qa_review.get('correction_attempts') or 0)}."
            ),
        ]
        if substitutions:
            summary_parts.append(
                "Original drafts withdrawn by the delivery policy are identified "
                "beside their deterministic replacements."
            )
        if rendered_ids:
            summary_parts.append(
                "Retained material claims carry visible claim-level review notices."
            )
        if global_flags:
            summary_parts.append(
                "Process-level or unmapped issues are listed below and in the QA table."
            )
        blocks.append(
            ReportBlock(
                type="qa_warning",
                text=" ".join(summary_parts),
                meta={
                    "qa_status": qa_review.get("status"),
                    "severity_counts": severity_counts,
                    "correction_attempts": int(
                        qa_review.get("correction_attempts") or 0
                    ),
                },
            )
        )
        for flag in global_flags:
            audit = _mfi_flag_audit(flag, qa_context)
            blocks.append(
                ReportBlock(
                    type="qa_warning",
                    text=(
                        f"Global QA issue — {str(flag.get('severity') or '').upper()} "
                        f"[{flag.get('code') or 'uncoded'}]: "
                        f"{_mfi_safe_qa_text(flag.get('message'), qa_context)} "
                        f"Disposition: {audit['disposition']}; attempts: "
                        f"{audit['attempt_count']}; outcome: {audit['outcome']}."
                    ),
                    meta={
                        "global_flag_ids": [str(flag.get("flag_id"))],
                        "severity": flag.get("severity"),
                        "code": flag.get("code"),
                        "artifact_type": flag.get("artifact_type"),
                        "artifact_id": flag.get("artifact_id"),
                        "field_name": flag.get("field_name"),
                        "claim_id": flag.get("claim_id"),
                        "disposition": audit["disposition"],
                    },
                )
            )
        findings_table = _mfi_qa_findings_table(qa_context)
        if findings_table is not None:
            blocks.append(findings_table)
        _assert_mfi_qa_traceability(blocks, qa_context)
    elif qa_review:
        blocks.append(
            ReportBlock(
                type="methodology_note",
                text=f"Narrative QA status: {qa_review.get('status', 'not recorded')}.",
                meta={"qa_review": qa_review},
            )
        )
    return blocks

"""
MFI Drafter - Graph
===================
Workflow LangGraph per generazione Market Functionality Index Reports.

Struttura del grafo:
    mfi_data_agent → context_retrieval → context_extractor → mfi_graph_designer
    → dimension_drafter → executive_summary_drafter → red_team → [loop/END]
"""
from __future__ import annotations

import ast
import io
import re
import json
import uuid
import html as html_lib
import base64
import random
import logging
from math import pi
from datetime import datetime, timedelta
from typing import TypedDict, Annotated, Literal, List, Dict, Any, Optional, Callable

import operator
from collections import Counter

import pandas as pd
import numpy as np

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

from app.shared.llm import get_model
from app.shared.retrievers import ReliefWebRetriever, SeeristRetriever
from .schemas import (
    MFI_DIMENSIONS, RISK_COLORS, get_risk_level,
    Document, MFIMarketData, MFIDimensionScore, SurveyMetadata
)
from .data_loader import load_mfi_from_csv

logger = logging.getLogger(__name__)

OnStepCallback = Callable[[str, Dict[str, Any]], None]

WFP_BLUE = "#0072BC"


# ============================================================================
# STATE DEFINITION
# ============================================================================

class MFIReportState(TypedDict):
    """Stato principale del grafo MFI."""
    
    # ===== INPUTS =====
    country: str
    data_collection_start: str
    data_collection_end: str
    markets: List[str]

    csv_data: Optional[Dict[str, Any]]
    use_csv_data: bool
    
    # ===== BRANCH 1: MFI DATA =====
    raw_survey_data: Optional[str]
    markets_data: List[Dict[str, Any]]
    dimension_scores: List[Dict[str, Any]]
    survey_metadata: Optional[Dict[str, Any]]
    
    # ===== BRANCH 2: CONTEXT =====
    contextual_documents: List[Dict[str, Any]]
    document_references: List[Dict[str, Any]]
    country_context: Optional[str]
    context_counts: Dict[str, int]
    retriever_traces: List[Dict[str, Any]]
    
    # ===== VISUALIZATIONS =====
    visualizations: Dict[str, str]  # Base64
    
    # ===== DRAFTED SECTIONS =====
    executive_summary: Optional[str]
    dimension_findings: Dict[str, Dict[str, str]]
    market_recommendations: Dict[str, Dict[str, Any]]
    
    # ===== QA & CONTROL =====
    skeptic_flags: List[Dict[str, Any]]
    warnings: Annotated[List[str], operator.add]
    run_id: str
    correction_attempts: int
    llm_calls: int
    current_node: str


def create_initial_state(
    country: str,
    data_collection_start: str,
    data_collection_end: str,
    markets: List[str],
    csv_data: Optional[Dict[str, Any]] = None
) -> MFIReportState:
    """Crea stato iniziale per il grafo."""
    return MFIReportState(
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
        markets=markets,
        csv_data=csv_data,
        use_csv_data=csv_data is not None,
        raw_survey_data=None,
        markets_data=[],
        dimension_scores=[],
        survey_metadata=None,
        contextual_documents=[],
        document_references=[],
        country_context=None,
        context_counts={"Seerist": 0, "ReliefWeb": 0, "total": 0},
        retriever_traces=[],
        visualizations={},
        executive_summary=None,
        dimension_findings={},
        market_recommendations={},
        skeptic_flags=[],
        warnings=[],
        run_id=f"mfi_{uuid.uuid4().hex[:8]}",
        correction_attempts=0,
        llm_calls=0,
        current_node="init"
    )


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def robust_json_parse(response: Any) -> Optional[Dict]:
    """Helper per pulire e parsare l'output JSON dell'LLM."""
    if hasattr(response, 'content'):
        raw_output = response.content
    elif isinstance(response, str):
        raw_output = response
    else:
        return None

    try:
        raw_output = re.sub(r"```json\s*", "", raw_output)
        raw_output = re.sub(r"```", "", raw_output).strip()
        
        start_index = raw_output.find('{')
        end_index = raw_output.rfind('}')
        if start_index == -1 or end_index == -1:
            return None
        return json.loads(raw_output[start_index:end_index+1])
    except json.JSONDecodeError:
        return None


def _normalize_llm_text(value: Any, *, bulletify: bool = False) -> str:
    if value is None:
        return ""

    if isinstance(value, str):
        s = value.strip()

        if "<" in s and ">" in s:
            s = html_lib.unescape(s)

            def _strip_tags(text: str) -> str:
                return re.sub(r"<[^>]+>", "", text).strip()

            def _render_list(inner_html: str, *, ordered: bool) -> str:
                items = re.findall(r"<li[^>]*>(.*?)</li>", inner_html, flags=re.IGNORECASE | re.DOTALL)
                cleaned = [_strip_tags(item) for item in items]
                cleaned = [c for c in cleaned if c]
                if not cleaned:
                    return ""
                if ordered:
                    return "\n".join([f"{i + 1}. {c}" for i, c in enumerate(cleaned)])
                return "\n".join([f"- {c}" for c in cleaned])

            s = re.sub(
                r"<ol[^>]*>(.*?)</ol>",
                lambda m: _render_list(m.group(1), ordered=True),
                s,
                flags=re.IGNORECASE | re.DOTALL,
            )
            s = re.sub(
                r"<ul[^>]*>(.*?)</ul>",
                lambda m: _render_list(m.group(1), ordered=False),
                s,
                flags=re.IGNORECASE | re.DOTALL,
            )

            s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"</p\s*>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"<p\b[^>]*>", "", s, flags=re.IGNORECASE)

            s = re.sub(r"</li\s*>", "\n", s, flags=re.IGNORECASE)
            s = re.sub(r"<li\b[^>]*>", "- ", s, flags=re.IGNORECASE)

            s = re.sub(r"<[^>]+>", "", s)
            s = re.sub(r"\n\s*\n+", "\n\n", s).strip()

        if s.startswith("[") and s.endswith("]"):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return _normalize_llm_text(parsed, bulletify=bulletify)
            except Exception:
                try:
                    parsed = ast.literal_eval(s)
                    if isinstance(parsed, list):
                        return _normalize_llm_text(parsed, bulletify=bulletify)
                except Exception:
                    pass
        return s

    if isinstance(value, (list, tuple)):
        parts: list[str] = []
        for item in value:
            item_str = _normalize_llm_text(item, bulletify=bulletify).strip()
            if not item_str:
                continue
            if bulletify:
                stripped = item_str.lstrip()
                if not (
                    stripped.startswith("-")
                    or stripped.startswith("*")
                    or re.match(r"^\d+\.\s+", stripped)
                ):
                    item_str = f"- {item_str}"
            parts.append(item_str)
        return "\n".join(parts)

    return str(value).strip()


def save_plot_to_base64() -> str:
    """Salva il plot corrente come base64."""
    import matplotlib.pyplot as plt
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


def _generate_simple_geographic_map(
    state: MFIReportState,
    markets_with_coords: List[Dict[str, Any]],
    visualizations: Dict[str, str],
) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    lats = [float(m["latitude"]) for m in markets_with_coords]
    lons = [float(m["longitude"]) for m in markets_with_coords]
    mfi_scores = [float(m.get("overall_mfi", 0) or 0) for m in markets_with_coords]
    names = [str(m.get("market_name", "")).strip() for m in markets_with_coords]

    fig, ax = plt.subplots(figsize=(12, 10))
    scatter = ax.scatter(
        lons,
        lats,
        c=mfi_scores,
        cmap="RdYlGn",
        s=100,
        vmin=0,
        vmax=10,
        edgecolors="black",
        linewidths=0.5,
    )

    for lon, lat, score, name in zip(lons, lats, mfi_scores, names):
        if not name:
            continue
        if score < 5.5:
            ax.annotate(name, (lon, lat), fontsize=6, xytext=(3, 3), textcoords="offset points")

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    country = str(state.get("country", "")).strip()
    suffix = f" ({country})" if country else ""
    ax.set_title(
        f"MFI Scores - Geographic Distribution{suffix}",
        fontsize=12,
        fontweight="bold",
    )

    cbar = plt.colorbar(scatter, ax=ax, shrink=0.6)
    cbar.set_label("MFI Score")

    legend_elements = [
        Patch(facecolor="#2ca02c", label="Low Risk (≥7)"),
        Patch(facecolor="#ffbb78", label="Medium Risk (5.5-7)"),
        Patch(facecolor="#ff7f0e", label="High Risk (4-5.5)"),
        Patch(facecolor="#d62728", label="Very High Risk (<4)"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=8)

    plt.tight_layout()
    visualizations["geographic_map"] = save_plot_to_base64()


# ============================================================================
# NODE: MFI DATA AGENT
# ============================================================================

def generate_mock_mfi_data(
    country: str, 
    markets: List[str],
    data_collection_start: str,
    data_collection_end: str
) -> Dict[str, Any]:
    """Genera dati MFI mock realistici."""
    logger.info(f"[MOCK] Generating MFI data for {country} ({len(markets)} markets)")
    
    # Mock admin mapping (API-like enrichment)
    admin0 = country
    admin1_pool = [f"{country} - Admin1 {i + 1}" for i in range(min(3, max(1, len(markets))))]
    market_admin_map: Dict[str, Dict[str, str]] = {}
    for i, market in enumerate(markets):
        admin1 = admin1_pool[i % len(admin1_pool)]
        admin2 = f"{admin1} - Admin2 {(i % 2) + 1}"
        market_admin_map[market] = {"admin0": admin0, "admin1": admin1, "admin2": admin2}
    
    markets_data = []
    for market in markets:
        admin_info = market_admin_map.get(market, {"admin0": country, "admin1": "Unknown", "admin2": "Unknown"})
        region = admin_info["admin1"]
        dimension_scores = {}
        sub_scores = {}
        
        for dim in MFI_DIMENSIONS:
            base_score = random.uniform(4.5, 9.5)
            dimension_scores[dim] = round(base_score, 1)
            
            # Sub-scores specifici per dimensione
            if dim == "Assortment":
                sub_scores[dim] = {
                    "breadth": round(base_score + random.uniform(-0.5, 0.5), 1),
                    "depth": round(base_score + random.uniform(-0.5, 0.5), 1)
                }
            elif dim == "Availability":
                sub_scores[dim] = {
                    "scarce_cereals_pct": round(random.uniform(0.1, 0.5), 2),
                    "runout_cereals_pct": round(random.uniform(0.1, 0.4), 2)
                }
            elif dim == "Price":
                sub_scores[dim] = {
                    "increase_cereals_pct": round(random.uniform(0.2, 0.6), 2),
                    "unstable_cereals_pct": round(random.uniform(0.3, 0.8), 2)
                }
            elif dim == "Resilience":
                sub_scores[dim] = {
                    "low_density_pct": round(random.uniform(0.05, 0.6), 2),
                    "high_complexity_pct": round(random.uniform(0.05, 0.5), 2),
                    "high_criticality_pct": round(random.uniform(0.05, 0.5), 2)
                }
            elif dim == "Competition":
                sub_scores[dim] = {
                    "less_than_five_competitors": random.choice([0, 1]),
                    "monopoly_risk": random.choice([0, 1])
                }
            elif dim == "Infrastructure":
                sub_scores[dim] = {
                    "condition_good": random.choice([0, 1]),
                    "condition_medium": random.choice([0, 1]),
                    "condition_poor": random.choice([0, 1])
                }
            elif dim == "Service":
                sub_scores[dim] = {
                    "checkout_score": round(random.uniform(4, 9), 1),
                    "shopping_experience_score": round(random.uniform(3, 8), 1)
                }
            elif dim == "Food Quality":
                standards_met = round(random.uniform(0.5, 0.95), 2)
                sub_scores[dim] = {
                    "quality_standards_met_pct": standards_met,
                    "quality_problems_pct": round(max(0, 1 - standards_met), 2)
                }
            elif dim == "Access & Protection":
                sub_scores[dim] = {
                    "access_issues_pct": round(random.uniform(0, 0.3), 2),
                    "protection_issues_pct": round(random.uniform(0, 0.2), 2)
                }
        
        overall_mfi = round(np.mean(list(dimension_scores.values())), 1)
        markets_data.append({
            "market_name": market,
            "admin0": admin_info["admin0"],
            "admin1": admin_info["admin1"],
            "admin2": admin_info["admin2"],
            "region": region,
            "overall_mfi": overall_mfi,
            "dimension_scores": dimension_scores,
            "sub_scores": sub_scores,
            "risk_level": get_risk_level(overall_mfi),
            "traders_surveyed": random.randint(15, 30)
        })
    
    # Aggregazione per dimensione
    regions = sorted({m["region"] for m in markets_data})
    dimension_aggregations = []
    for dim in MFI_DIMENSIONS:
        national_score = round(np.mean([m["dimension_scores"][dim] for m in markets_data]), 1)
        regional_scores = {}
        for region in regions:
            region_markets = [m for m in markets_data if m["region"] == region]
            if region_markets:
                regional_scores[region] = round(
                    np.mean([m["dimension_scores"][dim] for m in region_markets]), 1
                )
        market_scores = {m["market_name"]: m["dimension_scores"][dim] for m in markets_data}
        dimension_aggregations.append({
            "dimension": dim,
            "national_score": national_score,
            "regional_scores": regional_scores,
            "market_scores": market_scores
        })
    
    survey_metadata = {
        "country": country,
        "collection_period": f"{data_collection_start} to {data_collection_end}",
        "total_traders": sum(m["traders_surveyed"] for m in markets_data),
        "total_markets": len(markets_data),
        "regions_covered": regions
    }
    
    return {
        "markets_data": markets_data,
        "dimension_scores": dimension_aggregations,
        "survey_metadata": survey_metadata
    }


def node_mfi_data_agent(state: MFIReportState) -> dict:
    """Nodo: Recupera/genera dati MFI."""
    logger.info(f"[MFIDataAgent] Processing for {state['country']}")

    if state.get("use_csv_data") and state.get("csv_data"):
        logger.info("[MFIDataAgent] Using CSV data")
        data = state["csv_data"]
    else:
        logger.info("[MFIDataAgent] Using mock data (no CSV provided)")
        data = generate_mock_mfi_data(
            state["country"],
            state["markets"],
            state["data_collection_start"],
            state["data_collection_end"],
        )
    
    # Log risk distribution
    risk_dist = {}
    for m in data["markets_data"]:
        risk_dist[m["risk_level"]] = risk_dist.get(m["risk_level"], 0) + 1
    logger.info(f"Risk distribution: {risk_dist}")
    
    return {
        "markets_data": data["markets_data"],
        "dimension_scores": data["dimension_scores"],
        "survey_metadata": data["survey_metadata"],
        "current_node": "mfi_data_agent"
    }

# NODE: CONTEXT RETRIEVAL (Mock)
# ============================================================================

def node_context_retrieval(state: MFIReportState) -> dict:
    """Nodo: Recupera notizie contestuali (mock)."""
    logger.info(f"[ContextRetrieval] Fetching context for {state['country']}")

    docs: List[Dict[str, Any]] = []
    retriever_traces: List[Dict[str, Any]] = []
    warnings: List[str] = []

    country = state.get("country", "")
    start_date = state.get("data_collection_start", "")
    end_date = state.get("data_collection_end", "")

    rw = ReliefWebRetriever(verbose=False)
    rw_query = ReliefWebRetriever.build_economy_query(
        extra_terms=["market functionality", "food security", "supply", "availability", "access"]
    )
    rw_docs = rw.fetch(country=country, start_date=start_date, end_date=end_date, max_records=8, query=rw_query)
    if getattr(rw, "last_trace", None):
        retriever_traces.append(rw.last_trace)

    seerist = SeeristRetriever(verbose=False)
    seerist_queries = [
        SeeristRetriever.build_lucene_or_query(
            list(SeeristRetriever.DEFAULT_ECON_TERMS)
            + ["market functionality", "food security", "supply", "availability", "access"]
        ),
        SeeristRetriever.build_lucene_or_query(
            ["market functionality", "market", "availability", "access", "food security"]
        ),
        "",
    ]
    seerist_docs = seerist.fetch_batch(
        queries=seerist_queries,
        start_date=start_date,
        end_date=end_date,
        country=country,
        max_per_query=8,
    )
    if len(seerist_docs) > 8:
        seerist_docs = seerist_docs[:8]
    if getattr(seerist, "last_trace", None):
        retriever_traces.append(seerist.last_trace)
        if seerist.last_trace.get("error"):
            warnings.append(f"Seerist retrieval unavailable for {country}: {seerist.last_trace['error']}")

    combined = list(rw_docs) + list(seerist_docs)
    seen_keys = set()
    deduped: List[Dict[str, Any]] = []
    for d in combined:
        url = (d.get("url") or "").strip()
        key = url or d.get("doc_id")
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        if not d.get("content"):
            d["content"] = d.get("title", "")
        deduped.append(d)
    docs = deduped

    counts = Counter([d.get("source", "Unknown") for d in docs])
    context_counts = {
        "Seerist": int(counts.get("Seerist", 0)),
        "ReliefWeb": int(counts.get("ReliefWeb", 0)),
        "total": int(len(docs)),
    }

    refs = [
        {
            "doc_id": d.get("doc_id"),
            "source": d.get("source"),
            "title": d.get("title"),
            "url": d.get("url"),
            "date": d.get("date"),
        }
        for d in docs
    ]
    
    updates = {
        "contextual_documents": docs,
        "document_references": refs,
        "context_counts": context_counts,
        "retriever_traces": retriever_traces,
        "current_node": "context_retrieval",
    }
    if warnings:
        updates["warnings"] = warnings
    return updates

# NODE: CONTEXT EXTRACTOR
# ============================================================================

def node_context_extractor(state: MFIReportState) -> dict:
    """Nodo: Estrae contesto con LLM."""
    logger.info("[ContextExtractor] Extracting context")
    
    docs = state.get("contextual_documents", [])
    
    if not docs:
        return {
            "country_context": None,
            "current_node": "context_extractor",
        }
    
    llm = get_model()
    doc_text = "\n".join([f"[{d['source']}]: {d['content'][:400]}..." for d in docs[:5]])
    
    prompt = f"""Extract a brief country context (3-4 sentences) relevant for MFI report from these sources about {state['country']}.

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.
- If the sources do not contain relevant information about the economic situation, food security, or market-affecting factors, return an empty string for country_context.
- Do not include disclaimers like 'cannot be extracted' or 'insufficient information'.

Focus on: economic situation, food security, market-affecting factors.

SOURCES:
{doc_text}

Return JSON: {{"country_context": "Your 3-4 sentence context..."}}"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = robust_json_parse(response)
        context = result.get("country_context", "") if result else ""
        llm_calls = 1
    except Exception as e:
        logger.error(f"Context extraction failed: {e}")
        context = ""
        llm_calls = 0

    context = (context or "").strip()

    if context:
        lc = context.lower()
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
            context = ""

    if not context:
        return {
            "country_context": None,
            "llm_calls": state.get("llm_calls", 0) + llm_calls,
            "current_node": "context_extractor",
        }
    
    return {
        "country_context": context,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "context_extractor"
    }


def node_mfi_graph_designer(state: MFIReportState) -> dict:
    logger.info("[GraphDesigner] Generating visualizations")

    visualizations: Dict[str, str] = {}

    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors

        dim_scores = state.get("dimension_scores") or []
        score_map: Dict[str, float] = {}
        for d in dim_scores:
            if isinstance(d, dict) and d.get("dimension") is not None:
                try:
                    score_map[str(d.get("dimension"))] = float(d.get("national_score", 0.0))
                except Exception:
                    score_map[str(d.get("dimension"))] = 0.0

        dims = list(MFI_DIMENSIONS)
        values = [float(score_map.get(dim, 0.0)) for dim in dims]
        if any(values):
            angles = [n / float(len(dims)) * 2 * pi for n in range(len(dims))]
            values_loop = values + values[:1]
            angles_loop = angles + angles[:1]

            fig, ax = plt.subplots(figsize=(8, 8), subplot_kw={"polar": True})
            ax.set_theta_offset(pi / 2)
            ax.set_theta_direction(-1)
            plt.xticks(angles, dims, size=8)
            ax.set_ylim(0, 10)
            ax.plot(angles_loop, values_loop, color=WFP_BLUE, linewidth=2)
            ax.fill(angles_loop, values_loop, color=WFP_BLUE, alpha=0.25)
            plt.tight_layout()
            visualizations["mfi_radar"] = save_plot_to_base64()

        risk_dist: Dict[str, int] = {}
        for m in state.get("markets_data", []) or []:
            if not isinstance(m, dict):
                continue
            risk = m.get("risk_level")
            if not risk:
                continue
            risk_dist[str(risk)] = risk_dist.get(str(risk), 0) + 1

        if risk_dist:
            labels = ["Low Risk", "Medium Risk", "High Risk", "Very High Risk"]
            counts = [risk_dist.get(lbl, 0) for lbl in labels]
            colors = [RISK_COLORS.get(lbl, "#999999") for lbl in labels]
            xs = list(range(len(labels)))

            fig, ax = plt.subplots(figsize=(8, 4))
            ax.bar(xs, counts, color=colors)
            ax.set_xticks(xs)
            ax.set_xticklabels(labels, rotation=20, ha="right")
            ax.set_ylabel("Number of markets")
            ax.set_title("Risk distribution")
            plt.tight_layout()
            visualizations["risk_distribution"] = save_plot_to_base64()

        markets_data = [
            m for m in (state.get("markets_data", []) or [])
            if isinstance(m, dict)
        ]

        if markets_data:
            market_names = [str(m.get("market_name", "")).strip() for m in markets_data]
            dims = list(MFI_DIMENSIONS)

            data_matrix: list[list[float]] = []
            for m in markets_data:
                dim_scores = m.get("dimension_scores") or {}
                row = []
                for dim in dims:
                    try:
                        row.append(float(dim_scores.get(dim, 0) or 0))
                    except Exception:
                        row.append(0.0)
                data_matrix.append(row)

            data_matrix_np = np.array(data_matrix, dtype=float)

            heat_colors = ["#d62728", "#ff7f0e", "#ffbb78", "#98df8a", "#2ca02c"]
            cmap = mcolors.LinearSegmentedColormap.from_list("mfi_risk", heat_colors)

            fig_height = max(8, len(market_names) * 0.35)
            fig_width = max(12, len(dims) * 1.2)
            fig, ax = plt.subplots(figsize=(fig_width, fig_height))

            im = ax.imshow(data_matrix_np, cmap=cmap, aspect="auto", vmin=0, vmax=10)

            ax.set_xticks(np.arange(len(dims)))
            ax.set_yticks(np.arange(len(market_names)))
            ax.set_xticklabels(dims, rotation=45, ha="right", fontsize=9)
            ax.set_yticklabels(market_names, fontsize=8)

            for i in range(len(market_names)):
                for j in range(len(dims)):
                    score = float(data_matrix_np[i, j])
                    text_color = "white" if score < 4 or score > 8 else "black"
                    ax.text(
                        j,
                        i,
                        f"{score:.1f}",
                        ha="center",
                        va="center",
                        color=text_color,
                        fontsize=7,
                        fontweight="bold",
                    )

            cbar = ax.figure.colorbar(im, ax=ax, shrink=0.5)
            cbar.set_label("MFI Score (0-10)", rotation=270, labelpad=15)

            ax.set_title(
                "Market Functionality Index - Overview by Market and Dimension",
                fontsize=12,
                fontweight="bold",
                pad=10,
            )

            plt.tight_layout()
            visualizations["overview_table"] = save_plot_to_base64()

        dim_scores = state.get("dimension_scores") or []

        for dim_data in dim_scores:
            if not isinstance(dim_data, dict):
                continue

            dim_name = dim_data.get("dimension")
            market_scores = dim_data.get("market_scores") or {}

            if not dim_name or not isinstance(market_scores, dict) or not market_scores:
                continue

            sorted_markets = sorted(
                [(str(k), float(v)) for k, v in market_scores.items() if v is not None],
                key=lambda x: x[1],
            )
            markets = [m[0] for m in sorted_markets]
            scores = [m[1] for m in sorted_markets]

            bar_colors: list[str] = []
            for s in scores:
                if s < 4:
                    bar_colors.append("#d62728")
                elif s < 5.5:
                    bar_colors.append("#ff7f0e")
                elif s < 7:
                    bar_colors.append("#ffbb78")
                else:
                    bar_colors.append("#2ca02c")

            fig_height = max(6, len(markets) * 0.3)
            fig, ax = plt.subplots(figsize=(10, fig_height))

            y_pos = np.arange(len(markets))
            ax.barh(y_pos, scores, color=bar_colors, edgecolor="white", linewidth=0.5)

            ax.set_yticks(y_pos)
            ax.set_yticklabels(markets, fontsize=8)
            ax.set_xlabel("Score (0-10)")
            ax.set_xlim(0, 10)
            ax.set_title(f"{dim_name} - Score by Market", fontsize=11, fontweight="bold")

            ax.axvline(x=5.5, color="gray", linestyle="--", linewidth=1, alpha=0.7)
            ax.text(5.6, max(0, len(markets) - 1), "Medium Risk\nThreshold", fontsize=7, color="gray")

            for i, score in enumerate(scores):
                ax.text(score + 0.1, i, f"{score:.1f}", va="center", fontsize=7)

            plt.tight_layout()

            safe_dim_name = str(dim_name).lower().replace(" ", "_").replace("&", "and")
            safe_dim_name = re.sub(r"[^a-z0-9_]+", "_", safe_dim_name).strip("_")
            visualizations[f"dim_{safe_dim_name}_bars"] = save_plot_to_base64()

        markets_with_coords = [
            m
            for m in markets_data
            if m.get("latitude") is not None and m.get("longitude") is not None
        ]

        if markets_with_coords:
            try:
                import cartopy.crs as ccrs
                import cartopy.feature as cfeature
                from matplotlib.patches import Patch

                lats = [float(m["latitude"]) for m in markets_with_coords]
                lons = [float(m["longitude"]) for m in markets_with_coords]
                mfi_scores = [float(m.get("overall_mfi", 0) or 0) for m in markets_with_coords]
                names = [str(m.get("market_name", "")).strip() for m in markets_with_coords]

                lat_min, lat_max = min(lats) - 0.5, max(lats) + 0.5
                lon_min, lon_max = min(lons) - 0.5, max(lons) + 0.5

                def get_risk_color(score: float) -> str:
                    if score < 4.0:
                        return "#d62728"
                    if score < 5.5:
                        return "#ff7f0e"
                    if score < 7.0:
                        return "#ffbb78"
                    return "#2ca02c"

                colors = [get_risk_color(s) for s in mfi_scores]

                fig = plt.figure(figsize=(12, 10))
                ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
                ax.set_extent([lon_min, lon_max, lat_min, lat_max], crs=ccrs.PlateCarree())

                ax.add_feature(cfeature.LAND, facecolor="#f5f5f5")
                ax.add_feature(cfeature.OCEAN, facecolor="#e6f3ff")
                ax.add_feature(cfeature.BORDERS, linestyle="-", linewidth=0.5, edgecolor="gray")
                ax.add_feature(cfeature.COASTLINE, linewidth=0.5)

                try:
                    ax.add_feature(cfeature.STATES, linestyle=":", linewidth=0.3, edgecolor="gray")
                except Exception:
                    pass

                for lon, lat, score, name, color in zip(lons, lats, mfi_scores, names, colors):
                    size = 150 if score < 5.5 else 80
                    edge_width = 2 if score < 4.0 else 1
                    ax.scatter(
                        lon,
                        lat,
                        c=color,
                        s=size,
                        edgecolors="black",
                        linewidths=edge_width,
                        transform=ccrs.PlateCarree(),
                        zorder=5,
                    )
                    if name and score < 5.5:
                        ax.annotate(
                            name,
                            xy=(lon, lat),
                            xytext=(5, 5),
                            textcoords="offset points",
                            fontsize=7,
                            fontweight="bold",
                            color="black",
                            transform=ccrs.PlateCarree(),
                            zorder=6,
                        )

                gl = ax.gridlines(draw_labels=True, linewidth=0.3, color="gray", alpha=0.5)
                gl.top_labels = False
                gl.right_labels = False
                gl.xlabel_style = {"size": 8}
                gl.ylabel_style = {"size": 8}

                country = str(state.get("country", "")).strip()
                ax.set_title(
                    f"Market Functionality Index - {country}\nGeographic Distribution by Risk Level",
                    fontsize=12,
                    fontweight="bold",
                    pad=10,
                )

                legend_elements = [
                    Patch(facecolor="#2ca02c", edgecolor="black", label="Low Risk (≥7.0)"),
                    Patch(facecolor="#ffbb78", edgecolor="black", label="Medium Risk (5.5-6.9)"),
                    Patch(facecolor="#ff7f0e", edgecolor="black", label="High Risk (4.0-5.4)"),
                    Patch(facecolor="#d62728", edgecolor="black", label="Very High Risk (<4.0)"),
                ]
                ax.legend(handles=legend_elements, loc="lower right", fontsize=9, framealpha=0.9)

                plt.tight_layout()
                visualizations["geographic_map"] = save_plot_to_base64()
            except ImportError:
                logger.warning("Cartopy not available, falling back to simple scatter plot")
                _generate_simple_geographic_map(state, markets_with_coords, visualizations)
            except Exception as e:
                logger.error(f"Error generating cartopy map: {e}")
                _generate_simple_geographic_map(state, markets_with_coords, visualizations)
    except Exception as e:
        logger.error(f"Error generating visualizations: {e}")

    return {
        "visualizations": visualizations,
        "current_node": "mfi_graph_designer",
    }


# ============================================================================
# NODE: DIMENSION DRAFTER
# ============================================================================

DIMENSION_DESCRIPTIONS = {
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


def node_dimension_drafter(state: MFIReportState) -> dict:
    """Nodo: Genera findings per ogni dimensione."""
    logger.info("[DimensionDrafter] Generating dimension findings")
    
    if not state.get("dimension_scores"):
        return {"current_node": "dimension_drafter"}
    
    llm = get_model()
    dimension_findings = state.get("dimension_findings", {})
    markets_data = state["markets_data"]
    llm_calls = 0
    
    for dim_data in state["dimension_scores"]:
        dimension = dim_data["dimension"]
        logger.info(f"Processing dimension: {dimension}")
        
        try:
            # Aggregate sub-scores
            sub_scores_combined = {}
            for market in markets_data:
                if dimension in market.get("sub_scores", {}):
                    for k, v in market["sub_scores"][dimension].items():
                        sub_scores_combined.setdefault(k, []).append(v)
            sub_scores_avg = {k: round(np.mean(v), 2) for k, v in sub_scores_combined.items()}
            
            prompt = f"""Generate findings for the **{dimension}** MFI dimension.

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.
- Do not use HTML tags (no <ul>, <li>, <ol>, <br>, etc). Output plain text only.
- For bullets, use plain text lines starting with '- '.

Data:
- National Score: {dim_data['national_score']}/10
- Regional: {json.dumps(dim_data['regional_scores'])}
- Markets: {json.dumps(dim_data['market_scores'])}
- Sub-scores (0-1 fractions): {json.dumps(sub_scores_avg)}

Sub-score interpretation rules (MANDATORY):
- All values are 0-1 fractions. To write percentages, multiply by 100.
- DO NOT INVERT these values.
- Unless explicitly stated otherwise, higher *_pct means a higher rate of the named problem (worse).
- Exception: quality_standards_met_pct is positive (higher = better). quality_problems_pct is negative (higher = worse).
- For Availability and Price sub-scores in particular, treat them strictly as problem rates even if the National Score is high.

Examples (do not contradict these):
- scarce_cereals_pct=0.80 means 80% of traders report cereal SCARCITY (not 20%).
- runout_cereals_pct=0.82 means 82% of traders report cereal STOCKOUTS (not 18%).
- increase_cereals_pct=0.83 means 83% of traders report cereal PRICE INCREASES.
- unstable_cereals_pct=0.58 means 58% of traders report cereal PRICE INSTABILITY.

Description: {DIMENSION_DESCRIPTIONS.get(dimension, '')}

Generate:
1. KEY FINDINGS: 2-3 bullet points
2. SCORE INTERPRETATION: 1-2 sentences
3. RECOMMENDATIONS: 1-2 actionable items

Output JSON:
{{"key_findings": "...", "score_interpretation": "...", "recommendations": "..."}}"""
            
            response = llm.invoke([HumanMessage(content=prompt)])
            result = robust_json_parse(response)
            llm_calls += 1
            
            if result:
                dimension_findings[dimension] = {
                    "key_findings": _normalize_llm_text(result.get("key_findings"), bulletify=True),
                    "score_interpretation": _normalize_llm_text(result.get("score_interpretation")),
                    "recommendations": _normalize_llm_text(result.get("recommendations"), bulletify=True),
                }
            else:
                dimension_findings[dimension] = {
                    "key_findings": f"Score: {dim_data['national_score']}/10",
                    "score_interpretation": "Review manually.",
                    "recommendations": "Monitor."
                }
        except Exception as e:
            logger.error(f"Dimension {dimension} error: {e}")
            dimension_findings[dimension] = {
                "key_findings": "Error generating findings",
                "score_interpretation": "",
                "recommendations": ""
            }
    
    return {
        "dimension_findings": dimension_findings,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "dimension_drafter"
    }


def node_market_recommendations_drafter(state: MFIReportState) -> dict:
    """Nodo: Genera raccomandazioni per mercato invece che per dimensione."""
    logger.info("[MarketRecDrafter] Generating market-level recommendations")

    markets_data = state.get("markets_data", [])
    if not markets_data:
        return {
            "market_recommendations": {},
            "current_node": "market_recommendations_drafter",
        }

    llm = get_model()
    market_recommendations: Dict[str, Dict[str, Any]] = {}
    llm_calls = 0

    critical_markets = [
        m
        for m in markets_data
        if isinstance(m, dict) and m.get("risk_level") in ["High Risk", "Very High Risk"]
    ]

    if len(critical_markets) > 15:
        critical_markets = sorted(
            critical_markets,
            key=lambda x: float(x.get("overall_mfi", 0) or 0),
        )[:15]

    for market in critical_markets:
        market_name = str(market.get("market_name", "")).strip()
        if not market_name:
            continue

        region = str(market.get("region", market.get("admin1", "")) or "").strip()

        weak_dims = [
            (dim, score)
            for dim, score in (market.get("dimension_scores") or {}).items()
            if score is not None and float(score) < 6.0
        ]
        weak_dims = sorted(weak_dims, key=lambda x: float(x[1]))

        if not weak_dims:
            continue

        prompt = f"""Generate targeted recommendations for {market_name} market ({region}).

MARKET DATA:
- Overall MFI: {market.get('overall_mfi')}/10 ({market.get('risk_level')})
- Weak dimensions: {json.dumps([{'dim': d, 'score': float(s)} for d, s in weak_dims[:4]])}
- Sub-scores: {json.dumps(market.get('sub_scores', {}))}

RULES:
- Language: English only
- Do not use HTML tags (no <ul>, <li>, <ol>, <br>, etc). Output plain text only.
- Focus on the 2-3 weakest dimensions
- Provide specific, actionable interventions
- Link interventions to specific issues identified

Output JSON:
{{
    \"priority_issues\": [\"issue1\", \"issue2\"],
    \"recommended_interventions\": [\"intervention1\", \"intervention2\", \"intervention3\"],
    \"modality_considerations\": \"Brief note on CBT feasibility given market conditions\"
}}"""

        try:
            response = llm.invoke([HumanMessage(content=prompt)])
            result = robust_json_parse(response)
            llm_calls += 1

            if not isinstance(result, dict):
                continue

            priority_issues_raw = result.get("priority_issues", []) or []
            if not isinstance(priority_issues_raw, list):
                priority_issues_raw = []
            priority_issues: list[str] = []
            for item in priority_issues_raw:
                s = _normalize_llm_text(item).strip()
                if s:
                    priority_issues.append(s)

            interventions_raw = result.get("recommended_interventions", []) or []
            if not isinstance(interventions_raw, list):
                interventions_raw = []
            recommended_interventions: list[str] = []
            for item in interventions_raw:
                s = _normalize_llm_text(item).strip()
                if s:
                    recommended_interventions.append(s)

            modality_considerations = _normalize_llm_text(result.get("modality_considerations", "")).strip()

            market_recommendations[market_name] = {
                "region": region,
                "mfi_score": float(market.get("overall_mfi", 0) or 0),
                "risk_level": str(market.get("risk_level", "")).strip(),
                "weak_dimensions": [d for d, _s in weak_dims[:3]],
                "priority_issues": priority_issues,
                "recommended_interventions": recommended_interventions,
                "modality_considerations": modality_considerations,
            }
        except Exception as e:
            logger.error(f"Market {market_name} recommendation error: {e}")

    return {
        "market_recommendations": market_recommendations,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "market_recommendations_drafter",
    }


# ============================================================================
# NODE: EXECUTIVE SUMMARY DRAFTER
# ============================================================================

def node_executive_summary_drafter(state: MFIReportState) -> dict:
    """Nodo: Genera executive summary."""
    logger.info("[ExecSummaryDrafter] Generating executive summary")
    
    if not state.get("dimension_scores"):
        return {"current_node": "executive_summary_drafter"}
    
    llm = get_model()
    markets_data = state["markets_data"]
    dimension_scores = state["dimension_scores"]
    survey_meta = state.get("survey_metadata", {})
    
    # Calculate aggregates
    risk_dist = {}
    for m in markets_data:
        risk_dist[m["risk_level"]] = risk_dist.get(m["risk_level"], 0) + 1
    
    sorted_dims = sorted(dimension_scores, key=lambda x: x["national_score"], reverse=True)
    market_mfis = [float(m.get("overall_mfi", 0) or 0) for m in markets_data if isinstance(m, dict)]
    national_mfi = round(np.mean(market_mfis), 1) if market_mfis else 0.0
    
    collection_period = f"{state['data_collection_start']} to {state['data_collection_end']}"
    regions_covered = survey_meta.get("regions_covered", [])
    prompt = f"""Generate Executive Summary for {state['country']} MFI Report ({collection_period}).

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.
- Do not use HTML tags (no <ul>, <li>, <ol>, <br>, etc). Output plain text only.
- For bullets, use plain text lines starting with '- '.
- For numbered lists, use plain text lines starting with '1. ', '2. ', etc.

Survey: {len(markets_data)} markets, {len(regions_covered)} admin1 areas, {survey_meta.get('total_traders', 'N/A')} traders
Risk Distribution: {json.dumps(risk_dist)}
Best: {sorted_dims[0]['dimension']} ({sorted_dims[0]['national_score']}), Worst: {sorted_dims[-1]['dimension']} ({sorted_dims[-1]['national_score']})
National MFI: {national_mfi}

Context: {state.get('country_context', '')}

Generate:
1. MOTIVATION: 1-2 sentences
2. KEY FINDINGS: 4-5 bullets
3. RECOMMENDATIONS: 2-3 items

Output JSON:
{{"motivation": "...", "key_findings": "...", "recommendations": "..."}}"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = robust_json_parse(response)
        llm_calls = 1
        
        if result:
            motivation = _normalize_llm_text(result.get("motivation"))
            key_findings = _normalize_llm_text(result.get("key_findings"), bulletify=True)
            recommendations = _normalize_llm_text(result.get("recommendations"), bulletify=True)
            executive_summary = f"""**MOTIVATION**
{motivation}

**KEY FINDINGS**
{key_findings}

**RECOMMENDATIONS**
{recommendations}"""
        else:
            executive_summary = f"MFI Assessment for {state['country']} - {collection_period}"
    except Exception as e:
        logger.error(f"Executive summary error: {e}")
        executive_summary = f"MFI Assessment for {state['country']} - {collection_period}"
        llm_calls = 0
    
    return {
        "executive_summary": executive_summary,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "executive_summary_drafter"
    }


# ============================================================================
# NODE: RED TEAM (QA)
# ============================================================================

def node_red_team(state: MFIReportState) -> dict:
    """Nodo: Quality Assurance."""
    logger.info("[RedTeam] Fact-checking MFI report")
    
    if not state.get("executive_summary"):
        return {"skeptic_flags": [], "current_node": "red_team"}
    
    llm = get_model()
    
    markets_summary = [
        {"name": m["market_name"], "mfi": m["overall_mfi"], "risk": m["risk_level"]} 
        for m in state["markets_data"]
    ]
    
    dim_findings_text = "\n".join([
        f"{dim}: {f.get('key_findings', '')}" 
        for dim, f in state.get("dimension_findings", {}).items()
    ])
    
    prompt = f"""Fact-check this MFI report against the source data.

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.

GROUND TRUTH:
Dimension Scores: {json.dumps([{'dimension': d['dimension'], 'score': d['national_score']} for d in state['dimension_scores']])}
Markets: {json.dumps(markets_summary)}

EXECUTIVE SUMMARY:
{state['executive_summary']}

DIMENSION FINDINGS:
{dim_findings_text}

Check for:
1. Score mismatches
2. Interpretation errors
3. Missing critical content

Return JSON: {{"flags": [
    {{"section": "...", "claim": "...", "issue_type": "score_mismatch|interpretation_error|missing_content", "severity": "high|medium|low", "details": "...", "recommendation": "..."}}
]}}

If no errors, return {{"flags": []}}"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = robust_json_parse(response)
        flags = result.get("flags", []) if result else []
        llm_calls = 1
    except Exception as e:
        logger.error(f"Red team error: {e}")
        flags = []
        llm_calls = 0
    
    return {
        "skeptic_flags": flags,
        "correction_attempts": state.get("correction_attempts", 0) + 1,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "red_team"
    }


# ============================================================================
# ROUTING & GRAPH BUILDER
# ============================================================================

MAX_CORRECTION_ATTEMPTS = 3

def should_correct(state: MFIReportState) -> Literal["correct", "finish"]:
    """Determina se servono correzioni."""
    flags = state.get("skeptic_flags", [])
    attempts = state.get("correction_attempts", 0)
    
    if flags and attempts < MAX_CORRECTION_ATTEMPTS:
        logger.info(f"Corrections needed: {len(flags)} flags, attempt {attempts}")
        return "correct"
    return "finish"


def build_graph(on_step: Optional[OnStepCallback] = None):
    """Costruisce il grafo LangGraph per MFI Report."""

    def wrap_node(node_name: str, fn):
        def wrapped(state: MFIReportState):
            state_dict = dict(state)
            if on_step is not None:
                on_step(node_name, state_dict)

            updates = fn(state)

            if on_step is not None:
                merged = dict(state_dict)
                if isinstance(updates, dict):
                    merged.update(updates)
                on_step(node_name, merged)

            return updates

        return wrapped

    graph = StateGraph(MFIReportState)
    
    # Add nodes
    graph.add_node("mfi_data_agent", wrap_node("mfi_data_agent", node_mfi_data_agent))
    graph.add_node("context_retrieval", wrap_node("context_retrieval", node_context_retrieval))
    graph.add_node("context_extractor", wrap_node("context_extractor", node_context_extractor))
    graph.add_node("mfi_graph_designer", wrap_node("mfi_graph_designer", node_mfi_graph_designer))
    graph.add_node("dimension_drafter", wrap_node("dimension_drafter", node_dimension_drafter))
    graph.add_node(
        "market_recommendations_drafter",
        wrap_node("market_recommendations_drafter", node_market_recommendations_drafter),
    )
    graph.add_node(
        "executive_summary_drafter",
        wrap_node("executive_summary_drafter", node_executive_summary_drafter),
    )
    graph.add_node("red_team", wrap_node("red_team", node_red_team))
    
    # Set entry point
    graph.set_entry_point("mfi_data_agent")
    
    # Linear flow
    graph.add_edge("mfi_data_agent", "context_retrieval")
    graph.add_edge("context_retrieval", "context_extractor")
    graph.add_edge("context_extractor", "mfi_graph_designer")
    graph.add_edge("mfi_graph_designer", "dimension_drafter")
    graph.add_edge("dimension_drafter", "market_recommendations_drafter")
    graph.add_edge("market_recommendations_drafter", "executive_summary_drafter")
    graph.add_edge("executive_summary_drafter", "red_team")
    
    # QA Loop
    graph.add_conditional_edges(
        "red_team",
        should_correct,
        {
            "correct": "dimension_drafter",
            "finish": END
        }
    )
    
    return graph.compile()


# ============================================================================
# PUBLIC API
# ============================================================================

def run_mfi_report_generation(
    country: str,
    data_collection_start: str,
    data_collection_end: str,
    markets: List[str],
    csv_data: Optional[Dict[str, Any]] = None,
    on_step: Optional[OnStepCallback] = None
) -> dict:
    """
    Entry point per la generazione del MFI Report.
    
    Returns:
        Stato finale con report completo
    """
    initial_state = create_initial_state(
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
        markets=markets,
        csv_data=csv_data,
    )
    
    agent = build_graph(on_step=on_step)
    return agent.invoke(initial_state)

"""
MFI Drafter - Graph
===================
Workflow LangGraph per generazione Market Functionality Index Reports.

Struttura del grafo:
    mfi_data_agent → context_retrieval → context_extractor → mfi_graph_designer
    → dimension_drafter → executive_summary_drafter → red_team → [loop/END]
"""
from __future__ import annotations

import io
import re
import json
import uuid
import base64
import random
import logging
from math import pi
from datetime import datetime, timedelta
from typing import TypedDict, Annotated, Literal, List, Dict, Any, Optional
import operator

import pandas as pd
import numpy as np

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

from app.shared.llm import get_model
from .schemas import (
    MFI_DIMENSIONS, RISK_COLORS, get_risk_level,
    Document, MFIMarketData, MFIDimensionScore, SurveyMetadata
)

logger = logging.getLogger(__name__)

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
    
    # ===== BRANCH 1: MFI DATA =====
    raw_survey_data: Optional[str]
    markets_data: List[Dict[str, Any]]
    dimension_scores: List[Dict[str, Any]]
    survey_metadata: Optional[Dict[str, Any]]
    
    # ===== BRANCH 2: CONTEXT =====
    contextual_documents: List[Dict[str, Any]]
    country_context: Optional[str]
    
    # ===== VISUALIZATIONS =====
    visualizations: Dict[str, str]  # Base64
    
    # ===== DRAFTED SECTIONS =====
    executive_summary: Optional[str]
    dimension_findings: Dict[str, Dict[str, str]]
    
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
    markets: List[str]
) -> MFIReportState:
    """Crea stato iniziale per il grafo."""
    return MFIReportState(
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
        markets=markets,
        raw_survey_data=None,
        markets_data=[],
        dimension_scores=[],
        survey_metadata=None,
        contextual_documents=[],
        country_context=None,
        visualizations={},
        executive_summary=None,
        dimension_findings={},
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


def save_plot_to_base64() -> str:
    """Salva il plot corrente come base64."""
    import matplotlib.pyplot as plt
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close()
    buf.seek(0)
    return base64.b64encode(buf.read()).decode('utf-8')


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
                    "node_complexity": random.choice([0, 1]),
                    "node_criticality": random.choice([0, 1]),
                    "node_density": random.choice([0, 1])
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
                sub_scores[dim] = {
                    "quality_features_pct": round(random.uniform(0.5, 0.95), 2)
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
        "total_markets": len(markets),
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
    
    data = generate_mock_mfi_data(
        state["country"],
        state["markets"],
        state["data_collection_start"],
        state["data_collection_end"]
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


# ============================================================================
# NODE: CONTEXT RETRIEVAL (Mock)
# ============================================================================

def node_context_retrieval(state: MFIReportState) -> dict:
    """Nodo: Recupera notizie contestuali (mock)."""
    logger.info(f"[ContextRetrieval] Fetching context for {state['country']}")
    
    # Mock documents
    docs = [
        {
            "doc_id": f"doc_{uuid.uuid4().hex[:6]}",
            "title": f"Food Security Update - {state['country']}",
            "url": "https://reliefweb.int/example",
            "source": "ReliefWeb",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "content": f"The food security situation in {state['country']} continues to be monitored. "
                       f"Market functionality assessments indicate varying levels of access across regions."
        },
        {
            "doc_id": f"doc_{uuid.uuid4().hex[:6]}",
            "title": f"Market Assessment - {state['country']}",
            "url": "https://example.com/market",
            "source": "WFP",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "content": f"Recent market assessments in {state['country']} show mixed results across "
                       f"different dimensions of market functionality."
        }
    ]
    
    return {
        "contextual_documents": docs,
        "current_node": "context_retrieval"
    }


# ============================================================================
# NODE: CONTEXT EXTRACTOR
# ============================================================================

def node_context_extractor(state: MFIReportState) -> dict:
    """Nodo: Estrae contesto con LLM."""
    logger.info("[ContextExtractor] Extracting context")
    
    docs = state.get("contextual_documents", [])
    
    if not docs:
        context = f"{state['country']} market assessment conducted to evaluate market functionality."
        return {
            "country_context": context,
            "current_node": "context_extractor"
        }
    
    llm = get_model()
    doc_text = "\n".join([f"[{d['source']}]: {d['content'][:400]}..." for d in docs[:5]])
    
    prompt = f"""Extract a brief country context (3-4 sentences) relevant for MFI report from these sources about {state['country']}.
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
        context = f"{state['country']} market assessment conducted to evaluate market functionality."
        llm_calls = 0
    
    if not context:
        context = f"{state['country']} market assessment conducted to evaluate market functionality."
    
    return {
        "country_context": context,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "context_extractor"
    }


# ============================================================================
# NODE: MFI GRAPH DESIGNER
# ============================================================================

def node_mfi_graph_designer(state: MFIReportState) -> dict:
    """Nodo: Genera visualizzazioni MFI."""
    logger.info("[GraphDesigner] Generating MFI visualizations")
    
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import seaborn as sns
    
    sns.set_style("whitegrid")
    
    visualizations = {}
    markets_data = state["markets_data"]
    dimension_scores = state["dimension_scores"]
    survey_meta = state.get("survey_metadata", {}) or {}
    regions = survey_meta.get("regions_covered", [])
    if not regions:
        regions = sorted({m.get("region", "Unknown") for m in markets_data})
    
    # 1. Radar Chart - Regional Comparison
    try:
        categories = [d["dimension"] for d in dimension_scores]
        N = len(categories)
        angles = [n / float(N) * 2 * pi for n in range(N)]
        angles += angles[:1]
        
        fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(polar=True))
        colors = plt.cm.Set2(np.linspace(0, 1, len(regions)))
        
        for idx, region in enumerate(regions):
            values = []
            for dim_data in dimension_scores:
                val = dim_data["regional_scores"].get(region, 0)
                values.append(val)
            values += values[:1]
            ax.plot(angles, values, 'o-', linewidth=2, label=region, color=colors[idx])
            ax.fill(angles, values, alpha=0.15, color=colors[idx])
        
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(categories, size=9)
        ax.set_ylim(0, 10)
        ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))
        plt.title(f"MFI Regional Comparison - {state['country']}", size=14, fontweight='bold', y=1.08)
        
        visualizations["radar_regional"] = save_plot_to_base64()
        logger.info("Radar chart generated")
    except Exception as e:
        logger.error(f"Radar chart error: {e}")
    
    # 2. Dimension Heatmap
    try:
        market_names = [m["market_name"] for m in markets_data]
        dims = [d["dimension"] for d in dimension_scores]
        
        matrix = []
        for market in markets_data:
            row = [market["dimension_scores"].get(dim, 0) for dim in dims]
            matrix.append(row)
        
        df_heatmap = pd.DataFrame(matrix, index=market_names, columns=dims)
        
        fig, ax = plt.subplots(figsize=(14, max(8, len(market_names) * 0.4)))
        sns.heatmap(df_heatmap, annot=True, fmt='.1f', cmap='RdYlGn',
                   vmin=0, vmax=10, linewidths=0.5, ax=ax,
                   cbar_kws={'label': 'MFI Score'})
        ax.set_title(f"MFI Scores by Market and Dimension - {state['country']}", 
                    fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        visualizations["dimension_heatmap"] = save_plot_to_base64()
        logger.info("Heatmap generated")
    except Exception as e:
        logger.error(f"Heatmap error: {e}")
    
    # 3. Risk Distribution
    try:
        risk_counts = {}
        for m in markets_data:
            risk_counts[m["risk_level"]] = risk_counts.get(m["risk_level"], 0) + 1
        
        risk_order = ["Low Risk", "Medium Risk", "High Risk", "Very High Risk"]
        labels = [r for r in risk_order if r in risk_counts]
        values = [risk_counts[r] for r in labels]
        colors = [RISK_COLORS[r] for r in labels]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.bar(labels, values, color=colors, edgecolor='black')
        
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                   str(val), ha='center', fontsize=12, fontweight='bold')
        
        ax.set_title(f"Market Risk Distribution - {state['country']}", 
                    fontsize=14, fontweight='bold')
        ax.set_ylabel("Number of Markets")
        sns.despine()
        
        visualizations["risk_distribution"] = save_plot_to_base64()
        logger.info("Risk distribution generated")
    except Exception as e:
        logger.error(f"Risk distribution error: {e}")
    
    # 4. National Overview (Horizontal Bar)
    try:
        dims = [d["dimension"] for d in dimension_scores]
        national_scores = [d["national_score"] for d in dimension_scores]
        bar_colors = [
            RISK_COLORS["Low Risk"] if s >= 7 else 
            RISK_COLORS["Medium Risk"] if s >= 5.5 else 
            RISK_COLORS["High Risk"] if s >= 4 else 
            RISK_COLORS["Very High Risk"] 
            for s in national_scores
        ]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.barh(dims, national_scores, color=bar_colors, edgecolor='black')
        
        for bar, score in zip(bars, national_scores):
            ax.text(score + 0.1, bar.get_y() + bar.get_height()/2, 
                   f'{score:.1f}', va='center', fontsize=10)
        
        ax.set_xlim(0, 10)
        ax.set_xlabel("MFI Score")
        ax.set_title(f"National MFI Scores - {state['country']}", 
                    fontsize=14, fontweight='bold')
        ax.axvline(x=7, color='green', linestyle='--', alpha=0.5)
        ax.axvline(x=5.5, color='orange', linestyle='--', alpha=0.5)
        ax.axvline(x=4, color='red', linestyle='--', alpha=0.5)
        sns.despine()
        plt.tight_layout()
        
        visualizations["national_overview"] = save_plot_to_base64()
        logger.info("National overview generated")
    except Exception as e:
        logger.error(f"National overview error: {e}")
    
    return {
        "visualizations": visualizations,
        "current_node": "mfi_graph_designer"
    }


# ============================================================================
# NODE: DIMENSION DRAFTER
# ============================================================================

DIMENSION_DESCRIPTIONS = {
    "Assortment": "Variety of essential goods available.",
    "Availability": "Risk of stockouts and scarcity.",
    "Price": "Price stability and predictability.",
    "Resilience": "Supply chain robustness.",
    "Competition": "Market competition levels.",
    "Infrastructure": "Physical market conditions.",
    "Service": "Quality of retail service.",
    "Food Quality": "Food safety standards.",
    "Access & Protection": "Accessibility and safety."
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

Data:
- National Score: {dim_data['national_score']}/10
- Regional: {json.dumps(dim_data['regional_scores'])}
- Markets: {json.dumps(dim_data['market_scores'])}
- Sub-scores: {json.dumps(sub_scores_avg)}

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
                dimension_findings[dimension] = result
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
    national_mfi = round(np.mean([d["national_score"] for d in dimension_scores]), 1)
    
    collection_period = f"{state['data_collection_start']} to {state['data_collection_end']}"
    regions_covered = survey_meta.get("regions_covered", [])
    prompt = f"""Generate Executive Summary for {state['country']} MFI Report ({collection_period}).

Survey: {len(markets_data)} markets, {len(regions_covered)} admin1 areas, {survey_meta.get('total_traders', 'N/A')} traders
Risk Distribution: {json.dumps(risk_dist)}
Best: {sorted_dims[0]['dimension']} ({sorted_dims[0]['national_score']}), Worst: {sorted_dims[-1]['dimension']} ({sorted_dims[-1]['national_score']})
National MFI: {national_mfi}

Context: {state.get('country_context', '')}

Generate:
1. MOTIVATION: 1-2 sentences
2. KEY FINDINGS: 4-5 bullets
3. RECOMMENDATIONS: 2-3 items

Output JSON: {{"motivation": "...", "key_findings": "...", "recommendations": "..."}}"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = robust_json_parse(response)
        llm_calls = 1
        
        if result:
            executive_summary = f"""**MOTIVATION**
{result.get('motivation', '')}

**KEY FINDINGS**
{result.get('key_findings', '')}

**RECOMMENDATIONS**
{result.get('recommendations', '')}"""
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


def build_graph():
    """Costruisce il grafo LangGraph per MFI Report."""
    
    graph = StateGraph(MFIReportState)
    
    # Add nodes
    graph.add_node("mfi_data_agent", node_mfi_data_agent)
    graph.add_node("context_retrieval", node_context_retrieval)
    graph.add_node("context_extractor", node_context_extractor)
    graph.add_node("mfi_graph_designer", node_mfi_graph_designer)
    graph.add_node("dimension_drafter", node_dimension_drafter)
    graph.add_node("executive_summary_drafter", node_executive_summary_drafter)
    graph.add_node("red_team", node_red_team)
    
    # Set entry point
    graph.set_entry_point("mfi_data_agent")
    
    # Linear flow
    graph.add_edge("mfi_data_agent", "context_retrieval")
    graph.add_edge("context_retrieval", "context_extractor")
    graph.add_edge("context_extractor", "mfi_graph_designer")
    graph.add_edge("mfi_graph_designer", "dimension_drafter")
    graph.add_edge("dimension_drafter", "executive_summary_drafter")
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
    markets: List[str]
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
        markets=markets
    )
    
    agent = build_graph()
    return agent.invoke(initial_state)
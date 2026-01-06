"""
Market Monitor - Graph
======================
Workflow LangGraph per generazione Market Monitor Reports.

Struttura del grafo:
    data_agent → graph_designer → news_retrieval → event_mapper 
    → trend_analyst → module_orchestrator → highlights_drafter 
    → narrative_drafter → red_team → [loop/END]
"""
from __future__ import annotations

import io
import re
import json
import uuid
import base64
import random
import logging
import requests
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timedelta
from typing import TypedDict, Annotated, Literal, List, Dict, Any, Optional
from collections import Counter
import operator

import pandas as pd
import numpy as np

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

from app.shared.llm import get_model

logger = logging.getLogger(__name__)


# ============================================================================
# CURRENCY SYMBOLS (Trading Economics)
# ============================================================================

CURRENCY_SYMBOLS = {
    "SDG": "USDSDG:CUR",  # Sudan Pound
    "MMK": "USDMMK:CUR",  # Myanmar Kyat
    "YER": "USDYER:CUR",  # Yemeni Rial
    "SYP": "USDSYP:CUR",  # Syrian Pound
    "AFN": "USDAFN:CUR",  # Afghan Afghani
    "ETB": "USDETB:CUR",  # Ethiopian Birr
    "NGN": "USDNGN:CUR",  # Nigerian Naira
    "PKR": "USDPKR:CUR",  # Pakistani Rupee
    "BDT": "USDBDT:CUR",  # Bangladeshi Taka
    "KES": "USDKES:CUR",  # Kenyan Shilling
    "UGX": "USDUGX:CUR",  # Ugandan Shilling
    "TZS": "USDTZS:CUR",  # Tanzanian Shilling
    "ZMW": "USDZMW:CUR",  # Zambian Kwacha
    "MWK": "USDMWK:CUR",  # Malawian Kwacha
    "HTG": "USDHTG:CUR",  # Haitian Gourde
    "CDF": "USDCDF:CUR",  # Congolese Franc
    "SOS": "USDSOS:CUR",  # Somali Shilling
    "SSP": "USDSSP:CUR",  # South Sudanese Pound
}

# Base rates per mock data
BASE_EXCHANGE_RATES = {
    "SDG": 550.0, "MMK": 2100.0, "YER": 250.0, "ETB": 56.0,
    "NGN": 1550.0, "PKR": 278.0, "KES": 130.0, "UGX": 3700.0,
    "TZS": 2500.0, "ZMW": 25.0, "MWK": 1700.0, "HTG": 130.0,
    "CDF": 2800.0, "SOS": 570.0, "SSP": 130.0
}


# ============================================================================
# STATE DEFINITION
# ============================================================================

class MarketReportState(TypedDict):
    """Stato principale del grafo."""
    
    # ===== INPUTS =====
    country: str
    time_period: str
    commodity_list: List[str]
    admin1_list: List[str]
    previous_report_text: str
    currency_code: str
    use_mock_data: bool
    
    # ===== MODULE CONFIG =====
    enabled_modules: List[str]
    
    # ===== BRANCH 1 OUTPUTS (Data & Graphs) =====
    time_series_data_national: Optional[str]  # JSON
    time_series_data_regional: Optional[str]  # JSON
    data_statistics: Optional[Dict[str, Any]]
    visualizations: Dict[str, str]  # Base64 images

    # ===== BRANCH 2 OUTPUTS (Contextual Intelligence) =====
    documents: List[Dict[str, Any]]
    events: List[Dict[str, Any]]
    trend_analysis: Optional[Dict[str, Any]]

    # ===== MODULE OUTPUTS =====
    exchange_rate_data: Optional[Dict[str, Any]]
    module_sections: Dict[str, str]
    
    # ===== CENTRAL & QA OUTPUTS =====
    report_draft_sections: Dict[str, str]
    skeptic_flags: List[Dict[str, Any]]

    # ===== CONTROL & METADATA =====
    warnings: Annotated[List[str], operator.add]
    run_id: str
    correction_attempts: int
    llm_calls: int
    current_node: str


def create_initial_state(
    country: str,
    time_period: str,
    commodity_list: List[str],
    admin1_list: List[str],
    currency_code: str,
    enabled_modules: List[str],
    previous_report_text: str = "",
    use_mock_data: bool = True
) -> MarketReportState:
    """Crea stato iniziale per il grafo."""
    return MarketReportState(
        country=country,
        time_period=time_period,
        commodity_list=commodity_list,
        admin1_list=admin1_list,
        previous_report_text=previous_report_text,
        currency_code=currency_code,
        use_mock_data=use_mock_data,
        enabled_modules=enabled_modules,
        time_series_data_national=None,
        time_series_data_regional=None,
        data_statistics=None,
        visualizations={},
        documents=[],
        events=[],
        trend_analysis=None,
        exchange_rate_data=None,
        module_sections={},
        report_draft_sections={},
        skeptic_flags=[],
        warnings=[],
        run_id=f"run_{uuid.uuid4().hex[:8]}",
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


def format_pct(value) -> str:
    """Formatta la percentuale con frecce ↑/↓."""
    try:
        val = float(value)
        if val > 0:
            return f"↑{abs(val):.1f}%"
        elif val < 0:
            return f"↓{abs(val):.1f}%"
        else:
            return f"{val:.1f}%"
    except (TypeError, ValueError):
        return "N/A"


# ============================================================================
# MODULE INTERFACE
# ============================================================================

class ReportModule(ABC):
    """Interfaccia base per moduli opzionali del report."""
    
    @property
    @abstractmethod
    def module_id(self) -> str:
        pass
    
    @property
    @abstractmethod
    def display_name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def required_inputs(self) -> List[str]:
        pass
    
    def validate_inputs(self, state: dict) -> bool:
        missing = [f for f in self.required_inputs if f not in state or state[f] is None]
        if missing:
            logger.warning(f"Module '{self.module_id}' missing inputs: {missing}")
            return False
        return True
    
    @abstractmethod
    def fetch_data(self, state: dict) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def generate_section(self, state: dict, llm) -> Dict[str, Any]:
        pass


# ============================================================================
# EXCHANGE RATE MODULE
# ============================================================================

class ExchangeRateModule(ReportModule):
    """Modulo per l'analisi del tasso di cambio."""
    
    TE_API_BASE = "https://api.tradingeconomics.com"
    
    def __init__(self, api_key: Optional[str] = None):
        import os
        self.api_key = api_key or os.getenv("TE_API_KEY")
        self.use_mock = self.api_key is None
    
    @property
    def module_id(self) -> str:
        return "exchange_rate"
    
    @property
    def display_name(self) -> str:
        return "Exchange Rate Analysis"
    
    @property
    def required_inputs(self) -> List[str]:
        return ["currency_code", "country", "time_period"]
    
    def _get_symbol(self, currency_code: str) -> str:
        return CURRENCY_SYMBOLS.get(currency_code, f"USD{currency_code}:CUR")
    
    def _generate_mock_data(self, currency_code: str) -> Dict[str, Any]:
        """Genera dati mock realistici."""
        base = BASE_EXCHANGE_RATES.get(currency_code, 100.0)
        current_rate = base * (1 + random.uniform(-0.05, 0.15))
        mom_change = random.uniform(-2, 15)
        yoy_change = random.uniform(10, 80)
        
        # Determina trend
        if yoy_change > 30:
            trend = "rapid_depreciation"
        elif yoy_change > 10:
            trend = "depreciation"
        elif yoy_change < -10:
            trend = "appreciation"
        else:
            trend = "stable"
        
        # Serie storica mock
        dates = pd.date_range(end=datetime.now(), periods=13*22, freq='B')
        prices = [base]
        for i in range(1, len(dates)):
            change = random.gauss(0.001, 0.01)
            prices.append(prices[-1] * (1 + change))
        
        df = pd.DataFrame({
            "Date": dates,
            "Close": prices,
        })
        
        return {
            "symbol": self._get_symbol(currency_code),
            "currency_code": currency_code,
            "current_rate": round(current_rate, 2),
            "daily_change_pct": round(random.uniform(-1, 2), 2),
            "weekly_change_pct": round(random.uniform(-2, 5), 2),
            "monthly_change_pct": round(mom_change, 2),
            "yearly_change_pct": round(yoy_change, 2),
            "trend": trend,
            "last_update": datetime.now().isoformat(),
            "historical_data_json": df.to_json(date_format='iso'),
            "is_mock": True,
        }
    
    def fetch_data(self, state: dict) -> Dict[str, Any]:
        """Recupera dati tasso di cambio."""
        logger.info(f"[ExchangeRateModule] Fetching data for {state['currency_code']}")
        
        currency_code = state["currency_code"]
        
        if self.use_mock or state.get("use_mock_data", True):
            data = self._generate_mock_data(currency_code)
        else:
            # API reale (da implementare)
            data = self._generate_mock_data(currency_code)
            data["is_mock"] = False
        
        return {"exchange_rate_data": data}
    
    def generate_section(self, state: dict, llm) -> Dict[str, Any]:
        """Genera la sezione narrativa."""
        exchange_data = state.get("exchange_rate_data", {})
        
        if not exchange_data or exchange_data.get("current_rate") is None:
            return {
                "section_title": self.display_name,
                "narrative": "Exchange rate data is currently unavailable.",
            }
        
        prompt = f"""You are a WFP economic analyst writing the Exchange Rate Analysis section.

CONTEXT:
- Country: {state.get('country', 'Unknown')}
- Currency: {exchange_data.get('currency_code', 'LCU')}
- Current exchange rate: {exchange_data.get('current_rate', 'N/A')} per 1 USD
- Month-on-month change: {exchange_data.get('monthly_change_pct', 'N/A')}%
- Year-on-year change: {exchange_data.get('yearly_change_pct', 'N/A')}%
- Recent trend: {exchange_data.get('trend', 'unknown')}

Write a concise analysis (100-150 words) covering:
1. Current exchange rate status and recent trend
2. Month-on-month change and its significance for import costs
3. Year-on-year comparison showing longer-term trajectory
4. Implications for food prices (wheat, rice, cooking oil, fuel)

Return ONLY the narrative text. No headers or formatting."""
        
        try:
            response = llm.invoke([HumanMessage(content=prompt)])
            narrative = response.content if hasattr(response, 'content') else str(response)
        except Exception as e:
            logger.error(f"Error generating narrative: {e}")
            narrative = f"The {exchange_data.get('currency_code')} is at {exchange_data.get('current_rate')} per USD."
        
        return {
            "section_title": self.display_name,
            "narrative": narrative,
            "key_metrics": {
                "current_rate": exchange_data.get("current_rate"),
                "mom_change_pct": exchange_data.get("monthly_change_pct"),
                "yoy_change_pct": exchange_data.get("yearly_change_pct"),
                "trend": exchange_data.get("trend"),
            }
        }


# Registry moduli disponibili
AVAILABLE_MODULES: Dict[str, type] = {
    "exchange_rate": ExchangeRateModule,
}


# ============================================================================
# NODE: DATA AGENT
# ============================================================================

def generate_mock_time_series(
    country: str, 
    time_period: str, 
    commodities: List[str], 
    admin1s: List[str]
) -> tuple:
    """Genera serie temporali mock (13 mesi)."""
    try:
        end_date = pd.to_datetime(time_period + "-01")
    except:
        end_date = pd.to_datetime("2025-01-01")
    
    dates = pd.date_range(end=end_date, periods=13, freq='MS')
    
    # National data
    national_data = []
    base_prices = {c: random.uniform(500, 2000) for c in commodities}
    base_prices["FoodBasket"] = random.uniform(10000, 30000)
    base_prices["ExchangeRate"] = random.uniform(1000, 5000)
    base_prices["FuelPrice"] = random.uniform(50, 500)
    
    for date in dates:
        row = {"Date": date}
        for item, base in base_prices.items():
            trend = (date.to_julian_date() - dates[0].to_julian_date()) / 365 * 0.15
            seasonality = np.sin((date.month - 3) * np.pi / 6) * 0.1
            shock = random.uniform(0.05, 0.20) if random.random() > 0.9 else 0
            price = base * (1 + trend + seasonality + shock)
            row[item] = round(price, 2)
        national_data.append(row)
    
    df_national = pd.DataFrame(national_data).set_index("Date")
    
    # Regional data
    regional_data = []
    for date in dates:
        for i, region in enumerate(admin1s):
            national_fb = df_national.loc[date, "FoodBasket"]
            regional_factor = 1.0 + i * 0.05 + random.uniform(-0.05, 0.15)
            price = national_fb * regional_factor
            regional_data.append({"Date": date, "Region": region, "FoodBasket": round(price, 2)})
    
    df_regional = pd.DataFrame(regional_data)
    
    return df_national, df_regional


def calculate_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calcola MoM e YoY dai dati."""
    stats = {"food_basket": {}, "commodities": {}, "auxiliary": {}}
    
    if df.empty or len(df) < 13:
        return stats
    
    current = df.iloc[-1]
    mom = df.iloc[-2]
    yoy = df.iloc[0]
    
    for col in df.columns:
        current_val = current[col]
        mom_val = mom[col]
        yoy_val = yoy[col]
        
        mom_pct = round(((current_val - mom_val) / mom_val * 100) if mom_val else 0, 1)
        yoy_pct = round(((current_val - yoy_val) / yoy_val * 100) if yoy_val else 0, 1)
        
        data = {
            "current_price": round(current_val, 2),
            "mom_change_pct": mom_pct,
            "yoy_change_pct": yoy_pct
        }
        
        if col == "FoodBasket":
            stats["food_basket"] = data
        elif col in ["ExchangeRate", "FuelPrice"]:
            stats["auxiliary"][col] = data
        else:
            stats["commodities"][col] = data
    
    return stats


def node_data_agent(state: MarketReportState) -> dict:
    """Nodo: Recupera e processa i dati."""
    logger.info(f"[DataAgent] Processing data for {state['country']}")
    
    df_national, df_regional = generate_mock_time_series(
        state["country"],
        state["time_period"],
        state["commodity_list"],
        state["admin1_list"]
    )
    
    stats = calculate_statistics(df_national)
    
    return {
        "time_series_data_national": df_national.to_json(date_format='iso'),
        "time_series_data_regional": df_regional.to_json(date_format='iso'),
        "data_statistics": stats,
        "current_node": "data_agent"
    }


# ============================================================================
# NODE: GRAPH DESIGNER
# ============================================================================

def node_graph_designer(state: MarketReportState) -> dict:
    """Nodo: Genera visualizzazioni."""
    logger.info("[GraphDesigner] Generating visualizations")
    
    visualizations = {}
    
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        
        # Parse data
        df_national = pd.read_json(io.StringIO(state["time_series_data_national"]))
        
        # 1. Food Basket Trend
        fig, ax = plt.subplots(figsize=(10, 5))
        if "FoodBasket" in df_national.columns:
            ax.plot(df_national.index, df_national["FoodBasket"], 
                   marker='o', linewidth=2, color='#1f77b4')
            ax.set_title(f"Food Basket Cost Trend - {state['country']}", fontweight='bold')
            ax.set_ylabel("Cost (LCU)")
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close()
            buf.seek(0)
            visualizations["food_basket_trend"] = base64.b64encode(buf.read()).decode('utf-8')
        
        # 2. Commodity Trends
        commodity_cols = [c for c in df_national.columns 
                         if c not in ["FoodBasket", "ExchangeRate", "FuelPrice"]]
        
        if commodity_cols:
            fig, ax = plt.subplots(figsize=(12, 6))
            for col in commodity_cols[:5]:
                ax.plot(df_national.index, df_national[col], marker='o', label=col)
            ax.set_title(f"Commodity Price Trends - {state['country']}", fontweight='bold')
            ax.set_ylabel("Price (LCU)")
            ax.legend(loc='upper left')
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close()
            buf.seek(0)
            visualizations["commodity_trends"] = base64.b64encode(buf.read()).decode('utf-8')
        
        # 3. Regional Comparison (if data available)
        if state.get("time_series_data_regional"):
            df_regional = pd.read_json(io.StringIO(state["time_series_data_regional"]))
            if not df_regional.empty and "Region" in df_regional.columns:
                latest = df_regional[df_regional["Date"] == df_regional["Date"].max()]
                
                fig, ax = plt.subplots(figsize=(10, 6))
                bars = ax.barh(latest["Region"], latest["FoodBasket"], color='#2ecc71')
                ax.set_title(f"Regional Food Basket Cost - {state['time_period']}", fontweight='bold')
                ax.set_xlabel("Cost (LCU)")
                plt.tight_layout()
                
                buf = io.BytesIO()
                plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
                plt.close()
                buf.seek(0)
                visualizations["regional_comparison"] = base64.b64encode(buf.read()).decode('utf-8')
    
    except Exception as e:
        logger.error(f"Error generating visualizations: {e}")
    
    return {
        "visualizations": visualizations,
        "current_node": "graph_designer"
    }


# ============================================================================
# NODE: NEWS RETRIEVAL (Simplified - uses mock)
# ============================================================================

def node_news_retrieval(state: MarketReportState) -> dict:
    """Nodo: Recupera notizie (mock per ora)."""
    logger.info(f"[NewsRetrieval] Fetching news for {state['country']}")
    
    # Mock documents
    documents = [
        {
            "doc_id": f"doc_{uuid.uuid4().hex[:6]}",
            "title": f"Food Security Update - {state['country']}",
            "url": "https://reliefweb.int/example",
            "source": "ReliefWeb",
            "date": state["time_period"] + "-15",
            "content": f"Food prices in {state['country']} have continued to rise due to ongoing economic challenges and supply chain disruptions. The cost of basic commodities has increased significantly compared to the previous month."
        },
        {
            "doc_id": f"doc_{uuid.uuid4().hex[:6]}",
            "title": f"Economic Outlook - {state['country']}",
            "url": "https://example.com/economic",
            "source": "GDELT",
            "date": state["time_period"] + "-10",
            "content": f"The local currency has experienced depreciation against the USD, putting additional pressure on import costs. Fuel prices remain elevated, affecting transportation and production costs."
        }
    ]
    
    return {
        "documents": documents,
        "current_node": "news_retrieval"
    }


# ============================================================================
# NODE: EVENT MAPPER
# ============================================================================

def node_event_mapper(state: MarketReportState) -> dict:
    """Nodo: Estrae eventi dai documenti."""
    logger.info("[EventMapper] Extracting events")
    
    llm = get_model()
    documents = state.get("documents", [])
    
    if not documents:
        # Fallback events
        events = [{
            "event_id": "evt_fallback",
            "category": "economic",
            "statement": f"Ongoing price increases in {state['country']} due to economic factors.",
            "location": state["country"],
            "date": state["time_period"] + "-01",
            "source_ids": []
        }]
        return {"events": events, "current_node": "event_mapper"}
    
    # Prepare context
    context = "\n\n".join([
        f"[{d['doc_id']}] {d['date']}: {d['content'][:500]}"
        for d in documents[:5]
    ])
    
    prompt = f"""Extract key market events from these documents for {state['country']}.

DOCUMENTS:
{context}

Return JSON with events:
{{
  "events": [
    {{
      "event_id": "evt_unique_id",
      "category": "economic|political|climate|security|logistics|agriculture|other",
      "statement": "Brief description (Who, What, Where)",
      "location": "City or Region",
      "date": "YYYY-MM-DD",
      "source_ids": ["doc_id"]
    }}
  ]
}}"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = robust_json_parse(response)
        events = result.get("events", []) if result else []
        llm_calls = 1
    except Exception as e:
        logger.error(f"Event extraction failed: {e}")
        events = []
        llm_calls = 0
    
    if not events:
        events = [{
            "event_id": "evt_fallback",
            "category": "economic",
            "statement": f"Market conditions in {state['country']} remain challenging.",
            "location": state["country"],
            "date": state["time_period"] + "-01",
            "source_ids": []
        }]
    
    return {
        "events": events,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "event_mapper"
    }


# ============================================================================
# NODE: TREND ANALYST
# ============================================================================

def node_trend_analyst(state: MarketReportState) -> dict:
    """Nodo: Analizza i trend."""
    logger.info("[TrendAnalyst] Analyzing trends")
    
    llm = get_model()
    stats = state.get("data_statistics", {})
    events = state.get("events", [])
    
    prompt = f"""Analyze the market trend for {state['country']} based on these statistics and events.

STATISTICS:
{json.dumps(stats, indent=2)}

EVENTS:
{json.dumps(events, indent=2)}

Return JSON:
{{
    "trajectory": "increasing_prices|decreasing_prices|stable|volatile",
    "key_market_drivers": ["driver 1", "driver 2"],
    "commodity_analysis": {{"CommodityName": "Analysis text..."}},
    "regional_analysis": {{"RegionName": "Analysis text..."}},
    "outlook": "Forecast for next month..."
}}"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        trend_analysis = robust_json_parse(response)
        llm_calls = 1
    except Exception as e:
        logger.error(f"Trend analysis failed: {e}")
        trend_analysis = {
            "trajectory": "volatile",
            "key_market_drivers": ["Economic uncertainty", "Currency depreciation"],
            "commodity_analysis": {},
            "regional_analysis": {},
            "outlook": "Continued price volatility expected."
        }
        llm_calls = 0
    
    return {
        "trend_analysis": trend_analysis,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "trend_analyst"
    }


# ============================================================================
# NODE: MODULE ORCHESTRATOR
# ============================================================================

def node_module_orchestrator(state: MarketReportState) -> dict:
    """Nodo: Esegue i moduli opzionali."""
    logger.info("[ModuleOrchestrator] Running optional modules")
    
    enabled_modules = state.get("enabled_modules", [])
    
    if not enabled_modules:
        return {"current_node": "module_orchestrator"}
    
    llm = get_model()
    module_sections = {}
    updates = {}
    llm_calls = 0
    
    for module_id in enabled_modules:
        if module_id not in AVAILABLE_MODULES:
            logger.warning(f"Unknown module: {module_id}")
            continue
        
        try:
            module_class = AVAILABLE_MODULES[module_id]
            module = module_class()
            
            if not module.validate_inputs(state):
                continue
            
            # Fetch data
            data_update = module.fetch_data(state)
            updates.update(data_update)
            state.update(data_update)
            
            # Generate section
            output = module.generate_section(state, llm)
            module_sections[module_id] = output.get("narrative", "")
            llm_calls += 1
            
            logger.info(f"Module '{module_id}' completed successfully")
            
        except Exception as e:
            logger.error(f"Module '{module_id}' failed: {e}")
    
    updates["module_sections"] = module_sections
    updates["llm_calls"] = state.get("llm_calls", 0) + llm_calls
    updates["current_node"] = "module_orchestrator"
    
    return updates


# ============================================================================
# NODE: HIGHLIGHTS DRAFTER
# ============================================================================

def node_highlights_drafter(state: MarketReportState) -> dict:
    """Nodo: Genera la sezione Highlights."""
    logger.info("[HighlightsDrafter] Generating highlights")
    
    llm = get_model()
    stats = state.get("data_statistics", {})
    trend = state.get("trend_analysis", {})
    
    # Format statistics with arrows
    formatted_stats = {}
    if stats.get("food_basket"):
        fb = stats["food_basket"]
        formatted_stats["food_basket"] = {
            "current_price": fb.get("current_price"),
            "mom_change": format_pct(fb.get("mom_change_pct")),
            "yoy_change": format_pct(fb.get("yoy_change_pct"))
        }
    
    for name, data in stats.get("commodities", {}).items():
        formatted_stats[name] = {
            "current_price": data.get("current_price"),
            "mom_change": format_pct(data.get("mom_change_pct")),
            "yoy_change": format_pct(data.get("yoy_change_pct"))
        }
    
    prompt = f"""Draft the HIGHLIGHTS section for {state['country']} Market Monitor ({state['time_period']}).

DATA (with formatted MoM/YoY):
{json.dumps(formatted_stats, indent=2)}

TREND ANALYSIS:
{json.dumps(trend, indent=2)}

Include:
1. Overview (1-2 sentences)
2. Food Basket Cost with MoM% and YoY%
3. Top 3 Commodities with changes
4. Key Drivers (bullet list)

Return JSON: {{"HIGHLIGHTS": "The complete formatted text block"}}"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = robust_json_parse(response)
        highlights = result.get("HIGHLIGHTS", "") if result else ""
        llm_calls = 1
    except Exception as e:
        logger.error(f"Highlights generation failed: {e}")
        highlights = f"Market Monitor - {state['country']} - {state['time_period']}"
        llm_calls = 0
    
    sections = state.get("report_draft_sections", {})
    sections["HIGHLIGHTS"] = highlights

    correction_attempts = state.get("correction_attempts", 0)
    if state.get("skeptic_flags"):
        correction_attempts += 1
    
    return {
        "report_draft_sections": sections,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "correction_attempts": correction_attempts,
        "current_node": "highlights_drafter"
    }


# ============================================================================
# NODE: NARRATIVE DRAFTER
# ============================================================================

def node_narrative_drafter(state: MarketReportState) -> dict:
    """Nodo: Genera le sezioni narrative."""
    logger.info("[NarrativeDrafter] Generating narrative sections")
    
    llm = get_model()
    trend = state.get("trend_analysis", {})
    events = state.get("events", [])
    module_sections = state.get("module_sections", {})
    
    prompt = f"""Draft narrative sections for {state['country']} Market Monitor ({state['time_period']}).

TREND ANALYSIS:
{json.dumps(trend, indent=2)}

EVENTS:
{json.dumps(events, indent=2)}

MODULE SECTIONS TO REFERENCE:
{json.dumps(module_sections, indent=2) if module_sections else "None"}

SECTIONS TO GENERATE:
1. MARKET_OVERVIEW (200-250 words): Overall market conditions, price trajectory, key drivers.
2. COMMODITY_ANALYSIS (200-300 words): Analysis of each commodity. Use [INSERT GRAPH: commodity_trends] placeholder.
3. REGIONAL_HIGHLIGHTS (150-200 words): Regional variations. Use [INSERT GRAPH: regional_comparison] placeholder.

Return JSON with keys: MARKET_OVERVIEW, COMMODITY_ANALYSIS, REGIONAL_HIGHLIGHTS"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = robust_json_parse(response)
        llm_calls = 1
    except Exception as e:
        logger.error(f"Narrative generation failed: {e}")
        result = {}
        llm_calls = 0
    
    sections = state.get("report_draft_sections", {})
    
    if result:
        sections.update(result)
    
    # Add module sections
    for module_id, section_text in module_sections.items():
        section_key = f"{module_id.upper()}_ANALYSIS"
        sections[section_key] = section_text
    
    return {
        "report_draft_sections": sections,
        "skeptic_flags": [],
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "narrative_drafter"
    }


# ============================================================================
# NODE: RED TEAM (QA)
# ============================================================================

def node_red_team(state: MarketReportState) -> dict:
    """Nodo: Quality Assurance - verifica il draft."""
    logger.info("[RedTeam] Fact-checking draft")
    
    llm = get_model()
    sections = state.get("report_draft_sections", {})
    stats = state.get("data_statistics", {})
    
    if not sections:
        return {"skeptic_flags": [], "current_node": "red_team"}
    
    draft_text = "\n\n".join([f"== {k} ==\n{v}" for k, v in sections.items()])
    
    prompt = f"""Fact-check this Market Monitor draft against the source data.

GROUND TRUTH DATA:
{json.dumps(stats, indent=2)}

DRAFT:
{draft_text}

Check for:
1. Numerical accuracy (prices, MoM%, YoY%)
2. Formatting (arrows ↑/↓)
3. Unsupported claims

Return JSON: {{"flags": [
    {{"section": "SECTION_NAME", "claim": "...", "issue_type": "numeracy_error|template_violation|unsupported_speculation", "severity": "high|medium|low", "details": "...", "recommendation": "..."}}
]}}

If no errors, return {{"flags": []}}"""
    
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        result = robust_json_parse(response)
        flags = result.get("flags", []) if result else []
        llm_calls = 1
    except Exception as e:
        logger.error(f"Red team check failed: {e}")
        flags = []
        llm_calls = 0
    
    return {
        "skeptic_flags": flags,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "red_team"
    }


# ============================================================================
# ROUTING & GRAPH BUILDER
# ============================================================================

MAX_CORRECTION_ATTEMPTS = 3

def should_correct(state: MarketReportState) -> Literal["correct", "finish"]:
    """Determina se servono correzioni."""
    flags = state.get("skeptic_flags", [])
    attempts = state.get("correction_attempts", 0)
    
    if flags and attempts < MAX_CORRECTION_ATTEMPTS:
        return "correct"
    return "finish"


def build_graph():
    """Costruisce il grafo LangGraph per Market Monitor."""
    
    graph = StateGraph(MarketReportState)
    
    # Add nodes
    graph.add_node("data_agent", node_data_agent)
    graph.add_node("graph_designer", node_graph_designer)
    graph.add_node("news_retrieval", node_news_retrieval)
    graph.add_node("event_mapper", node_event_mapper)
    graph.add_node("trend_analyst", node_trend_analyst)
    graph.add_node("module_orchestrator", node_module_orchestrator)
    graph.add_node("highlights_drafter", node_highlights_drafter)
    graph.add_node("narrative_drafter", node_narrative_drafter)
    graph.add_node("red_team", node_red_team)
    
    # Set entry point
    graph.set_entry_point("data_agent")
    
    # Linear flow
    graph.add_edge("data_agent", "graph_designer")
    graph.add_edge("graph_designer", "news_retrieval")
    graph.add_edge("news_retrieval", "event_mapper")
    graph.add_edge("event_mapper", "trend_analyst")
    graph.add_edge("trend_analyst", "module_orchestrator")
    graph.add_edge("module_orchestrator", "highlights_drafter")
    graph.add_edge("highlights_drafter", "narrative_drafter")
    graph.add_edge("narrative_drafter", "red_team")
    
    # QA Loop
    graph.add_conditional_edges(
        "red_team",
        should_correct,
        {
            "correct": "highlights_drafter",
            "finish": END
        }
    )
    
    return graph.compile()


# ============================================================================
# PUBLIC API
# ============================================================================

def run_report_generation(
    country: str,
    time_period: str,
    commodity_list: List[str],
    admin1_list: List[str],
    currency_code: str = "USD",
    enabled_modules: List[str] = None,
    previous_report_text: str = "",
    use_mock_data: bool = True
) -> dict:
    """
    Entry point per la generazione del Market Monitor.
    
    Returns:
        Stato finale con report completo
    """
    if enabled_modules is None:
        enabled_modules = ["exchange_rate"]
    
    initial_state = create_initial_state(
        country=country,
        time_period=time_period,
        commodity_list=commodity_list,
        admin1_list=admin1_list,
        currency_code=currency_code,
        enabled_modules=enabled_modules,
        previous_report_text=previous_report_text,
        use_mock_data=use_mock_data
    )
    
    agent = build_graph()
    return agent.invoke(initial_state)
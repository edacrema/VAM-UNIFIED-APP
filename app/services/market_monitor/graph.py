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
from typing import TypedDict, Annotated, Literal, List, Dict, Any, Optional, Callable

from collections import Counter
import operator

import pandas as pd
import numpy as np

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

from app.shared.llm import get_model
from app.shared.retrievers import ReliefWebRetriever, SeeristRetriever

from .data_loader import (
    extract_time_series_from_csv,
    calculate_statistics_from_csv,
    check_data_availability,
)

logger = logging.getLogger(__name__)

OnStepCallback = Callable[[str, Dict[str, Any]], None]


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

TERMINOLOGY_THRESHOLDS = {
    "hyperinflation": {"monthly_min": 50.0},
    "severe_inflation": {"yoy_min": 100.0},
    "high_inflation": {"yoy_min": 50.0},
    "severe_depreciation": {"yoy_min": 30.0, "mom_min": 10.0},
    "significant_depreciation": {"yoy_min": 15.0},
    "stable_currency": {"mom_range": (-5.0, 5.0)},
}


# ============================================================================
# STATE DEFINITION
# ============================================================================

class MarketReportState(TypedDict):
    """Stato principale del grafo."""
    
    # ===== INPUTS =====
    country: str
    time_period: str
    news_start_date: Optional[str]
    news_end_date: Optional[str]
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
    document_references: List[Dict[str, Any]]
    news_counts: Dict[str, int]
    retriever_traces: List[Dict[str, Any]]
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
    news_start_date: Optional[str] = None,
    news_end_date: Optional[str] = None,
    previous_report_text: str = "",
    use_mock_data: bool = False
) -> MarketReportState:
    """Crea stato iniziale per il grafo."""
    return MarketReportState(
        country=country,
        time_period=time_period,
        news_start_date=news_start_date,
        news_end_date=news_end_date,
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
        document_references=[],
        news_counts={"Seerist": 0, "ReliefWeb": 0, "total": 0},
        retriever_traces=[],
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


def _is_auxiliary_series(name: str) -> bool:
    s = str(name or "").strip().lower()
    if not s:
        return False
    patterns = [
        "exchange",
        "fx",
        "fuel",
        "petrol",
        "diesel",
        "gasoline",
        "wage",
        "salary",
        "labour",
        "labor",
        "milling",
        "transport",
        "freight",
    ]
    return any(p in s for p in patterns)


def _categorize_commodity(name: str) -> str:
    s = str(name or "").strip().lower()
    if not s:
        return "Other"

    if any(x in s for x in ["sorghum", "maize", "wheat", "rice", "millet", "bread", "teff", "barley", "flour"]):
        return "Cereals"
    if any(x in s for x in ["beans", "lentil", "pea", "chickpea", "pulse", "cowpea", "groundnut"]):
        return "Pulses"
    if "oil" in s:
        return "Oil"
    if "sugar" in s:
        return "Sugar"
    if "salt" in s:
        return "Condiments"
    if any(x in s for x in ["cabbage", "tomato", "onion", "vegetable", "leaves", "sukuma", "pumpkin", "cassava", "okra", "spinach"]):
        return "Vegetables"
    if "livestock" in s or any(x in s for x in ["goat", "sheep", "cattle", "chicken", "camel", "beef", "mutton"]):
        return "Livestock"

    return "Other"


def _slugify(text: str) -> str:
    s = str(text or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "other"


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    if size <= 0:
        return [list(items)]
    out: List[List[str]] = []
    for i in range(0, len(items), size):
        out.append(items[i : i + size])
    return out


def _commodity_importance_score(stats: Dict[str, Any], commodity: str) -> float:
    if not isinstance(stats, dict):
        return 0.0
    comm = stats.get("commodities") or {}
    if not isinstance(comm, dict):
        return 0.0
    s = comm.get(commodity) or {}
    if not isinstance(s, dict):
        return 0.0

    yoy = s.get("yoy_change_pct")
    mom = s.get("mom_change_pct")
    try:
        if yoy is not None:
            return abs(float(yoy))
    except Exception:
        pass
    try:
        if mom is not None:
            return abs(float(mom))
    except Exception:
        pass
    return 0.0


def _has_currency_depreciation_driver(drivers: Any) -> bool:
    if not drivers:
        return False
    try:
        for d in drivers:
            s = str(d or "").lower()
            if "currency" in s and (
                "depreciat" in s
                or "devalu" in s
                or "weak" in s
                or "collapse" in s
            ):
                return True
            if "fx" in s and "depreciat" in s:
                return True
    except Exception:
        return False
    return False


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

# EXCHANGE RATE MODULE
# ============================================================================

class ExchangeRateModule(ReportModule):
    """Modulo per l'analisi del tasso di cambio."""
    
    TE_API_BASE = "https://api.tradingeconomics.com"
    
    def __init__(self, api_key: Optional[str] = None):
        import os
        self.api_key = api_key or os.getenv("TE_API_KEY")
        if not self.api_key:
            raise RuntimeError("TE_API_KEY is required to run the exchange_rate module")
    
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
        raise RuntimeError("Mock exchange rate data generation is not allowed")

    def _fetch_historical_series(self, symbol: str, d1: str, d2: str) -> pd.DataFrame:
        url = f"{self.TE_API_BASE}/markets/historical/{symbol}"
        params = {"c": self.api_key, "d1": d1, "d2": d2, "f": "json"}
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()

        payload = resp.json()
        if not isinstance(payload, list) or not payload:
            raise RuntimeError(f"TradingEconomics returned no historical data for symbol '{symbol}'")

        df = pd.DataFrame(payload)
        if "Date" not in df.columns or "Close" not in df.columns:
            raise RuntimeError("TradingEconomics response missing required fields 'Date' and/or 'Close'")

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
        if df.empty:
            raise RuntimeError(f"TradingEconomics returned only invalid rows for symbol '{symbol}'")

        df = df.set_index("Date")
        return df[["Close"]]

    def _pct_change(self, current: float, previous: Optional[float]) -> Optional[float]:
        if previous is None:
            return None
        try:
            prev = float(previous)
            curr = float(current)
        except Exception:
            return None
        if prev == 0:
            return None
        return (curr - prev) / prev * 100.0
    
    def fetch_data(self, state: dict) -> Dict[str, Any]:
        """Recupera dati tasso di cambio."""
        logger.info(f"[ExchangeRateModule] Fetching data for {state['currency_code']}")
        
        currency_code = state["currency_code"]
        symbol = self._get_symbol(currency_code)

        try:
            period_start = pd.to_datetime(state["time_period"] + "-01")
        except Exception as e:
            raise ValueError(f"Invalid time_period: {state.get('time_period')}") from e

        period_end = (period_start + pd.offsets.MonthEnd(0)).normalize()
        end_dt = period_end.to_pydatetime()

        d2 = end_dt.strftime("%Y-%m-%d")
        d1 = (period_end - pd.Timedelta(days=400)).strftime("%Y-%m-%d")

        df = self._fetch_historical_series(symbol=symbol, d1=d1, d2=d2)
        df_upto_end = df.loc[:period_end]
        if df_upto_end.empty:
            raise RuntimeError(f"No exchange rate data for {symbol} up to {d2}")

        current_close = float(df_upto_end.iloc[-1]["Close"])

        def close_on_or_before(ts: pd.Timestamp) -> Optional[float]:
            sub = df_upto_end.loc[:ts]
            if sub.empty:
                return None
            return float(sub.iloc[-1]["Close"])

        prev_day = close_on_or_before(period_end - pd.Timedelta(days=1))
        prev_week = close_on_or_before(period_end - pd.Timedelta(days=7))
        prev_month = close_on_or_before(period_end - pd.DateOffset(months=1))
        prev_year = close_on_or_before(period_end - pd.DateOffset(years=1))

        daily_change_pct = self._pct_change(current_close, prev_day)
        weekly_change_pct = self._pct_change(current_close, prev_week)
        monthly_change_pct = self._pct_change(current_close, prev_month)
        yearly_change_pct = self._pct_change(current_close, prev_year)

        yoy_for_trend = yearly_change_pct if yearly_change_pct is not None else 0.0
        if yoy_for_trend > 30:
            trend = "rapid_depreciation"
        elif yoy_for_trend > 10:
            trend = "depreciation"
        elif yoy_for_trend < -10:
            trend = "appreciation"
        else:
            trend = "stable"

        data = {
            "symbol": symbol,
            "currency_code": currency_code,
            "current_rate": round(current_close, 6),
            "daily_change_pct": None if daily_change_pct is None else round(daily_change_pct, 2),
            "weekly_change_pct": None if weekly_change_pct is None else round(weekly_change_pct, 2),
            "monthly_change_pct": None if monthly_change_pct is None else round(monthly_change_pct, 2),
            "yearly_change_pct": None if yearly_change_pct is None else round(yearly_change_pct, 2),
            "trend": trend,
            "last_update": end_dt.isoformat(),
            "historical_data_json": df.to_json(date_format='iso'),
            "is_mock": False,
        }

        return {"exchange_rate_data": data}
    
    def generate_section(self, state: dict, llm) -> Dict[str, Any]:
        """Genera la sezione narrativa."""
        exchange_data = state.get("exchange_rate_data", {})
        
        if not exchange_data or exchange_data.get("current_rate") is None:
            raise RuntimeError("Exchange rate data is unavailable (no mock fallback is permitted)")
        
        prompt = f"""You are a WFP economic analyst writing the Exchange Rate Analysis section.

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.

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


def _select_default_commodities(available: List[str], max_items: int = 6) -> List[str]:
    """Select default food basket commodities from available list."""
    defaults = []

    priority_patterns = [
        "sorghum", "maize", "wheat", "rice",
        "beans", "lentil",
        "oil",
        "salt",
        "sugar",
    ]

    for pattern in priority_patterns:
        for commodity in available:
            if pattern in commodity.lower() and commodity not in defaults:
                defaults.append(commodity)
                break
        if len(defaults) >= max_items:
            break

    return defaults


def node_data_agent(state: MarketReportState) -> dict:
    """
    Nodo: Recupera e processa i dati.
    
    Supports two modes:
    - use_mock_data=True: Uses generated mock data for explicit testing only
    - use_mock_data=False: Loads Databridges price data

    Databridges failures are raised so users see actionable errors instead of
    silently receiving generated data.
    """
    logger.info(f"[DataAgent] Processing data for {state['country']}")

    country = state["country"]
    use_mock = state.get("use_mock_data", False)
    commodity_list = state.get("commodity_list", []) or []

    if not commodity_list and not use_mock:
        from .data_loader import (
            get_available_commodities,
        )
        try:
            available = get_available_commodities(country)
            commodity_list = _select_default_commodities(available)
            logger.info(f"[DataAgent] Auto-selected commodities: {commodity_list}")
        except Exception as e:
            raise RuntimeError(f"Could not auto-select Databridges commodities: {e}") from e

    warnings = []
    
    if use_mock:
        # =====================================================================
        # MOCK DATA MODE (Original behavior)
        # =====================================================================
        logger.info("[DataAgent] Using MOCK data generation")
        df_national, df_regional = generate_mock_time_series(
            state["country"],
            state["time_period"],
            commodity_list,
            state["admin1_list"]
        )
        stats = calculate_statistics(df_national)
        
    else:
        # =====================================================================
        # DATABRIDGES DATA MODE
        # =====================================================================
        logger.info("[DataAgent] Loading data from Databridges")
        
        try:
            # First, check what data is available
            availability = check_data_availability(
                country=state["country"],
                time_period=state["time_period"],
                commodities=commodity_list
            )
            
            # If country not available, raise error
            if not availability["available"]:
                raise ValueError(
                    f"Country '{state['country']}' not found in price data. "
                    f"Available countries: {availability['countries']}"
                )
            
            # Add any warnings from availability check
            if availability.get("warnings"):
                warnings.extend(availability["warnings"])
            
            # Extract time series from Databridges
            df_national, df_regional = extract_time_series_from_csv(
                country=state["country"],
                time_period=state["time_period"],
                commodities=commodity_list,
                admin1_list=state["admin1_list"]
            )
            
            # Calculate statistics using the existing report statistics contract
            stats = calculate_statistics_from_csv(
                df_national, 
                commodity_list
            )
            food_basket_stats = stats.get("food_basket", {}) if isinstance(stats, dict) else {}
            missing_latest_components = food_basket_stats.get("missing_latest_component_names") or []
            selected_component_count = food_basket_stats.get("selected_component_count")
            latest_component_count = food_basket_stats.get("latest_component_count")
            latest_component_names = food_basket_stats.get("latest_component_names") or []
            if missing_latest_components and selected_component_count:
                warnings.append(
                    f"Food basket for {state['time_period']} is based on "
                    f"{latest_component_count} of {selected_component_count} selected commodities "
                    f"with target-month data ({', '.join(latest_component_names) or 'none'}). "
                    f"Missing target-month components: {', '.join(missing_latest_components)}."
                )
            
            logger.info(
                f"[DataAgent] Successfully loaded {len(df_national)} months of data "
                f"with {len(df_national.columns)} columns"
            )
            
        except Exception as e:
            logger.exception(f"[DataAgent] Failed to load Databridges price data: {e}")
            raise
    
    # =========================================================================
    # RETURN STATE UPDATE
    # =========================================================================
    return {
        "commodity_list": commodity_list,
        "time_series_data_national": df_national.to_json(date_format='iso'),
        "time_series_data_regional": df_regional.to_json(date_format='iso'),
        "data_statistics": stats,
        "warnings": warnings,
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
        
        # 2. Commodity Trends (Grouped)
        stats = state.get("data_statistics", {}) or {}
        commodity_cols = []
        for c in df_national.columns:
            if c == "FoodBasket":
                continue
            if _is_auxiliary_series(c):
                continue
            try:
                if df_national[c].dropna().empty:
                    continue
            except Exception:
                pass
            commodity_cols.append(c)

        if commodity_cols:
            grouped: Dict[str, List[str]] = {}
            for c in commodity_cols:
                cat = _categorize_commodity(c)
                grouped.setdefault(cat, []).append(c)

            category_order = ["Cereals", "Pulses", "Oil", "Sugar", "Condiments", "Vegetables", "Livestock", "Other"]
            ordered_categories = [c for c in category_order if c in grouped]
            for extra in sorted([c for c in grouped.keys() if c not in ordered_categories]):
                ordered_categories.append(extra)

            max_lines_per_chart = 6
            for cat in ordered_categories:
                cols = grouped.get(cat) or []
                cols = sorted(
                    cols,
                    key=lambda x: (-_commodity_importance_score(stats, x), str(x).lower()),
                )

                pages = _chunk_list(cols, max_lines_per_chart)
                cat_slug = _slugify(cat)
                for page_idx, page_cols in enumerate(pages, start=1):
                    fig, ax = plt.subplots(figsize=(12, 6))
                    for col in page_cols:
                        ax.plot(df_national.index, df_national[col], marker='o', label=col)
                    title_suffix = f"{cat}"
                    if len(pages) > 1:
                        title_suffix = f"{cat} (Page {page_idx}/{len(pages)})"
                    ax.set_title(f"Commodity Price Trends - {state['country']} - {title_suffix}", fontweight='bold')
                    ax.set_ylabel("Price (LCU)")
                    ax.legend(loc='upper left')
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
                    plt.xticks(rotation=45)
                    plt.tight_layout()

                    buf = io.BytesIO()
                    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
                    plt.close()
                    buf.seek(0)
                    fig_id = f"commodity_trends_{cat_slug}_p{page_idx}"
                    fig_b64 = base64.b64encode(buf.read()).decode('utf-8')
                    visualizations[fig_id] = fig_b64
                    if "commodity_trends" not in visualizations:
                        visualizations["commodity_trends"] = fig_b64
        
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
# NODE: NEWS RETRIEVAL 
# ============================================================================

def node_news_retrieval(state: MarketReportState) -> dict:
    """Nodo: Recupera notizie (mock per ora)."""
    logger.info(f"[NewsRetrieval] Fetching news for {state['country']}")

    documents: List[Dict[str, Any]] = []
    retriever_traces: List[Dict[str, Any]] = []
    warnings: List[str] = []

    country = state.get("country", "")
    time_period = state.get("time_period", "")

    explicit_start = (state.get("news_start_date") or "").strip()
    explicit_end = (state.get("news_end_date") or "").strip()

    if explicit_start and explicit_end:
        start_date = explicit_start[:10]
        end_date = explicit_end[:10]
    else:
        try:
            start_dt = datetime.strptime(time_period + "-01", "%Y-%m-%d")
        except Exception:
            start_dt = datetime.utcnow().replace(day=1)

        end_dt = (start_dt + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        prev_month_start = (start_dt - timedelta(days=1)).replace(day=1)
        start_date = prev_month_start.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")
    

    rw = ReliefWebRetriever(verbose=False)
    rw_query = ReliefWebRetriever.build_economy_query(
        extra_terms=["food security", "supply", "shortage", "subsidy"]
    )
    rw_docs = rw.fetch(country=country, start_date=start_date, end_date=end_date, max_records=10, query=rw_query)
    if getattr(rw, "last_trace", None):
        retriever_traces.append(rw.last_trace)

    seerist = SeeristRetriever(verbose=False)
    seerist_queries = [
        SeeristRetriever.build_lucene_or_query(
            list(SeeristRetriever.DEFAULT_ECON_TERMS)
            + ["food security", "wheat", "sorghum", "rice", "cooking oil"]
        ),
        SeeristRetriever.build_lucene_or_query(
            ["market", "food security", "inflation", "currency", "availability"]
        ),
        "",
    ]
    seerist_docs = seerist.fetch_batch(
        queries=seerist_queries,
        start_date=start_date,
        end_date=end_date,
        country=country,
        max_per_query=10,
    )
    if len(seerist_docs) > 10:
        seerist_docs = seerist_docs[:10]
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
    documents = deduped

    refs = [
        {
            "doc_id": d.get("doc_id"),
            "source": d.get("source"),
            "title": d.get("title"),
            "url": d.get("url"),
            "date": d.get("date"),
        }
        for d in documents
    ]

    counts = Counter([d.get("source", "Unknown") for d in documents])
    news_counts = {
        "Seerist": int(counts.get("Seerist", 0)),
        "ReliefWeb": int(counts.get("ReliefWeb", 0)),
        "total": int(len(documents)),
    }

    updates = {
        "documents": documents,
        "document_references": refs,
        "news_counts": news_counts,
        "retriever_traces": retriever_traces,
        "current_node": "news_retrieval",
    }
    if warnings:
        updates["warnings"] = warnings
    return updates


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

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.

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
    
    prompt = f"""Analyze the market trend based on these inputs.

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.

QUANTITATIVE DATA (use for specific claims about current status; do not invent metrics):
{json.dumps(stats, indent=2)}

CONTEXTUAL EVENTS (use for background only, NOT as primary drivers unless supported by quantitative data):
{json.dumps(events, indent=2)}

TERMINOLOGY THRESHOLDS (enforce in wording; do not use stronger terms unless thresholds are met):
{json.dumps(TERMINOLOGY_THRESHOLDS, indent=2)}

RULES:
- Key market drivers MUST be supported by quantitative data above (prices/food basket/auxiliary where available).
- Contextual events can explain *why* a quantitative trend might exist, but cannot replace the data.
- If contextual documents mention issues (e.g., "currency pressure") but quantitative data shows stability,
  note the discrepancy rather than asserting the contextual claim as current fact.
- Distinguish between "historically X has been a problem" vs "currently X is occurring".
- If quantitative coverage is missing/insufficient, explicitly say so and keep key_market_drivers empty or generic (e.g., "insufficient data").

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
            "trajectory": "unknown",
            "key_market_drivers": [],
            "note": "Trend analysis failed - no drivers inferred",
            "commodity_analysis": {},
            "regional_analysis": {},
            "outlook": "Trend analysis unavailable due to an internal error."
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
    warnings: List[str] = []
    
    for module_id in enabled_modules:
        if module_id not in AVAILABLE_MODULES:
            logger.warning(f"Unknown module: {module_id}")
            continue

        if module_id == "exchange_rate":
            currency_code = str(state.get("currency_code") or "").strip().upper()
            if not currency_code or currency_code == "USD":
                warnings.append(
                    "Skipped exchange_rate module because currency_code is USD (no exchange-rate pair to fetch)."
                )
                continue
        
        try:
            module_class = AVAILABLE_MODULES[module_id]
            module = module_class()
            
            if not module.validate_inputs(state):
                if module_id == "exchange_rate":
                    missing = [
                        f
                        for f in getattr(module, "required_inputs", [])
                        if f not in state or state[f] is None
                    ]
                    warnings.append(f"Skipped exchange_rate module due to missing required inputs: {missing}")
                    continue
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
            if module_id == "exchange_rate":
                warnings.append(f"Skipped exchange_rate module due to TradingEconomics error: {e}")
            continue
    
    updates["module_sections"] = module_sections
    if warnings:
        updates["warnings"] = warnings
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
    exchange_data = state.get("exchange_rate_data", {}) or {}
 
    validation_warnings: List[str] = []
    if exchange_data and exchange_data.get("trend") == "stable":
        drivers = (trend or {}).get("key_market_drivers") or []
        if _has_currency_depreciation_driver(drivers):
            validation_warnings.append(
                "Exchange rate classification is 'stable' but trend_analysis.key_market_drivers references currency depreciation. Avoid asserting current depreciation; if mentioned, frame as context/discrepancy."
            )
     
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

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.

DATA (with formatted MoM/YoY):
{json.dumps(formatted_stats, indent=2)}

EXCHANGE RATE (quantitative ground truth, if available):
{json.dumps(exchange_data, indent=2) if exchange_data else "None"}

TREND ANALYSIS:
{json.dumps(trend, indent=2)}

TERMINOLOGY THRESHOLDS (enforce in wording):
{json.dumps(TERMINOLOGY_THRESHOLDS, indent=2)}

VALIDATION WARNINGS (must obey):
{json.dumps(validation_warnings, indent=2)}

Include:
1. Overview (1-2 sentences)
2. Food Basket Cost with MoM% and YoY%
3. Top 3 Commodities with changes
4. Key Drivers (bullet list)

RULES:
- Key Drivers must be supported by the quantitative DATA above.
- Do not treat contextual drivers (from TREND ANALYSIS) as current facts if they conflict with exchange-rate ground truth.
- If exchange rate is stable, do not claim current currency depreciation as a key driver.

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
    
    updates: Dict[str, Any] = {
        "report_draft_sections": sections,
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "correction_attempts": correction_attempts,
        "current_node": "highlights_drafter"
    }
    if validation_warnings:
        updates["warnings"] = validation_warnings
    return updates


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

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.

TREND ANALYSIS:
{json.dumps(trend, indent=2)}

EVENTS:
{json.dumps(events, indent=2)}

MODULE SECTIONS TO REFERENCE:
{json.dumps(module_sections, indent=2) if module_sections else "None"}

SECTIONS TO GENERATE:
1. MARKET_OVERVIEW (200-250 words): Overall market conditions, price trajectory, key drivers.
2. COMMODITY_ANALYSIS (200-300 words): Analysis of each commodity.
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
    
    document_references = state.get("document_references", []) or []
    if document_references:
        lines = ["REFERENCES"]
        for ref in document_references:
            doc_id = ref.get("doc_id", "")
            source = ref.get("source", "")
            date = ref.get("date", "")
            title = ref.get("title", "")
            url = ref.get("url", "")
            lines.append(f"[{doc_id}] {source} ({date}) {title}")
            if url:
                lines.append(url)
            lines.append("")
        sections["REFERENCES"] = "\n".join(lines).strip()
    
    return {
        "report_draft_sections": sections,
        "skeptic_flags": [],
        "llm_calls": state.get("llm_calls", 0) + llm_calls,
        "current_node": "narrative_drafter"
    }

# NODE: RED TEAM (QA)
# ============================================================================

def node_red_team(state: MarketReportState) -> dict:
    """Nodo: Quality Assurance - verifica il draft."""
    logger.info("[RedTeam] Fact-checking draft")
    
    llm = get_model()
    sections = state.get("report_draft_sections", {})
    stats = state.get("data_statistics", {})
    trend = state.get("trend_analysis", {}) or {}
    exchange_data = state.get("exchange_rate_data", {}) or {}
     
    if not sections:
        return {"skeptic_flags": [], "current_node": "red_team"}
     
    draft_text = "\n\n".join([f"== {k} ==\n{v}" for k, v in sections.items()])
     
    prompt = f"""Fact-check this Market Monitor draft.

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.

QUANTITATIVE GROUND TRUTH:
- Price Statistics: {json.dumps(stats, indent=2)}
- Exchange Rate: MoM={exchange_data.get('monthly_change_pct')}%, YoY={exchange_data.get('yearly_change_pct')}%, Classification={exchange_data.get('trend')}
  Full Object: {json.dumps(exchange_data, indent=2) if exchange_data else "None"}

TREND ANALYSIS (for cross-validation of drivers; treat as hypotheses unless supported by data):
{json.dumps(trend, indent=2)}

TERMINOLOGY THRESHOLDS (must be enforced):
{json.dumps(TERMINOLOGY_THRESHOLDS, indent=2)}

DRAFT:
{draft_text}

CROSS-VALIDATION RULES:
1. If exchange rate classification is "stable", the report must NOT claim "severe currency depreciation" (or similar) as a current driver.
2. Cross-check trend_analysis.key_market_drivers against exchange-rate classification and quantitative thresholds (do not allow drivers that contradict the ground truth).
3. If the draft uses terms like "severe depreciation" or "rapid depreciation", they must satisfy TERMINOLOGY_THRESHOLDS.severe_depreciation using exchange-rate MoM/YoY.
4. "Hyperinflation" requires monthly inflation > 50%. If no inflation metric exists in the ground truth data, any use of "hyperinflation" is a terminology misuse.
5. Key drivers in HIGHLIGHTS must not contradict data in specialized sections (e.g., EXCHANGE_RATE_ANALYSIS).
6. Distinguish between historical context and current data claims.
7. Flag internal contradictions between sections (e.g., one section says stable while another says rapid depreciation).

Check for:
1. Numerical accuracy (prices, MoM%, YoY%)
2. Formatting (arrows ↑/↓)
3. Unsupported claims
4. Context-data conflicts and terminology misuse

 
Return JSON: {{"flags": [
    {{"section": "SECTION_NAME", "claim": "...", "issue_type": "numeracy_error|template_violation|unsupported_speculation|context_data_conflict|terminology_misuse|internal_contradiction", "severity": "high|medium|low", "details": "...", "recommendation": "..."}}
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


def build_graph(on_step: Optional[OnStepCallback] = None):
    """Costruisce il grafo LangGraph per Market Monitor."""
    
    def wrap_node(node_name: str, fn):
        def wrapped(state: MarketReportState):
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

    graph = StateGraph(MarketReportState)
    
    # Add nodes
    graph.add_node("data_agent", wrap_node("data_agent", node_data_agent))
    graph.add_node("graph_designer", wrap_node("graph_designer", node_graph_designer))
    graph.add_node("news_retrieval", wrap_node("news_retrieval", node_news_retrieval))
    graph.add_node("event_mapper", wrap_node("event_mapper", node_event_mapper))
    graph.add_node("trend_analyst", wrap_node("trend_analyst", node_trend_analyst))
    graph.add_node("module_orchestrator", wrap_node("module_orchestrator", node_module_orchestrator))
    graph.add_node("highlights_drafter", wrap_node("highlights_drafter", node_highlights_drafter))
    graph.add_node("narrative_drafter", wrap_node("narrative_drafter", node_narrative_drafter))
    graph.add_node("red_team", wrap_node("red_team", node_red_team))
    
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
    news_start_date: Optional[str] = None,
    news_end_date: Optional[str] = None,
    previous_report_text: str = "",
    use_mock_data: bool = False,
    on_step: Optional[OnStepCallback] = None
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
        news_start_date=news_start_date,
        news_end_date=news_end_date,
        previous_report_text=previous_report_text,
        use_mock_data=use_mock_data
    )
    
    agent = build_graph(on_step=on_step)
    return agent.invoke(initial_state)

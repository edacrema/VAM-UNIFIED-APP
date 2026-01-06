"""
Price Validator - Graph
=======================
Logica di validazione Price Data con LangGraph (Layer 0-4).

Layer:
- Layer 0: File Validation (deterministic)
- Layer 1: Structural Parsing (deterministic)
- Layer 2: Schema Validation (deterministic)
- Layer 3: Product Classification (deterministic + LLM)
- Layer 4: Diagnosis & Reporting (LLM)
"""
from __future__ import annotations

import csv
import io
import re
import json
import logging
from pathlib import Path
from typing import TypedDict, Annotated, Literal
from collections import Counter
import operator

import pandas as pd
import chardet

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage

from app.shared.llm import get_model
from .schemas import ValidationError, LayerResult, PriceDataTemplate, Severity

logger = logging.getLogger(__name__)


# ============================================================================
# WFP PRODUCTS DICTIONARY
# ============================================================================

WFP_PRODUCTS = {
    "Apples": 144, "Apples (golden delicious)": 629, "Apples (red delicious)": 628,
    "Apricots": 195, "Apricots (dry)": 236, "Avocados": 157,
    "Bananas": 89, "Bananas (cooking)": 457, "Bananas (green)": 456,
    "Bananas (ripe)": 289, "Batteries (big)": 808,
    "Batteries (small)": 807, "Battery lamp": 1238, "Bean (mung dry)": 1211,
    "Beans": 50, "Beans (black gram)": 923, "Beans (black)": 85,
    "Beans (black dry)": 1259, "Beans (kidney red)": 206, "Beans (kidney white)": 389,
    "Beans (kidney)": 180, "Beans (mung)": 393, "Beans (niebe)": 120,
    "Beans (red)": 78, "Beans (white)": 66,
    "Bread": 55, "Bread (brown)": 159, "Bread (white)": 375, "Bread (wheat)": 375,
    "Butter": 372, "Cabbage": 181, "Carrots": 166,
    "Cashew nut": 167, "Cassava": 68, "Cassava (dry)": 290, "Cassava (fresh)": 291,
    "Cassava flour": 74, "Cassava meal": 217, "Cassava meal (gari)": 403,
    "Charcoal": 446, "Cheese": 414, "Chicken": 182, "Chickpeas": 244,
    "Chili": 820, "Chili (green)": 90, "Chili (red)": 91,
    "Rice": 52, "Rice (aromatic)": 894, "Rice (basmati)": 1172,
    "Rice (broken imported)": 438, "Rice (coarse)": 60, "Rice (fortified)": 1280,
    "Rice (high quality)": 247, "Rice (imported)": 64, "Rice (local)": 71,
    "Rice (long grain)": 162, "Rice (low quality)": 145, "Rice (medium grain)": 131,
    "Rice (paddy)": 203, "Rice (white)": 133, "Rice (white imported)": 400,
    "Sorghum": 65, "Sorghum (brown)": 481, "Sorghum (local)": 511,
    "Sorghum (red)": 282, "Sorghum (white)": 135, "Sorghum flour": 253,
    "Sugar": 97, "Sugar (brown)": 214, "Sugar (white)": 349, "Sugar (local)": 754,
    "Salt": 185, "Salt (iodised)": 334, "Salt (imported)": 823, "Salt (local)": 822,
    "Wheat": 84, "Wheat flour": 58, "Wheat flour (imported)": 339,
    "Wheat flour (local)": 179, "Wheat flour (fortified)": 496,
    "Oil (vegetable)": 189, "Oil (palm)": 192, "Oil (groundnut)": 190,
    "Oil (cooking)": 188, "Oil (sunflower)": 191, "Oil (imported)": 378,
    "Maize": 56, "Maize (white)": 136, "Maize (yellow)": 137, "Maize flour": 63,
    "Maize (local)": 512, "Maize (imported)": 202,
    "Lentils": 164, "Lentils (local)": 395, "Lentils (imported)": 396,
    "Eggs": 92, "Eggs (local)": 430, "Eggs (imported)": 298,
    "Milk": 93, "Milk (fresh)": 352, "Milk (powdered)": 186, "Milk (imported)": 353,
    "Meat (beef)": 94, "Meat (goat)": 140, "Meat (sheep)": 141, "Meat (camel)": 312,
    "Fish": 139, "Fish (fresh)": 401, "Fish (dry)": 171, "Fish (frozen)": 486,
    "Potatoes": 83, "Potatoes (Irish)": 148, "Potatoes (local)": 687,
    "Onions": 100, "Onions (red)": 168, "Onions (white)": 169,
    "Tomatoes": 114, "Tomatoes (local)": 346, "Tomatoes (paste)": 317,
    "Fuel (diesel)": 447, "Fuel (petrol)": 448, "Fuel (kerosene)": 495,
    "Firewood": 804,
    "Transport (public)": 304, "Exchange rate": 305, "Wage (casual labour)": 276,
    "Millet": 57, "Millet (finger)": 147, "Millet (pearl)": 146,
    "Groundnuts": 105, "Groundnuts (shelled)": 327, "Groundnuts (unshelled)": 328,
    "Cowpeas": 119, "Cowpeas (white)": 491, "Cowpeas (brown)": 879,
    "Peas": 165, "Peas (dry)": 265, "Peas (green)": 266,
    "Garlic": 170, "Ginger": 194, "Peppers": 116, "Peppers (green)": 212,
    "Spinach": 210, "Lettuce": 211, "Cucumber": 213, "Eggplant": 215,
    "Pumpkin": 216, "Okra": 218, "Watermelon": 219,
    "Oranges": 143, "Lemons": 172, "Mangoes": 158, "Papaya": 160,
    "Pineapple": 161, "Grapes": 196, "Dates": 197,
    "Tea": 187, "Coffee": 183, "Cocoa": 184,
    "Soap": 449, "Soap (bar)": 450, "Soap (washing)": 451,
    "Water (bottled)": 452, "Water (mineral)": 453
}

# Lista come stringa per i prompt LLM
WFP_PRODUCT_LIST_STR = "\n".join([f"{name}\t{id}" for name, id in WFP_PRODUCTS.items()])


# ============================================================================
# EXPECTED COLUMNS AND CONSTANTS
# ============================================================================

EXPECTED_PRICE_DATA_COLUMNS = {
    'commodity', 'product', 'item', 'commodity_name', 'product_name',
    'price', 'value', 'cost', 'unit_price',
    'market', 'market_name', 'location',
    'date', 'month', 'year', 'period',
    'unit', 'unit_of_measure', 'uom',
    'currency', 'curr',
    'adm0', 'adm1', 'adm2', 'country', 'region', 'district'
}

REQUIRED_COLUMN_CATEGORIES = {
    'product': {'commodity', 'product', 'item', 'commodity_name', 'product_name'},
    'price': {'price', 'value', 'cost', 'unit_price'},
    'date': {'date', 'month', 'year', 'period'}
}

MONTHS_EN = ['january', 'february', 'march', 'april', 'may', 'june',
             'july', 'august', 'september', 'october', 'november', 'december',
             'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

MONTHS_FR = ['janvier', 'février', 'mars', 'avril', 'mai', 'juin',
             'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre',
             'janv', 'févr', 'avr', 'juil', 'sept', 'déc']

MONTHS_ES = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
             'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre',
             'ene', 'feb', 'mar', 'abr', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']

MONTHS_AR = ['يناير', 'فبراير', 'مارس', 'أبريل', 'مايو', 'يونيو',
             'يوليو', 'أغسطس', 'سبتمبر', 'أكتوبر', 'نوفمبر', 'ديسمبر']

ALL_MONTHS = {
    'en': MONTHS_EN,
    'fr': MONTHS_FR,
    'es': MONTHS_ES,
    'ar': MONTHS_AR
}


# ============================================================================
# PROMPTS
# ============================================================================

PROMPTS = {
    "batch_product_classification": {
        "system": """You are a food product classification expert for WFP.
You will classify multiple products at once against the WFP standard product list.

For each product, find the best matching WFP standard product.
Consider common variations, typos, abbreviations, and language differences.

Respond ONLY with valid JSON array:
[
    {
        "original_name": "input name",
        "matched_name": "WFP standard name or null",
        "product_id": id_or_null,
        "confidence": 0.0-1.0
    },
    ...
]""",
        "user": """Classify these products:
{products_to_classify}

Against this WFP product list:
{product_list}"""
    },
    
    "diagnosis_report": {
        "system": """You are a WFP data quality analyst producing a formal technical report for a Price Data dataset validation.

STYLE AND OUTPUT RULES (MANDATORY):
- Language: English only.
- Tone: formal, technical, concise.
- Do NOT include greetings, salutations, or sign-offs.
- Do NOT address the reader directly.
- Do NOT mention being an AI, a model, or the prompt.
- Do NOT add filler such as "Certainly", "Here is", "I have analyzed".
- Output ONLY the report text (no markdown code fences).

REPORT STRUCTURE (MANDATORY):
1) Title
2) Dataset Metadata
3) Executive Summary
4) Critical Issues (Blocking)
5) Non-Blocking Findings / Warnings
6) Product Classification Summary
7) Recommended Actions (prioritized, step-by-step)
8) Final Checklist

Use only the validation results provided. Do not invent issues not supported by the validation results.""",
        "user": """Generate a formal technical validation report for this Price Data dataset.

File: {file_name}
Country: {country}
Number of products: {num_products}
Number of markets: {num_markets}
File type: {file_type}
Detected language: {detected_language}

VALIDATION RESULTS (by layer):
{validation_results}

PRODUCT CLASSIFICATIONS:
{product_classifications}
"""
    }
}


# ============================================================================
# STATE DEFINITION
# ============================================================================

class PriceDataState(TypedDict):
    """Stato tipizzato per il grafo LangGraph."""
    # Input
    file_path: str
    template_path: str | None
    template: dict | None
    
    # Computed during execution
    encoding: str | None
    delimiter: str | None
    file_type: str | None
    dataframe_json: str | None
    detected_language: str | None
    template_df_json: str | None
    product_classifications: list
    
    # Results accumulator
    layer_results: Annotated[list[dict], operator.add]
    
    # Control flow
    can_continue: bool
    current_layer: int
    llm_calls: int
    
    # Output
    final_report: str | None
    
    # Metadata
    file_name: str
    country: str | None
    num_products: int | None
    num_markets: int | None


def create_initial_state(
    file_path: str,
    template_path: str | None = None
) -> PriceDataState:
    """Crea stato iniziale per il grafo."""
    
    template_dict = None
    if template_path:
        if template_path.endswith('.xlsx') or template_path.endswith('.xls'):
            template_dict = PriceDataTemplate.from_excel(template_path).to_dict()
        elif template_path.endswith('.csv'):
            template_dict = PriceDataTemplate.from_csv(template_path).to_dict()
    
    return PriceDataState(
        file_path=file_path,
        template_path=template_path,
        template=template_dict,
        encoding=None,
        delimiter=None,
        file_type=None,
        dataframe_json=None,
        detected_language=None,
        template_df_json=None,
        product_classifications=[],
        layer_results=[],
        can_continue=True,
        current_layer=0,
        llm_calls=0,
        final_report=None,
        file_name=Path(file_path).name,
        country=None,
        num_products=None,
        num_markets=None
    )


# ============================================================================
# LAYER 0: FILE VALIDATION (DETERMINISTIC)
# ============================================================================

def layer0_file_validation(state: PriceDataState) -> dict:
    """
    Layer 0: Validazione file base.
    Supporta sia CSV che Excel.
    """
    errors = []
    warnings = []
    metadata = {}
    
    file_path = Path(state["file_path"])
    
    # F0.0: File exists
    if not file_path.exists():
        errors.append(ValidationError(
            code="F0.0",
            severity=Severity.CRITICAL,
            message="File non trovato"
        ))
        result = LayerResult(
            layer_id=0,
            layer_name="File Validation",
            passed=False,
            can_continue=False,
            errors=errors,
            metadata=metadata
        )
        return {
            "layer_results": [result.to_dict()],
            "can_continue": False,
            "current_layer": 0
        }
    
    # F0.1: Extension check
    suffix = file_path.suffix.lower()
    detected_file_type = None
    
    if suffix in ['.xlsx', '.xls']:
        detected_file_type = "EXCEL"
        metadata['file_type'] = 'EXCEL'
    elif suffix == '.csv':
        detected_file_type = "CSV"
        metadata['file_type'] = 'CSV'
    else:
        errors.append(ValidationError(
            code="F0.1",
            severity=Severity.CRITICAL,
            message=f"Formato file non supportato: {suffix}",
            suggestion="Usare file Excel (.xlsx) o CSV (.csv)"
        ))
    
    # F0.2: Binary file detection (for CSV)
    detected_encoding = None
    if suffix == '.csv':
        try:
            with open(file_path, 'rb') as f:
                header = f.read(8)
            
            if header[:4] == b'PK\x03\x04':  # XLSX
                errors.append(ValidationError(
                    code="F0.2",
                    severity=Severity.CRITICAL,
                    message="File è Excel (.xlsx), non CSV",
                    suggestion="Rinominare in .xlsx o esportare come CSV"
                ))
            elif header[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':  # XLS
                errors.append(ValidationError(
                    code="F0.2",
                    severity=Severity.CRITICAL,
                    message="File è Excel (.xls), non CSV",
                    suggestion="Rinominare in .xls o esportare come CSV"
                ))
        except Exception as e:
            logger.warning(f"Binary detection failed: {e}")
        
        # F0.3: Encoding detection
        try:
            with open(file_path, 'rb') as f:
                raw = f.read(100000)
            
            if raw.startswith(b'\xef\xbb\xbf'):
                detected_encoding = 'utf-8-sig'
                metadata['bom_detected'] = 'UTF-8 BOM'
            elif raw.startswith(b'\xff\xfe'):
                detected_encoding = 'utf-16-le'
                metadata['bom_detected'] = 'UTF-16 LE BOM'
            else:
                result = chardet.detect(raw)
                detected_encoding = result.get('encoding', 'utf-8')
                metadata['chardet_confidence'] = result.get('confidence', 0)
            
            metadata['detected_encoding'] = detected_encoding
            
        except Exception as e:
            warnings.append(ValidationError(
                code="F0.3",
                severity=Severity.WARNING,
                message=f"Impossibile rilevare encoding: {e}"
            ))
            detected_encoding = 'utf-8'
    
    has_critical = any(e.severity == Severity.CRITICAL for e in errors)
    
    result = LayerResult(
        layer_id=0,
        layer_name="File Validation",
        passed=len(errors) == 0,
        can_continue=not has_critical,
        errors=errors,
        warnings=warnings,
        metadata=metadata
    )
    
    return {
        "layer_results": [result.to_dict()],
        "can_continue": not has_critical,
        "current_layer": 0,
        "encoding": detected_encoding,
        "file_type": detected_file_type
    }


# ============================================================================
# LAYER 1: STRUCTURAL PARSING (DETERMINISTIC)
# ============================================================================

def detect_delimiter_robust(content: str) -> tuple[str, dict]:
    """Rileva delimiter contando occorrenze fuori dalle virgolette."""
    lines = content.split('\n')[:20]
    candidates = [',', ';', '\t', '|']
    scores = {}
    
    for delim in candidates:
        counts = []
        for line in lines:
            if not line.strip():
                continue
            in_quotes = False
            count = 0
            for char in line:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == delim and not in_quotes:
                    count += 1
            counts.append(count)
        
        if counts:
            mean_count = sum(counts) / len(counts)
            variance = sum((c - mean_count) ** 2 for c in counts) / len(counts) if len(counts) > 1 else 0
            scores[delim] = {
                'mean': mean_count,
                'variance': variance,
                'score': mean_count / (variance + 0.1) if mean_count > 0 else 0
            }
    
    best_delim = max(scores.keys(), key=lambda d: scores[d]['score']) if scores else ','
    return best_delim, scores


def layer1_structural_parsing(state: PriceDataState) -> dict:
    """
    Layer 1: Parsing strutturale.
    Carica il file e rileva la struttura.
    """
    errors = []
    warnings = []
    metadata = {}
    
    file_path = state["file_path"]
    file_type = state["file_type"]
    encoding = state["encoding"] or 'utf-8'
    
    df = None
    delimiter = None
    
    # Load based on file type
    if file_type == "EXCEL":
        try:
            df = pd.read_excel(file_path)
            metadata['sheets_loaded'] = 1
        except Exception as e:
            errors.append(ValidationError(
                code="S1.0",
                severity=Severity.CRITICAL,
                message=f"Impossibile leggere file Excel: {e}"
            ))
    
    elif file_type == "CSV":
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                content = f.read()
            
            # Detect delimiter
            delimiter, delim_scores = detect_delimiter_robust(content)
            metadata['detected_delimiter'] = delimiter
            metadata['delimiter_scores'] = {k: v['score'] for k, v in delim_scores.items()}
            
            if delimiter == ';':
                warnings.append(ValidationError(
                    code="S1.1",
                    severity=Severity.WARNING,
                    message="Delimitatore ';' rilevato (tipico tastiera francese)",
                    suggestion="Usare ',' come separatore standard"
                ))
            
            # Check for broken rows
            broken_rows = []
            buffer = io.StringIO(content)
            reader = csv.reader(buffer, delimiter=delimiter)
            header = next(reader)
            expected_cols = len(header)
            metadata['expected_columns'] = expected_cols
            metadata['header'] = header[:10]
            
            for i, row in enumerate(reader, start=2):
                if len(row) != expected_cols:
                    broken_rows.append({
                        'row_number': i,
                        'expected': expected_cols,
                        'actual': len(row)
                    })
                    if len(broken_rows) >= 10:
                        break
            
            if broken_rows:
                errors.append(ValidationError(
                    code="S1.2",
                    severity=Severity.CRITICAL,
                    message=f"Rilevate {len(broken_rows)}+ righe con numero colonne errato",
                    details={'broken_rows': broken_rows},
                    suggestion="Verificare ritorni a capo dentro celle"
                ))
            
            # Load DataFrame
            df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, on_bad_lines='warn')
            
        except Exception as e:
            errors.append(ValidationError(
                code="S1.0",
                severity=Severity.CRITICAL,
                message=f"Impossibile leggere il file CSV: {e}"
            ))
    
    if df is not None:
        metadata['total_rows'] = len(df)
        metadata['total_columns'] = len(df.columns)
        metadata['columns'] = df.columns.tolist()[:20]
    
    has_critical = any(e.severity == Severity.CRITICAL for e in errors)
    
    result = LayerResult(
        layer_id=1,
        layer_name="Structural Parsing",
        passed=len(errors) == 0,
        can_continue=not has_critical and df is not None,
        errors=errors,
        warnings=warnings,
        metadata=metadata
    )
    
    # Serialize DataFrame
    df_json = df.to_json(orient='split', date_format='iso') if df is not None else None
    
    return {
        "layer_results": [result.to_dict()],
        "can_continue": not has_critical and df is not None,
        "current_layer": 1,
        "delimiter": delimiter,
        "dataframe_json": df_json
    }


# ============================================================================
# LAYER 2: SCHEMA VALIDATION
# ============================================================================

def detect_language_from_months(df: pd.DataFrame) -> str | None:
    """Rileva la lingua dai nomi dei mesi nelle colonne stringa."""
    string_cols = df.select_dtypes(include='object')
    
    all_values = []
    for col in string_cols.columns:
        values = string_cols[col].dropna().astype(str).str.lower().unique()
        all_values.extend(values[:100])
    
    all_values_set = set(all_values)
    
    lang_scores = {}
    for lang, months in ALL_MONTHS.items():
        matches = len(all_values_set.intersection(set(m.lower() for m in months)))
        if matches > 0:
            lang_scores[lang] = matches
    
    if lang_scores:
        return max(lang_scores.keys(), key=lambda k: lang_scores[k])
    return None


def layer2_schema_validation(state: PriceDataState) -> dict:
    """
    Layer 2: Validazione schema.
    Verifica colonne e formati.
    """
    errors = []
    warnings = []
    metadata = {}
    llm_calls = 0
    
    df = pd.read_json(io.StringIO(state["dataframe_json"]), orient='split')
    df_columns = df.columns.tolist()
    df_cols_lower = [c.lower().strip() for c in df_columns]
    df_set = set(df_cols_lower)
    
    # SC2.1: Check required column categories
    missing_categories = []
    for category, expected_cols in REQUIRED_COLUMN_CATEGORIES.items():
        if not df_set.intersection(expected_cols):
            missing_categories.append(category)
    
    if missing_categories:
        errors.append(ValidationError(
            code="SC2.1",
            severity=Severity.ERROR,
            message=f"Categorie di colonne mancanti: {missing_categories}",
            details={'missing': missing_categories},
            suggestion=f"Aggiungere almeno una colonna per: {', '.join(missing_categories)}"
        ))
    
    # SC2.2: Duplicate columns
    col_counter = Counter(df_cols_lower)
    duplicates = {col: count for col, count in col_counter.items() if count > 1}
    
    if duplicates:
        errors.append(ValidationError(
            code="SC2.2",
            severity=Severity.CRITICAL,
            message=f"Colonne duplicate: {list(duplicates.keys())}",
            details={'duplicates': duplicates}
        ))
    
    # SC2.3: Language detection from months
    detected_language = detect_language_from_months(df)
    metadata['detected_language'] = detected_language
    
    if detected_language and detected_language != 'en':
        warnings.append(ValidationError(
            code="SC2.3",
            severity=Severity.WARNING,
            message=f"Lingua rilevata: {detected_language.upper()} (non inglese)",
            details={'language': detected_language},
            suggestion="Standardizzare i mesi in inglese (January, February, ...)"
        ))
    
    # SC2.4: Template comparison if provided
    if state["template"]:
        template_cols = [c.lower() for c in state["template"].get('columns', [])]
        template_set = set(template_cols)
        
        missing_from_template = template_set - df_set
        extra_in_submitted = df_set - template_set
        
        if missing_from_template:
            errors.append(ValidationError(
                code="SC2.4a",
                severity=Severity.ERROR,
                message=f"Colonne mancanti rispetto al template: {list(missing_from_template)[:10]}"
            ))
        
        if extra_in_submitted:
            warnings.append(ValidationError(
                code="SC2.4b",
                severity=Severity.INFO,
                message=f"Colonne extra non nel template: {list(extra_in_submitted)[:10]}"
            ))
        
        # Check column order
        common_cols = [c for c in df_cols_lower if c in template_set]
        expected_order = [c for c in template_cols if c in df_set]
        
        if common_cols != expected_order:
            warnings.append(ValidationError(
                code="SC2.4c",
                severity=Severity.INFO,
                message="Ordine colonne diverso dal template"
            ))
    
    # Extract metadata
    cols_map = {c.lower(): c for c in df_columns}
    
    # Try to find country
    country = None
    for col_name in ['country', 'adm0', 'adm0name']:
        if col_name in cols_map and len(df) > 0:
            country = str(df[cols_map[col_name]].iloc[0])
            break
    
    # Count products and markets
    num_products = None
    num_markets = None
    
    for col_name in ['commodity', 'product', 'item', 'commodity_name', 'product_name']:
        if col_name in cols_map:
            num_products = df[cols_map[col_name]].nunique()
            break
    
    for col_name in ['market', 'market_name', 'location']:
        if col_name in cols_map:
            num_markets = df[cols_map[col_name]].nunique()
            break
    
    metadata['num_products'] = num_products
    metadata['num_markets'] = num_markets
    metadata['total_columns'] = len(df_columns)
    
    has_critical = any(e.severity == Severity.CRITICAL for e in errors)
    
    result = LayerResult(
        layer_id=2,
        layer_name="Schema Validation",
        passed=len(errors) == 0,
        can_continue=not has_critical,
        errors=errors,
        warnings=warnings,
        metadata=metadata
    )
    
    return {
        "layer_results": [result.to_dict()],
        "can_continue": not has_critical,
        "current_layer": 2,
        "llm_calls": state["llm_calls"] + llm_calls,
        "detected_language": detected_language,
        "country": country,
        "num_products": num_products,
        "num_markets": num_markets
    }


# ============================================================================
# LAYER 3: PRODUCT CLASSIFICATION
# ============================================================================

def exact_match_product(product_name: str) -> dict | None:
    """Prova matching esatto (case-insensitive) prima di usare LLM."""
    product_lower = product_name.lower().strip()
    
    for wfp_name, wfp_id in WFP_PRODUCTS.items():
        if wfp_name.lower() == product_lower:
            return {
                "original_name": product_name,
                "matched_name": wfp_name,
                "product_id": wfp_id,
                "confidence": 1.0,
                "method": "exact_match"
            }
    
    # Try partial match
    for wfp_name, wfp_id in WFP_PRODUCTS.items():
        if product_lower in wfp_name.lower() or wfp_name.lower() in product_lower:
            return {
                "original_name": product_name,
                "matched_name": wfp_name,
                "product_id": wfp_id,
                "confidence": 0.9,
                "method": "partial_match"
            }
    
    return None


def layer3_product_classification(state: PriceDataState) -> dict:
    """
    Layer 3: Classificazione prodotti.
    Prima prova matching deterministico, poi usa LLM per prodotti non trovati.
    """
    errors = []
    warnings = []
    metadata = {}
    llm_calls = 0
    
    df = pd.read_json(io.StringIO(state["dataframe_json"]), orient='split')
    cols_map = {c.lower(): c for c in df.columns}
    
    # Find product column
    product_col = None
    for col_name in ['commodity', 'product', 'item', 'commodity_name', 'product_name']:
        if col_name in cols_map:
            product_col = cols_map[col_name]
            break
    
    if not product_col:
        warnings.append(ValidationError(
            code="PC3.0",
            severity=Severity.WARNING,
            message="Colonna prodotti non trovata, classificazione saltata"
        ))
        result = LayerResult(
            layer_id=3,
            layer_name="Product Classification",
            passed=True,
            can_continue=True,
            warnings=warnings,
            metadata=metadata
        )
        return {
            "layer_results": [result.to_dict()],
            "can_continue": True,
            "current_layer": 3,
            "product_classifications": []
        }
    
    # Get unique products
    unique_products = df[product_col].dropna().unique().tolist()
    metadata['total_unique_products'] = len(unique_products)
    
    classifications = []
    unmatched = []
    
    # First pass: deterministic matching
    for product in unique_products:
        match = exact_match_product(str(product))
        if match:
            classifications.append(match)
        else:
            unmatched.append(str(product))
    
    metadata['deterministic_matches'] = len(classifications)
    metadata['unmatched_for_llm'] = len(unmatched)
    
    # Second pass: LLM for unmatched (batch if many)
    if unmatched and len(unmatched) <= 20:
        model = get_model()
        
        try:
            response = model.invoke([
                SystemMessage(content=PROMPTS["batch_product_classification"]["system"]),
                HumanMessage(content=PROMPTS["batch_product_classification"]["user"].format(
                    products_to_classify="\n".join(unmatched),
                    product_list=WFP_PRODUCT_LIST_STR[:8000]
                ))
            ])
            llm_calls += 1
            
            result_text = response.content.strip()
            if result_text.startswith('```'):
                result_text = re.sub(r'^```json?\s*', '', result_text)
                result_text = re.sub(r'\s*```$', '', result_text)
            
            llm_results = json.loads(result_text)
            
            for item in llm_results:
                item['method'] = 'llm'
                classifications.append(item)
                
        except Exception as e:
            logger.warning(f"LLM classification failed: {e}")
            warnings.append(ValidationError(
                code="PC3.1",
                severity=Severity.WARNING,
                message=f"Classificazione LLM fallita per {len(unmatched)} prodotti"
            ))
    
    elif len(unmatched) > 20:
        warnings.append(ValidationError(
            code="PC3.2",
            severity=Severity.INFO,
            message=f"{len(unmatched)} prodotti non classificati (troppi per batch LLM)"
        ))
    
    # Count unmatched (confidence < 0.5 or null)
    low_confidence = [c for c in classifications if c.get('confidence', 0) < 0.5 or c.get('matched_name') is None]
    
    if low_confidence:
        warnings.append(ValidationError(
            code="PC3.3",
            severity=Severity.WARNING,
            message=f"{len(low_confidence)} prodotti con match bassa confidenza o non trovati",
            details={'products': [c['original_name'] for c in low_confidence[:10]]}
        ))
    
    metadata['classifications_completed'] = len(classifications)
    metadata['high_confidence'] = len([c for c in classifications if c.get('confidence', 0) >= 0.8])
    
    result = LayerResult(
        layer_id=3,
        layer_name="Product Classification",
        passed=True,
        can_continue=True,
        errors=errors,
        warnings=warnings,
        metadata=metadata
    )
    
    return {
        "layer_results": [result.to_dict()],
        "can_continue": True,
        "current_layer": 3,
        "llm_calls": state["llm_calls"] + llm_calls,
        "product_classifications": classifications
    }


# ============================================================================
# LAYER 4: DIAGNOSIS REPORT (LLM)
# ============================================================================

def layer4_generate_report(state: PriceDataState) -> dict:
    """
    Layer 4: Genera report diagnostico finale con LLM.
    """
    model = get_model()
    
    # Format validation results
    validation_summary = []
    for layer_result in state["layer_results"]:
        layer_summary = f"\n### Layer {layer_result['layer_id']}: {layer_result['layer_name']}\n"
        layer_summary += f"Passed: {layer_result['passed']}\n"
        
        if layer_result['errors']:
            layer_summary += "Errors:\n"
            for err in layer_result['errors']:
                layer_summary += f"  - [{err['code']}] {err['severity']}: {err['message']}\n"
        
        if layer_result['warnings']:
            layer_summary += "Warnings:\n"
            for warn in layer_result['warnings']:
                layer_summary += f"  - [{warn['code']}] {warn['severity']}: {warn['message']}\n"
        
        if layer_result.get('metadata'):
            layer_summary += f"Metadata: {json.dumps(layer_result['metadata'], indent=2, default=str)}\n"
        
        validation_summary.append(layer_summary)
    
    # Format product classifications
    classifications = state.get("product_classifications", [])
    classifications_summary = ""
    if classifications:
        high_conf = [c for c in classifications if c.get('confidence', 0) >= 0.8]
        low_conf = [c for c in classifications if c.get('confidence', 0) < 0.8]
        
        classifications_summary = f"""Total products: {len(classifications)}
High confidence matches (>=0.8): {len(high_conf)}
Low confidence matches (<0.8): {len(low_conf)}

Unrecognized products examples:
"""
        for c in low_conf[:5]:
            classifications_summary += f"  - '{c.get('original_name')}' → '{c.get('matched_name', 'N/A')}' (conf: {c.get('confidence', 0):.2f})\n"
    
    try:
        response = model.invoke([
            SystemMessage(content=PROMPTS["diagnosis_report"]["system"]),
            HumanMessage(content=PROMPTS["diagnosis_report"]["user"].format(
                file_name=state["file_name"],
                country=state["country"] or "Not detected",
                num_products=state["num_products"] or "N/A",
                num_markets=state["num_markets"] or "N/A",
                file_type=state["file_type"] or "Not detected",
                detected_language=state["detected_language"] or "English (default)",
                validation_results="\n".join(validation_summary),
                product_classifications=classifications_summary
            ))
        ])
        
        final_report = response.content
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        final_report = f"Error generating report: {e}\n\nRaw results:\n{json.dumps(state['layer_results'], indent=2)}"
    
    return {
        "final_report": final_report,
        "llm_calls": state["llm_calls"] + 1,
        "current_layer": 4
    }


# ============================================================================
# ROUTING FUNCTIONS
# ============================================================================

def route_after_layer(state: PriceDataState) -> Literal["continue", "report"]:
    """Router generico: continua o vai al report."""
    if state["can_continue"]:
        return "continue"
    return "report"


# ============================================================================
# GRAPH BUILDER
# ============================================================================

def build_graph():
    """
    Costruisce il grafo LangGraph per Price Data troubleshooting.
    
    Struttura:
        L0 → [route] → L1 → [route] → L2 → [route] → L3 → report → END
    """
    graph = StateGraph(PriceDataState)
    
    # Add nodes
    graph.add_node("layer0", layer0_file_validation)
    graph.add_node("layer1", layer1_structural_parsing)
    graph.add_node("layer2", layer2_schema_validation)
    graph.add_node("layer3", layer3_product_classification)
    graph.add_node("report", layer4_generate_report)
    
    # Set entry point
    graph.set_entry_point("layer0")
    
    # Add conditional edges for early exit
    graph.add_conditional_edges(
        "layer0",
        route_after_layer,
        {"continue": "layer1", "report": "report"}
    )
    
    graph.add_conditional_edges(
        "layer1",
        route_after_layer,
        {"continue": "layer2", "report": "report"}
    )
    
    graph.add_conditional_edges(
        "layer2",
        route_after_layer,
        {"continue": "layer3", "report": "report"}
    )
    
    # Layer 3 sempre va al report
    graph.add_edge("layer3", "report")
    
    # End
    graph.add_edge("report", END)
    
    return graph.compile()


# ============================================================================
# PUBLIC API
# ============================================================================

def run_troubleshooting(
    file_path: str,
    template_path: str | None = None
) -> dict:
    """
    Entry point per validazione Price Data.
    
    Args:
        file_path: Path al file Excel o CSV
        template_path: Path al template corretto (opzionale)
    
    Returns:
        Stato finale con report
    """
    initial_state = create_initial_state(file_path, template_path)
    agent = build_graph()
    return agent.invoke(initial_state)
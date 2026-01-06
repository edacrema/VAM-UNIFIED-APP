"""
MFI Validator - Graph
=====================
Logica di validazione MFI con LangGraph (Layer 0-5).

Layer:
- Layer 0: File Validation (deterministic)
- Layer 1: Structural Parsing (deterministic)
- Layer 2: Schema Validation (deterministic + LLM fuzzy matching)
- Layer 3: Business Rules (deterministic, adaptive Raw/Processed)
- Layer 5: Diagnosis & Reporting (LLM)
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
from .schemas import ValidationError, LayerResult, MFITemplate, Severity, FileType

logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTS
# ============================================================================

DEFAULT_VALID_DIMENSIONS = {
    1: "Assortment",
    2: "Availability",
    3: "Price",
    4: "Resilience",
    10: "MFI Aggregate"
}

DEFAULT_VALID_LEVELS = {
    1: "Normalized Score",
    2: "Trader Aggregate Score",
    3: "Market Level",
    4: "Trader Median",
    5: "Trader Mean"
}

PROCESSED_FILE_INDICATORS = {
    'MFIOUTPUTID', 'TRADERSSAMPLESIZE', 'LEVELID', 'LEVELNAME',
    'DIMENSIONID', 'DIMENSIONNAME', 'VARIABLEID', 'OUTPUTVALUE'
}

RAW_FILE_INDICATORS = {
    'SVY_MOD', 'SURVEY_TYPE', 'RESPONSEID', 'SUBMISSIONDATE',
    '_UUID', 'ENUMERATOR', 'ENUMERATORID', 'TRADER_NAME',
    'INTERVIEW_DATE', 'DEVICEID', '_SUBMISSION_TIME'
}


# ============================================================================
# PROMPTS
# ============================================================================

PROMPTS = {
    "fuzzy_column_match": {
        "system": """You are a data validation expert for WFP MFI datasets.
Your task is to determine if a submitted column name matches any expected column name, 
even if there are typos, case differences, or slight variations.

Respond ONLY with valid JSON in this format:
{
    "is_match": true/false,
    "matched_to": "expected_column_name or null",
    "confidence": 0.0-1.0,
    "reason": "brief explanation"
}""",
        "user": """Submitted column: "{submitted_column}"
Expected columns: {expected_columns}

Does the submitted column match any expected column?"""
    },
    
    "diagnosis_report": {
        "system": """You are a WFP data quality analyst producing a formal technical report for an MFI dataset validation.

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
6) Recommended Actions (prioritized, step-by-step)
7) Final Checklist

Use only the validation results provided. Do not invent issues not supported by the validation results.""",
        "user": """Generate a formal technical validation report for this MFI dataset.

File: {file_name}
Country: {country}
Survey period: {survey_period}
Detected file type: {file_type}
Survey type: {survey_type}

VALIDATION RESULTS (by layer):
{validation_results}
"""
    }
}


# ============================================================================
# STATE DEFINITION
# ============================================================================

class MFIState(TypedDict):
    """Stato tipizzato per il grafo LangGraph."""
    # Input
    file_path: str
    template: dict | None
    survey_type: str
    
    # Computed during execution
    encoding: str | None
    delimiter: str | None
    dataframe_json: str | None
    detected_file_type: str | None
    
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
    survey_period: str | None


def create_initial_state(
    file_path: str,
    template: MFITemplate | str | None = None,
    survey_type: str = "full mfi"
) -> MFIState:
    """Crea stato iniziale per il grafo."""
    
    # Load template
    if template is None:
        template_dict = None
    elif isinstance(template, str):
        if template.endswith('.json'):
            template_dict = MFITemplate.from_json(template).to_dict()
        elif template.endswith('.csv'):
            template_dict = MFITemplate.from_csv(template).to_dict()
        else:
            raise ValueError(f"Template file must be .json or .csv: {template}")
    elif isinstance(template, MFITemplate):
        template_dict = template.to_dict()
    else:
        raise ValueError(f"Invalid template type: {type(template)}")
    
    return MFIState(
        file_path=file_path,
        template=template_dict,
        survey_type=survey_type,
        encoding=None,
        delimiter=None,
        dataframe_json=None,
        detected_file_type=None,
        layer_results=[],
        can_continue=True,
        current_layer=0,
        llm_calls=0,
        final_report=None,
        file_name=Path(file_path).name,
        country=None,
        survey_period=None
    )


# ============================================================================
# LAYER 0: FILE VALIDATION (DETERMINISTIC)
# ============================================================================

def layer0_file_validation(state: MFIState) -> dict:
    """
    Layer 0: Validazione file base.
    Nodo deterministico, no LLM.
    """
    errors = []
    warnings = []
    metadata = {}
    
    file_path = Path(state["file_path"])
    
    # F0.1: Extension check
    if file_path.suffix.lower() != '.csv':
        errors.append(ValidationError(
            code="F0.1",
            severity=Severity.CRITICAL,
            message=f"File non è CSV. Estensione: {file_path.suffix}",
            suggestion="Salvare il file come CSV (non Excel)"
        ))
    
    # F0.2: File exists
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
    
    # F0.3: Binary file detection
    try:
        with open(file_path, 'rb') as f:
            header = f.read(8)
        
        # Excel signatures
        if header[:4] == b'PK\x03\x04':  # XLSX
            errors.append(ValidationError(
                code="F0.2",
                severity=Severity.CRITICAL,
                message="File è Excel (.xlsx), non CSV",
                suggestion="Aprire in Excel → Salva con nome → CSV UTF-8"
            ))
        elif header[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':  # XLS
            errors.append(ValidationError(
                code="F0.2",
                severity=Severity.CRITICAL,
                message="File è Excel (.xls), non CSV",
                suggestion="Aprire in Excel → Salva con nome → CSV UTF-8"
            ))
        elif header[:4] == b'%PDF':
            errors.append(ValidationError(
                code="F0.2",
                severity=Severity.CRITICAL,
                message="File è PDF, non CSV"
            ))
    except Exception as e:
        logger.warning(f"Binary detection failed: {e}")
    
    # F0.4: Encoding detection
    detected_encoding = None
    try:
        with open(file_path, 'rb') as f:
            raw = f.read(100000)
        
        # BOM detection
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
            code="F0.4",
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
        "encoding": detected_encoding
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


def layer1_structural_parsing(state: MFIState) -> dict:
    """
    Layer 1: Parsing strutturale con modulo csv puro.
    Rileva righe corrotte PRIMA di Pandas.
    
    Check implementati:
    - S1.1: Delimiter detection
    - S1.4: Broken rows (numero colonne errato)
    - S1.6: Over-quoted rows (dati compressi nella prima colonna)
    """
    errors = []
    warnings = []
    metadata = {}
    
    file_path = state["file_path"]
    encoding = state["encoding"] or 'utf-8'
    
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            content = f.read()
    except Exception as e:
        errors.append(ValidationError(
            code="S1.0",
            severity=Severity.CRITICAL,
            message=f"Impossibile leggere il file: {e}"
        ))
        result = LayerResult(
            layer_id=1,
            layer_name="Structural Parsing",
            passed=False,
            can_continue=False,
            errors=errors,
            metadata=metadata
        )
        return {
            "layer_results": [result.to_dict()],
            "can_continue": False,
            "current_layer": 1
        }
    
    # S1.1: Delimiter detection
    detected_delimiter, delim_scores = detect_delimiter_robust(content)
    metadata['detected_delimiter'] = detected_delimiter
    metadata['delimiter_scores'] = {k: v['score'] for k, v in delim_scores.items()}
    
    if detected_delimiter == ';':
        warnings.append(ValidationError(
            code="S1.1",
            severity=Severity.WARNING,
            message="Delimitatore ';' rilevato (tipico tastiera francese)",
            suggestion="Usare ',' come separatore standard"
        ))
    
    # S1.4: Broken rows detection
    broken_rows = []
    broken_row_numbers = []
    
    # S1.6: Over-quoted rows detection
    overquoted_rows = []
    overquoted_row_numbers = []
    
    try:
        buffer = io.StringIO(content)
        reader = csv.reader(buffer, delimiter=detected_delimiter)
        header = next(reader)
        expected_cols = len(header)
        metadata['expected_columns'] = expected_cols
        metadata['header'] = header[:10]
        
        for i, row in enumerate(reader, start=2):
            row_len = len(row)
            
            # S1.4: Check numero colonne errato
            if row_len != expected_cols:
                broken_row_numbers.append(i)
                if len(broken_rows) < 20:
                    broken_rows.append({
                        'row_number': i,
                        'expected': expected_cols,
                        'actual': row_len,
                        'preview': str(row[:3])[:100]
                    })
            
            # S1.6: Check over-quoted (solo se numero colonne è corretto)
            elif row_len == expected_cols and row:
                first_col = row[0]
                # Prima colonna contiene virgole e sembra una riga CSV intera
                if detected_delimiter in first_col and len(first_col) > 100:
                    # Conta colonne vuote (esclusa la prima)
                    empty_count = sum(1 for cell in row[1:] if cell.strip() == '')
                    # Se >80% delle altre colonne sono vuote, è over-quoted
                    if empty_count > (expected_cols - 1) * 0.8:
                        overquoted_row_numbers.append(i)
                        if len(overquoted_rows) < 20:
                            overquoted_rows.append({
                                'row_number': i,
                                'first_col_length': len(first_col),
                                'empty_cols': empty_count,
                                'preview': first_col[:80]
                            })
                    
    except Exception as e:
        errors.append(ValidationError(
            code="S1.5",
            severity=Severity.ERROR,
            message=f"Errore parsing CSV: {e}"
        ))
    
    # Genera errore S1.4 se ci sono broken rows
    if broken_row_numbers:
        total_broken = len(broken_row_numbers)
        errors.append(ValidationError(
            code="S1.4",
            severity=Severity.CRITICAL,
            message=f"Rilevate {total_broken} righe con numero colonne errato",
            details={
                'total_broken_rows': total_broken,
                'broken_row_numbers': broken_row_numbers,
                'broken_rows_details': broken_rows
            },
            suggestion="Aprire in editor testo, cercare ritorni a capo dentro celle con virgolette",
            affected_rows=broken_row_numbers[:50]
        ))
    
    # Genera errore S1.6 se ci sono over-quoted rows
    if overquoted_row_numbers:
        total_overquoted = len(overquoted_row_numbers)
        errors.append(ValidationError(
            code="S1.6",
            severity=Severity.CRITICAL,
            message=f"Rilevate {total_overquoted} righe con dati compressi nella prima colonna (over-quoted)",
            details={
                'total_overquoted_rows': total_overquoted,
                'overquoted_row_numbers': overquoted_row_numbers,
                'overquoted_rows_details': overquoted_rows
            },
            suggestion="Righe quotate erroneamente durante export. Rimuovere le virgolette esterne.",
            affected_rows=overquoted_row_numbers[:50]
        ))
    
    metadata['total_lines'] = content.count('\n') + 1
    
    has_critical = any(e.severity == Severity.CRITICAL for e in errors)
    
    result = LayerResult(
        layer_id=1,
        layer_name="Structural Parsing",
        passed=len(errors) == 0,
        can_continue=not has_critical,
        errors=errors,
        warnings=warnings,
        metadata=metadata
    )
    
    return {
        "layer_results": [result.to_dict()],
        "can_continue": not has_critical,
        "current_layer": 1,
        "delimiter": detected_delimiter
    }


# ============================================================================
# LAYER 2: SCHEMA VALIDATION
# ============================================================================

def layer2_schema_validation(state: MFIState) -> dict:
    """
    Layer 2: Validazione schema con template.
    Usa LLM solo per fuzzy matching se necessario.
    """
    errors = []
    warnings = []
    metadata = {}
    llm_calls = 0
    
    file_path = state["file_path"]
    encoding = state["encoding"] or 'utf-8'
    delimiter = state["delimiter"] or ','
    template_dict = state["template"]
    
    # Load DataFrame
    try:
        df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, on_bad_lines='warn')
    except Exception as e:
        errors.append(ValidationError(
            code="SC2.0",
            severity=Severity.CRITICAL,
            message=f"Impossibile caricare CSV: {e}"
        ))
        result = LayerResult(
            layer_id=2,
            layer_name="Schema Validation",
            passed=False,
            can_continue=False,
            errors=errors,
            metadata=metadata
        )
        return {
            "layer_results": [result.to_dict()],
            "can_continue": False,
            "current_layer": 2,
            "llm_calls": state["llm_calls"]
        }
    
    df_columns = df.columns.tolist()
    df_cols_lower = [c.lower().strip() for c in df_columns]
    df_set = set(df_cols_lower)
    
    # Get template columns
    if template_dict:
        template_columns = template_dict.get('columns', [])
        required_columns = template_dict.get('required_columns', template_columns)
        metadata['template_name'] = template_dict.get('name', 'Unknown')
    else:
        # Infer from file type
        cols_upper = {c.upper() for c in df_columns}
        if cols_upper & PROCESSED_FILE_INDICATORS:
            template_columns = list(PROCESSED_FILE_INDICATORS)
        else:
            template_columns = list(RAW_FILE_INDICATORS)
        required_columns = []
        metadata['template_name'] = 'Inferred (no template provided)'
    
    template_lower = [c.lower() for c in template_columns]
    required_lower = [c.lower() for c in required_columns]
    template_set = set(template_lower)
    required_set = set(required_lower)
    
    # SC2.3: Duplicate columns (CRITICAL)
    col_counter = Counter(df_cols_lower)
    duplicates = {col: count for col, count in col_counter.items() if count > 1}
    
    if duplicates:
        errors.append(ValidationError(
            code="SC2.3",
            severity=Severity.CRITICAL,
            message=f"Colonne duplicate: {list(duplicates.keys())}",
            details={'duplicates': duplicates},
            suggestion="Rimuovere colonne duplicate"
        ))
    
    # SC2.1: Missing required columns
    missing_required = required_set - df_set
    if missing_required:
        errors.append(ValidationError(
            code="SC2.1",
            severity=Severity.ERROR,
            message=f"Colonne obbligatorie mancanti: {list(missing_required)}",
            details={'missing': list(missing_required)}
        ))
    
    # SC2.2: Extra columns
    extra = df_set - template_set
    if extra and template_set:
        warnings.append(ValidationError(
            code="SC2.2",
            severity=Severity.INFO,
            message=f"Colonne extra non nel template: {list(extra)[:10]}",
            details={'extra': list(extra)}
        ))
    
    # SC2.5: Fuzzy matching with LLM (only if extra + missing)
    if extra and missing_required and len(extra) <= 5:
        model = get_model()
        fuzzy_matches = {}
        
        for extra_col in list(extra)[:5]:
            try:
                response = model.invoke([
                    SystemMessage(content=PROMPTS["fuzzy_column_match"]["system"]),
                    HumanMessage(content=PROMPTS["fuzzy_column_match"]["user"].format(
                        submitted_column=extra_col,
                        expected_columns=", ".join(list(missing_required)[:20])
                    ))
                ])
                llm_calls += 1
                
                result_text = response.content.strip()
                if result_text.startswith('```'):
                    result_text = re.sub(r'^```json?\s*', '', result_text)
                    result_text = re.sub(r'\s*```$', '', result_text)
                
                match_result = json.loads(result_text)
                
                if match_result.get('is_match') and match_result.get('confidence', 0) > 0.7:
                    fuzzy_matches[extra_col] = match_result
            except Exception as e:
                logger.warning(f"Fuzzy match failed: {e}")
        
        if fuzzy_matches:
            metadata['fuzzy_matches'] = fuzzy_matches
            warnings.append(ValidationError(
                code="SC2.5",
                severity=Severity.INFO,
                message=f"Possibili match: {list(fuzzy_matches.keys())}",
                details={'matches': fuzzy_matches}
            ))
    
    metadata['total_columns'] = len(df_columns)
    metadata['template_columns'] = len(template_columns)
    metadata['match_percentage'] = 100 * len(df_set & template_set) / len(template_set) if template_set else 0
    
    has_critical = any(e.severity == Severity.CRITICAL for e in errors)
    
    # Serialize DataFrame for next layers
    df_json = df.to_json(orient='split', date_format='iso')
    
    # Extract metadata
    cols_map = {c.upper(): c for c in df_columns}
    country = None
    survey_period = None
    
    adm0_col = cols_map.get('ADM0NAME')
    start_col = cols_map.get('STARTDATE')
    
    if adm0_col and len(df) > 0:
        country = str(df[adm0_col].iloc[0])
    if start_col and len(df) > 0:
        survey_period = str(df[start_col].iloc[0])
    
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
        "dataframe_json": df_json,
        "country": country,
        "survey_period": survey_period
    }


# ============================================================================
# LAYER 3: BUSINESS RULES
# ============================================================================

def detect_file_type(df: pd.DataFrame) -> FileType:
    """Rileva se il file è RAW o PROCESSED."""
    cols_upper = {c.upper() for c in df.columns}
    
    processed_score = len(cols_upper & PROCESSED_FILE_INDICATORS)
    raw_score = len(cols_upper & RAW_FILE_INDICATORS)
    
    # Additional heuristics
    if 'LEVELID' in cols_upper:
        try:
            level_col = [c for c in df.columns if c.upper() == 'LEVELID'][0]
            if set(df[level_col].dropna().unique()).issubset({1, 2, 3, 4, 5}):
                processed_score += 3
        except:
            pass
    
    if 'TRADERSSAMPLESIZE' in cols_upper:
        try:
            sample_col = [c for c in df.columns if c.upper() == 'TRADERSSAMPLESIZE'][0]
            if df[sample_col].dropna().mean() > 1:
                processed_score += 2
        except:
            pass
    
    if processed_score > raw_score:
        return FileType.PROCESSED
    elif raw_score > processed_score:
        return FileType.RAW
    else:
        return FileType.UNKNOWN


def layer3_business_rules(state: MFIState) -> dict:
    """
    Layer 3: Business rules con logica adattiva Raw/Processed.
    """
    errors = []
    warnings = []
    metadata = {}
    
    # Load DataFrame from state
    df = pd.read_json(io.StringIO(state["dataframe_json"]), orient='split')
    template_dict = state["template"]
    survey_type = state["survey_type"]
    
    # Detect file type
    file_type = detect_file_type(df)
    metadata['detected_file_type'] = file_type.value
    
    cols_map = {c.upper(): c for c in df.columns}
    
    # Get validation values from template
    if template_dict:
        valid_dimensions = {int(k): v for k, v in template_dict.get('valid_dimensions', {}).items()} or DEFAULT_VALID_DIMENSIONS
        valid_levels = {int(k): v for k, v in template_dict.get('valid_levels', {}).items()} or DEFAULT_VALID_LEVELS
        value_ranges = template_dict.get('value_ranges', {})
    else:
        valid_dimensions = DEFAULT_VALID_DIMENSIONS
        valid_levels = DEFAULT_VALID_LEVELS
        value_ranges = {}
    
    # === ADAPTIVE LOGIC ===
    if file_type == FileType.RAW and survey_type.lower() == "full mfi":
        # Raw file validation: check survey completeness
        svy_col = cols_map.get('SVY_MOD') or cols_map.get('SURVEY_TYPE')
        market_col = cols_map.get('MARKETID')
        
        if svy_col and market_col:
            try:
                survey_counts = df.groupby(market_col)[svy_col].value_counts().unstack(fill_value=0)
                
                incomplete = []
                for market_id in survey_counts.index:
                    trader_count = survey_counts.loc[market_id].get(1, 0)
                    market_count = survey_counts.loc[market_id].get(2, 0)
                    
                    if market_count < 1 or trader_count < 5:
                        incomplete.append({
                            'market_id': int(market_id),
                            'trader_surveys': int(trader_count),
                            'market_surveys': int(market_count)
                        })
                
                if incomplete:
                    errors.append(ValidationError(
                        code="BR3.1_RAW",
                        severity=Severity.ERROR,
                        message=f"{len(incomplete)} mercati non hanno survey complete (1 market + 5 trader)",
                        details={'incomplete_markets': incomplete[:10]}
                    ))
            except Exception as e:
                logger.warning(f"Raw survey check failed: {e}")
    
    elif file_type == FileType.PROCESSED:
        # Processed file validation
        sample_col = cols_map.get('TRADERSSAMPLESIZE')
        
        if sample_col:
            try:
                low_sample = df[df[sample_col] < 5]
                if len(low_sample) > 0:
                    warnings.append(ValidationError(
                        code="BR3.7",
                        severity=Severity.WARNING,
                        message=f"{len(low_sample)} record con TradersSampleSize < 5",
                        details={'min_sample': int(df[sample_col].min())}
                    ))
            except Exception as e:
                logger.warning(f"Sample size check failed: {e}")
        
        # Dimension coverage check
        dim_col = cols_map.get('DIMENSIONID')
        market_col = cols_map.get('MARKETID')
        
        if dim_col and market_col:
            try:
                market_dims = df.groupby(market_col)[dim_col].apply(lambda x: set(x.unique()))
                expected_dims = set(valid_dimensions.keys())
                
                incomplete = []
                for mkt, dims in market_dims.items():
                    missing = expected_dims - dims
                    if missing:
                        incomplete.append({
                            'market_id': int(mkt),
                            'missing_dimensions': [valid_dimensions.get(d, str(d)) for d in missing]
                        })
                
                if incomplete:
                    warnings.append(ValidationError(
                        code="BR3.8",
                        severity=Severity.WARNING,
                        message=f"{len(incomplete)} mercati senza tutte le 4 dimensioni",
                        details={'incomplete': incomplete[:10]}
                    ))
            except Exception as e:
                logger.warning(f"Dimension coverage check failed: {e}")
    
    # === COMMON CHECKS ===
    
    # BR3.2: Dimension validation
    dim_col = cols_map.get('DIMENSIONID')
    if dim_col:
        try:
            invalid_dims = df[~df[dim_col].isin(valid_dimensions.keys())][dim_col].unique()
            if len(invalid_dims) > 0:
                errors.append(ValidationError(
                    code="BR3.2",
                    severity=Severity.ERROR,
                    message=f"DimensionID non validi: {list(invalid_dims)}"
                ))
        except:
            pass
    
    # BR3.3: Level validation
    level_col = cols_map.get('LEVELID')
    if level_col:
        try:
            invalid_levels = df[~df[level_col].isin(valid_levels.keys())][level_col].unique()
            if len(invalid_levels) > 0:
                errors.append(ValidationError(
                    code="BR3.3",
                    severity=Severity.ERROR,
                    message=f"LevelID non validi: {list(invalid_levels)}"
                ))
        except:
            pass
    
    # BR3.4: Value range
    output_col = cols_map.get('OUTPUTVALUE')
    if output_col:
        output_range = value_ranges.get('OutputValue', {'min': 0, 'max': 10})
        try:
            out_of_range = df[
                (df[output_col] < output_range.get('min', 0)) |
                (df[output_col] > output_range.get('max', 10))
            ]
            if len(out_of_range) > 0:
                warnings.append(ValidationError(
                    code="BR3.4",
                    severity=Severity.WARNING,
                    message=f"{len(out_of_range)} valori OutputValue fuori range"
                ))
        except:
            pass
    
    # BR3.5: Coordinates
    lat_col = cols_map.get('MARKETLATITUDE')
    lon_col = cols_map.get('MARKETLONGITUDE')
    if lat_col and lon_col:
        try:
            invalid_coords = df[
                (df[lat_col].abs() > 90) |
                (df[lon_col].abs() > 180) |
                ((df[lat_col] == 0) & (df[lon_col] == 0))
            ]
            if len(invalid_coords) > 0:
                warnings.append(ValidationError(
                    code="BR3.5",
                    severity=Severity.WARNING,
                    message=f"Coordinate non valide per {len(invalid_coords)} record"
                ))
        except:
            pass
    
    # BR3.6: Date validation
    start_col = cols_map.get('STARTDATE')
    end_col = cols_map.get('ENDDATE')
    if start_col and end_col:
        try:
            start_dates = pd.to_datetime(df[start_col], errors='coerce')
            end_dates = pd.to_datetime(df[end_col], errors='coerce')
            invalid_range = df[start_dates > end_dates]
            if len(invalid_range) > 0:
                errors.append(ValidationError(
                    code="BR3.6",
                    severity=Severity.ERROR,
                    message=f"StartDate > EndDate per {len(invalid_range)} record"
                ))
        except:
            pass
    
    metadata['total_rows'] = len(df)
    metadata['unique_markets'] = df[cols_map.get('MARKETID')].nunique() if cols_map.get('MARKETID') else 0
    
    result = LayerResult(
        layer_id=3,
        layer_name="Business Rules",
        passed=len(errors) == 0,
        can_continue=True,
        errors=errors,
        warnings=warnings,
        metadata=metadata
    )
    
    return {
        "layer_results": [result.to_dict()],
        "can_continue": True,
        "current_layer": 3,
        "detected_file_type": file_type.value
    }


# ============================================================================
# LAYER 5: REPORT GENERATION (LLM)
# ============================================================================

def layer5_generate_report(state: MFIState) -> dict:
    """
    Layer 5: Genera report diagnostico finale con LLM.
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
                
                # Aggiungi i details se presenti
                if err.get('details'):
                    if err['code'] == 'S1.4' and 'broken_row_numbers' in err['details']:
                        row_nums = err['details']['broken_row_numbers']
                        total = err['details'].get('total_broken_rows', len(row_nums))
                        layer_summary += f"    Total broken rows: {total}\n"
                        layer_summary += f"    Row numbers: {row_nums}\n"
                        
                        details_list = err['details'].get('broken_rows_details', [])
                        if details_list:
                            layer_summary += "    Details (first rows):\n"
                            for rd in details_list[:5]:
                                layer_summary += f"      - Row {rd['row_number']}: {rd['actual']} cols (expected {rd['expected']})\n"
                    else:
                        layer_summary += f"    Details: {json.dumps(err['details'], default=str)[:500]}\n"
        
        if layer_result['warnings']:
            layer_summary += "Warnings:\n"
            for warn in layer_result['warnings']:
                layer_summary += f"  - [{warn['code']}] {warn['severity']}: {warn['message']}\n"
        
        if layer_result.get('metadata'):
            layer_summary += f"Metadata: {json.dumps(layer_result['metadata'], indent=2, default=str)}\n"
        
        validation_summary.append(layer_summary)
    
    try:
        response = model.invoke([
            SystemMessage(content=PROMPTS["diagnosis_report"]["system"]),
            HumanMessage(content=PROMPTS["diagnosis_report"]["user"].format(
                file_name=state["file_name"],
                country=state["country"] or "Not detected",
                survey_period=state["survey_period"] or "Not detected",
                file_type=state["detected_file_type"] or "Not detected",
                survey_type=state["survey_type"],
                validation_results="\n".join(validation_summary)
            ))
        ])
        
        final_report = response.content
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        final_report = f"Errore generazione report: {e}\n\nRisultati raw:\n{json.dumps(state['layer_results'], indent=2)}"
    
    return {
        "final_report": final_report,
        "llm_calls": state["llm_calls"] + 1,
        "current_layer": 5
    }


# ============================================================================
# ROUTING FUNCTIONS
# ============================================================================

def route_after_layer(state: MFIState) -> Literal["continue", "report"]:
    """Router generico: continua o vai al report."""
    if state["can_continue"]:
        return "continue"
    return "report"


# ============================================================================
# GRAPH BUILDER
# ============================================================================

def build_graph():
    """
    Costruisce il grafo LangGraph per MFI troubleshooting.
    
    Struttura:
        L0 → [route] → L1 → [route] → L2 → [route] → L3 → report → END
    """
    graph = StateGraph(MFIState)
    
    # Add nodes
    graph.add_node("layer0", layer0_file_validation)
    graph.add_node("layer1", layer1_structural_parsing)
    graph.add_node("layer2", layer2_schema_validation)
    graph.add_node("layer3", layer3_business_rules)
    graph.add_node("report", layer5_generate_report)
    
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
    template: str | None = None,
    survey_type: str = "full mfi"
) -> dict:
    """
    Entry point per validazione MFI.
    
    Args:
        file_path: Path al file CSV
        template: Path al template (opzionale)
        survey_type: "full mfi" o "reduced mfi"
    
    Returns:
        Stato finale con report
    """
    initial_state = create_initial_state(file_path, template, survey_type)
    agent = build_graph()
    return agent.invoke(initial_state)
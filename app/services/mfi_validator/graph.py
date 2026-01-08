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
from typing import TypedDict, Annotated, Literal, Dict, Any, Optional, Callable
from collections import Counter
import operator

import pandas as pd
import chardet

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, SystemMessage

from app.shared.llm import get_model
from .schemas import ValidationError, LayerResult, MFITemplate, Severity, FileType

logger = logging.getLogger(__name__)

OnStepCallback = Callable[[str, Dict[str, Any]], None]


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


RAW_FILE_INDICATORS = {
    'SVYMOD', 'MARKETID'
}


# ============================================================================
# PROMPTS
# ============================================================================

PROMPTS = {
    
    "diagnosis_report": {
    "system": """You are a WFP data quality analyst producing a technical validation report for an MFI dataset.

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
2) Dataset Metadata (file name, country, survey period, file type, survey type)
3) Critical Issues
   - For each critical issue: describe the problem, specify where it is located (layer, row numbers if available), and provide a concrete suggestion to fix it
4) Warnings (minor issues that do not block validation but should be reviewed)

If there are no critical issues, state that the dataset passed validation.
If there are no warnings, omit that section.

Use only the validation results provided. Do not invent issues not supported by the validation results.""",
    "user": """Generate a technical validation report for this MFI dataset.

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
    
    Checks:
    - F0.0: File exists
    - F0.1: Extension is .csv
    - F0.2: File is not binary (Excel, PDF)
    - F0.4: Encoding detection
    - F0.5: RAW file indicators (all required columns present)
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
            message="File is not CSV. Extension: {}".format(file_path.suffix),
            suggestion="Save the file as CSV (not Excel)"
        ))
    
    # F0.0: File exists
    if not file_path.exists():
        errors.append(ValidationError(
            code="F0.0",
            severity=Severity.CRITICAL,
            message="File not found"
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
    
    # F0.2: Binary file detection
    try:
        with open(file_path, 'rb') as f:
            header = f.read(8)
        
        # Excel signatures
        if header[:4] == b'PK\x03\x04':  # XLSX
            errors.append(ValidationError(
                code="F0.2",
                severity=Severity.CRITICAL,
                message="File is Excel (.xlsx), not CSV",
                suggestion="Open in Excel → Save As → CSV UTF-8"
            ))
        elif header[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':  # XLS
            errors.append(ValidationError(
                code="F0.2",
                severity=Severity.CRITICAL,
                message="File is Excel (.xls), not CSV",
                suggestion="Open in Excel → Save As → CSV UTF-8"
            ))
        elif header[:4] == b'%PDF':
            errors.append(ValidationError(
                code="F0.2",
                severity=Severity.CRITICAL,
                message="File is PDF, not CSV"
            ))
    except Exception as e:
        logger.warning("Binary detection failed: {}".format(e))
    
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
            message="Unable to detect encoding: {}".format(e)
        ))
        detected_encoding = 'utf-8'
    
    # F0.5: RAW file indicators validation (all columns required)
    # Only proceed if no critical errors so far (file exists and is readable)
    has_critical_so_far = any(e.severity == Severity.CRITICAL for e in errors)
    
    if not has_critical_so_far:
        try:
            with open(file_path, 'r', encoding=detected_encoding or 'utf-8', errors='replace') as f:
                first_line = f.readline().strip()
            
            # Parse header - try common delimiters to find the best one
            header_cols = []
            for delim in [',', ';', '\t', '|']:
                cols = [c.strip().strip('"').strip("'").upper() for c in first_line.split(delim)]
                if len(cols) > len(header_cols):
                    header_cols = cols
            
            file_columns_upper = set(header_cols)
            
            # Check for ALL required RAW indicators
            missing_indicators = RAW_FILE_INDICATORS - file_columns_upper
            found_indicators = RAW_FILE_INDICATORS & file_columns_upper
            
            metadata['raw_indicators_found'] = sorted(list(found_indicators))
            metadata['raw_indicators_missing'] = sorted(list(missing_indicators))
            metadata['raw_indicators_required'] = sorted(list(RAW_FILE_INDICATORS))
            
            if missing_indicators:
                errors.append(ValidationError(
                    code="F0.5",
                    severity=Severity.CRITICAL,
                    message="File is not a valid RAW MFI dataset. Missing {} of {} required columns.".format(
                        len(missing_indicators), len(RAW_FILE_INDICATORS)
                    ),
                    details={
                        'missing_columns': sorted(list(missing_indicators)),
                        'found_columns': sorted(list(found_indicators)),
                        'required_columns': sorted(list(RAW_FILE_INDICATORS))
                    },
                    suggestion="Ensure the file is a RAW MFI dataset containing ALL required columns: {}".format(
                        ", ".join(sorted(RAW_FILE_INDICATORS))
                    )
                ))
        
        except Exception as e:
            errors.append(ValidationError(
                code="F0.5",
                severity=Severity.CRITICAL,
                message="Unable to validate RAW file indicators: {}".format(e),
                suggestion="Ensure the file is a valid CSV with a proper header row"
            ))
    
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
    - S1.0: File reading
    - S1.1: Delimiter detection
    - S1.4: Broken rows (numero colonne errato)
    - S1.5: CSV parsing errors
    - S1.6: Over-quoted rows (dati compressi nella prima colonna)
    """
    errors = []
    warnings = []
    metadata = {}
    
    file_path = state["file_path"]
    encoding = state["encoding"] or 'utf-8'
    
    # S1.0: Read file
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            content = f.read()
    except Exception as e:
        errors.append(ValidationError(
            code="S1.0",
            severity=Severity.CRITICAL,
            message="Unable to read file: {}".format(e)
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
            message="Detected ';' delimiter (common with French locale settings)",
            suggestion="Use ',' as the standard delimiter"
        ))
    
    # S1.4: Broken rows detection (no limit)
    broken_rows = []
    broken_row_numbers = []
    
    # S1.6: Over-quoted rows detection (no limit)
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
            
            # S1.4: Check numero colonne errato (no limit - collect all)
            if row_len != expected_cols:
                broken_row_numbers.append(i)
                broken_rows.append({
                    'row_number': i,
                    'expected': expected_cols,
                    'actual': row_len,
                    'preview': str(row[:3])[:100]
                })
            
            # S1.6: Check over-quoted (solo se numero colonne è corretto, no limit)
            elif row_len == expected_cols and row:
                first_col = row[0]
                # Prima colonna contiene virgole e sembra una riga CSV intera
                if detected_delimiter in first_col and len(first_col) > 100:
                    # Conta colonne vuote (esclusa la prima)
                    empty_count = sum(1 for cell in row[1:] if cell.strip() == '')
                    # Se >80% delle altre colonne sono vuote, è over-quoted
                    if empty_count > (expected_cols - 1) * 0.8:
                        overquoted_row_numbers.append(i)
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
            message="CSV parsing error: {}".format(e)
        ))
    
    # Genera errore S1.4 se ci sono broken rows
    if broken_row_numbers:
        total_broken = len(broken_row_numbers)
        errors.append(ValidationError(
            code="S1.4",
            severity=Severity.CRITICAL,
            message="Detected {} rows with an incorrect number of columns".format(total_broken),
            details={
                'total_broken_rows': total_broken,
                'broken_row_numbers': broken_row_numbers,
                'broken_rows_details': broken_rows
            },
            suggestion="Open in a text editor and check for line breaks inside quoted cells",
            affected_rows=broken_row_numbers
        ))
    
    # Genera errore S1.6 se ci sono over-quoted rows
    if overquoted_row_numbers:
        total_overquoted = len(overquoted_row_numbers)
        errors.append(ValidationError(
            code="S1.6",
            severity=Severity.CRITICAL,
            message="Detected {} rows with data compressed into the first column (over-quoted)".format(total_overquoted),
            details={
                'total_overquoted_rows': total_overquoted,
                'overquoted_row_numbers': overquoted_row_numbers,
                'overquoted_rows_details': overquoted_rows
            },
            suggestion="Rows were incorrectly quoted during export. Remove the outer quotes.",
            affected_rows=overquoted_row_numbers
        ))
    
    metadata['total_lines'] = content.count('\n') + 1
    metadata['broken_rows_count'] = len(broken_row_numbers)
    metadata['overquoted_rows_count'] = len(overquoted_row_numbers)
    
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
    Layer 2: Validazione schema e caricamento DataFrame.
    
    Checks:
    - SC2.0: CSV loading into DataFrame
    - SC2.3: Duplicate columns detection
    
    Note: RAW file indicators (required columns) are already validated in Layer 0.
    This layer focuses on loading the data and detecting structural schema issues.
    """
    errors = []
    warnings = []
    metadata = {}
    
    file_path = state["file_path"]
    encoding = state["encoding"] or 'utf-8'
    delimiter = state["delimiter"] or ','
    
    # SC2.0: Load DataFrame
    try:
        df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding, on_bad_lines='warn')
    except Exception as e:
        errors.append(ValidationError(
            code="SC2.0",
            severity=Severity.CRITICAL,
            message="Unable to load CSV: {}".format(e)
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
    
    # SC2.3: Duplicate columns (CRITICAL)
    col_counter = Counter(df_cols_lower)
    duplicates = {col: count for col, count in col_counter.items() if count > 1}
    
    if duplicates:
        errors.append(ValidationError(
            code="SC2.3",
            severity=Severity.CRITICAL,
            message="Duplicate columns: {}".format(list(duplicates.keys())),
            details={'duplicates': duplicates},
            suggestion="Remove duplicate columns"
        ))
    
    # Metadata
    metadata['total_columns'] = len(df_columns)
    metadata['total_rows'] = len(df)
    metadata['file_type'] = 'RAW'
    metadata['columns'] = df_columns
    
    has_critical = any(e.severity == Severity.CRITICAL for e in errors)
    
    # Serialize DataFrame for next layers
    df_json = df.to_json(orient='split', date_format='iso')
    
    # Helper function to find column with multiple name variants
    def get_column(*variants):
        """Return the first matching column name from variants."""
        cols_map = {c.upper(): c for c in df_columns}
        for variant in variants:
            if variant in cols_map:
                return cols_map[variant]
        return None
    
    # Extract metadata from RAW file columns
    country = None
    survey_period = None
    
    # Try to extract country from ADM0NAME or ADM0CODE column
    adm0_col = get_column('ADM0NAME', 'ADM0_NAME', 'ADM0CODE', 'ADM0_CODE')
    if adm0_col and len(df) > 0:
        country = str(df[adm0_col].iloc[0])
    
    # Try to extract survey period from date columns (RAW file specific)
    # Using actual column names found in MFI files
    date_col = get_column(
        'SVYDATE', 'SVY_DATE',                    # Primary: Survey date
        'SVYSTARTTIME', 'SVY_START_TIME',         # Alternative: Survey start time
        '_SUBMISSION_TIME', 'SUBMISSION_TIME',     # Alternative: Submission time
        'INTERVIEW_DATE', 'INTERVIEWDATE',         # Legacy names
        'SUBMISSIONDATE', 'STARTDATE'              # Legacy names
    )
    
    if date_col and len(df) > 0:
        try:
            dates = pd.to_datetime(df[date_col], errors='coerce')
            valid_dates = dates.dropna()
            if len(valid_dates) > 0:
                min_date = valid_dates.min().strftime('%Y-%m-%d')
                max_date = valid_dates.max().strftime('%Y-%m-%d')
                survey_period = "{} to {}".format(min_date, max_date)
                metadata['date_column_used'] = date_col
        except Exception:
            survey_period = str(df[date_col].iloc[0])
    
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
        "llm_calls": state["llm_calls"],
        "dataframe_json": df_json,
        "country": country,
        "survey_period": survey_period
    }


# ============================================================================
# LAYER 3: BUSINESS RULES
# ============================================================================

def detect_file_type(df: pd.DataFrame) -> FileType:
    """File type is always RAW (PROCESSED no longer supported)."""
    return FileType.RAW


def layer3_business_rules(state: MFIState) -> dict:
    """
    Layer 3: Business rules for RAW MFI datasets.
    
    Checks:
    - BR3.1: Survey completeness (for full MFI: 1 market survey + 5 trader surveys per market)
    - BR3.2: Response ID uniqueness
    - BR3.7: UUID uniqueness
    """
    errors = []
    warnings = []
    metadata = {}
    
    # Load DataFrame from state
    df = pd.read_json(io.StringIO(state["dataframe_json"]), orient='split')
    survey_type = state["survey_type"]
    
    # File type is always RAW (PROCESSED no longer supported)
    file_type = FileType.RAW
    metadata['detected_file_type'] = file_type.value
    
    cols_map = {c.upper(): c for c in df.columns}
    
    # Helper function to find column with multiple name variants
    def get_column(*variants):
        """Return the first matching column name from variants."""
        for variant in variants:
            if variant in cols_map:
                return cols_map[variant]
        return None
    
    # === RAW FILE SPECIFIC CHECKS ===
    
    # BR3.1: Survey completeness (only for "full mfi")
    if survey_type.lower() == "full mfi":
        # Support multiple column name variants
        svy_col = get_column('SVYMOD', 'SVY_MOD', 'SURVEYTYPE', 'SURVEY_TYPE')
        market_col = get_column('MARKETID', 'MARKET_ID')
        
        if svy_col and market_col:
            try:
                # Detect SvyMod format (numeric vs string)
                svy_values = df[svy_col].dropna().unique()
                svy_format = 'unknown'
                
                # Check if values are numeric (1, 2) or string ("Trader", "Market")
                has_numeric = any(v in [1, 2, 1.0, 2.0, '1', '2'] for v in svy_values)
                has_string = any(str(v).lower() in ['trader', 'market'] for v in svy_values)
                
                if has_numeric:
                    svy_format = 'numeric'
                elif has_string:
                    svy_format = 'string'
                
                metadata['svymod_format'] = svy_format
                metadata['svymod_values_found'] = [str(v) for v in svy_values[:10]]
                
                # Group by market and count survey types
                survey_counts = df.groupby(market_col)[svy_col].value_counts().unstack(fill_value=0)
                
                incomplete = []
                for market_id in survey_counts.index:
                    # Support both numeric (1, 2) and string ("Trader", "Market") values
                    # Trader surveys: 1, "1", 1.0, "Trader", "trader"
                    # Market surveys: 2, "2", 2.0, "Market", "market"
                    
                    trader_count = 0
                    market_count = 0
                    
                    for val in survey_counts.columns:
                        count = survey_counts.loc[market_id, val]
                        val_str = str(val).lower().strip()
                        
                        # Check if this is a trader survey value
                        if val in [1, 1.0] or val_str in ['1', '1.0', 'trader']:
                            trader_count += count
                        # Check if this is a market survey value
                        elif val in [2, 2.0] or val_str in ['2', '2.0', 'market']:
                            market_count += count
                    
                    if market_count < 1 or trader_count < 5:
                        incomplete.append({
                            'market_id': int(market_id) if pd.notna(market_id) and isinstance(market_id, (int, float)) else str(market_id),
                            'trader_surveys': int(trader_count),
                            'market_surveys': int(market_count)
                        })
                
                metadata['total_markets'] = len(survey_counts.index)
                metadata['complete_markets'] = len(survey_counts.index) - len(incomplete)
                metadata['incomplete_markets'] = len(incomplete)
                metadata['survey_column_used'] = svy_col
                
                if incomplete:
                    # Determine the correct values to show in suggestion based on format
                    if svy_format == 'string':
                        trader_val = "'Trader'"
                        market_val = "'Market'"
                    else:
                        trader_val = "1"
                        market_val = "2"
                    
                    errors.append(ValidationError(
                        code="BR3.1",
                        severity=Severity.ERROR,
                        message="{} of {} markets do not have complete surveys (required: 1 market + 5 trader surveys)".format(
                            len(incomplete), len(survey_counts.index)
                        ),
                        details={'incomplete_markets': incomplete[:20]},
                        suggestion="Ensure each market has at least 1 market survey ({}={}) and 5 trader surveys ({}={})".format(
                            svy_col, market_val, svy_col, trader_val
                        )
                    ))
            except Exception as e:
                logger.warning("Survey completeness check failed: {}".format(e))
        else:
            missing_cols = []
            if not svy_col:
                missing_cols.append("SVYMOD/SVY_MOD/SURVEYTYPE")
            if not market_col:
                missing_cols.append("MARKETID")
            warnings.append(ValidationError(
                code="BR3.1",
                severity=Severity.WARNING,
                message="Unable to check survey completeness: missing {} column(s)".format(", ".join(missing_cols))
            ))
    
    # BR3.2: Response ID uniqueness
    response_col = get_column('INSTANCEID', 'INSTANCE_ID', '_ID', 'RESPONSEID', 'RESPONSE_ID')
    if response_col:
        try:
            duplicates = df[df.duplicated(subset=[response_col], keep=False)]
            if len(duplicates) > 0:
                duplicate_ids = duplicates[response_col].unique().tolist()
                errors.append(ValidationError(
                    code="BR3.2",
                    severity=Severity.ERROR,
                    message="{} duplicate {} values found".format(len(duplicate_ids), response_col),
                    details={'duplicate_response_ids': duplicate_ids[:20]},
                    suggestion="Each {} must be unique. Remove or fix duplicate entries.".format(response_col)
                ))
            metadata['unique_responses'] = df[response_col].nunique()
            metadata['response_column_used'] = response_col
        except Exception as e:
            logger.warning("ResponseID uniqueness check failed: {}".format(e))
    
    # BR3.7: UUID uniqueness
    uuid_col = get_column('_UUID', 'UUID', 'INSTANCEID', 'INSTANCE_ID')
    if uuid_col:
        try:
            duplicates = df[df.duplicated(subset=[uuid_col], keep=False)]
            if len(duplicates) > 0:
                duplicate_uuids = duplicates[uuid_col].unique().tolist()
                errors.append(ValidationError(
                    code="BR3.7",
                    severity=Severity.ERROR,
                    message="{} duplicate {} values found".format(len(duplicate_uuids), uuid_col),
                    details={'duplicate_uuids': duplicate_uuids[:20]},
                    suggestion="Each {} must be unique. This may indicate duplicate submissions.".format(uuid_col)
                ))
            metadata['uuid_column_used'] = uuid_col
        except Exception as e:
            logger.warning("UUID uniqueness check failed: {}".format(e))
    
    # === METADATA SUMMARY ===
    metadata['total_rows'] = len(df)
    
    market_col = get_column('MARKETID', 'MARKET_ID')
    if market_col:
        metadata['unique_markets'] = df[market_col].nunique()
    
    result = LayerResult(
        layer_id=3,
        layer_name="Business Rules (RAW)",
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
        layer_summary = "\n### Layer {}: {}\n".format(layer_result['layer_id'], layer_result['layer_name'])
        layer_summary += "Passed: {}\n".format(layer_result['passed'])
        
        if layer_result['errors']:
            layer_summary += "Errors:\n"
            for err in layer_result['errors']:
                layer_summary += "  - [{}] {}: {}\n".format(err['code'], err['severity'], err['message'])
                
                # Aggiungi i details se presenti
                if err.get('details'):
                    if err['code'] == 'S1.4' and 'broken_row_numbers' in err['details']:
                        row_nums = err['details']['broken_row_numbers']
                        total = err['details'].get('total_broken_rows', len(row_nums))
                        layer_summary += "    Total broken rows: {}\n".format(total)
                        layer_summary += "    Row numbers: {}\n".format(row_nums)
                        
                        details_list = err['details'].get('broken_rows_details', [])
                        if details_list:
                            layer_summary += "    Details (first rows):\n"
                            for rd in details_list[:5]:
                                layer_summary += "      - Row {}: {} cols (expected {})".format(rd['row_number'], rd['actual'], rd['expected'])
                    
        if layer_result['warnings']:
            layer_summary += "Warnings:\n"
            for warn in layer_result['warnings']:
                layer_summary += "  - [{}] {}: {}\n".format(warn['code'], warn['severity'], warn['message'])
        
        if layer_result.get('metadata'):
            layer_summary += "Metadata: {}\n".format(json.dumps(layer_result['metadata'], indent=2, default=str))
        
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
        logger.error("Report generation failed: {}".format(e))
        final_report = "Error generating report: {}\n\nRaw results:\n{}".format(e, json.dumps(state['layer_results'], indent=2))
    
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

def build_graph(on_step: Optional[OnStepCallback] = None):
    """
    Costruisce il grafo LangGraph per MFI troubleshooting.
    
    Struttura:
        L0 → [route] → L1 → [route] → L2 → [route] → L3 → report → END
    """
    def wrap_node(node_name: str, fn):
        def wrapped(state: MFIState):
            if on_step is not None:
                on_step(node_name, dict(state))
            return fn(state)

        return wrapped

    graph = StateGraph(MFIState)
    
    # Add nodes
    graph.add_node("layer0", wrap_node("layer0", layer0_file_validation))
    graph.add_node("layer1", wrap_node("layer1", layer1_structural_parsing))
    graph.add_node("layer2", wrap_node("layer2", layer2_schema_validation))
    graph.add_node("layer3", wrap_node("layer3", layer3_business_rules))
    graph.add_node("report", wrap_node("report", layer5_generate_report))
    
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
    survey_type: str = "full mfi",
    on_step: Optional[OnStepCallback] = None
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
    agent = build_graph(on_step=on_step)
    return agent.invoke(initial_state)
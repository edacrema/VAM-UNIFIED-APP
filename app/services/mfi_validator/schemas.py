"""
MFI Validator - Schemas
=======================
Classi e modelli per la validazione dataset MFI.
"""
from __future__ import annotations

from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from enum import Enum
from dataclasses import dataclass, field, asdict


# ============================================================================
# ENUMS
# ============================================================================

class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class FileType(str, Enum):
    RAW = "RAW"
    PROCESSED = "PROCESSED"
    UNKNOWN = "UNKNOWN"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class ValidationError:
    """Errore o warning rilevato durante la validazione."""
    code: str
    severity: Severity
    message: str
    details: dict = field(default_factory=dict)
    suggestion: str = ""
    affected_rows: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "severity": self.severity.value,
            "message": self.message,
            "details": self.details,
            "suggestion": self.suggestion,
            "affected_rows": self.affected_rows[:10]  # Limit for serialization
        }


@dataclass
class LayerResult:
    """Risultato di un layer di validazione."""
    layer_id: int
    layer_name: str
    passed: bool
    can_continue: bool
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "layer_id": self.layer_id,
            "layer_name": self.layer_name,
            "passed": self.passed,
            "can_continue": self.can_continue,
            "errors": [e.to_dict() if hasattr(e, 'to_dict') else e for e in self.errors],
            "warnings": [w.to_dict() if hasattr(w, 'to_dict') else w for w in self.warnings],
            "metadata": self.metadata
        }


@dataclass
class MFITemplate:
    """Template per validazione MFI, caricato dinamicamente."""
    name: str
    columns: List[str]
    column_types: Dict[str, str] = field(default_factory=dict)
    required_columns: List[str] = field(default_factory=list)
    valid_dimensions: Dict[int, str] = field(default_factory=dict)
    valid_levels: Dict[int, str] = field(default_factory=dict)
    value_ranges: Dict[str, dict] = field(default_factory=dict)
    file_type: str = "UNKNOWN"

    @classmethod
    def from_csv(cls, file_path: str, template_name: str = "MFI Template") -> MFITemplate:
        import pandas as pd
        df = pd.read_csv(file_path, nrows=0)
        columns = df.columns.tolist()
        cols_upper = {c.upper() for c in columns}

        if 'SVY_MOD' in cols_upper or 'SURVEY_TYPE' in cols_upper:
            file_type = "RAW"
        elif 'MFIOUTPUTID' in cols_upper or 'TRADERSSAMPLESIZE' in cols_upper:
            file_type = "PROCESSED"
        else:
            file_type = "UNKNOWN"

        return cls(
            name=template_name,
            columns=columns,
            required_columns=columns,
            file_type=file_type
        )

    @classmethod
    def from_json(cls, file_path: str) -> MFITemplate:
        import json
        with open(file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        valid_dims = {int(k): v for k, v in config.get('valid_dimensions', {}).items()}
        valid_lvls = {int(k): v for k, v in config.get('valid_levels', {}).items()}

        return cls(
            name=config.get('name', 'MFI Template'),
            columns=config.get('columns', []),
            column_types=config.get('column_types', {}),
            required_columns=config.get('required_columns', config.get('columns', [])),
            valid_dimensions=valid_dims,
            valid_levels=valid_lvls,
            value_ranges=config.get('value_ranges', {}),
            file_type=config.get('file_type', 'UNKNOWN')
        )

    @classmethod
    def from_dataframe(cls, df, template_name: str = "MFI Template") -> MFITemplate:
        columns = df.columns.tolist()
        cols_upper = {c.upper() for c in columns}

        if 'SVY_MOD' in cols_upper:
            file_type = "RAW"
        elif 'MFIOUTPUTID' in cols_upper:
            file_type = "PROCESSED"
        else:
            file_type = "UNKNOWN"

        return cls(
            name=template_name,
            columns=columns,
            required_columns=columns,
            file_type=file_type
        )

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================================
# PYDANTIC MODELS (API)
# ============================================================================

class ValidateFileInput(BaseModel):
    """Input per l'endpoint di validazione."""
    survey_type: str = "full mfi"


class ValidateFileOutput(BaseModel):
    """Output dell'endpoint di validazione."""
    file_name: str
    country: Optional[str] = None
    survey_period: Optional[str] = None
    detected_file_type: Optional[str] = None
    llm_calls: int = 0
    layer_results: List[Dict[str, Any]]
    final_report: str
    success: bool
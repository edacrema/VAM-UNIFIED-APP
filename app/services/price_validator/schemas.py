"""
Price Validator - Schemas
=========================
Classi e modelli per la validazione dataset Price Data WFP.
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
            "affected_rows": self.affected_rows[:10]
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
class PriceDataTemplate:
    """Template per validazione Price Data."""
    name: str
    columns: List[str]
    column_types: Dict[str, str] = field(default_factory=dict)
    required_columns: List[str] = field(default_factory=list)

    @classmethod
    def from_csv(cls, file_path: str, template_name: str = "Price Data Template") -> PriceDataTemplate:
        import pandas as pd
        df = pd.read_csv(file_path, nrows=0)
        columns = df.columns.tolist()
        return cls(
            name=template_name,
            columns=columns,
            required_columns=columns
        )

    @classmethod
    def from_excel(cls, file_path: str, template_name: str = "Price Data Template") -> PriceDataTemplate:
        import pandas as pd
        df = pd.read_excel(file_path, nrows=0)
        columns = df.columns.tolist()
        return cls(
            name=template_name,
            columns=columns,
            required_columns=columns
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ProductClassification:
    """Risultato classificazione di un prodotto."""
    original_name: str
    matched_name: Optional[str]
    product_id: Optional[int]
    confidence: float
    method: str = "unknown"  # exact_match, partial_match, llm

    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================================
# PYDANTIC MODELS (API)
# ============================================================================

class ValidateFileInput(BaseModel):
    """Input per l'endpoint di validazione."""
    pass  # Solo file upload, nessun parametro extra richiesto


class ValidateFileOutput(BaseModel):
    """Output dell'endpoint di validazione."""
    file_name: str
    file_type: Optional[str] = None
    country: Optional[str] = None
    num_products: Optional[int] = None
    num_markets: Optional[int] = None
    detected_language: Optional[str] = None
    llm_calls: int = 0
    layer_results: List[Dict[str, Any]]
    product_classifications: List[Dict[str, Any]] = []
    final_report: str
    success: bool
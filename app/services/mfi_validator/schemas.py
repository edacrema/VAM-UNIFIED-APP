"""
MFI Validator - Schemas
=======================
Classi e modelli per la validazione dataset MFI.
"""
from __future__ import annotations

from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Literal
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
    """
    Template for RAW MFI dataset validation.
    
    Can be loaded from:
    - CSV file (columns inferred from header)
    - JSON file (full configuration)
    - DataFrame (columns inferred)
    
    Note: Only RAW MFI datasets are supported.
    """
    name: str
    columns: List[str]
    column_types: Dict[str, str] = field(default_factory=dict)
    required_columns: List[str] = field(default_factory=list)
    file_type: str = "RAW"

    @classmethod
    def from_csv(cls, file_path: str, template_name: str = "RAW MFI Template") -> MFITemplate:
        """
        Create template from a CSV file header.
        
        Args:
            file_path: Path to the CSV file
            template_name: Name for the template
            
        Returns:
            MFITemplate with columns from CSV header
        """
        import pandas as pd
        df = pd.read_csv(file_path, nrows=0)
        columns = df.columns.tolist()

        return cls(
            name=template_name,
            columns=columns,
            required_columns=columns,
            file_type="RAW"
        )

    @classmethod
    def from_json(cls, file_path: str) -> MFITemplate:
        """
        Create template from a JSON configuration file.
        
        Expected JSON structure:
        {
            "name": "Template Name",
            "columns": ["COL1", "COL2", ...],
            "column_types": {"COL1": "string", "COL2": "int", ...},
            "required_columns": ["COL1", "COL2", ...]
        }
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            MFITemplate with configuration from JSON
        """
        import json
        with open(file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        return cls(
            name=config.get('name', 'RAW MFI Template'),
            columns=config.get('columns', []),
            column_types=config.get('column_types', {}),
            required_columns=config.get('required_columns', config.get('columns', [])),
            file_type="RAW"
        )

    @classmethod
    def from_dataframe(cls, df, template_name: str = "RAW MFI Template") -> MFITemplate:
        """
        Create template from a pandas DataFrame.
        
        Args:
            df: pandas DataFrame
            template_name: Name for the template
            
        Returns:
            MFITemplate with columns from DataFrame
        """
        columns = df.columns.tolist()

        return cls(
            name=template_name,
            columns=columns,
            required_columns=columns,
            file_type="RAW"
        )
    
    @classmethod
    def default_raw_template(cls) -> MFITemplate:
        """
        Create the default RAW MFI template with standard indicators.
        
        Returns:
            MFITemplate with RAW_FILE_INDICATORS as required columns
        """
        # Import here to avoid circular dependency
        raw_indicators = [
            'SVYSTARTTIME',
            '_SUBMISSION_TIME',
            '_UOALATLNG_ALTITUDE',
            '_UOALATLNG_LATITUDE',
            '_UOALATLNG_LONGITUDE',
            '_UUID',
            'ADM0CODE',
            'ADM1CODE',
            'ADM2CODE',
            'ENUMNAME',
            'INSTANCEID',
            'MARKETID',
            'MARKETNAME',
            'MKTACCESSCNSTR',
            'MKTAVAILRUNOUT_GR',
            'MKTCOMPETLESSFIVE_GR',
            'MKTCOMPETONECONTR_GR',
            'MKTPRICESTAB_GR',
            'MKTPROTCNSTR',
            'MKTSTRUCTURECOND',
            'MKTSTRUCTURETYPE',
            'MKTTRADERNB',
            'MKTTRDSKUNB_CL',
            'SHOPCHECKOUTNB',
            'SHOPEMPLOYEENB',
            'SHOPSIZE',
            'SVYDATE',
            'SVYENDTIME',
            'SVYMOD',
            'SVYMODCONF',
            'TRDAVAILRUNOUT_GR',
            'TRDCONSENT2NF2F',
            'TRDCONSENTF2F',
            'TRDCONSENTNF2F',
            'TRDCUSTMGROUP',
            'TRDNODDENSLOCNAMEADM0',
            'TRDNODDENSLOCNAMEADM1',
            'TRDNODDENSLOCNAMEADM2',
            'TRDPRICESTAB_GR',
            'TRDRESILLEADTIME',
            'TRDRESILNODCOMPLEX_GR',
            'TRDRESILNODCRIT_GR',
            'TRDRESILNODDENS_GR',
            'TRDRESILSTOCKOUT',
            'TRDSERVICECHECKOUTEXP',
            'TRDSERVICELOYALTY',
            'TRDSERVICEPAYTYPE',
            'TRDSERVICEPOS',
            'TRDSERVICEPOSANALYSIS',
            'TRDSERVICESHOPEXP',
            'TRDSKUNB_CL',
            'TRDSTRUCTURECOND',
            'TRDSTRUCTURETYPE',
            'UOAAVAILSCARCE_GR',
            'UOALATLNG',
            'UOAPICTURE',
            'UOAPRICEINCR_GR',
            'UOAQLTYFANIMREFRIG',
            'UOAQLTYFANIMREFRIGWORK',
            'UOAQLTYFOOD',
            'UOAQLTYFPACKGOOD',
            'UOAQLTYFVEGFRUGOOD',
            'UOAQLTYFVEGFRUSEPARATE',
            'UOAQLTYPACKEXPIRY',
            'UOAQLTYPLASTGOOD',
            'UOASOLDGROUP_FCER',
            'UOASOLDGROUP_FOTH',
            'UOASOLDGROUP_GR',
            'UOASOLDGROUP_NF',
            'UOASTRUCTUREFEAT',
        ]
        
        return cls(
            name="Default RAW MFI Template",
            columns=raw_indicators,
            required_columns=raw_indicators,
            file_type="RAW"
        )

    def to_dict(self) -> dict:
        """Convert template to dictionary for serialization."""
        return asdict(self)
    
    def __repr__(self) -> str:
        return "MFITemplate(name='{}', columns={}, required={}, type='{}')".format(
            self.name,
            len(self.columns),
            len(self.required_columns),
            self.file_type
        )


# ============================================================================
# PYDANTIC MODELS (API)
# ============================================================================

class ValidateFileInput(BaseModel):
    """Input for the validation endpoint."""
    survey_type: str = "full mfi"


class ValidateFileOutput(BaseModel):
    """Output of the validation endpoint."""
    file_name: str
    country: Optional[str] = None
    survey_period: Optional[str] = None
    detected_file_type: Optional[str] = None
    llm_calls: int = 0
    layer_results: List[Dict[str, Any]]
    final_report: str
    success: bool


class ValidateFileStatusOutput(BaseModel):
    """Status of an in-progress validation."""
    run_id: str
    status: Literal["pending", "running", "completed", "failed"]
    current_node: Optional[str] = None
    progress_pct: int = 0
    warnings: List[str] = []
    error: Optional[str] = None
    traceback: Optional[str] = None
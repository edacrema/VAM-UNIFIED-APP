"""
MFI Validator - Router
======================
Endpoint FastAPI per il servizio di validazione MFI.
"""
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import tempfile
import os
import logging

from .graph import run_troubleshooting
from .schemas import ValidateFileOutput

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/validate-file", response_model=ValidateFileOutput)
async def validate_mfi_file(
    file: UploadFile = File(..., description="Dataset MFI in formato CSV"),
    survey_type: str = Form("full mfi", description="Tipo di survey: 'full mfi' o 'reduced mfi'"),
    template: Optional[UploadFile] = File(None, description="Template opzionale (.csv o .json)")
):
    """
    Valida un dataset MFI CSV.
    
    Esegue 5 layer di validazione:
    - Layer 0: File Validation (encoding, formato)
    - Layer 1: Structural Parsing (delimiter, righe corrotte)
    - Layer 2: Schema Validation (colonne richieste)
    - Layer 3: Business Rules (regole specifiche MFI)
    - Layer 5: Report Generation (diagnosi LLM)
    
    Returns:
        ValidateFileOutput con risultati di ogni layer e report finale
    """
    
    # Validazione input
    if not file.filename:
        raise HTTPException(status_code=400, detail="Nome file mancante")
    
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(
            status_code=400, 
            detail=f"Il file deve essere CSV. Ricevuto: {file.filename}"
        )
    
    if survey_type.lower() not in ["full mfi", "reduced mfi"]:
        raise HTTPException(
            status_code=400,
            detail=f"survey_type deve essere 'full mfi' o 'reduced mfi'. Ricevuto: {survey_type}"
        )
    
    # Salva file temporaneo
    tmp_path = None
    template_path = None
    
    try:
        # Salva file principale
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        # Salva template se fornito
        if template:
            suffix = ".json" if template.filename.lower().endswith(".json") else ".csv"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_t:
                template_content = await template.read()
                tmp_t.write(template_content)
                template_path = tmp_t.name
        
        # Esegui validazione
        logger.info(f"Starting validation for: {file.filename}")
        
        result = run_troubleshooting(
            file_path=tmp_path,
            template=template_path,
            survey_type=survey_type
        )
        
        # Calcola success
        layer_results = result.get("layer_results", [])
        success = all(lr.get("passed", False) for lr in layer_results)
        
        # Costruisci output
        output = ValidateFileOutput(
            file_name=file.filename,
            country=result.get("country"),
            survey_period=result.get("survey_period"),
            detected_file_type=result.get("detected_file_type"),
            llm_calls=result.get("llm_calls", 0),
            layer_results=layer_results,
            final_report=result.get("final_report", ""),
            success=success
        )
        
        logger.info(f"Validation completed for {file.filename}: success={success}")
        
        return output
        
    except Exception as e:
        logger.error(f"Validation error for {file.filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        # Cleanup file temporanei
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        if template_path and os.path.exists(template_path):
            os.unlink(template_path)


@router.get("/info")
def get_service_info():
    """
    Restituisce metadata del servizio per il frontend.
    
    Il frontend può usare queste informazioni per costruire
    dinamicamente il form di input.
    """
    return {
        "id": "mfi-validator",
        "name": "MFI Dataset Validator",
        "description": "Valida dataset MFI (CSV) contro template WFP con 5 layer di controllo. "
                       "Rileva errori strutturali, schema non conforme, regole business violate "
                       "e genera un report diagnostico dettagliato.",
        "version": "1.0.0",
        "inputs": [
            {
                "name": "file",
                "type": "file",
                "required": True,
                "accept": ".csv",
                "label": "Dataset MFI",
                "description": "File CSV contenente i dati MFI da validare"
            },
            {
                "name": "survey_type",
                "type": "select",
                "required": True,
                "options": [
                    {"value": "full mfi", "label": "Full MFI"},
                    {"value": "reduced mfi", "label": "Reduced MFI"}
                ],
                "default": "full mfi",
                "label": "Tipo Survey",
                "description": "Tipo di survey MFI (influenza le regole di validazione)"
            },
            {
                "name": "template",
                "type": "file",
                "required": False,
                "accept": ".csv,.json",
                "label": "Template (opzionale)",
                "description": "Template personalizzato per la validazione delle colonne"
            }
        ],
        "outputs": {
            "file_name": "Nome del file validato",
            "country": "Paese rilevato dal dataset",
            "survey_period": "Periodo della survey",
            "detected_file_type": "Tipo file (RAW/PROCESSED)",
            "llm_calls": "Numero di chiamate LLM effettuate",
            "layer_results": "Risultati dettagliati per ogni layer",
            "final_report": "Report diagnostico generato da LLM",
            "success": "True se tutti i layer sono passati"
        },
        "layers": [
            {"id": 0, "name": "File Validation", "description": "Verifica encoding, formato, estensione"},
            {"id": 1, "name": "Structural Parsing", "description": "Rileva delimiter, righe corrotte, over-quoted"},
            {"id": 2, "name": "Schema Validation", "description": "Verifica colonne richieste, duplicati"},
            {"id": 3, "name": "Business Rules", "description": "Regole specifiche MFI (dimensioni, livelli, range)"},
            {"id": 5, "name": "Report Generation", "description": "Genera report diagnostico con LLM"}
        ]
    }


@router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "mfi-validator"}
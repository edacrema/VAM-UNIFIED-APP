"""
Price Validator - Router
========================
Endpoint FastAPI per il servizio di validazione Price Data.
"""
from fastapi import APIRouter, UploadFile, File, HTTPException
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
async def validate_price_data_file(
    file: UploadFile = File(..., description="Dataset Price Data in formato CSV o Excel"),
    template: Optional[UploadFile] = File(None, description="Template opzionale (.csv o .xlsx)")
):
    """
    Valida un dataset Price Data (CSV o Excel).
    
    Esegue 5 layer di validazione:
    - Layer 0: File Validation (encoding, formato)
    - Layer 1: Structural Parsing (delimiter, righe corrotte)
    - Layer 2: Schema Validation (colonne richieste, lingua)
    - Layer 3: Product Classification (matching prodotti WFP)
    - Layer 4: Report Generation (diagnosi LLM)
    
    Returns:
        ValidateFileOutput con risultati di ogni layer e report finale
    """
    
    # Validazione input
    if not file.filename:
        raise HTTPException(status_code=400, detail="Nome file mancante")
    
    valid_extensions = ['.csv', '.xlsx', '.xls']
    file_ext = os.path.splitext(file.filename)[1].lower()
    
    if file_ext not in valid_extensions:
        raise HTTPException(
            status_code=400, 
            detail=f"Formato file non supportato. Usare: {', '.join(valid_extensions)}"
        )
    
    # Salva file temporaneo
    tmp_path = None
    template_path = None
    
    try:
        # Salva file principale
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        # Salva template se fornito
        if template:
            template_ext = os.path.splitext(template.filename)[1].lower()
            with tempfile.NamedTemporaryFile(delete=False, suffix=template_ext) as tmp_t:
                template_content = await template.read()
                tmp_t.write(template_content)
                template_path = tmp_t.name
        
        # Esegui validazione
        logger.info(f"Starting validation for: {file.filename}")
        
        result = run_troubleshooting(
            file_path=tmp_path,
            template_path=template_path
        )
        
        # Calcola success
        layer_results = result.get("layer_results", [])
        success = all(lr.get("passed", False) for lr in layer_results)
        
        # Costruisci output
        output = ValidateFileOutput(
            file_name=file.filename,
            file_type=result.get("file_type"),
            country=result.get("country"),
            num_products=result.get("num_products"),
            num_markets=result.get("num_markets"),
            detected_language=result.get("detected_language"),
            llm_calls=result.get("llm_calls", 0),
            layer_results=layer_results,
            product_classifications=result.get("product_classifications", []),
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
        "id": "price-validator",
        "name": "Price Data Validator",
        "description": "Valida dataset Price Data (CSV o Excel) con 5 layer di controllo. "
                       "Rileva errori strutturali, schema non conforme, "
                       "classifica prodotti contro lista WFP ufficiale "
                       "e genera un report diagnostico dettagliato.",
        "version": "1.0.0",
        "inputs": [
            {
                "name": "file",
                "type": "file",
                "required": True,
                "accept": ".csv,.xlsx,.xls",
                "label": "Dataset Price Data",
                "description": "File CSV o Excel contenente i dati di prezzo da validare"
            },
            {
                "name": "template",
                "type": "file",
                "required": False,
                "accept": ".csv,.xlsx,.xls",
                "label": "Template (opzionale)",
                "description": "Template personalizzato per la validazione delle colonne"
            }
        ],
        "outputs": {
            "file_name": "Nome del file validato",
            "file_type": "Tipo file (CSV/EXCEL)",
            "country": "Paese rilevato dal dataset",
            "num_products": "Numero di prodotti unici",
            "num_markets": "Numero di mercati unici",
            "detected_language": "Lingua rilevata (en, fr, es, ar)",
            "llm_calls": "Numero di chiamate LLM effettuate",
            "layer_results": "Risultati dettagliati per ogni layer",
            "product_classifications": "Classificazioni prodotti con confidenza",
            "final_report": "Report diagnostico generato da LLM",
            "success": "True se tutti i layer sono passati"
        },
        "layers": [
            {"id": 0, "name": "File Validation", "description": "Verifica encoding, formato, estensione"},
            {"id": 1, "name": "Structural Parsing", "description": "Rileva delimiter, righe corrotte"},
            {"id": 2, "name": "Schema Validation", "description": "Verifica colonne richieste, lingua"},
            {"id": 3, "name": "Product Classification", "description": "Classifica prodotti contro lista WFP"},
            {"id": 4, "name": "Report Generation", "description": "Genera report diagnostico con LLM"}
        ],
        "supported_formats": ["CSV", "Excel (.xlsx, .xls)"],
        "product_list_size": "~200 prodotti WFP standard"
    }


@router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "price-validator"}


@router.get("/products")
def get_wfp_products():
    """
    Restituisce la lista dei prodotti WFP riconosciuti.
    Utile per debug e reference.
    """
    from .graph import WFP_PRODUCTS
    
    return {
        "total": len(WFP_PRODUCTS),
        "products": [
            {"name": name, "id": pid}
            for name, pid in sorted(WFP_PRODUCTS.items())
        ]
    }
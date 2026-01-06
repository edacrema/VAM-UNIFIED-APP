"""
MFI Drafter - Router
====================
Endpoint FastAPI per il servizio MFI Report Generator.
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Optional
import logging
import numpy as np

from .graph import run_mfi_report_generation
from .schemas import (
    GenerateMFIReportInput,
    GenerateMFIReportOutput,
    MFIReportStatusOutput,
    MFI_DIMENSIONS,
    get_risk_level
)

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory store per report status
_report_status: dict = {}


@router.post("/generate", response_model=GenerateMFIReportOutput)
async def generate_mfi_report(input_data: GenerateMFIReportInput):
    """
    Genera un MFI Report completo.
    
    Il processo include:
    1. MFI Data Agent: Recupera/genera dati MFI per mercati
    2. Context Retrieval: Recupera notizie contestuali
    3. Context Extractor: Estrae contesto con LLM
    4. Graph Designer: Genera visualizzazioni (radar, heatmap, etc.)
    5. Dimension Drafter: Genera findings per ogni dimensione
    6. Executive Summary Drafter: Genera executive summary
    7. Red Team: Quality assurance con possibile loop di correzione
    
    Returns:
        GenerateMFIReportOutput con tutte le sezioni del report
    """
    try:
        logger.info(f"Starting MFI report generation for {input_data.country}")
        
        result = run_mfi_report_generation(
            country=input_data.country,
            data_collection_start=input_data.data_collection_start,
            data_collection_end=input_data.data_collection_end,
            markets=input_data.markets
        )
        
        # Calculate national MFI
        dimension_scores = result.get("dimension_scores", [])
        national_mfi = round(
            np.mean([d["national_score"] for d in dimension_scores]), 1
        ) if dimension_scores else 0.0
        
        # Calculate risk distribution
        risk_dist = {}
        for m in result.get("markets_data", []):
            risk_dist[m["risk_level"]] = risk_dist.get(m["risk_level"], 0) + 1
        
        output = GenerateMFIReportOutput(
            run_id=result.get("run_id", "unknown"),
            country=input_data.country,
            data_collection_start=input_data.data_collection_start,
            data_collection_end=input_data.data_collection_end,
            survey_metadata=result.get("survey_metadata", {}),
            national_mfi=national_mfi,
            risk_distribution=risk_dist,
            markets_data=result.get("markets_data", []),
            dimension_scores=result.get("dimension_scores", []),
            executive_summary=result.get("executive_summary", ""),
            dimension_findings=result.get("dimension_findings", {}),
            country_context=result.get("country_context"),
            visualizations=result.get("visualizations", {}),
            warnings=result.get("warnings", []),
            llm_calls=result.get("llm_calls", 0),
            correction_attempts=result.get("correction_attempts", 0),
            success=True
        )
        
        logger.info(f"MFI report generation completed: {output.run_id}")
        
        return output
        
    except Exception as e:
        logger.error(f"MFI report generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate-async")
async def generate_mfi_report_async(
    input_data: GenerateMFIReportInput,
    background_tasks: BackgroundTasks
):
    """
    Avvia generazione report in background.
    
    Returns:
        run_id per polling dello status
    """
    import uuid
    run_id = f"mfi_{uuid.uuid4().hex[:8]}"
    
    _report_status[run_id] = {
        "status": "pending",
        "current_node": None,
        "progress_pct": 0,
        "warnings": []
    }
    
    async def run_in_background():
        try:
            _report_status[run_id]["status"] = "running"
            
            result = run_mfi_report_generation(
                country=input_data.country,
                data_collection_start=input_data.data_collection_start,
                data_collection_end=input_data.data_collection_end,
                markets=input_data.markets
            )
            
            _report_status[run_id] = {
                "status": "completed",
                "current_node": "END",
                "progress_pct": 100,
                "result": result,
                "warnings": result.get("warnings", [])
            }
            
        except Exception as e:
            _report_status[run_id] = {
                "status": "failed",
                "error": str(e),
                "progress_pct": 0,
                "warnings": []
            }
    
    background_tasks.add_task(run_in_background)
    
    return {"run_id": run_id, "status": "pending"}


@router.get("/status/{run_id}", response_model=MFIReportStatusOutput)
async def get_report_status(run_id: str):
    """Controlla lo status di un report in generazione."""
    if run_id not in _report_status:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")
    
    status = _report_status[run_id]
    
    return MFIReportStatusOutput(
        run_id=run_id,
        status=status.get("status", "pending"),
        current_node=status.get("current_node"),
        progress_pct=status.get("progress_pct", 0),
        warnings=status.get("warnings", [])
    )


@router.get("/result/{run_id}", response_model=GenerateMFIReportOutput)
async def get_report_result(run_id: str):
    """Recupera il risultato di un report completato."""
    if run_id not in _report_status:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")
    
    status = _report_status[run_id]
    
    if status.get("status") != "completed":
        raise HTTPException(
            status_code=400, 
            detail=f"Report not completed. Current status: {status.get('status')}"
        )
    
    result = status.get("result", {})
    dimension_scores = result.get("dimension_scores", [])
    national_mfi = round(
        np.mean([d["national_score"] for d in dimension_scores]), 1
    ) if dimension_scores else 0.0
    
    risk_dist = {}
    for m in result.get("markets_data", []):
        risk_dist[m["risk_level"]] = risk_dist.get(m["risk_level"], 0) + 1
    
    return GenerateMFIReportOutput(
        run_id=run_id,
        country=result.get("country", "Unknown"),
        data_collection_start=result.get("data_collection_start", "Unknown"),
        data_collection_end=result.get("data_collection_end", "Unknown"),
        survey_metadata=result.get("survey_metadata", {}),
        national_mfi=national_mfi,
        risk_distribution=risk_dist,
        markets_data=result.get("markets_data", []),
        dimension_scores=result.get("dimension_scores", []),
        executive_summary=result.get("executive_summary", ""),
        dimension_findings=result.get("dimension_findings", {}),
        country_context=result.get("country_context"),
        visualizations=result.get("visualizations", {}),
        warnings=result.get("warnings", []),
        llm_calls=result.get("llm_calls", 0),
        correction_attempts=result.get("correction_attempts", 0),
        success=True
    )


@router.get("/info")
def get_service_info():
    """Restituisce metadata del servizio per il frontend."""
    return {
        "id": "mfi-drafter",
        "name": "MFI Report Generator",
        "description": "Genera Market Functionality Index (MFI) Reports completi. "
                       "Analizza 9 dimensioni di funzionalità di mercato, genera "
                       "visualizzazioni, executive summary e raccomandazioni.",
        "version": "1.0.0",
        "inputs": [
            {
                "name": "country",
                "type": "string",
                "required": True,
                "label": "Country",
                "description": "Nome del paese"
            },
            {
                "name": "data_collection_start",
                "type": "string",
                "required": True,
                "label": "Data Collection Start",
                "description": "Data inizio raccolta dati (YYYY-MM-DD)"
            },
            {
                "name": "data_collection_end",
                "type": "string",
                "required": True,
                "label": "Data Collection End",
                "description": "Data fine raccolta dati (YYYY-MM-DD)"
            },
            {
                "name": "markets",
                "type": "array",
                "required": True,
                "label": "Markets",
                "description": "Lista dei mercati surveyati (nomi)"
            }
        ],
        "outputs": {
            "run_id": "Identificativo univoco della generazione",
            "national_mfi": "Score MFI nazionale (0-10)",
            "risk_distribution": "Distribuzione mercati per livello di rischio",
            "markets_data": "Dati dettagliati per ogni mercato",
            "dimension_scores": "Score per ogni dimensione MFI",
            "executive_summary": "Executive summary generato",
            "dimension_findings": "Findings per ogni dimensione",
            "visualizations": "Grafici in formato Base64",
            "llm_calls": "Numero di chiamate LLM effettuate",
            "success": "True se la generazione è completata"
        },
        "workflow_nodes": [
            {"id": "mfi_data_agent", "name": "MFI Data Agent", "description": "Recupera/genera dati MFI"},
            {"id": "context_retrieval", "name": "Context Retrieval", "description": "Recupera notizie contestuali"},
            {"id": "context_extractor", "name": "Context Extractor", "description": "Estrae contesto con LLM"},
            {"id": "mfi_graph_designer", "name": "Graph Designer", "description": "Genera visualizzazioni"},
            {"id": "dimension_drafter", "name": "Dimension Drafter", "description": "Genera findings per dimensione"},
            {"id": "executive_summary_drafter", "name": "Executive Summary", "description": "Genera executive summary"},
            {"id": "red_team", "name": "Red Team QA", "description": "Quality assurance"}
        ],
        "mfi_dimensions": MFI_DIMENSIONS,
        "risk_levels": ["Low Risk", "Medium Risk", "High Risk", "Very High Risk"]
    }


@router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "mfi-drafter"}


@router.get("/dimensions")
def get_mfi_dimensions():
    """Restituisce le 9 dimensioni MFI con descrizioni."""
    from .graph import DIMENSION_DESCRIPTIONS
    
    return {
        "dimensions": [
            {
                "name": dim,
                "description": DIMENSION_DESCRIPTIONS.get(dim, ""),
                "score_range": "0-10",
                "thresholds": {
                    "low_risk": "≥7.0",
                    "medium_risk": "5.5-6.9",
                    "high_risk": "4.0-5.4",
                    "very_high_risk": "<4.0"
                }
            }
            for dim in MFI_DIMENSIONS
        ]
    }


@router.get("/sample-markets")
def get_sample_markets():
    """Restituisce mercati di esempio per testing."""
    return {
        "Ghana": {
            "markets": [
                "Gushegu", "Karaga", "Nanton", "Sang", "Tamale Aboabo", "Yendi",
                "Fumbisi", "Bussie", "Gwollu", "Nyoli", "Tangasie", "Tumu"
            ]
        },
        "Sudan": {
            "markets": [
                "Omdurman", "Khartoum Central", "El Fasher", "Nyala",
                "Kassala City", "Gedaref", "Port Sudan"
            ]
        },
        "Yemen": {
            "markets": [
                "Sana'a Central", "Aden Port", "Taiz City",
                "Hodeidah", "Mukalla", "Ibb"
            ]
        }
    }
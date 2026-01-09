"""
MFI Drafter - Router
====================
FastAPI endpoints for the MFI Report Generator service.
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

from app.shared.async_runs import (
    create_run,
    get_run,
    set_run_completed,
    set_run_failed,
    update_run,
    update_run_progress,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory store per report status
_report_status: dict = {}


@router.post("/generate", response_model=GenerateMFIReportOutput)
async def generate_mfi_report(input_data: GenerateMFIReportInput):
    """
    Generates a full MFI report.

    The process includes:
    1. MFI Data Agent: Retrieves/generates MFI data for markets
    2. Context Retrieval: Retrieves contextual news
    3. Context Extractor: Extracts context with the LLM
    4. Graph Designer: Generates visualizations (radar, heatmap, etc.)
    5. Dimension Drafter: Drafts findings for each dimension
    6. Executive Summary Drafter: Drafts the executive summary
    7. Red Team: Quality assurance with possible correction loop

    Returns:
        GenerateMFIReportOutput with all report sections
    """
    try:
        logger.info(f"Starting MFI report generation for {input_data.country}")
        
        result = run_mfi_report_generation(
            country=input_data.country,
            data_collection_start=input_data.data_collection_start,
            data_collection_end=input_data.data_collection_end,
            markets=input_data.markets,
            use_mock_data=input_data.use_mock_data,
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
    Starts report generation in the background.

    Returns:
        run_id for polling status
    """
    import uuid
    run_id = f"mfi_{uuid.uuid4().hex[:8]}"

    create_run(run_id)

    progress_map = {
        "mfi_data_agent": 10,
        "context_retrieval": 25,
        "context_extractor": 40,
        "mfi_graph_designer": 55,
        "dimension_drafter": 75,
        "executive_summary_drafter": 90,
        "red_team": 95,
    }
    
    def run_in_background():
        try:
            update_run(run_id, status="running", error=None, traceback=None)

            def on_step(node_name: str, _state: dict):
                progress = progress_map.get(node_name)
                if progress is not None:
                    update_run_progress(run_id, current_node=node_name, progress_pct=progress)
                else:
                    update_run(run_id, current_node=node_name)

                context_counts = _state.get("context_counts")
                if isinstance(context_counts, dict):
                    meta_update = {"context_counts": context_counts}
                    retriever_traces = _state.get("retriever_traces")
                    if isinstance(retriever_traces, list):
                        meta_update["retriever_traces"] = retriever_traces
                    update_run(run_id, metadata=meta_update)
            
            result = run_mfi_report_generation(
                country=input_data.country,
                data_collection_start=input_data.data_collection_start,
                data_collection_end=input_data.data_collection_end,
                markets=input_data.markets,
                use_mock_data=input_data.use_mock_data,
                on_step=on_step
            )
            
            update_run(run_id, warnings=result.get("warnings", []))
            set_run_completed(run_id, result=result)
            
        except Exception as e:
            import traceback

            tb_str = traceback.format_exc()
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(e), traceback=tb_str, current_node=current_node)
    
    background_tasks.add_task(run_in_background)
    
    return {"run_id": run_id, "status": "pending"}


@router.get("/status/{run_id}", response_model=MFIReportStatusOutput)
async def get_report_status(run_id: str):
    """Checks the status of an in-progress report."""
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")

    return MFIReportStatusOutput(
        run_id=run_id,
        status=run.status,
        current_node=run.current_node,
        progress_pct=run.progress_pct,
        warnings=run.warnings,
        metadata=getattr(run, "metadata", {}) or {},
        error=run.error,
        traceback=run.traceback,
    )


@router.get("/result/{run_id}", response_model=GenerateMFIReportOutput)
async def get_report_result(run_id: str):
    """Retrieves the result of a completed report."""
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")

    if run.status != "completed":
        raise HTTPException(
            status_code=400, 
            detail=f"Report not completed. Current status: {run.status}"
        )

    result = run.result or {}

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
    """Returns service metadata for the frontend."""
    return {
        "id": "mfi-drafter",
        "name": "MFI Report Generator",
        "description": "Generates full Market Functionality Index (MFI) reports. "
                       "Analyzes 9 market functionality dimensions and generates "
                       "visualizations, an executive summary, and recommendations.",
        "version": "1.0.0",
        "inputs": [
            {
                "name": "country",
                "type": "string",
                "required": True,
                "label": "Country",
                "description": "Country name"
            },
            {
                "name": "data_collection_start",
                "type": "string",
                "required": True,
                "label": "Data Collection Start",
                "description": "Data collection start date (YYYY-MM-DD)"
            },
            {
                "name": "data_collection_end",
                "type": "string",
                "required": True,
                "label": "Data Collection End",
                "description": "Data collection end date (YYYY-MM-DD)"
            },
            {
                "name": "markets",
                "type": "array",
                "required": True,
                "label": "Markets",
                "description": "List of surveyed markets (names)"
            },
            {
                "name": "use_mock_data",
                "type": "boolean",
                "required": False,
                "label": "Use Mock Data",
                "description": "If True, use simulated context documents instead of calling real external APIs",
                "default": True
            }
        ],
        "outputs": {
            "run_id": "Unique generation identifier",
            "national_mfi": "National MFI score (0-10)",
            "risk_distribution": "Distribution of markets by risk level",
            "markets_data": "Detailed data for each market",
            "dimension_scores": "Score for each MFI dimension",
            "executive_summary": "Generated executive summary",
            "dimension_findings": "Findings for each dimension",
            "visualizations": "Charts in Base64 format",
            "llm_calls": "Number of LLM calls performed",
            "success": "True if generation is completed"
        },
        "workflow_nodes": [
            {"id": "mfi_data_agent", "name": "MFI Data Agent", "description": "Retrieves/generates MFI data"},
            {"id": "context_retrieval", "name": "Context Retrieval", "description": "Retrieves contextual news"},
            {"id": "context_extractor", "name": "Context Extractor", "description": "Extracts context with the LLM"},
            {"id": "mfi_graph_designer", "name": "Graph Designer", "description": "Generates visualizations"},
            {"id": "dimension_drafter", "name": "Dimension Drafter", "description": "Drafts findings per dimension"},
            {"id": "executive_summary_drafter", "name": "Executive Summary", "description": "Drafts executive summary"},
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
    """Returns the 9 MFI dimensions with descriptions."""
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
    """Returns sample markets for testing."""
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
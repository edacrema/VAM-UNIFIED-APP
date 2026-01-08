"""
Market Monitor - Router
=======================
FastAPI endpoints for the Market Monitor service.
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Optional
import logging
import traceback

from .graph import run_report_generation, AVAILABLE_MODULES
from .schemas import (
    GenerateReportInput,
    GenerateReportOutput,
    ReportStatusOutput
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

# In-memory store per report status (in produzione usare Redis/DB)
_report_status: dict = {}


@router.post("/generate", response_model=GenerateReportOutput)
async def generate_market_monitor(input_data: GenerateReportInput):
    """
    Generates a full Market Monitor report.

    The process includes:
    1. Data Agent: Retrieves/generates price data (mock or API)
    2. Graph Designer: Creates visualizations
    3. News Retrieval: Retrieves contextual news
    4. Event Mapper: Extracts key events
    5. Trend Analyst: Analyzes market trends
    6. Module Orchestrator: Runs optional modules (e.g., exchange rate)
    7. Highlights Drafter: Drafts the highlights section
    8. Narrative Drafter: Drafts narrative sections
    9. Red Team: Quality assurance with possible correction loop

    Returns:
        GenerateReportOutput with all report sections
    """
    try:
        logger.info(f"Starting report generation for {input_data.country} - {input_data.time_period}")
        
        # If admin1_list is empty, use default
        admin1_list = input_data.admin1_list
        if not admin1_list:
            admin1_list = [f"{input_data.country} North", f"{input_data.country} South", 
                          f"{input_data.country} Central"]
        
        # Run generation
        result = run_report_generation(
            country=input_data.country,
            time_period=input_data.time_period,
            commodity_list=input_data.commodity_list,
            admin1_list=admin1_list,
            currency_code=input_data.currency_code,
            enabled_modules=input_data.enabled_modules,
            previous_report_text=input_data.previous_report_text,
            use_mock_data=input_data.use_mock_data
        )
        
        # Build output
        output = GenerateReportOutput(
            run_id=result.get("run_id", "unknown"),
            country=input_data.country,
            time_period=input_data.time_period,
            report_sections=result.get("report_draft_sections", {}),
            visualizations=result.get("visualizations", {}),
            data_statistics=result.get("data_statistics", {}),
            trend_analysis=result.get("trend_analysis"),
            events=result.get("events", []),
            module_sections=result.get("module_sections", {}),
            document_references=result.get("document_references", []),
            news_counts=result.get("news_counts", {}),
            warnings=result.get("warnings", []),
            llm_calls=result.get("llm_calls", 0),
            success=True
        )
        
        logger.info(f"Report generation completed: {output.run_id}")
        
        return output
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate-async")
async def generate_market_monitor_async(
    input_data: GenerateReportInput,
    background_tasks: BackgroundTasks
):
    """
    Starts report generation in the background.
    Useful for reports that take a long time.

    Returns:
        run_id for polling status
    """
    import uuid
    run_id = f"run_{uuid.uuid4().hex[:8]}"

    create_run(run_id)

    progress_map = {
        "data_agent": 10,
        "graph_designer": 20,
        "news_retrieval": 30,
        "event_mapper": 40,
        "trend_analyst": 55,
        "module_orchestrator": 65,
        "highlights_drafter": 75,
        "narrative_drafter": 85,
        "red_team": 95,
    }
    
    def run_in_background():
        try:
            update_run(run_id, status="running", error=None, traceback=None)
            
            admin1_list = input_data.admin1_list or [
                f"{input_data.country} North", 
                f"{input_data.country} South"
            ]
            
            def on_step(node_name: str, _state: dict):
                progress = progress_map.get(node_name)
                if progress is not None:
                    update_run_progress(run_id, current_node=node_name, progress_pct=progress)
                else:
                    update_run(run_id, current_node=node_name)

                news_counts = _state.get("news_counts")
                if isinstance(news_counts, dict):
                    update_run(run_id, metadata={"news_counts": news_counts})
                    
            result = run_report_generation(
                country=input_data.country,
                time_period=input_data.time_period,
                commodity_list=input_data.commodity_list,
                admin1_list=admin1_list,
                currency_code=input_data.currency_code,
                enabled_modules=input_data.enabled_modules,
                previous_report_text=input_data.previous_report_text,
                use_mock_data=input_data.use_mock_data,
                on_step=on_step
            )

            update_run(run_id, warnings=result.get("warnings", []))
            set_run_completed(run_id, result=result)
            
        except Exception as e:
            tb_str = traceback.format_exc()
            logger.exception(f"Report generation failed for {run_id}: {e}")

            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(e), traceback=tb_str, current_node=current_node)
    
    background_tasks.add_task(run_in_background)
    
    return {"run_id": run_id, "status": "pending"}


@router.get("/status/{run_id}", response_model=ReportStatusOutput)
async def get_report_status(run_id: str):
    """
    Checks the status of an in-progress report.
    """
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")

    return ReportStatusOutput(
        run_id=run_id,
        status=run.status,
        current_node=run.current_node,
        progress_pct=run.progress_pct,
        warnings=run.warnings,
        metadata=getattr(run, "metadata", {}) or {},
        error=run.error,
        traceback=run.traceback,
    )


@router.get("/result/{run_id}", response_model=GenerateReportOutput)
async def get_report_result(run_id: str):
    """
    Retrieves the result of a completed report.
    """
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")

    if run.status != "completed":
        raise HTTPException(
            status_code=400, 
            detail=f"Report not completed. Current status: {run.status}"
        )

    result = run.result or {}
    
    return GenerateReportOutput(
        run_id=run_id,
        country=result.get("country", "Unknown"),
        time_period=result.get("time_period", "Unknown"),
        report_sections=result.get("report_draft_sections", {}),
        visualizations=result.get("visualizations", {}),
        data_statistics=result.get("data_statistics", {}),
        trend_analysis=result.get("trend_analysis"),
        events=result.get("events", []),
        module_sections=result.get("module_sections", {}),
        document_references=result.get("document_references", []),
        news_counts=result.get("news_counts", {}),
        warnings=result.get("warnings", []) or run.warnings,
        llm_calls=result.get("llm_calls", 0),
        success=True
    )


@router.get("/info")
def get_service_info():
    """
    Returns service metadata for the frontend.
    """
    return {
        "id": "market-monitor",
        "name": "Market Monitor Generator",
        "description": "Generates full Market Monitor reports with price analysis, "
                       "market trend analysis, visualizations, and narrative sections. "
                       "Includes optional modules such as exchange rate analysis.",
        "version": "1.0.0",
        "inputs": [
            {
                "name": "country",
                "type": "string",
                "required": True,
                "label": "Country",
                "description": "Country name (e.g., 'Sudan', 'Yemen', 'Myanmar')"
            },
            {
                "name": "time_period",
                "type": "string",
                "required": True,
                "label": "Time Period",
                "description": "Period in YYYY-MM format (e.g., '2025-01')"
            },
            {
                "name": "commodity_list",
                "type": "array",
                "required": False,
                "label": "Commodities",
                "description": "List of commodities to analyze",
                "default": ["Sorghum", "Wheat flour", "Cooking oil", "Sugar"]
            },
            {
                "name": "admin1_list",
                "type": "array",
                "required": False,
                "label": "Regions (Admin1)",
                "description": "List of regions to include"
            },
            {
                "name": "currency_code",
                "type": "string",
                "required": False,
                "label": "Currency Code",
                "description": "ISO 4217 currency code (e.g., 'SDG', 'YER')",
                "default": "USD"
            },
            {
                "name": "enabled_modules",
                "type": "array",
                "required": False,
                "label": "Optional Modules",
                "description": "Optional modules to enable",
                "default": ["exchange_rate"],
                "options": list(AVAILABLE_MODULES.keys())
            },
            {
                "name": "use_mock_data",
                "type": "boolean",
                "required": False,
                "label": "Use Mock Data",
                "description": "If True, use simulated data instead of real APIs",
                "default": True
            }
        ],
        "outputs": {
            "run_id": "Unique generation identifier",
            "report_sections": "Report sections (HIGHLIGHTS, MARKET_OVERVIEW, etc.)",
            "visualizations": "Charts in Base64 format",
            "data_statistics": "Computed statistics (MoM, YoY)",
            "trend_analysis": "Market trend analysis",
            "events": "Events extracted from news",
            "module_sections": "Sections generated by optional modules",
            "llm_calls": "Number of LLM calls performed",
            "success": "True if generation completed successfully"
        },
        "workflow_nodes": [
            {"id": "data_agent", "name": "Data Agent", "description": "Retrieves and processes price data"},
            {"id": "graph_designer", "name": "Graph Designer", "description": "Generates visualizations"},
            {"id": "news_retrieval", "name": "News Retrieval", "description": "Retrieves contextual news"},
            {"id": "event_mapper", "name": "Event Mapper", "description": "Extracts key events"},
            {"id": "trend_analyst", "name": "Trend Analyst", "description": "Analyzes market trends"},
            {"id": "module_orchestrator", "name": "Module Orchestrator", "description": "Runs optional modules"},
            {"id": "highlights_drafter", "name": "Highlights Drafter", "description": "Drafts highlights section"},
            {"id": "narrative_drafter", "name": "Narrative Drafter", "description": "Drafts narrative sections"},
            {"id": "red_team", "name": "Red Team QA", "description": "Quality assurance and fact-checking"}
        ],
        "available_modules": [
            {
                "id": "exchange_rate",
                "name": "Exchange Rate Analysis",
                "description": "Exchange rate analysis using Trading Economics data"
            }
        ]
    }


@router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "market-monitor"}


@router.get("/countries")
def get_supported_countries():
    """
    Returns the list of supported countries with their currencies.
    """
    from .graph import CURRENCY_SYMBOLS
    
    countries = [
        {"name": "Sudan", "currency_code": "SDG", "currency_name": "Sudanese Pound"},
        {"name": "Yemen", "currency_code": "YER", "currency_name": "Yemeni Rial"},
        {"name": "Myanmar", "currency_code": "MMK", "currency_name": "Myanmar Kyat"},
        {"name": "Syria", "currency_code": "SYP", "currency_name": "Syrian Pound"},
        {"name": "Afghanistan", "currency_code": "AFN", "currency_name": "Afghan Afghani"},
        {"name": "Ethiopia", "currency_code": "ETB", "currency_name": "Ethiopian Birr"},
        {"name": "Nigeria", "currency_code": "NGN", "currency_name": "Nigerian Naira"},
        {"name": "Pakistan", "currency_code": "PKR", "currency_name": "Pakistani Rupee"},
        {"name": "Bangladesh", "currency_code": "BDT", "currency_name": "Bangladeshi Taka"},
        {"name": "Kenya", "currency_code": "KES", "currency_name": "Kenyan Shilling"},
        {"name": "Uganda", "currency_code": "UGX", "currency_name": "Ugandan Shilling"},
        {"name": "Tanzania", "currency_code": "TZS", "currency_name": "Tanzanian Shilling"},
        {"name": "Zambia", "currency_code": "ZMW", "currency_name": "Zambian Kwacha"},
        {"name": "Malawi", "currency_code": "MWK", "currency_name": "Malawian Kwacha"},
        {"name": "Haiti", "currency_code": "HTG", "currency_name": "Haitian Gourde"},
        {"name": "Democratic Republic of Congo", "currency_code": "CDF", "currency_name": "Congolese Franc"},
        {"name": "Somalia", "currency_code": "SOS", "currency_name": "Somali Shilling"},
        {"name": "South Sudan", "currency_code": "SSP", "currency_name": "South Sudanese Pound"},
    ]
    
    return {"countries": countries}


@router.get("/commodities")
def get_default_commodities():
    """
    Returns the list of standard WFP commodities.
    """
    commodities = [
        {"name": "Sorghum", "category": "Cereals"},
        {"name": "Wheat flour", "category": "Cereals"},
        {"name": "Maize", "category": "Cereals"},
        {"name": "Rice", "category": "Cereals"},
        {"name": "Millet", "category": "Cereals"},
        {"name": "Cooking oil", "category": "Oil"},
        {"name": "Vegetable oil", "category": "Oil"},
        {"name": "Sugar", "category": "Sugar"},
        {"name": "Salt", "category": "Condiments"},
        {"name": "Beans", "category": "Pulses"},
        {"name": "Lentils", "category": "Pulses"},
        {"name": "Chickpeas", "category": "Pulses"},
        {"name": "Meat (beef)", "category": "Protein"},
        {"name": "Meat (goat)", "category": "Protein"},
        {"name": "Fish", "category": "Protein"},
        {"name": "Eggs", "category": "Protein"},
        {"name": "Milk", "category": "Dairy"},
        {"name": "Potatoes", "category": "Vegetables"},
        {"name": "Onions", "category": "Vegetables"},
        {"name": "Tomatoes", "category": "Vegetables"},
    ]
    
    return {"commodities": commodities}
"""
Market Monitor - Router
=======================
FastAPI endpoints for the Market Monitor service.
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks, Body
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from typing import Optional, List
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

from app.shared.docx_export import build_content_disposition, build_docx_bytes_from_report_blocks
from app.shared.report_blocks import build_market_monitor_report_blocks

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory store per report status (in produzione usare Redis/DB)
_report_status: dict = {}


class ExportDocxOptions(BaseModel):
    filename: Optional[str] = None
    include_sources: bool = True
    include_visualizations: bool = True
    template: Optional[str] = None


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

        admin1_list = input_data.admin1_list
        if not admin1_list and input_data.use_mock_data:
            admin1_list = [
                f"{input_data.country} North",
                f"{input_data.country} South",
                f"{input_data.country} Central"
            ]

        # Run generation
        result = run_report_generation(
            country=input_data.country,
            time_period=input_data.time_period,
            commodity_list=input_data.commodity_list,
            admin1_list=admin1_list,
            currency_code=input_data.currency_code,
            enabled_modules=input_data.enabled_modules,
            news_start_date=input_data.news_start_date,
            news_end_date=input_data.news_end_date,
            previous_report_text=input_data.previous_report_text,
            use_mock_data=input_data.use_mock_data
        )

        # Build output
        output = GenerateReportOutput(
            run_id=result.get("run_id", "unknown"),
            country=input_data.country,
            time_period=input_data.time_period,
            report_sections=result.get("report_draft_sections", {}),
            report_blocks=build_market_monitor_report_blocks(
                {**(result or {}), "country": input_data.country, "time_period": input_data.time_period}
            ),
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

            admin1_list = input_data.admin1_list
            if not admin1_list and input_data.use_mock_data:
                admin1_list = [
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
                    meta_update = {"news_counts": news_counts}
                    retriever_traces = _state.get("retriever_traces")
                    if isinstance(retriever_traces, list):
                        meta_update["retriever_traces"] = retriever_traces
                    update_run(run_id, metadata=meta_update)

            result = run_report_generation(
                country=input_data.country,
                time_period=input_data.time_period,
                commodity_list=input_data.commodity_list,
                admin1_list=admin1_list,
                currency_code=input_data.currency_code,
                enabled_modules=input_data.enabled_modules,
                news_start_date=input_data.news_start_date,
                news_end_date=input_data.news_end_date,
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


@router.get("/data-availability")
def check_data_availability_endpoint(
    country: str,
    time_period: str = "2025-01",
    commodities: str = "Sugar,Wheat flour"
):
    '''
    Check what price data is available for a given country and period.

    Useful for:
    - Validating inputs before running a report
    - Showing users what data is available
    - Debugging data loading issues
    '''
    from .data_loader import check_data_availability

    commodity_list = [c.strip() for c in commodities.split(",")]

    availability = check_data_availability(
        country=country,
        time_period=time_period,
        commodities=commodity_list
    )

    return availability


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
        report_blocks=build_market_monitor_report_blocks(result),
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


@router.post("/export-docx/{run_id}")
async def export_market_monitor_docx(
    run_id: str,
    options: ExportDocxOptions = Body(default_factory=ExportDocxOptions),
):
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")

    if run.status != "completed":
        raise HTTPException(status_code=409, detail=f"Run not completed. Current status: {run.status}")

    result = run.result or {}

    try:
        report_blocks = build_market_monitor_report_blocks(result)
        docx_bytes = build_docx_bytes_from_report_blocks(
            report_blocks,
            visualizations=result.get("visualizations", {}),
            include_sources=options.include_sources,
            include_visualizations=options.include_visualizations,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DOCX generation failed: {str(e)}")

    filename = options.filename or f"market-monitor-{run_id}.docx"
    headers = {"Content-Disposition": build_content_disposition(filename)}
    return Response(
        content=docx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers=headers,
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
                "description": "List of commodities to analyze. Use /countries/{country}/metadata endpoint to get available commodities for a specific country.",
                "default": [],
                "note": "Defaults are country-specific. Query /countries/{country}/metadata for recommended defaults."
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
                "description": "Optional modules to enable (note: exchange_rate requires TE_API_KEY and has no mock fallback)",
                "default": [],
                "options": list(AVAILABLE_MODULES.keys())
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
                "description": "Exchange rate analysis using TradingEconomics API data (requires TE_API_KEY; no mock fallback)"
            }
        ]
    }


@router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "market-monitor"}


@router.get("/dataset/status")
def get_price_data_dataset_status():
    raise HTTPException(
        status_code=404,
        detail="The processed Price Bulletin dataset upload/status path has been removed. Data is loaded from Databridges.",
    )


@router.post("/dataset/upload")
async def upload_price_data_dataset():
    raise HTTPException(
        status_code=404,
        detail="The processed Price Bulletin dataset upload path has been removed. Data is loaded from Databridges.",
    )


@router.get("/countries")
def get_supported_countries():
    """
    Returns the list of configured Databridges country options with currencies.
    """
    from .data_loader import get_supported_countries as get_databridges_countries

    return {"countries": get_databridges_countries()}



@router.get("/commodities")
def get_commodities(country: Optional[str] = None):
    """
    Returns Databridges commodities available for a country.
    """
    from .data_loader import (
        get_available_commodities,
        get_commodity_categories,
        normalize_country_name
    )

    if country:
        country_normalized = normalize_country_name(country)
        commodity_list = get_available_commodities(country_normalized)
        categories = get_commodity_categories(commodity_list)

        return {
            "country": country_normalized,
            "commodities": [{"name": c} for c in commodity_list],
            "categories": categories
        }

    return {
        "commodities": [],
        "categories": {},
        "warning": "Select a country to load Databridges commodity options.",
    }


@router.get("/countries/{country}/metadata")
def get_country_metadata(country: str):
    """
    Returns all available metadata for a specific country:
    - Available commodities
    - Available regions (Admin1)
    - Available markets
    - Date range of available data
    """
    try:
        from .data_loader import get_country_metadata as get_databridges_country_metadata

        return get_databridges_country_metadata(country)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


def _get_food_basket_commodities(available: List[str]) -> List[str]:
    """
    Select default food basket commodities from available list.
    Prioritizes: cereals, pulses, oil, salt (standard WFP food basket).
    """
    defaults = []

    priority_patterns = [
        ("sorghum", "Cereals"),
        ("maize", "Cereals"),
        ("wheat", "Cereals"),
        ("rice", "Cereals"),
        ("beans", "Pulses"),
        ("lentil", "Pulses"),
        ("oil", "Oil"),
        ("salt", "Condiments"),
        ("sugar", "Sugar"),
    ]

    selected_categories = set()

    for pattern, category in priority_patterns:
        if category in selected_categories and category != "Cereals":
            continue
        for commodity in available:
            if pattern in commodity.lower() and commodity not in defaults:
                defaults.append(commodity)
                selected_categories.add(category)
                break

    return defaults[:6]

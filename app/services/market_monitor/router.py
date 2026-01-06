"""
Market Monitor - Router
=======================
Endpoint FastAPI per il servizio Market Monitor.
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

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory store per report status (in produzione usare Redis/DB)
_report_status: dict = {}


@router.post("/generate", response_model=GenerateReportOutput)
async def generate_market_monitor(input_data: GenerateReportInput):
    """
    Genera un Market Monitor Report completo.
    
    Il processo include:
    1. Data Agent: Recupera/genera dati di prezzo (mock o API)
    2. Graph Designer: Crea visualizzazioni
    3. News Retrieval: Recupera notizie contestuali
    4. Event Mapper: Estrae eventi chiave
    5. Trend Analyst: Analizza trend di mercato
    6. Module Orchestrator: Esegue moduli opzionali (es. exchange rate)
    7. Highlights Drafter: Genera sezione highlights
    8. Narrative Drafter: Genera sezioni narrative
    9. Red Team: Quality assurance con possibile loop di correzione
    
    Returns:
        GenerateReportOutput con tutte le sezioni del report
    """
    try:
        logger.info(f"Starting report generation for {input_data.country} - {input_data.time_period}")
        
        # Se admin1_list vuota, usa default
        admin1_list = input_data.admin1_list
        if not admin1_list:
            admin1_list = [f"{input_data.country} North", f"{input_data.country} South", 
                          f"{input_data.country} Central"]
        
        # Esegui generazione
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
        
        # Costruisci output
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
    Avvia generazione report in background.
    Utile per report che richiedono molto tempo.
    
    Returns:
        run_id per polling dello status
    """
    import uuid
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    
    _report_status[run_id] = {
        "status": "pending",
        "current_node": None,
        "progress_pct": 0,
        "warnings": [],
        "error": None,
        "traceback": None,
    }
    
    async def run_in_background():
        try:
            _report_status[run_id]["status"] = "running"
            _report_status[run_id]["error"] = None
            _report_status[run_id]["traceback"] = None
            
            admin1_list = input_data.admin1_list or [
                f"{input_data.country} North", 
                f"{input_data.country} South"
            ]
            
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
            
            _report_status[run_id] = {
                "status": "completed",
                "current_node": "END",
                "progress_pct": 100,
                "result": result,
                "warnings": result.get("warnings", [])
            }
            
        except Exception as e:
            tb_str = traceback.format_exc()
            logger.exception(f"Report generation failed for {run_id}: {e}")
            current_node = _report_status.get(run_id, {}).get("current_node")
            _report_status[run_id] = {
                "status": "failed",
                "current_node": current_node,
                "error": str(e),
                "traceback": tb_str,
                "progress_pct": 0,
                "warnings": []
            }
    
    background_tasks.add_task(run_in_background)
    
    return {"run_id": run_id, "status": "pending"}


@router.get("/status/{run_id}", response_model=ReportStatusOutput)
async def get_report_status(run_id: str):
    """
    Controlla lo status di un report in generazione.
    """
    if run_id not in _report_status:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")
    
    status = _report_status[run_id]
    
    return ReportStatusOutput(
        run_id=run_id,
        status=status.get("status", "unknown"),
        current_node=status.get("current_node"),
        progress_pct=status.get("progress_pct", 0),
        warnings=status.get("warnings", []),
        error=status.get("error"),
        traceback=status.get("traceback"),
    )


@router.get("/result/{run_id}", response_model=GenerateReportOutput)
async def get_report_result(run_id: str):
    """
    Recupera il risultato di un report completato.
    """
    if run_id not in _report_status:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")
    
    status = _report_status[run_id]
    
    if status.get("status") != "completed":
        raise HTTPException(
            status_code=400, 
            detail=f"Report not completed. Current status: {status.get('status')}"
        )
    
    result = status.get("result", {})
    
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
        warnings=result.get("warnings", []),
        llm_calls=result.get("llm_calls", 0),
        success=True
    )


@router.get("/info")
def get_service_info():
    """
    Restituisce metadata del servizio per il frontend.
    """
    return {
        "id": "market-monitor",
        "name": "Market Monitor Generator",
        "description": "Genera Market Monitor Reports completi con analisi prezzi, "
                       "trend di mercato, visualizzazioni e sezioni narrative. "
                       "Include moduli opzionali come analisi tasso di cambio.",
        "version": "1.0.0",
        "inputs": [
            {
                "name": "country",
                "type": "string",
                "required": True,
                "label": "Country",
                "description": "Nome del paese (es. 'Sudan', 'Yemen', 'Myanmar')"
            },
            {
                "name": "time_period",
                "type": "string",
                "required": True,
                "label": "Time Period",
                "description": "Periodo in formato YYYY-MM (es. '2025-01')"
            },
            {
                "name": "commodity_list",
                "type": "array",
                "required": False,
                "label": "Commodities",
                "description": "Lista delle commodity da analizzare",
                "default": ["Sorghum", "Wheat flour", "Cooking oil", "Sugar"]
            },
            {
                "name": "admin1_list",
                "type": "array",
                "required": False,
                "label": "Regions (Admin1)",
                "description": "Lista delle regioni da includere"
            },
            {
                "name": "currency_code",
                "type": "string",
                "required": False,
                "label": "Currency Code",
                "description": "Codice valuta ISO 4217 (es. 'SDG', 'YER')",
                "default": "USD"
            },
            {
                "name": "enabled_modules",
                "type": "array",
                "required": False,
                "label": "Optional Modules",
                "description": "Moduli opzionali da abilitare",
                "default": ["exchange_rate"],
                "options": list(AVAILABLE_MODULES.keys())
            },
            {
                "name": "use_mock_data",
                "type": "boolean",
                "required": False,
                "label": "Use Mock Data",
                "description": "Se True, usa dati simulati invece di API reali",
                "default": True
            }
        ],
        "outputs": {
            "run_id": "Identificativo univoco della generazione",
            "report_sections": "Sezioni del report (HIGHLIGHTS, MARKET_OVERVIEW, etc.)",
            "visualizations": "Grafici in formato Base64",
            "data_statistics": "Statistiche calcolate (MoM, YoY)",
            "trend_analysis": "Analisi del trend di mercato",
            "events": "Eventi estratti dalle notizie",
            "module_sections": "Sezioni generate dai moduli opzionali",
            "llm_calls": "Numero di chiamate LLM effettuate",
            "success": "True se la generazione è completata con successo"
        },
        "workflow_nodes": [
            {"id": "data_agent", "name": "Data Agent", "description": "Recupera e processa dati di prezzo"},
            {"id": "graph_designer", "name": "Graph Designer", "description": "Genera visualizzazioni"},
            {"id": "news_retrieval", "name": "News Retrieval", "description": "Recupera notizie contestuali"},
            {"id": "event_mapper", "name": "Event Mapper", "description": "Estrae eventi chiave"},
            {"id": "trend_analyst", "name": "Trend Analyst", "description": "Analizza trend di mercato"},
            {"id": "module_orchestrator", "name": "Module Orchestrator", "description": "Esegue moduli opzionali"},
            {"id": "highlights_drafter", "name": "Highlights Drafter", "description": "Genera sezione highlights"},
            {"id": "narrative_drafter", "name": "Narrative Drafter", "description": "Genera sezioni narrative"},
            {"id": "red_team", "name": "Red Team QA", "description": "Quality assurance e fact-checking"}
        ],
        "available_modules": [
            {
                "id": "exchange_rate",
                "name": "Exchange Rate Analysis",
                "description": "Analisi tasso di cambio con dati Trading Economics"
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
    Restituisce la lista dei paesi supportati con relative valute.
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
    Restituisce la lista delle commodity standard WFP.
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
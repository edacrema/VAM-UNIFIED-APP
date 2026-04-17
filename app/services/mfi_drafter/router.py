"""
MFI Drafter - Router
====================
FastAPI endpoints for the MFI Report Generator service.
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks, Body, UploadFile, File, Form
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from typing import Optional, Any, Dict, List
import logging
import traceback
import numpy as np

from .graph import run_mfi_report_generation
from .data_loader import load_mfi_from_csv, load_mfi_from_databridges_rows, validate_csv_structure
from .databridges_loader import (
    find_survey,
    list_mfi_countries,
    list_mfi_surveys_for_country,
    load_mfi_survey_from_databridges,
)
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
    get_run_artifact,
    set_run_completed,
    set_run_failed,
    update_run,
    update_run_progress,
)
from app.shared.databridges import get_databridges_client
from app.shared.live_outputs import (
    build_databridges_live_output,
    build_document_live_output,
    create_databridges_artifacts,
    create_document_previews_with_artifacts,
)

from app.shared.docx_export import build_content_disposition, build_docx_bytes_from_report_blocks
from app.shared.report_blocks import build_mfi_report_blocks

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory store per report status
_report_status: dict = {}

class ExportDocxOptions(BaseModel):
    filename: Optional[str] = None
    include_sources: bool = True
    include_visualizations: bool = True
    template: Optional[str] = None


class GenerateMFIReportFromSurveyInput(BaseModel):
    survey_id: int


def _trace_error(traces: List[Dict[str, Any]], retriever_name: str) -> Optional[str]:
    for trace in traces:
        if not isinstance(trace, dict):
            continue
        if str(trace.get("retriever") or "") != retriever_name:
            continue
        error = trace.get("error")
        if error:
            return str(error)
    return None


def _update_live_metadata(
    run_id: str,
    *,
    section_updates: Optional[Dict[str, Any]] = None,
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> None:
    if not section_updates and not extra_metadata:
        return

    current_run = get_run(run_id)
    current_metadata = dict(getattr(current_run, "metadata", {}) or {})
    meta_update: Dict[str, Any] = dict(extra_metadata or {})

    if section_updates:
        live_outputs = dict(current_metadata.get("live_outputs") or {})
        live_outputs.update(section_updates)
        meta_update["live_outputs"] = live_outputs

    update_run(run_id, metadata=meta_update)

def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if item is None:
                continue
            s = item if isinstance(item, str) else str(item)
            s = s.strip()
            if s:
                parts.append(s)
        return "\n".join(parts)
    return str(value)

def _normalize_dimension_findings(findings: Any) -> Dict[str, Dict[str, str]]:
    if not isinstance(findings, dict):
        return {}
    normalized: Dict[str, Dict[str, str]] = {}
    for dim, payload in findings.items():
        if not isinstance(payload, dict):
            continue
        normalized[str(dim)] = {
            "key_findings": _normalize_text(payload.get("key_findings")),
            "score_interpretation": _normalize_text(payload.get("score_interpretation")),
            "recommendations": _normalize_text(payload.get("recommendations")),
        }
    return normalized


def _build_mfi_output(
    *,
    result: Dict[str, Any],
    country: str,
    data_collection_start: str,
    data_collection_end: str,
) -> GenerateMFIReportOutput:
    market_mfis = [
        float(m.get("overall_mfi", 0) or 0)
        for m in (result.get("markets_data", []) or [])
        if isinstance(m, dict)
    ]
    national_mfi = round(np.mean(market_mfis), 1) if market_mfis else 0.0

    risk_dist: Dict[str, int] = {}
    for market in result.get("markets_data", []) or []:
        if isinstance(market, dict):
            risk = str(market.get("risk_level") or "Unknown")
            risk_dist[risk] = risk_dist.get(risk, 0) + 1

    normalized_dimension_findings = _normalize_dimension_findings(result.get("dimension_findings"))

    return GenerateMFIReportOutput(
        run_id=result.get("run_id", "unknown"),
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
        survey_metadata=result.get("survey_metadata", {}),
        national_mfi=national_mfi,
        risk_distribution=risk_dist,
        markets_data=result.get("markets_data", []),
        dimension_scores=result.get("dimension_scores", []),
        executive_summary=result.get("executive_summary", ""),
        dimension_findings=normalized_dimension_findings,
        market_recommendations=result.get("market_recommendations", {}) or {},
        country_context=result.get("country_context"),
        document_references=result.get("document_references", []),
        report_blocks=build_mfi_report_blocks(
            {
                **(result or {}),
                "country": country,
                "data_collection_start": data_collection_start,
                "data_collection_end": data_collection_end,
                "dimension_findings": normalized_dimension_findings,
                "market_recommendations": result.get("market_recommendations", {}) or {},
            }
        ),
        visualizations=result.get("visualizations", {}),
        warnings=result.get("warnings", []),
        llm_calls=result.get("llm_calls", 0),
        correction_attempts=result.get("correction_attempts", 0),
        success=True,
    )


def _run_mfi_from_structured_data(csv_data: Dict[str, Any]) -> GenerateMFIReportOutput:
    country = csv_data["country"]
    data_collection_start = csv_data["data_collection_start"]
    data_collection_end = csv_data["data_collection_end"]
    markets = csv_data["markets"]
    result = run_mfi_report_generation(
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
        markets=markets,
        csv_data=csv_data,
    )
    return _build_mfi_output(
        result=result,
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
    )

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
    6. Market Recommendations Drafter: Drafts recommendations by market
    7. Executive Summary Drafter: Drafts the executive summary
    8. Red Team: Quality assurance with possible correction loop

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
        )
        
        # Calculate national MFI
        market_mfis = [
            float(m.get("overall_mfi", 0) or 0)
            for m in (result.get("markets_data", []) or [])
            if isinstance(m, dict)
        ]
        national_mfi = round(np.mean(market_mfis), 1) if market_mfis else 0.0
        
        # Calculate risk distribution
        risk_dist = {}
        for m in result.get("markets_data", []):
            risk_dist[m["risk_level"]] = risk_dist.get(m["risk_level"], 0) + 1
        
        normalized_dimension_findings = _normalize_dimension_findings(result.get("dimension_findings"))

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
            dimension_findings=normalized_dimension_findings,
            market_recommendations=result.get("market_recommendations", {}) or {},
            country_context=result.get("country_context"),
            document_references=result.get("document_references", []),
            report_blocks=build_mfi_report_blocks(
                {
                    **(result or {}),
                    "country": input_data.country,
                    "data_collection_start": input_data.data_collection_start,
                    "data_collection_end": input_data.data_collection_end,
                    "dimension_findings": normalized_dimension_findings,
                    "market_recommendations": result.get("market_recommendations", {}) or {},
                }
            ),
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


@router.get("/countries")
def get_mfi_countries():
    """Return countries with available MFI surveys in Databridges."""
    try:
        return {"countries": list_mfi_countries()}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.get("/countries/{country}/surveys")
def get_mfi_surveys(
    country: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """Return Databridges MFI surveys for a selected country."""
    try:
        return {
            "country": country,
            "surveys": list_mfi_surveys_for_country(
                country,
                start_date=start_date,
                end_date=end_date,
            ),
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@router.post("/generate-from-survey", response_model=GenerateMFIReportOutput)
async def generate_mfi_report_from_survey(input_data: GenerateMFIReportFromSurveyInput):
    """Generate an MFI report from a selected Databridges survey."""
    try:
        csv_data = load_mfi_survey_from_databridges(input_data.survey_id)
        return _run_mfi_from_structured_data(csv_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.exception("MFI report generation from Databridges failed")
        raise HTTPException(status_code=502, detail=str(exc))


@router.post("/generate-from-survey-async")
async def generate_mfi_report_from_survey_async(
    input_data: GenerateMFIReportFromSurveyInput,
    background_tasks: BackgroundTasks,
):
    """Start MFI report generation from a Databridges survey in the background."""
    import uuid as uuid_module

    run_id = f"mfi_{uuid_module.uuid4().hex[:8]}"
    create_run(run_id)

    progress_map = {
        "databridges_fetch": 8,
        "mfi_data_agent": 18,
        "context_retrieval": 32,
        "context_extractor": 46,
        "mfi_graph_designer": 60,
        "dimension_drafter": 75,
        "market_recommendations_drafter": 85,
        "executive_summary_drafter": 93,
        "red_team": 98,
    }

    def run_in_background():
        try:
            update_run(run_id, status="running", error=None, traceback=None)
            update_run_progress(run_id, current_node="databridges_fetch", progress_pct=progress_map["databridges_fetch"])

            survey = find_survey(input_data.survey_id)
            if survey is None:
                raise ValueError(f"Survey ID not found: {input_data.survey_id}")

            raw_rows = get_databridges_client().list_mfi_processed_data(int(input_data.survey_id), page_size=1000)
            csv_data = load_mfi_from_databridges_rows(raw_rows, survey=survey)
            databridges_artifacts = create_databridges_artifacts(
                run_id=run_id,
                service_slug="mfi-drafter",
                label_prefix="Databridges survey rows",
                file_stem=f"mfi-databridges-survey-{input_data.survey_id}",
                rows=raw_rows,
            )
            _update_live_metadata(
                run_id,
                section_updates={
                    "databridges": build_databridges_live_output(
                        title="Databridges Data",
                        summary=(
                            f"{len(raw_rows)} processed survey rows retrieved for survey "
                            f"{input_data.survey_id} ({csv_data['country']})."
                        ),
                        rows=raw_rows,
                        download_artifacts=databridges_artifacts,
                    )
                },
            )

            def on_step(node_name: str, _state: dict):
                progress = progress_map.get(node_name)
                if progress is not None:
                    update_run_progress(run_id, current_node=node_name, progress_pct=progress)
                else:
                    update_run(run_id, current_node=node_name)

                meta_update: Dict[str, Any] = {}
                context_counts = _state.get("context_counts")
                if isinstance(context_counts, dict):
                    meta_update["context_counts"] = context_counts

                retriever_traces = _state.get("retriever_traces")
                traces_list = retriever_traces if isinstance(retriever_traces, list) else []
                if traces_list:
                    meta_update["retriever_traces"] = traces_list

                section_updates: Dict[str, Any] = {}
                if node_name == "context_retrieval":
                    seerist_docs = _state.get("seerist_documents") or []
                    reliefweb_docs = _state.get("reliefweb_documents") or []
                    if isinstance(seerist_docs, list):
                        seerist_error = _trace_error(traces_list, "Seerist")
                        seerist_previews = create_document_previews_with_artifacts(
                            run_id=run_id,
                            service_slug="mfi-drafter",
                            source_slug="seerist",
                            documents=seerist_docs,
                        )
                        section_updates["seerist"] = build_document_live_output(
                            title="Seerist Documents",
                            summary=(
                                f"Seerist retrieval unavailable: {seerist_error}"
                                if seerist_error
                                else f"{len(seerist_docs)} Seerist documents retrieved."
                            ),
                            documents=seerist_previews,
                            status="failed" if seerist_error else "completed",
                        )
                    if isinstance(reliefweb_docs, list):
                        reliefweb_error = _trace_error(traces_list, "ReliefWeb")
                        reliefweb_previews = create_document_previews_with_artifacts(
                            run_id=run_id,
                            service_slug="mfi-drafter",
                            source_slug="reliefweb",
                            documents=reliefweb_docs,
                        )
                        section_updates["reliefweb"] = build_document_live_output(
                            title="ReliefWeb Documents",
                            summary=(
                                f"ReliefWeb retrieval unavailable: {reliefweb_error}"
                                if reliefweb_error
                                else f"{len(reliefweb_docs)} ReliefWeb documents retrieved."
                            ),
                            documents=reliefweb_previews,
                            status="failed" if reliefweb_error else "completed",
                        )

                _update_live_metadata(
                    run_id,
                    section_updates=section_updates,
                    extra_metadata=meta_update,
                )

            result = run_mfi_report_generation(
                country=csv_data["country"],
                data_collection_start=csv_data["data_collection_start"],
                data_collection_end=csv_data["data_collection_end"],
                markets=csv_data["markets"],
                csv_data=csv_data,
                on_step=on_step,
            )
            result = {
                **(result or {}),
                "country": csv_data["country"],
                "data_collection_start": csv_data["data_collection_start"],
                "data_collection_end": csv_data["data_collection_end"],
            }
            output = _build_mfi_output(
                result=result,
                country=csv_data["country"],
                data_collection_start=csv_data["data_collection_start"],
                data_collection_end=csv_data["data_collection_end"],
            )
            set_run_completed(
                run_id,
                result=output.model_dump() if hasattr(output, "model_dump") else output.dict(),
            )
        except Exception as exc:
            logger.exception("MFI async report generation from Databridges failed")
            _update_live_metadata(
                run_id,
                section_updates={
                    "databridges": build_databridges_live_output(
                        title="Databridges Data",
                        summary=f"Databridges fetch failed: {exc}",
                        rows=[],
                        download_artifacts=[],
                        status="failed",
                    )
                },
            )
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else "databridges_fetch"
            set_run_failed(run_id, error=str(exc), traceback=traceback.format_exc(), current_node=current_node)

    background_tasks.add_task(run_in_background)
    return {"run_id": run_id, "status": "pending"}


@router.post("/generate-from-csv", response_model=GenerateMFIReportOutput)
async def generate_mfi_report_from_csv(
    file: UploadFile = File(..., description="Processed MFI CSV file"),
    country_override: Optional[str] = Form(None, description="Override country name"),
    data_collection_start_override: Optional[str] = Form(None, description="Override start date"),
    data_collection_end_override: Optional[str] = Form(None, description="Override end date"),
):
    """Generates a full MFI report from an uploaded CSV file."""
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    try:
        content = await file.read()

        logger.info(f"Loading CSV file: {filename}")
        csv_data = load_mfi_from_csv(
            file_content=content,
            country_override=country_override,
            start_date_override=data_collection_start_override,
            end_date_override=data_collection_end_override,
        )
        logger.info(
            "Starting MFI report generation from CSV for %s (%s markets)",
            csv_data["country"],
            len(csv_data["markets"]),
        )
        output = _run_mfi_from_structured_data(csv_data)
        logger.info(f"MFI report generation from CSV completed: {output.run_id}")
        return output
    except ValueError as e:
        logger.error(f"CSV validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"MFI report generation from CSV failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate-csv")
async def validate_mfi_csv(
    file: UploadFile = File(..., description="CSV file to validate"),
):
    """Validates a CSV file structure before processing."""
    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    try:
        content = await file.read()
        return validate_csv_structure(content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV validation failed: {str(e)}")


@router.post("/generate-from-csv-async")
async def generate_mfi_report_from_csv_async(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="Processed MFI CSV file"),
    country_override: Optional[str] = Form(None),
    data_collection_start_override: Optional[str] = Form(None),
    data_collection_end_override: Optional[str] = Form(None),
):
    """Starts report generation from CSV in the background."""
    import uuid as uuid_module

    filename = file.filename or ""
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    content = await file.read()

    try:
        csv_data = load_mfi_from_csv(
            file_content=content,
            country_override=country_override,
            start_date_override=data_collection_start_override,
            end_date_override=data_collection_end_override,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    run_id = f"mfi_{uuid_module.uuid4().hex[:8]}"
    create_run(run_id)

    progress_map = {
        "mfi_data_agent": 10,
        "context_retrieval": 25,
        "context_extractor": 40,
        "mfi_graph_designer": 55,
        "dimension_drafter": 72,
        "market_recommendations_drafter": 82,
        "executive_summary_drafter": 92,
        "red_team": 97,
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

                meta_update: Dict[str, Any] = {}
                context_counts = _state.get("context_counts")
                if isinstance(context_counts, dict):
                    meta_update["context_counts"] = context_counts

                retriever_traces = _state.get("retriever_traces")
                traces_list = retriever_traces if isinstance(retriever_traces, list) else []
                if traces_list:
                    meta_update["retriever_traces"] = traces_list

                section_updates: Dict[str, Any] = {}
                if node_name == "context_retrieval":
                    seerist_docs = _state.get("seerist_documents") or []
                    reliefweb_docs = _state.get("reliefweb_documents") or []
                    if isinstance(seerist_docs, list):
                        seerist_error = _trace_error(traces_list, "Seerist")
                        seerist_previews = create_document_previews_with_artifacts(
                            run_id=run_id,
                            service_slug="mfi-drafter",
                            source_slug="seerist",
                            documents=seerist_docs,
                        )
                        section_updates["seerist"] = build_document_live_output(
                            title="Seerist Documents",
                            summary=(
                                f"Seerist retrieval unavailable: {seerist_error}"
                                if seerist_error
                                else f"{len(seerist_docs)} Seerist documents retrieved."
                            ),
                            documents=seerist_previews,
                            status="failed" if seerist_error else "completed",
                        )
                    if isinstance(reliefweb_docs, list):
                        reliefweb_error = _trace_error(traces_list, "ReliefWeb")
                        reliefweb_previews = create_document_previews_with_artifacts(
                            run_id=run_id,
                            service_slug="mfi-drafter",
                            source_slug="reliefweb",
                            documents=reliefweb_docs,
                        )
                        section_updates["reliefweb"] = build_document_live_output(
                            title="ReliefWeb Documents",
                            summary=(
                                f"ReliefWeb retrieval unavailable: {reliefweb_error}"
                                if reliefweb_error
                                else f"{len(reliefweb_docs)} ReliefWeb documents retrieved."
                            ),
                            documents=reliefweb_previews,
                            status="failed" if reliefweb_error else "completed",
                        )

                _update_live_metadata(
                    run_id,
                    section_updates=section_updates,
                    extra_metadata=meta_update,
                )

            result = run_mfi_report_generation(
                country=csv_data["country"],
                data_collection_start=csv_data["data_collection_start"],
                data_collection_end=csv_data["data_collection_end"],
                markets=csv_data["markets"],
                csv_data=csv_data,
                on_step=on_step,
            )

            update_run(run_id, warnings=result.get("warnings", []))
            set_run_completed(run_id, result=result)
        except Exception as e:
            import traceback

            tb_str = traceback.format_exc()
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(e), traceback=tb_str, current_node=current_node)

    background_tasks.add_task(run_in_background)

    return {
        "run_id": run_id,
        "status": "pending",
        "preview": {
            "country": csv_data["country"],
            "markets_count": len(csv_data["markets"]),
            "collection_period": csv_data["survey_metadata"]["collection_period"],
        },
    }


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
        "dimension_drafter": 72,
        "market_recommendations_drafter": 82,
        "executive_summary_drafter": 92,
        "red_team": 97,
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
    normalized_dimension_findings = _normalize_dimension_findings(result.get("dimension_findings"))
    result_for_blocks = dict(result)
    result_for_blocks["dimension_findings"] = normalized_dimension_findings
    result_for_blocks["market_recommendations"] = result.get("market_recommendations", {}) or {}

    market_mfis = [
        float(m.get("overall_mfi", 0) or 0)
        for m in (result.get("markets_data", []) or [])
        if isinstance(m, dict)
    ]
    national_mfi = round(np.mean(market_mfis), 1) if market_mfis else 0.0
    
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
        dimension_findings=normalized_dimension_findings,
        market_recommendations=result.get("market_recommendations", {}) or {},
        country_context=result.get("country_context"),
        document_references=result.get("document_references", []),
        report_blocks=build_mfi_report_blocks(result_for_blocks),
        visualizations=result.get("visualizations", {}),
        warnings=result.get("warnings", []),
        llm_calls=result.get("llm_calls", 0),
        correction_attempts=result.get("correction_attempts", 0),
        success=True
    )


@router.get("/artifacts/{run_id}/{artifact_id}")
async def get_report_artifact(run_id: str, artifact_id: str):
    artifact = get_run_artifact(run_id, artifact_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail=f"Artifact not found: {artifact_id}")

    return Response(
        content=artifact.content,
        media_type=artifact.mime_type,
        headers={"Content-Disposition": build_content_disposition(artifact.file_name)},
    )


@router.post("/export-docx/{run_id}")
async def export_mfi_docx(
    run_id: str,
    options: ExportDocxOptions = Body(default_factory=ExportDocxOptions),
):
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run ID not found: {run_id}")

    if run.status != "completed":
        raise HTTPException(status_code=409, detail=f"Run not completed. Current status: {run.status}")

    result = run.result or {}
    result_for_blocks = dict(result)
    result_for_blocks["dimension_findings"] = _normalize_dimension_findings(result.get("dimension_findings"))
    result_for_blocks["market_recommendations"] = result.get("market_recommendations", {}) or {}

    try:
        report_blocks = build_mfi_report_blocks(result_for_blocks)
        docx_bytes = build_docx_bytes_from_report_blocks(
            report_blocks,
            visualizations=result.get("visualizations", {}),
            include_sources=options.include_sources,
            include_visualizations=options.include_visualizations,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DOCX generation failed: {str(e)}")

    filename = options.filename or f"mfi-drafter-{run_id}.docx"
    headers = {"Content-Disposition": build_content_disposition(filename)}
    return Response(
        content=docx_bytes,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers=headers,
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
        "version": "1.1.0",
        "supports_csv_upload": True,
        "data_source": "Databridges or uploaded processed CSV",
        "inputs": [
            {
                "name": "country",
                "type": "string",
                "required": True,
                "label": "Country",
                "description": "Country with available MFI surveys in Databridges"
            },
            {
                "name": "survey_id",
                "type": "integer",
                "required": True,
                "label": "MFI Survey",
                "description": "Databridges survey ID selected after choosing a country"
            }
        ],
        "databridges": {
            "countries_endpoint": "/countries",
            "surveys_endpoint": "/countries/{country}/surveys",
            "endpoint": "/generate-from-survey",
            "async_endpoint": "/generate-from-survey-async",
        },
        "csv_upload": {
            "endpoint": "/generate-from-csv",
            "async_endpoint": "/generate-from-csv-async",
            "validate_endpoint": "/validate-csv",
            "required_columns": [
                "MarketName",
                "Adm0Name",
                "Adm1Name",
                "LevelID",
                "DimensionName",
                "VariableName",
                "OutputValue",
                "TradersSampleSize",
            ],
            "optional_columns": [
                "MarketLatitude",
                "MarketLongitude",
                "Adm2Name",
                "StartDate",
                "EndDate",
            ],
            "description": "Upload the final processed MFI CSV instead of selecting a Databridges survey",
        },
        "outputs": {
            "run_id": "Unique generation identifier",
            "national_mfi": "National MFI score (0-10)",
            "risk_distribution": "Distribution of markets by risk level",
            "markets_data": "Detailed data for each market",
            "dimension_scores": "Score for each MFI dimension",
            "executive_summary": "Generated executive summary",
            "dimension_findings": "Findings for each dimension",
            "market_recommendations": "Recommendations by market",
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
            {"id": "market_recommendations_drafter", "name": "Market Recommendations", "description": "Drafts recommendations by market"},
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

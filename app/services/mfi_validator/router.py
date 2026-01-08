"""
MFI Validator - Router
======================
FastAPI endpoints for the RAW MFI validation service.

Note: This service only supports RAW MFI datasets.
PROCESSED datasets are no longer supported.
"""
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Optional
import tempfile
import os
import logging
import traceback

from .graph import run_troubleshooting, RAW_FILE_INDICATORS
from .schemas import ValidateFileOutput, ValidateFileStatusOutput

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


@router.post("/validate-file", response_model=ValidateFileOutput)
async def validate_mfi_file(
    file: UploadFile = File(..., description="RAW MFI dataset in CSV format"),
    survey_type: str = Form("full mfi", description="Survey type: 'full mfi' or 'reduced mfi'"),
    template: Optional[UploadFile] = File(None, description="Optional template (.csv or .json)")
):
    """
    Validates a RAW MFI CSV dataset.
    
    The file must be a RAW MFI dataset containing the following required columns:
    SVY_MOD, SURVEY_TYPE, RESPONSEID, SUBMISSIONDATE, _UUID, ENUMERATOR,
    ENUMERATORID, TRADER_NAME, INTERVIEW_DATE, DEVICEID, _SUBMISSION_TIME
    
    Runs 5 validation layers:
    - Layer 0: File Validation (encoding, format, RAW indicators)
    - Layer 1: Structural Parsing (delimiter, broken rows)
    - Layer 2: Schema Validation (required columns, duplicates)
    - Layer 3: Business Rules (RAW MFI-specific rules)
    - Layer 5: Report Generation (diagnostic report)
    
    Args:
        file: RAW MFI dataset in CSV format
        survey_type: 'full mfi' (requires 1 market + 5 trader surveys) or 'reduced mfi'
        template: Optional custom template for additional column validation
    
    Returns:
        ValidateFileOutput with results for each layer and final report
    """
    
    # Input validation
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(
            status_code=400, 
            detail="File must be CSV. Received: {}".format(file.filename)
        )
    
    if survey_type.lower() not in ["full mfi", "reduced mfi"]:
        raise HTTPException(
            status_code=400,
            detail="survey_type must be 'full mfi' or 'reduced mfi'. Received: {}".format(survey_type)
        )
    
    # Save temporary file
    tmp_path = None
    template_path = None
    
    try:
        # Save main file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        # Save template if provided
        if template:
            suffix = ".json" if template.filename.lower().endswith(".json") else ".csv"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_t:
                template_content = await template.read()
                tmp_t.write(template_content)
                template_path = tmp_t.name
        
        # Run validation
        logger.info("Starting RAW MFI validation for: {}".format(file.filename))
        
        result = run_troubleshooting(
            file_path=tmp_path,
            template=template_path,
            survey_type=survey_type
        )
        
        # Calculate success
        layer_results = result.get("layer_results", [])
        success = all(lr.get("passed", False) for lr in layer_results)
        
        # Build output
        output = ValidateFileOutput(
            file_name=file.filename,
            country=result.get("country"),
            survey_period=result.get("survey_period"),
            detected_file_type=result.get("detected_file_type", "RAW"),
            llm_calls=result.get("llm_calls", 0),
            layer_results=layer_results,
            final_report=result.get("final_report", ""),
            success=success
        )
        
        logger.info("Validation completed for {}: success={}".format(file.filename, success))
        
        return output
        
    except Exception as e:
        logger.error("Validation error for {}: {}".format(file.filename, e))
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        # Cleanup temporary files
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        if template_path and os.path.exists(template_path):
            os.unlink(template_path)


@router.post("/validate-file-async")
async def validate_mfi_file_async(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="RAW MFI dataset in CSV format"),
    survey_type: str = Form("full mfi", description="Survey type: 'full mfi' or 'reduced mfi'"),
    template: Optional[UploadFile] = File(None, description="Optional template (.csv or .json)")
):
    """
    Validates a RAW MFI CSV dataset asynchronously.
    
    Same validation as /validate-file but runs in background.
    Use /status/{run_id} to check progress and /result/{run_id} to get results.
    
    Returns:
        run_id: Unique identifier to track the validation progress
        status: Initial status ('pending')
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(
            status_code=400,
            detail="File must be CSV. Received: {}".format(file.filename)
        )

    if survey_type.lower() not in ["full mfi", "reduced mfi"]:
        raise HTTPException(
            status_code=400,
            detail="survey_type must be 'full mfi' or 'reduced mfi'. Received: {}".format(survey_type)
        )

    import uuid

    run_id = "mfi_val_{}".format(uuid.uuid4().hex[:8])
    create_run(run_id)

    original_filename = file.filename

    # Save file(s) now, run graph in background later
    tmp_path = None
    template_path = None

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        content = await file.read()
        tmp.write(content)
        tmp_path = tmp.name

    if template:
        suffix = ".json" if template.filename.lower().endswith(".json") else ".csv"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_t:
            template_content = await template.read()
            tmp_t.write(template_content)
            template_path = tmp_t.name

    progress_map = {
        "layer0": 10,
        "layer1": 30,
        "layer2": 60,
        "layer3": 80,
        "report": 95,
    }

    def run_in_background():
        try:
            update_run(run_id, status="running", error=None, traceback=None)
            logger.info("Starting async RAW MFI validation for: {} (run_id={})".format(
                original_filename, run_id
            ))

            def on_step(node_name: str, _state: dict):
                progress = progress_map.get(node_name)
                if progress is not None:
                    update_run_progress(run_id, current_node=node_name, progress_pct=progress)
                else:
                    update_run(run_id, current_node=node_name)

            result = run_troubleshooting(
                file_path=tmp_path,
                template=template_path,
                survey_type=survey_type,
                on_step=on_step,
            )

            layer_results = result.get("layer_results", [])
            success = all(lr.get("passed", False) for lr in layer_results)

            output = {
                "file_name": original_filename,
                "country": result.get("country"),
                "survey_period": result.get("survey_period"),
                "detected_file_type": result.get("detected_file_type", "RAW"),
                "llm_calls": result.get("llm_calls", 0),
                "layer_results": layer_results,
                "final_report": result.get("final_report", ""),
                "success": success,
            }

            set_run_completed(run_id, result=output)
            logger.info("Async validation completed for {} (run_id={}): success={}".format(
                original_filename, run_id, success
            ))

        except Exception as e:
            tb_str = traceback.format_exc()
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(e), traceback=tb_str, current_node=current_node)
            logger.error("Async validation failed for {} (run_id={}): {}".format(
                original_filename, run_id, e
            ))

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            if template_path and os.path.exists(template_path):
                os.unlink(template_path)

    background_tasks.add_task(run_in_background)
    return {"run_id": run_id, "status": "pending"}


@router.get("/status/{run_id}", response_model=ValidateFileStatusOutput)
async def get_validate_status(run_id: str):
    """
    Get the status of an async validation run.
    
    Args:
        run_id: The run ID returned by /validate-file-async
        
    Returns:
        ValidateFileStatusOutput with current status, progress, and any errors
    """
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run ID not found: {}".format(run_id))

    return ValidateFileStatusOutput(
        run_id=run_id,
        status=run.status,
        current_node=run.current_node,
        progress_pct=run.progress_pct,
        warnings=run.warnings,
        error=run.error,
        traceback=run.traceback,
    )


@router.get("/result/{run_id}", response_model=ValidateFileOutput)
async def get_validate_result(run_id: str):
    """
    Get the result of a completed async validation run.
    
    Args:
        run_id: The run ID returned by /validate-file-async
        
    Returns:
        ValidateFileOutput with full validation results
        
    Raises:
        HTTPException 400 if validation is not yet completed
        HTTPException 404 if run_id not found
    """
    run = get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run ID not found: {}".format(run_id))

    if run.status != "completed":
        raise HTTPException(
            status_code=400,
            detail="Validation not completed. Current status: {}".format(run.status)
        )

    result = run.result or {}
    return ValidateFileOutput(**result)


@router.get("/info")
def get_service_info():
    """
    Returns metadata for the frontend.
    
    The frontend can use this information to dynamically build the input form.
    """
    return {
        "id": "mfi-validator",
        "name": "RAW MFI Dataset Validator",
        "description": "Validates RAW MFI datasets (CSV) against WFP standards with 5 validation layers. "
                       "Checks file format, structure, schema conformance, and RAW-specific business rules. "
                       "Generates a detailed diagnostic report. "
                       "Note: Only RAW MFI datasets are supported (not PROCESSED).",
        "version": "2.0.0",
        "supported_file_types": ["RAW"],
        "required_columns": sorted(list(RAW_FILE_INDICATORS)),
        "inputs": [
            {
                "name": "file",
                "type": "file",
                "required": True,
                "accept": ".csv",
                "label": "RAW MFI Dataset",
                "description": "CSV file containing RAW MFI data. Must include all required columns: "
                               "SVY_MOD, SURVEY_TYPE, RESPONSEID, SUBMISSIONDATE, _UUID, ENUMERATOR, "
                               "ENUMERATORID, TRADER_NAME, INTERVIEW_DATE, DEVICEID, _SUBMISSION_TIME"
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
                "label": "Survey Type",
                "description": "Full MFI requires 1 market survey + 5 trader surveys per market. "
                               "Reduced MFI has relaxed survey completeness requirements."
            },
            {
                "name": "template",
                "type": "file",
                "required": False,
                "accept": ".csv,.json",
                "label": "Template (optional)",
                "description": "Custom template for additional column validation beyond RAW indicators"
            }
        ],
        "outputs": {
            "file_name": "Validated file name",
            "country": "Country detected from ADM0NAME column",
            "survey_period": "Survey date range (from INTERVIEW_DATE)",
            "detected_file_type": "Always 'RAW' (PROCESSED not supported)",
            "llm_calls": "Number of LLM calls performed",
            "layer_results": "Detailed results for each validation layer",
            "final_report": "Diagnostic report generated by the LLM",
            "success": "True if all layers passed without errors"
        },
        "layers": [
            {
                "id": 0,
                "name": "File Validation",
                "description": "Checks file extension (.csv), encoding detection, binary file detection, "
                               "and validates presence of all required RAW file indicators"
            },
            {
                "id": 1,
                "name": "Structural Parsing",
                "description": "Detects delimiter, identifies broken rows with incorrect column counts, "
                               "and detects over-quoted rows"
            },
            {
                "id": 2,
                "name": "Schema Validation",
                "description": "Validates columns against template, checks for duplicates, "
                               "identifies missing required columns, fuzzy matches typos"
            },
            {
                "id": 3,
                "name": "Business Rules (RAW)",
                "description": "RAW-specific checks: survey completeness, ResponseID/UUID uniqueness, "
                               "date validation, coordinates, enumerator data, trader names"
            },
            {
                "id": 5,
                "name": "Report Generation",
                "description": "Generates a comprehensive diagnostic report using LLM analysis"
            }
        ]
    }


@router.get("/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "mfi-validator", "file_type": "RAW"}
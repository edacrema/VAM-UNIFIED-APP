"""Local dispatcher for Streamlit-only runtime."""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import tempfile
import threading
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import numpy as np

from app.shared.async_runs import (
    create_run,
    get_run,
    set_run_completed,
    set_run_failed,
    update_run,
    update_run_progress,
)
from app.shared.docx_export import build_content_disposition, build_docx_bytes_from_report_blocks
from app.shared.report_blocks import build_market_monitor_report_blocks, build_mfi_report_blocks

from app.services.mfi_validator.graph import RAW_FILE_INDICATORS, run_troubleshooting as run_mfi_troubleshooting
from app.services.mfi_drafter.data_loader import load_mfi_from_csv, validate_csv_structure
from app.services.mfi_drafter.databridges_loader import (
    list_mfi_countries,
    list_mfi_surveys_for_country,
    load_mfi_survey_from_databridges,
)
from app.services.mfi_drafter.graph import DIMENSION_DESCRIPTIONS, run_mfi_report_generation
from app.services.mfi_drafter.schemas import MFI_DIMENSIONS
from app.services.price_validator.graph import run_troubleshooting as run_price_troubleshooting
from app.services.market_monitor.graph import AVAILABLE_MODULES, CURRENCY_SYMBOLS, run_report_generation
from app.services.market_monitor.data_loader import (
    check_data_availability,
    get_available_commodities,
    get_country_metadata as get_market_monitor_country_metadata,
    get_commodity_categories,
    get_supported_countries as get_market_monitor_supported_countries,
    normalize_country_name,
)

logger = logging.getLogger(__name__)


@dataclass
class LocalResponse:
    status_code: int
    headers: Dict[str, str] = field(default_factory=dict)
    content: bytes = b""
    _json_data: Any = None

    @property
    def text(self) -> str:
        try:
            return self.content.decode("utf-8")
        except Exception:
            return ""

    def json(self) -> Any:
        if self._json_data is not None:
            return self._json_data
        if not self.content:
            raise ValueError("No JSON content")
        return json.loads(self.content.decode("utf-8"))


class LocalHTTPException(Exception):
    def __init__(self, status_code: int, detail: Any):
        super().__init__(str(detail))
        self.status_code = status_code
        self.detail = detail


@dataclass
class UploadedFile:
    filename: str
    content: bytes
    content_type: Optional[str] = None


def _json_default(obj: Any) -> Any:
    if obj is None:
        return None

    if dataclasses.is_dataclass(obj):
        return asdict(obj)

    if hasattr(obj, "model_dump"):
        return obj.model_dump()

    if hasattr(obj, "dict"):
        return obj.dict()

    if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
        return obj.to_dict()

    if isinstance(obj, (list, dict, str, int, float, bool)):
        return obj

    if isinstance(obj, (set, tuple)):
        return list(obj)

    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except Exception:
            return str(obj)

    iso = getattr(obj, "isoformat", None)
    if callable(iso):
        try:
            return iso()
        except Exception:
            pass

    try:
        if isinstance(obj, np.generic):
            return obj.item()
    except Exception:
        pass

    try:
        import pandas as pd  # type: ignore

        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
    except Exception:
        pass

    return str(obj)


def _normalize_payload(payload: Any) -> Any:
    try:
        return json.loads(json.dumps(payload, ensure_ascii=False, default=_json_default))
    except Exception:
        return payload


def _json_response(payload: Any, status_code: int = 200, headers: Optional[Dict[str, str]] = None) -> LocalResponse:
    normalized = _normalize_payload(payload)
    content = json.dumps(normalized, ensure_ascii=False, default=_json_default).encode("utf-8")
    final_headers = {"Content-Type": "application/json"}
    if headers:
        final_headers.update(headers)
    return LocalResponse(status_code=status_code, headers=final_headers, content=content, _json_data=normalized)


def _error_response(status_code: int, detail: Any) -> LocalResponse:
    return _json_response({"detail": detail}, status_code=status_code)


def _parse_path_and_params(path: str, params: Optional[Dict[str, Any]]) -> Tuple[List[str], Dict[str, Any]]:
    params = dict(params or {})
    path = (path or "").strip()

    if "://" in path:
        parsed = urlparse(path)
        path = parsed.path or ""
        query = parsed.query
    else:
        query = ""

    if "?" in path:
        path, query = path.split("?", 1)

    if query:
        for key, values in parse_qs(query, keep_blank_values=True).items():
            if not values:
                params.setdefault(key, "")
            elif len(values) == 1:
                params.setdefault(key, values[0])
            else:
                params.setdefault(key, values)

    if path.startswith("/api/"):
        path = path[4:]

    parts = [unquote(p) for p in path.strip("/").split("/") if p]
    return parts, params


def _get_form_value(data: Optional[Dict[str, Any]], key: str, default: Any = None) -> Any:
    if not data:
        return default
    value = data.get(key, default)
    if isinstance(value, list):
        return value[0] if value else default
    return value


def _extract_file(files: Any, key: str) -> Optional[UploadedFile]:
    if not files:
        return None

    file_item = None
    if isinstance(files, dict):
        file_item = files.get(key)
    elif isinstance(files, (list, tuple)):
        for item in files:
            if isinstance(item, tuple) and len(item) >= 2 and item[0] == key:
                file_item = item[1]
                break

    if file_item is None:
        return None

    filename = None
    content = None
    content_type = None

    if isinstance(file_item, (list, tuple)):
        if len(file_item) >= 1:
            filename = file_item[0]
        if len(file_item) >= 2:
            content = file_item[1]
        if len(file_item) >= 3:
            content_type = file_item[2]
    else:
        filename = getattr(file_item, "filename", None)
        content = file_item

    if hasattr(content, "read"):
        content = content.read()

    if content is None:
        return None

    if filename is None:
        filename = "upload"

    if isinstance(content, bytearray):
        content = bytes(content)

    if not isinstance(content, bytes):
        content = str(content).encode("utf-8")

    return UploadedFile(filename=str(filename), content=content, content_type=content_type)


def _save_temp_file(content: bytes, suffix: str) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(content)
    tmp.close()
    return tmp.name


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: List[str] = []
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


def _build_mfi_report_output(
    *,
    result: Dict[str, Any],
    run_id: str,
    country: str,
    data_collection_start: str,
    data_collection_end: str,
) -> Dict[str, Any]:
    market_mfis = [
        float(m.get("overall_mfi", 0) or 0)
        for m in (result.get("markets_data", []) or [])
        if isinstance(m, dict)
    ]
    national_mfi = round(np.mean(market_mfis), 1) if market_mfis else 0.0

    risk_dist: Dict[str, int] = {}
    for m in result.get("markets_data", []) or []:
        if not isinstance(m, dict):
            continue
        risk_dist[m.get("risk_level")] = risk_dist.get(m.get("risk_level"), 0) + 1

    normalized_dimension_findings = _normalize_dimension_findings(result.get("dimension_findings"))
    result_for_blocks = dict(result)
    result_for_blocks["country"] = country
    result_for_blocks["data_collection_start"] = data_collection_start
    result_for_blocks["data_collection_end"] = data_collection_end
    result_for_blocks["dimension_findings"] = normalized_dimension_findings
    result_for_blocks["market_recommendations"] = result.get("market_recommendations", {}) or {}

    return {
        "run_id": run_id,
        "country": country,
        "data_collection_start": data_collection_start,
        "data_collection_end": data_collection_end,
        "survey_metadata": result.get("survey_metadata", {}),
        "national_mfi": national_mfi,
        "risk_distribution": risk_dist,
        "markets_data": result.get("markets_data", []),
        "dimension_scores": result.get("dimension_scores", []),
        "executive_summary": result.get("executive_summary", ""),
        "dimension_findings": normalized_dimension_findings,
        "market_recommendations": result.get("market_recommendations", {}) or {},
        "country_context": result.get("country_context"),
        "document_references": result.get("document_references", []),
        "report_blocks": build_mfi_report_blocks(result_for_blocks),
        "visualizations": result.get("visualizations", {}),
        "warnings": result.get("warnings", []),
        "llm_calls": result.get("llm_calls", 0),
        "correction_attempts": result.get("correction_attempts", 0),
        "success": True,
    }


def _build_market_monitor_output(
    *,
    result: Dict[str, Any],
    run_id: str,
    country: str,
    time_period: str,
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "country": country,
        "time_period": time_period,
        "report_sections": result.get("report_draft_sections", {}),
        "report_blocks": build_market_monitor_report_blocks(result),
        "visualizations": result.get("visualizations", {}),
        "data_statistics": result.get("data_statistics", {}),
        "trend_analysis": result.get("trend_analysis"),
        "events": result.get("events", []),
        "module_sections": result.get("module_sections", {}),
        "document_references": result.get("document_references", []),
        "news_counts": result.get("news_counts", {}),
        "warnings": result.get("warnings", []),
        "llm_calls": result.get("llm_calls", 0),
        "success": True,
    }


def _dispatch_mfi_validator(
    method: str,
    parts: List[str],
    *,
    json_body: Any,
    data: Optional[Dict[str, Any]],
    files: Any,
    params: Dict[str, Any],
) -> LocalResponse:
    if method == "POST" and parts == ["validate-file"]:
        return _mfi_validate_sync(data=data, files=files, params=params)
    if method == "POST" and parts == ["validate-file-async"]:
        return _mfi_validate_async(data=data, files=files, params=params)
    if method == "GET" and len(parts) == 2 and parts[0] == "status":
        return _mfi_validate_status(parts[1])
    if method == "GET" and len(parts) == 2 and parts[0] == "result":
        return _mfi_validate_result(parts[1])
    if method == "GET" and parts == ["info"]:
        return _json_response(_mfi_validator_info())
    if method == "GET" and parts == ["health"]:
        return _json_response({"status": "healthy", "service": "mfi-validator", "file_type": "RAW"})
    raise LocalHTTPException(404, f"Unknown MFI validator endpoint: {'/'.join(parts)}")


def _mfi_validate_sync(*, data: Optional[Dict[str, Any]], files: Any, params: Dict[str, Any]) -> LocalResponse:
    upload = _extract_file(files, "file")
    if upload is None or not upload.filename:
        raise LocalHTTPException(400, "Missing filename")
    if not upload.filename.lower().endswith(".csv"):
        raise LocalHTTPException(400, f"File must be CSV. Received: {upload.filename}")

    survey_type = (
        _get_form_value(data, "survey_type", None)
        or _get_form_value(params, "survey_type", "full mfi")
        or "full mfi"
    )
    survey_type = str(survey_type).lower()
    if survey_type not in {"full mfi", "reduced mfi"}:
        raise LocalHTTPException(
            400,
            f"survey_type must be 'full mfi' or 'reduced mfi'. Received: {survey_type}",
        )

    tmp_path = None
    template_path = None
    try:
        tmp_path = _save_temp_file(upload.content, ".csv")

        template_upload = _extract_file(files, "template")
        if template_upload is not None:
            suffix = ".json" if template_upload.filename.lower().endswith(".json") else ".csv"
            template_path = _save_temp_file(template_upload.content, suffix)

        result = run_mfi_troubleshooting(file_path=tmp_path, template=template_path, survey_type=survey_type)

        layer_results = result.get("layer_results", [])
        success = all(lr.get("passed", False) for lr in layer_results)

        output = {
            "file_name": upload.filename,
            "country": result.get("country"),
            "survey_period": result.get("survey_period"),
            "detected_file_type": result.get("detected_file_type", "RAW"),
            "llm_calls": result.get("llm_calls", 0),
            "layer_results": layer_results,
            "final_report": result.get("final_report", ""),
            "success": success,
        }
        return _json_response(output)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        if template_path and os.path.exists(template_path):
            os.unlink(template_path)


def _mfi_validate_async(*, data: Optional[Dict[str, Any]], files: Any, params: Dict[str, Any]) -> LocalResponse:
    upload = _extract_file(files, "file")
    if upload is None or not upload.filename:
        raise LocalHTTPException(400, "Missing filename")
    if not upload.filename.lower().endswith(".csv"):
        raise LocalHTTPException(400, f"File must be CSV. Received: {upload.filename}")

    survey_type = (
        _get_form_value(data, "survey_type", None)
        or _get_form_value(params, "survey_type", "full mfi")
        or "full mfi"
    )
    survey_type = str(survey_type).lower()
    if survey_type not in {"full mfi", "reduced mfi"}:
        raise LocalHTTPException(
            400,
            f"survey_type must be 'full mfi' or 'reduced mfi'. Received: {survey_type}",
        )

    run_id = f"mfi_val_{uuid.uuid4().hex[:8]}"
    create_run(run_id)

    tmp_path = _save_temp_file(upload.content, ".csv")
    template_path = None
    template_upload = _extract_file(files, "template")
    if template_upload is not None:
        suffix = ".json" if template_upload.filename.lower().endswith(".json") else ".csv"
        template_path = _save_temp_file(template_upload.content, suffix)

    progress_map = {
        "layer0": 10,
        "layer1": 30,
        "layer2": 60,
        "layer3": 80,
        "report": 95,
    }

    def run_in_background() -> None:
        try:
            update_run(run_id, status="running", error=None, traceback=None)

            def on_step(node_name: str, _state: dict) -> None:
                progress = progress_map.get(node_name)
                if progress is not None:
                    update_run_progress(run_id, current_node=node_name, progress_pct=progress)
                else:
                    update_run(run_id, current_node=node_name)

            result = run_mfi_troubleshooting(
                file_path=tmp_path,
                template=template_path,
                survey_type=survey_type,
                on_step=on_step,
            )

            layer_results = result.get("layer_results", [])
            success = all(lr.get("passed", False) for lr in layer_results)

            output = {
                "file_name": upload.filename,
                "country": result.get("country"),
                "survey_period": result.get("survey_period"),
                "detected_file_type": result.get("detected_file_type", "RAW"),
                "llm_calls": result.get("llm_calls", 0),
                "layer_results": layer_results,
                "final_report": result.get("final_report", ""),
                "success": success,
            }

            set_run_completed(run_id, result=output)
        except Exception as exc:
            tb_str = traceback.format_exc()
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(exc), traceback=tb_str, current_node=current_node)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            if template_path and os.path.exists(template_path):
                os.unlink(template_path)

    threading.Thread(target=run_in_background, daemon=True).start()
    return _json_response({"run_id": run_id, "status": "pending"})


def _mfi_validate_status(run_id: str) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    return _json_response(
        {
            "run_id": run_id,
            "status": run.status,
            "current_node": run.current_node,
            "progress_pct": run.progress_pct,
            "warnings": run.warnings,
            "error": run.error,
            "traceback": run.traceback,
        }
    )


def _mfi_validate_result(run_id: str) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    if run.status != "completed":
        raise LocalHTTPException(400, f"Validation not completed. Current status: {run.status}")
    return _json_response(run.result or {})


def _mfi_validator_info() -> Dict[str, Any]:
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
                "description": "CSV file containing RAW MFI data. Must include all required columns (or accepted aliases): {}".format(
                    ", ".join(sorted(RAW_FILE_INDICATORS))
                ),
            },
            {
                "name": "survey_type",
                "type": "select",
                "required": True,
                "options": [
                    {"value": "full mfi", "label": "Full MFI"},
                    {"value": "reduced mfi", "label": "Reduced MFI"},
                ],
                "default": "full mfi",
                "label": "Survey Type",
                "description": "Full MFI requires 1 market survey + 5 trader surveys per market. "
                "Reduced MFI has relaxed survey completeness requirements.",
            },
            {
                "name": "template",
                "type": "file",
                "required": False,
                "accept": ".csv,.json",
                "label": "Template (optional)",
                "description": "Custom template for additional column validation beyond RAW indicators",
            },
        ],
        "outputs": {
            "file_name": "Validated file name",
            "country": "Country detected from ADM0CODE/ADM0NAME columns when available",
            "survey_period": "Survey date range inferred from SVYDATE/SVYSTARTTIME/_SUBMISSION_TIME (or legacy date columns)",
            "detected_file_type": "Always 'RAW' (PROCESSED not supported)",
            "llm_calls": "Number of LLM calls performed",
            "layer_results": "Detailed results for each validation layer",
            "final_report": "Diagnostic report generated by the LLM",
            "success": "True if all layers passed without errors",
        },
        "layers": [
            {
                "id": 0,
                "name": "File Validation",
                "description": "Checks file extension (.csv), encoding detection, binary file detection, "
                "and validates presence of all required RAW file indicators",
            },
            {
                "id": 1,
                "name": "Structural Parsing",
                "description": "Detects delimiter, identifies broken rows with incorrect column counts, "
                "and detects over-quoted rows",
            },
            {
                "id": 2,
                "name": "Schema Validation",
                "description": "Validates columns against template, checks for duplicates, "
                "identifies missing required columns, fuzzy matches typos",
            },
            {
                "id": 3,
                "name": "Business Rules (RAW)",
                "description": "RAW-specific checks: survey completeness, instanceID/ResponseID and UUID uniqueness, "
                "date validation, coordinates, enumerator data, trader names",
            },
            {
                "id": 5,
                "name": "Report Generation",
                "description": "Generates a comprehensive diagnostic report using LLM analysis",
            },
        ],
    }


def _dispatch_price_validator(
    method: str,
    parts: List[str],
    *,
    json_body: Any,
    data: Optional[Dict[str, Any]],
    files: Any,
    params: Dict[str, Any],
) -> LocalResponse:
    if method == "POST" and parts == ["validate-file"]:
        return _price_validate_sync(files=files)
    if method == "POST" and parts == ["validate-file-async"]:
        return _price_validate_async(files=files)
    if method == "GET" and len(parts) == 2 and parts[0] == "status":
        return _price_validate_status(parts[1])
    if method == "GET" and len(parts) == 2 and parts[0] == "result":
        return _price_validate_result(parts[1])
    if method == "GET" and parts == ["info"]:
        return _json_response(_price_validator_info())
    if method == "GET" and parts == ["health"]:
        return _json_response({"status": "healthy", "service": "price-validator"})
    if method == "GET" and parts == ["products"]:
        from app.services.price_validator.graph import WFP_PRODUCTS

        return _json_response(
            {
                "total": len(WFP_PRODUCTS),
                "products": [{"name": name, "id": pid} for name, pid in sorted(WFP_PRODUCTS.items())],
            }
        )
    raise LocalHTTPException(404, f"Unknown Price validator endpoint: {'/'.join(parts)}")


def _price_validate_sync(*, files: Any) -> LocalResponse:
    upload = _extract_file(files, "file")
    if upload is None or not upload.filename:
        raise LocalHTTPException(400, "Missing filename")

    valid_extensions = [".xlsx"]
    file_ext = os.path.splitext(upload.filename)[1].lower()
    if file_ext not in valid_extensions:
        raise LocalHTTPException(400, f"Unsupported file format. Use: {', '.join(valid_extensions)}")

    tmp_path = None
    template_path = None
    try:
        tmp_path = _save_temp_file(upload.content, file_ext)

        template_upload = _extract_file(files, "template")
        if template_upload is None or not template_upload.filename:
            raise LocalHTTPException(400, "Template file is required")

        template_ext = os.path.splitext(template_upload.filename)[1].lower()
        if template_ext != ".xlsx":
            raise LocalHTTPException(400, "Template must be an .xlsx file")

        template_path = _save_temp_file(template_upload.content, template_ext)

        result = run_price_troubleshooting(file_path=tmp_path, template_path=template_path)

        layer_results = result.get("layer_results", [])
        success = all(lr.get("passed", False) for lr in layer_results)

        output = {
            "file_name": upload.filename,
            "file_type": result.get("file_type"),
            "country": result.get("country"),
            "num_products": result.get("num_products"),
            "num_markets": result.get("num_markets"),
            "detected_language": result.get("detected_language"),
            "llm_calls": result.get("llm_calls", 0),
            "layer_results": layer_results,
            "column_roles": result.get("column_roles"),
            "product_classifications": result.get("product_classifications", []),
            "final_report": result.get("final_report", ""),
            "success": success,
        }
        return _json_response(output)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        if template_path and os.path.exists(template_path):
            os.unlink(template_path)


def _price_validate_async(*, files: Any) -> LocalResponse:
    upload = _extract_file(files, "file")
    if upload is None or not upload.filename:
        raise LocalHTTPException(400, "Missing filename")

    valid_extensions = [".xlsx"]
    file_ext = os.path.splitext(upload.filename)[1].lower()
    if file_ext not in valid_extensions:
        raise LocalHTTPException(400, f"Unsupported file format. Use: {', '.join(valid_extensions)}")

    run_id = f"price_val_{uuid.uuid4().hex[:8]}"
    create_run(run_id)

    tmp_path = _save_temp_file(upload.content, file_ext)
    template_path = None
    template_upload = _extract_file(files, "template")
    if template_upload is None or not template_upload.filename:
        raise LocalHTTPException(400, "Template file is required")

    template_ext = os.path.splitext(template_upload.filename)[1].lower()
    if template_ext != ".xlsx":
        raise LocalHTTPException(400, "Template must be an .xlsx file")

    template_path = _save_temp_file(template_upload.content, template_ext)

    progress_map = {
        "layer1": 25,
        "layer2": 55,
        "layer3": 85,
        "report": 95,
    }

    def run_in_background() -> None:
        try:
            update_run(run_id, status="running", error=None, traceback=None)

            def on_step(node_name: str, _state: dict) -> None:
                progress = progress_map.get(node_name)
                if progress is not None:
                    update_run_progress(run_id, current_node=node_name, progress_pct=progress)
                else:
                    update_run(run_id, current_node=node_name)

            result = run_price_troubleshooting(file_path=tmp_path, template_path=template_path, on_step=on_step)

            layer_results = result.get("layer_results", [])
            success = all(lr.get("passed", False) for lr in layer_results)

            output = {
                "file_name": upload.filename,
                "file_type": result.get("file_type"),
                "country": result.get("country"),
                "num_products": result.get("num_products"),
                "num_markets": result.get("num_markets"),
                "detected_language": result.get("detected_language"),
                "llm_calls": result.get("llm_calls", 0),
                "layer_results": layer_results,
                "column_roles": result.get("column_roles"),
                "product_classifications": result.get("product_classifications", []),
                "final_report": result.get("final_report", ""),
                "success": success,
            }

            set_run_completed(run_id, result=output)
        except Exception as exc:
            tb_str = traceback.format_exc()
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(exc), traceback=tb_str, current_node=current_node)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            if template_path and os.path.exists(template_path):
                os.unlink(template_path)

    threading.Thread(target=run_in_background, daemon=True).start()
    return _json_response({"run_id": run_id, "status": "pending"})


def _price_validate_status(run_id: str) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    return _json_response(
        {
            "run_id": run_id,
            "status": run.status,
            "current_node": run.current_node,
            "progress_pct": run.progress_pct,
            "warnings": run.warnings,
            "error": run.error,
            "traceback": run.traceback,
        }
    )


def _price_validate_result(run_id: str) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    if run.status != "completed":
        raise LocalHTTPException(400, f"Validation not completed. Current status: {run.status}")
    return _json_response(run.result or {})


def _price_validator_info() -> Dict[str, Any]:
    return {
        "id": "price-validator",
        "name": "Price Data Validator",
        "description": "Validates Price Data datasets (XLSX) with 4 layers of validation. "
        "Compares template columns, detects commodity/market/date fields, "
        "and generates a deterministic diagnostic report.",
        "version": "1.0.0",
        "inputs": [
            {
                "name": "file",
                "type": "file",
                "required": True,
                "accept": ".xlsx",
                "label": "Price Data dataset",
                "description": "Excel (.xlsx) file containing the price data to validate",
            },
            {
                "name": "template",
                "type": "file",
                "required": True,
                "accept": ".xlsx",
                "label": "Template",
                "description": "Official template for validating columns",
            },
        ],
        "outputs": {
            "file_name": "Validated file name",
            "file_type": "File type (XLSX)",
            "country": "Country detected from dataset",
            "num_products": "Number of unique products",
            "num_markets": "Number of unique markets",
            "detected_language": "Detected language (en, fr, es, ar)",
            "llm_calls": "Number of LLM calls performed",
            "layer_results": "Detailed results for each layer",
            "column_roles": "Detected commodity/market/date columns",
            "product_classifications": "Commodity name suggestions",
            "final_report": "Deterministic diagnostic report",
            "success": "True if all layers passed",
        },
        "layers": [
            {"id": 1, "name": "XLSX Validation", "description": "Checks XLSX integrity and loads data"},
            {"id": 2, "name": "Template Comparison", "description": "Compares submitted columns to template"},
            {"id": 3, "name": "Content Validation", "description": "Validates commodity, market, and date values"},
            {"id": 4, "name": "Deterministic Report", "description": "Summarizes errors and fixes"},
        ],
        "supported_formats": ["Excel (.xlsx)"],
        "product_list_size": "~200 WFP standard products",
    }


def _dispatch_mfi_drafter(
    method: str,
    parts: List[str],
    *,
    json_body: Any,
    data: Optional[Dict[str, Any]],
    files: Any,
    params: Dict[str, Any],
) -> LocalResponse:
    if method == "POST" and parts == ["generate"]:
        return _mfi_drafter_generate(json_body=json_body)
    if method == "GET" and parts == ["countries"]:
        return _mfi_drafter_countries()
    if method == "GET" and len(parts) == 3 and parts[0] == "countries" and parts[2] == "surveys":
        return _mfi_drafter_country_surveys(parts[1], params=params)
    if method == "POST" and parts == ["generate-from-survey"]:
        return _mfi_drafter_generate_from_survey(json_body=json_body)
    if method == "POST" and parts == ["generate-from-survey-async"]:
        return _mfi_drafter_generate_from_survey_async(json_body=json_body)
    if method == "POST" and parts == ["generate-async"]:
        return _mfi_drafter_generate_async(json_body=json_body)
    if method == "GET" and len(parts) == 2 and parts[0] == "status":
        return _mfi_drafter_status(parts[1])
    if method == "GET" and len(parts) == 2 and parts[0] == "result":
        return _mfi_drafter_result(parts[1])
    if method == "POST" and len(parts) == 2 and parts[0] == "export-docx":
        return _mfi_drafter_export_docx(parts[1], json_body=json_body)
    if method == "GET" and parts == ["info"]:
        return _json_response(_mfi_drafter_info())
    if method == "GET" and parts == ["health"]:
        return _json_response({"status": "healthy", "service": "mfi-drafter"})
    if method == "GET" and parts == ["dimensions"]:
        return _json_response(_mfi_drafter_dimensions())
    if method == "GET" and parts == ["sample-markets"]:
        return _json_response(_mfi_drafter_sample_markets())
    raise LocalHTTPException(404, f"Unknown MFI drafter endpoint: {'/'.join(parts)}")


def _mfi_drafter_generate(*, json_body: Any) -> LocalResponse:
    if not isinstance(json_body, dict):
        raise LocalHTTPException(400, "Invalid JSON body")

    country = json_body.get("country")
    data_collection_start = json_body.get("data_collection_start")
    data_collection_end = json_body.get("data_collection_end")
    markets = json_body.get("markets") or []
    if not isinstance(markets, list):
        markets = [markets] if markets else []

    try:
        result = run_mfi_report_generation(
            country=country,
            data_collection_start=data_collection_start,
            data_collection_end=data_collection_end,
            markets=markets,
        )
    except Exception as exc:
        raise LocalHTTPException(500, str(exc))

    run_id = result.get("run_id", "unknown")
    output = _build_mfi_report_output(
        result=result,
        run_id=run_id,
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
    )
    return _json_response(output)


def _mfi_drafter_countries() -> LocalResponse:
    try:
        return _json_response({"countries": list_mfi_countries()})
    except Exception as exc:
        raise LocalHTTPException(502, str(exc))


def _mfi_drafter_country_surveys(country: str, *, params: Dict[str, Any]) -> LocalResponse:
    start_date = _get_form_value(params, "start_date", None)
    end_date = _get_form_value(params, "end_date", None)
    try:
        surveys = list_mfi_surveys_for_country(
            country,
            start_date=str(start_date) if start_date else None,
            end_date=str(end_date) if end_date else None,
        )
        return _json_response({"country": country, "surveys": surveys})
    except Exception as exc:
        raise LocalHTTPException(502, str(exc))


def _mfi_drafter_generate_from_survey(*, json_body: Any) -> LocalResponse:
    if not isinstance(json_body, dict):
        raise LocalHTTPException(400, "Invalid JSON body")
    survey_id = json_body.get("survey_id")
    if survey_id is None:
        raise LocalHTTPException(400, "survey_id is required")
    try:
        csv_data = load_mfi_survey_from_databridges(int(survey_id))
        result = run_mfi_report_generation(
            country=csv_data["country"],
            data_collection_start=csv_data["data_collection_start"],
            data_collection_end=csv_data["data_collection_end"],
            markets=csv_data["markets"],
            csv_data=csv_data,
        )
        output = _build_mfi_report_output(
            result=result,
            run_id=result.get("run_id", "unknown"),
            country=csv_data["country"],
            data_collection_start=csv_data["data_collection_start"],
            data_collection_end=csv_data["data_collection_end"],
        )
        return _json_response(output)
    except ValueError as exc:
        raise LocalHTTPException(400, str(exc))
    except Exception as exc:
        raise LocalHTTPException(502, str(exc))


def _mfi_drafter_generate_from_survey_async(*, json_body: Any) -> LocalResponse:
    if not isinstance(json_body, dict):
        raise LocalHTTPException(400, "Invalid JSON body")
    survey_id = json_body.get("survey_id")
    if survey_id is None:
        raise LocalHTTPException(400, "survey_id is required")
    try:
        csv_data = load_mfi_survey_from_databridges(int(survey_id))
    except ValueError as exc:
        raise LocalHTTPException(400, str(exc))
    except Exception as exc:
        raise LocalHTTPException(502, str(exc))

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

    def run_in_background() -> None:
        try:
            update_run(run_id, status="running", error=None, traceback=None)

            def on_step(node_name: str, _state: dict) -> None:
                progress = progress_map.get(node_name)
                if progress is not None:
                    update_run_progress(run_id, current_node=node_name, progress_pct=progress)
                else:
                    update_run(run_id, current_node=node_name)

                context_counts = _state.get("context_counts")
                if isinstance(context_counts, dict):
                    update_run(run_id, metadata={"context_counts": context_counts})

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
            output = _build_mfi_report_output(
                result=result,
                run_id=run_id,
                country=csv_data["country"],
                data_collection_start=csv_data["data_collection_start"],
                data_collection_end=csv_data["data_collection_end"],
            )
            set_run_completed(run_id, result=output)
        except Exception as exc:
            logger.exception("MFI async report generation from Databridges failed")
            set_run_failed(run_id, error=str(exc), traceback=traceback.format_exc())

    thread = threading.Thread(target=run_in_background, name=f"mfi-survey-{run_id}", daemon=True)
    thread.start()
    return _json_response({"run_id": run_id, "status": "pending"})


def _mfi_drafter_generate_from_csv(
    *,
    data: Optional[Dict[str, Any]],
    files: Any,
    params: Dict[str, Any],
) -> LocalResponse:
    upload = _extract_file(files, "file")
    if upload is None or not upload.filename:
        raise LocalHTTPException(400, "Missing filename")
    if not upload.filename.lower().endswith(".csv"):
        raise LocalHTTPException(400, "File must be a CSV")

    country_override = _get_form_value(data, "country_override", None) or _get_form_value(
        params, "country_override", None
    )
    start_override = _get_form_value(data, "data_collection_start_override", None) or _get_form_value(
        params, "data_collection_start_override", None
    )
    end_override = _get_form_value(data, "data_collection_end_override", None) or _get_form_value(
        params, "data_collection_end_override", None
    )

    try:
        csv_data = load_mfi_from_csv(
            file_content=upload.content,
            country_override=country_override,
            start_date_override=start_override,
            end_date_override=end_override,
        )
    except ValueError as exc:
        raise LocalHTTPException(400, str(exc))
    except Exception as exc:
        raise LocalHTTPException(500, str(exc))

    try:
        result = run_mfi_report_generation(
            country=csv_data["country"],
            data_collection_start=csv_data["data_collection_start"],
            data_collection_end=csv_data["data_collection_end"],
            markets=csv_data["markets"],
            csv_data=csv_data,
        )
    except Exception as exc:
        raise LocalHTTPException(500, str(exc))

    run_id = result.get("run_id", "unknown")
    output = _build_mfi_report_output(
        result=result,
        run_id=run_id,
        country=csv_data["country"],
        data_collection_start=csv_data["data_collection_start"],
        data_collection_end=csv_data["data_collection_end"],
    )
    return _json_response(output)


def _mfi_drafter_validate_csv(*, files: Any) -> LocalResponse:
    upload = _extract_file(files, "file")
    if upload is None or not upload.filename:
        raise LocalHTTPException(400, "Missing filename")
    if not upload.filename.lower().endswith(".csv"):
        raise LocalHTTPException(400, "File must be a CSV")

    try:
        result = validate_csv_structure(upload.content)
    except Exception as exc:
        raise LocalHTTPException(400, f"CSV validation failed: {str(exc)}")
    return _json_response(result)


def _mfi_drafter_generate_from_csv_async(
    *,
    data: Optional[Dict[str, Any]],
    files: Any,
    params: Dict[str, Any],
) -> LocalResponse:
    upload = _extract_file(files, "file")
    if upload is None or not upload.filename:
        raise LocalHTTPException(400, "Missing filename")
    if not upload.filename.lower().endswith(".csv"):
        raise LocalHTTPException(400, "File must be a CSV")

    country_override = _get_form_value(data, "country_override", None) or _get_form_value(
        params, "country_override", None
    )
    start_override = _get_form_value(data, "data_collection_start_override", None) or _get_form_value(
        params, "data_collection_start_override", None
    )
    end_override = _get_form_value(data, "data_collection_end_override", None) or _get_form_value(
        params, "data_collection_end_override", None
    )

    try:
        csv_data = load_mfi_from_csv(
            file_content=upload.content,
            country_override=country_override,
            start_date_override=start_override,
            end_date_override=end_override,
        )
    except ValueError as exc:
        raise LocalHTTPException(400, str(exc))

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

    def run_in_background() -> None:
        try:
            update_run(run_id, status="running", error=None, traceback=None)

            def on_step(node_name: str, _state: dict) -> None:
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
                country=csv_data["country"],
                data_collection_start=csv_data["data_collection_start"],
                data_collection_end=csv_data["data_collection_end"],
                markets=csv_data["markets"],
                csv_data=csv_data,
                on_step=on_step,
            )

            update_run(run_id, warnings=result.get("warnings", []))
            set_run_completed(run_id, result=result)
        except Exception as exc:
            tb_str = traceback.format_exc()
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(exc), traceback=tb_str, current_node=current_node)

    threading.Thread(target=run_in_background, daemon=True).start()
    preview = {
        "country": csv_data.get("country"),
        "markets_count": len(csv_data.get("markets") or []),
        "collection_period": (csv_data.get("survey_metadata") or {}).get("collection_period"),
    }
    return _json_response({"run_id": run_id, "status": "pending", "preview": preview})


def _mfi_drafter_generate_async(*, json_body: Any) -> LocalResponse:
    if not isinstance(json_body, dict):
        raise LocalHTTPException(400, "Invalid JSON body")

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

    def run_in_background() -> None:
        try:
            update_run(run_id, status="running", error=None, traceback=None)

            def on_step(node_name: str, _state: dict) -> None:
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
                country=json_body.get("country"),
                data_collection_start=json_body.get("data_collection_start"),
                data_collection_end=json_body.get("data_collection_end"),
                markets=json_body.get("markets"),
                on_step=on_step,
            )

            update_run(run_id, warnings=result.get("warnings", []))
            set_run_completed(run_id, result=result)
        except Exception as exc:
            tb_str = traceback.format_exc()
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(exc), traceback=tb_str, current_node=current_node)

    threading.Thread(target=run_in_background, daemon=True).start()
    return _json_response({"run_id": run_id, "status": "pending"})


def _mfi_drafter_status(run_id: str) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    return _json_response(
        {
            "run_id": run_id,
            "status": run.status,
            "current_node": run.current_node,
            "progress_pct": run.progress_pct,
            "warnings": run.warnings,
            "metadata": getattr(run, "metadata", {}) or {},
            "error": run.error,
            "traceback": run.traceback,
        }
    )


def _mfi_drafter_result(run_id: str) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    if run.status != "completed":
        raise LocalHTTPException(400, f"Report not completed. Current status: {run.status}")

    result = run.result or {}
    country = result.get("country", "Unknown")
    data_collection_start = result.get("data_collection_start", "Unknown")
    data_collection_end = result.get("data_collection_end", "Unknown")

    output = _build_mfi_report_output(
        result=result,
        run_id=run_id,
        country=country,
        data_collection_start=data_collection_start,
        data_collection_end=data_collection_end,
    )
    return _json_response(output)


def _mfi_drafter_export_docx(run_id: str, *, json_body: Any) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    if run.status != "completed":
        raise LocalHTTPException(409, f"Run not completed. Current status: {run.status}")

    options = json_body if isinstance(json_body, dict) else {}
    filename = options.get("filename") or f"mfi-drafter-{run_id}.docx"
    include_sources = bool(options.get("include_sources", True))
    include_visualizations = bool(options.get("include_visualizations", True))

    result = run.result or {}
    result_for_blocks = dict(result)
    result_for_blocks["dimension_findings"] = _normalize_dimension_findings(result.get("dimension_findings"))
    result_for_blocks["market_recommendations"] = result.get("market_recommendations", {}) or {}

    try:
        report_blocks = build_mfi_report_blocks(result_for_blocks)
        docx_bytes = build_docx_bytes_from_report_blocks(
            report_blocks,
            visualizations=result.get("visualizations", {}),
            include_sources=include_sources,
            include_visualizations=include_visualizations,
        )
    except Exception as exc:
        raise LocalHTTPException(500, f"DOCX generation failed: {str(exc)}")

    headers = {
        "Content-Disposition": build_content_disposition(filename),
        "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    return LocalResponse(status_code=200, headers=headers, content=docx_bytes)


def _mfi_drafter_info() -> Dict[str, Any]:
    return {
        "id": "mfi-drafter",
        "name": "MFI Report Generator",
        "description": "Generates full Market Functionality Index (MFI) reports. "
        "Analyzes 9 market functionality dimensions and generates "
        "visualizations, an executive summary, and recommendations.",
        "version": "1.1.0",
        "supports_csv_upload": False,
        "data_source": "Databridges",
        "inputs": [
            {
                "name": "country",
                "type": "string",
                "required": True,
                "label": "Country",
                "description": "Country with available MFI surveys in Databridges",
            },
            {
                "name": "survey_id",
                "type": "integer",
                "required": True,
                "label": "MFI Survey",
                "description": "Databridges survey ID selected after choosing a country",
            },
        ],
        "databridges": {
            "countries_endpoint": "/countries",
            "surveys_endpoint": "/countries/{country}/surveys",
            "endpoint": "/generate-from-survey",
            "async_endpoint": "/generate-from-survey-async",
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
            "success": "True if generation is completed",
        },
        "workflow_nodes": [
            {"id": "mfi_data_agent", "name": "MFI Data Agent", "description": "Retrieves/generates MFI data"},
            {"id": "context_retrieval", "name": "Context Retrieval", "description": "Retrieves contextual news"},
            {"id": "context_extractor", "name": "Context Extractor", "description": "Extracts context with the LLM"},
            {"id": "mfi_graph_designer", "name": "Graph Designer", "description": "Generates visualizations"},
            {"id": "dimension_drafter", "name": "Dimension Drafter", "description": "Drafts findings per dimension"},
            {
                "id": "market_recommendations_drafter",
                "name": "Market Recommendations",
                "description": "Drafts recommendations by market",
            },
            {
                "id": "executive_summary_drafter",
                "name": "Executive Summary",
                "description": "Drafts executive summary",
            },
            {"id": "red_team", "name": "Red Team QA", "description": "Quality assurance"},
        ],
        "mfi_dimensions": MFI_DIMENSIONS,
        "risk_levels": ["Low Risk", "Medium Risk", "High Risk", "Very High Risk"],
    }


def _mfi_drafter_dimensions() -> Dict[str, Any]:
    return {
        "dimensions": [
            {
                "name": dim,
                "description": DIMENSION_DESCRIPTIONS.get(dim, ""),
                "score_range": "0-10",
                "thresholds": {
                    "low_risk": ">=7.0",
                    "medium_risk": "5.5-6.9",
                    "high_risk": "4.0-5.4",
                    "very_high_risk": "<4.0",
                },
            }
            for dim in MFI_DIMENSIONS
        ]
    }


def _mfi_drafter_sample_markets() -> Dict[str, Any]:
    return {
        "Ghana": {
            "markets": [
                "Gushegu",
                "Karaga",
                "Nanton",
                "Sang",
                "Tamale Aboabo",
                "Yendi",
                "Fumbisi",
                "Bussie",
                "Gwollu",
                "Nyoli",
                "Tangasie",
                "Tumu",
            ]
        },
        "Sudan": {
            "markets": [
                "Omdurman",
                "Khartoum Central",
                "El Fasher",
                "Nyala",
                "Kassala City",
                "Gedaref",
                "Port Sudan",
            ]
        },
        "Yemen": {
            "markets": [
                "Sana'a Central",
                "Aden Port",
                "Taiz City",
                "Hodeidah",
                "Mukalla",
                "Ibb",
            ]
        },
    }


def _dispatch_market_monitor(
    method: str,
    parts: List[str],
    *,
    json_body: Any,
    data: Optional[Dict[str, Any]],
    files: Any,
    params: Dict[str, Any],
) -> LocalResponse:
    if method == "POST" and parts == ["generate"]:
        return _market_monitor_generate(json_body=json_body)
    if method == "POST" and parts == ["generate-async"]:
        return _market_monitor_generate_async(json_body=json_body)
    if method == "GET" and parts == ["data-availability"]:
        return _market_monitor_data_availability(params=params)
    if method == "GET" and len(parts) == 2 and parts[0] == "status":
        return _market_monitor_status(parts[1])
    if method == "GET" and len(parts) == 2 and parts[0] == "result":
        return _market_monitor_result(parts[1])
    if method == "POST" and len(parts) == 2 and parts[0] == "export-docx":
        return _market_monitor_export_docx(parts[1], json_body=json_body)
    if method == "GET" and parts == ["info"]:
        return _json_response(_market_monitor_info())
    if method == "GET" and parts == ["health"]:
        return _json_response({"status": "healthy", "service": "market-monitor"})
    if method == "GET" and parts == ["countries"]:
        return _market_monitor_countries()
    if method == "GET" and parts == ["commodities"]:
        return _market_monitor_commodities(params=params)
    if method == "GET" and len(parts) == 3 and parts[0] == "countries" and parts[2] == "metadata":
        return _market_monitor_country_metadata(parts[1])
    raise LocalHTTPException(404, f"Unknown Market Monitor endpoint: {'/'.join(parts)}")


def _market_monitor_generate(*, json_body: Any) -> LocalResponse:
    if not isinstance(json_body, dict):
        raise LocalHTTPException(400, "Invalid JSON body")

    country = json_body.get("country")
    time_period = json_body.get("time_period")
    commodity_list = json_body.get("commodity_list") or []
    admin1_list = json_body.get("admin1_list") or []
    currency_code = json_body.get("currency_code") or "USD"
    enabled_modules = json_body.get("enabled_modules") or []
    news_start_date = json_body.get("news_start_date")
    news_end_date = json_body.get("news_end_date")
    previous_report_text = json_body.get("previous_report_text") or ""
    use_mock_data = bool(json_body.get("use_mock_data", False))

    if not admin1_list and use_mock_data:
        admin1_list = [f"{country} North", f"{country} South", f"{country} Central"]

    try:
        result = run_report_generation(
            country=country,
            time_period=time_period,
            commodity_list=commodity_list,
            admin1_list=admin1_list,
            currency_code=currency_code,
            enabled_modules=enabled_modules,
            news_start_date=news_start_date,
            news_end_date=news_end_date,
            previous_report_text=previous_report_text,
            use_mock_data=use_mock_data,
        )
    except Exception as exc:
        raise LocalHTTPException(500, str(exc))

    run_id = result.get("run_id", "unknown")
    output = _build_market_monitor_output(
        result=result,
        run_id=run_id,
        country=country,
        time_period=time_period,
    )
    return _json_response(output)


def _market_monitor_generate_async(*, json_body: Any) -> LocalResponse:
    if not isinstance(json_body, dict):
        raise LocalHTTPException(400, "Invalid JSON body")

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

    def run_in_background() -> None:
        try:
            update_run(run_id, status="running", error=None, traceback=None)

            admin1_list = json_body.get("admin1_list") or []
            if not admin1_list and json_body.get("use_mock_data"):
                country = json_body.get("country")
                admin1_list = [f"{country} North", f"{country} South"]

            def on_step(node_name: str, _state: dict) -> None:
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
                country=json_body.get("country"),
                time_period=json_body.get("time_period"),
                commodity_list=json_body.get("commodity_list") or [],
                admin1_list=admin1_list,
                currency_code=json_body.get("currency_code") or "USD",
                enabled_modules=json_body.get("enabled_modules") or [],
                news_start_date=json_body.get("news_start_date"),
                news_end_date=json_body.get("news_end_date"),
                previous_report_text=json_body.get("previous_report_text") or "",
                use_mock_data=bool(json_body.get("use_mock_data", False)),
                on_step=on_step,
            )

            update_run(run_id, warnings=result.get("warnings", []))
            set_run_completed(run_id, result=result)
        except Exception as exc:
            tb_str = traceback.format_exc()
            current_node = get_run(run_id).current_node if get_run(run_id) is not None else None
            set_run_failed(run_id, error=str(exc), traceback=tb_str, current_node=current_node)

    threading.Thread(target=run_in_background, daemon=True).start()
    return _json_response({"run_id": run_id, "status": "pending"})


def _market_monitor_data_availability(*, params: Dict[str, Any]) -> LocalResponse:
    country = _get_form_value(params, "country", "")
    time_period = _get_form_value(params, "time_period", "2025-01")
    commodities_raw = _get_form_value(params, "commodities", "Sugar,Wheat flour")

    commodity_list = [c.strip() for c in str(commodities_raw).split(",") if c.strip()]

    availability = check_data_availability(
        country=str(country),
        time_period=str(time_period),
        commodities=commodity_list,
    )
    return _json_response(availability)


def _market_monitor_status(run_id: str) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    return _json_response(
        {
            "run_id": run_id,
            "status": run.status,
            "current_node": run.current_node,
            "progress_pct": run.progress_pct,
            "warnings": run.warnings,
            "metadata": getattr(run, "metadata", {}) or {},
            "error": run.error,
            "traceback": run.traceback,
        }
    )


def _market_monitor_result(run_id: str) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    if run.status != "completed":
        raise LocalHTTPException(400, f"Report not completed. Current status: {run.status}")

    result = run.result or {}
    if not result.get("warnings") and run.warnings:
        result = {**result, "warnings": run.warnings}

    output = _build_market_monitor_output(
        result=result,
        run_id=run_id,
        country=result.get("country", "Unknown"),
        time_period=result.get("time_period", "Unknown"),
    )
    return _json_response(output)


def _market_monitor_export_docx(run_id: str, *, json_body: Any) -> LocalResponse:
    run = get_run(run_id)
    if run is None:
        raise LocalHTTPException(404, f"Run ID not found: {run_id}")
    if run.status != "completed":
        raise LocalHTTPException(409, f"Run not completed. Current status: {run.status}")

    options = json_body if isinstance(json_body, dict) else {}
    filename = options.get("filename") or f"market-monitor-{run_id}.docx"
    include_sources = bool(options.get("include_sources", True))
    include_visualizations = bool(options.get("include_visualizations", True))

    result = run.result or {}

    try:
        report_blocks = build_market_monitor_report_blocks(result)
        docx_bytes = build_docx_bytes_from_report_blocks(
            report_blocks,
            visualizations=result.get("visualizations", {}),
            include_sources=include_sources,
            include_visualizations=include_visualizations,
        )
    except Exception as exc:
        raise LocalHTTPException(500, f"DOCX generation failed: {str(exc)}")

    headers = {
        "Content-Disposition": build_content_disposition(filename),
        "Content-Type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    return LocalResponse(status_code=200, headers=headers, content=docx_bytes)


def _market_monitor_info() -> Dict[str, Any]:
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
                "description": "Country name (e.g., 'Sudan', 'Yemen', 'Myanmar')",
            },
            {
                "name": "time_period",
                "type": "string",
                "required": True,
                "label": "Time Period",
                "description": "Period in YYYY-MM format (e.g., '2025-01')",
            },
            {
                "name": "commodity_list",
                "type": "array",
                "required": False,
                "label": "Commodities",
                "description": "List of commodities to analyze. Use /countries/{country}/metadata endpoint to get available commodities for a specific country.",
                "default": [],
                "note": "Defaults are country-specific. Query /countries/{country}/metadata for recommended defaults.",
            },
            {
                "name": "admin1_list",
                "type": "array",
                "required": False,
                "label": "Regions (Admin1)",
                "description": "List of regions to include",
            },
            {
                "name": "currency_code",
                "type": "string",
                "required": False,
                "label": "Currency Code",
                "description": "ISO 4217 currency code (e.g., 'SDG', 'YER')",
                "default": "USD",
            },
            {
                "name": "enabled_modules",
                "type": "array",
                "required": False,
                "label": "Optional Modules",
                "description": "Optional modules to enable (note: exchange_rate requires TE_API_KEY and has no mock fallback)",
                "default": [],
                "options": list(AVAILABLE_MODULES.keys()),
            },
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
            "success": "True if generation completed successfully",
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
            {"id": "red_team", "name": "Red Team QA", "description": "Quality assurance and fact-checking"},
        ],
        "available_modules": [
            {
                "id": "exchange_rate",
                "name": "Exchange Rate Analysis",
                "description": "Exchange rate analysis using TradingEconomics API data (requires TE_API_KEY; no mock fallback)",
            }
        ],
    }


def _market_monitor_dataset_status() -> LocalResponse:
    raise LocalHTTPException(
        404,
        "The processed Price Bulletin dataset upload/status path has been removed. Data is loaded from Databridges.",
    )


def _market_monitor_dataset_upload(*, files: Any) -> LocalResponse:
    raise LocalHTTPException(
        404,
        "The processed Price Bulletin dataset upload path has been removed. Data is loaded from Databridges.",
    )


def _market_monitor_countries() -> LocalResponse:
    return _json_response({"countries": get_market_monitor_supported_countries()})


def _market_monitor_commodities(*, params: Dict[str, Any]) -> LocalResponse:
    country = _get_form_value(params, "country", None)
    if not country:
        return _json_response(
            {
                "commodities": [],
                "categories": {},
                "warning": "Select a country to load Databridges commodity options.",
            }
        )

    country_normalized = normalize_country_name(str(country))
    commodity_list = get_available_commodities(country_normalized)
    categories = get_commodity_categories(commodity_list)
    return _json_response(
        {
            "country": country_normalized,
            "commodities": [{"name": c} for c in commodity_list],
            "categories": categories,
        }
    )


def _market_monitor_country_metadata(country: str) -> LocalResponse:
    try:
        return _json_response(get_market_monitor_country_metadata(country))
    except Exception as exc:
        raise LocalHTTPException(502, str(exc))


def _get_food_basket_commodities(available: List[str]) -> List[str]:
    defaults: List[str] = []
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


def dispatch_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Any = None,
    data: Optional[Dict[str, Any]] = None,
    files: Any = None,
) -> LocalResponse:
    parts, merged_params = _parse_path_and_params(path, params)
    method = (method or "GET").upper()

    if not parts:
        return _json_response(
            {
                "status": "ok",
                "services": [
                    {"id": "mfi-validator", "name": "MFI Dataset Validator", "endpoint": "/mfi-validator/validate-file"},
                    {"id": "price-validator", "name": "Price Data Validator", "endpoint": "/price-validator/validate-file"},
                    {"id": "market-monitor", "name": "Market Monitor Generator", "endpoint": "/market-monitor/generate"},
                    {"id": "mfi-drafter", "name": "MFI Report Generator", "endpoint": "/mfi-drafter/generate"},
                ],
            }
        )

    if parts == ["health"]:
        return _json_response({"status": "healthy"})

    service = parts[0]
    remainder = parts[1:]

    try:
        if service == "mfi-validator":
            return _dispatch_mfi_validator(
                method,
                remainder,
                json_body=json_body,
                data=data,
                files=files,
                params=merged_params,
            )
        if service == "price-validator":
            return _dispatch_price_validator(
                method,
                remainder,
                json_body=json_body,
                data=data,
                files=files,
                params=merged_params,
            )
        if service == "market-monitor":
            return _dispatch_market_monitor(
                method,
                remainder,
                json_body=json_body,
                data=data,
                files=files,
                params=merged_params,
            )
        if service == "mfi-drafter":
            return _dispatch_mfi_drafter(
                method,
                remainder,
                json_body=json_body,
                data=data,
                files=files,
                params=merged_params,
            )
    except LocalHTTPException as exc:
        return _error_response(exc.status_code, exc.detail)
    except Exception as exc:
        logger.exception("Local dispatcher error")
        return _error_response(500, str(exc))

    return _error_response(404, f"Unknown service: {service}")

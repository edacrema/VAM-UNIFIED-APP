from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Tuple

RunStatus = Literal["pending", "running", "completed", "failed"]

_UNSET = object()

logger = logging.getLogger(__name__)

@dataclass
class RunRecord:
    status: RunStatus = "pending"
    current_node: Optional[str] = None
    progress_pct: int = 0
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    traceback: Optional[str] = None
    result: Any = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

_RUNS: Dict[str, RunRecord] = {}
_LOCK = threading.Lock()

_BACKEND: Optional[str] = None
_GCP_CLIENTS_LOCK = threading.Lock()
_FIRESTORE_CLIENT: Any = None
_STORAGE_CLIENT: Any = None
_INLINE_RESULT_MAX_BYTES = 900_000

def _parse_gcs_uri(uri: str) -> Tuple[str, str]:
    uri = (uri or "").strip()
    if not uri.startswith("gs://"):
        raise ValueError("Invalid GCS URI")
    path = uri[5:]
    bucket, _, prefix = path.partition("/")
    return bucket, prefix.strip("/")

def _json_default(obj: Any) -> Any:
    if obj is None:
        return None

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
        import numpy as np  # type: ignore

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

def _has_gcp_deps() -> bool:
    try:
        import google.cloud.firestore  # type: ignore
        import google.cloud.storage  # type: ignore

        return True
    except Exception:
        return False

def _select_backend() -> str:
    global _BACKEND
    if _BACKEND is not None:
        return _BACKEND

    backend_env = (os.getenv("RUNS_BACKEND") or "").strip().lower()
    if backend_env in {"firestore_gcs", "firestore", "gcs"}:
        backend = "firestore_gcs"
    elif backend_env in {"memory", ""}:
        backend = "memory"
    else:
        backend = "memory"

    if backend_env == "" and (os.getenv("RUNS_GCS_URI") or "").strip():
        backend = "firestore_gcs"

    if backend == "firestore_gcs":
        runs_gcs_uri = (os.getenv("RUNS_GCS_URI") or "").strip()
        if not runs_gcs_uri:
            logger.warning("RUNS_GCS_URI is not set; falling back to in-memory run store")
            backend = "memory"
        elif not _has_gcp_deps():
            logger.warning("GCP client libraries are not available; falling back to in-memory run store")
            backend = "memory"

    _BACKEND = backend
    return _BACKEND

def _use_durable_store() -> bool:
    return _select_backend() == "firestore_gcs"

def _get_firestore_client() -> Any:
    global _FIRESTORE_CLIENT
    if _FIRESTORE_CLIENT is not None:
        return _FIRESTORE_CLIENT
    with _GCP_CLIENTS_LOCK:
        if _FIRESTORE_CLIENT is not None:
            return _FIRESTORE_CLIENT
        try:
            from google.cloud import firestore  # type: ignore

            _FIRESTORE_CLIENT = firestore.Client()
        except Exception:
            logger.exception("Failed to initialize Firestore client")
            _FIRESTORE_CLIENT = None
        return _FIRESTORE_CLIENT

def _get_storage_client() -> Any:
    global _STORAGE_CLIENT
    if _STORAGE_CLIENT is not None:
        return _STORAGE_CLIENT
    with _GCP_CLIENTS_LOCK:
        if _STORAGE_CLIENT is not None:
            return _STORAGE_CLIENT
        try:
            from google.cloud import storage  # type: ignore

            _STORAGE_CLIENT = storage.Client()
        except Exception:
            logger.exception("Failed to initialize GCS client")
            _STORAGE_CLIENT = None
        return _STORAGE_CLIENT

def _get_firestore_collection() -> str:
    return (os.getenv("RUNS_FIRESTORE_COLLECTION") or "async_runs").strip() or "async_runs"

def _get_runs_gcs_bucket_prefix() -> Tuple[str, str]:
    uri = (os.getenv("RUNS_GCS_URI") or "").strip()
    bucket, prefix = _parse_gcs_uri(uri)
    if not prefix:
        prefix = "runs"
    return bucket, prefix

def _firestore_doc_ref(run_id: str) -> Any:
    client = _get_firestore_client()
    if client is None:
        return None
    collection = _get_firestore_collection()
    return client.collection(collection).document(run_id)

def _serialize_result_json_bytes(result: Any) -> Optional[bytes]:
    try:
        return json.dumps(result, ensure_ascii=False, default=_json_default).encode("utf-8")
    except Exception:
        logger.exception("Failed to serialize run result")
        return None

def _try_store_result_inline(doc_ref: Any, run_id: str, result: Any) -> bool:
    payload = _serialize_result_json_bytes(result)
    if payload is None:
        return False
    if len(payload) > _INLINE_RESULT_MAX_BYTES:
        return False

    try:
        inline_obj = json.loads(payload.decode("utf-8"))
    except Exception:
        logger.exception("Failed to decode serialized run result")
        return False

    try:
        doc_ref.set(
            {
                "result": inline_obj,
                "result_gcs_uri": None,
                "updated_at": time.time(),
            },
            merge=True,
        )
        return True
    except Exception:
        logger.exception("Failed to store run result inline in Firestore")
        return False

def _upload_run_result(run_id: str, result: Any) -> Optional[str]:
    client = _get_storage_client()
    if client is None:
        return None

    try:
        bucket_name, prefix = _get_runs_gcs_bucket_prefix()
    except Exception:
        logger.exception("Failed to read RUNS_GCS_URI")
        return None

    object_name = "/".join([p for p in [prefix, run_id, "result.json"] if p])
    payload = _serialize_result_json_bytes(result)
    if payload is None:
        return None

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        blob.upload_from_string(payload, content_type="application/json")
        return f"gs://{bucket_name}/{object_name}"
    except Exception:
        logger.exception("Failed to upload run result to GCS")
        return None

def _download_json_from_gcs(uri: str) -> Any:
    client = _get_storage_client()
    if client is None:
        return None

    try:
        bucket_name, object_name = _parse_gcs_uri(uri)
    except Exception:
        logger.exception("Invalid GCS URI")
        return None

    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        data = blob.download_as_bytes()
        return json.loads(data.decode("utf-8"))
    except Exception:
        logger.exception("Failed to download run result from GCS")
        return None

def create_run(run_id: str) -> None:
    if not _use_durable_store():
        with _LOCK:
            _RUNS[run_id] = RunRecord(status="pending", current_node=None, progress_pct=0, metadata={})
        return

    doc_ref = _firestore_doc_ref(run_id)
    if doc_ref is None:
        with _LOCK:
            _RUNS[run_id] = RunRecord(status="pending", current_node=None, progress_pct=0, metadata={})
        return

    now = time.time()
    try:
        doc_ref.set(
            {
                "status": "pending",
                "current_node": None,
                "progress_pct": 0,
                "warnings": [],
                "metadata": {},
                "error": None,
                "traceback": None,
                "result_gcs_uri": None,
                "created_at": now,
                "updated_at": now,
            },
            merge=True,
        )
    except Exception:
        logger.exception("Failed to create run in Firestore; falling back to in-memory")
        global _BACKEND
        _BACKEND = "memory"
        with _LOCK:
            _RUNS[run_id] = RunRecord(status="pending", current_node=None, progress_pct=0, metadata={})

def get_run(run_id: str) -> Optional[RunRecord]:
    if not _use_durable_store():
        with _LOCK:
            rec = _RUNS.get(run_id)
            if rec is None:
                return None
            return RunRecord(
                status=rec.status,
                current_node=rec.current_node,
                progress_pct=rec.progress_pct,
                warnings=list(rec.warnings),
                metadata=dict(rec.metadata),
                error=rec.error,
                traceback=rec.traceback,
                result=rec.result,
                created_at=rec.created_at,
                updated_at=rec.updated_at,
            )

    doc_ref = _firestore_doc_ref(run_id)
    if doc_ref is None:
        return None

    try:
        snap = doc_ref.get()
    except Exception:
        logger.exception("Failed to read run from Firestore")
        return None

    if not getattr(snap, "exists", False):
        return None

    data = snap.to_dict() or {}
    status: RunStatus = data.get("status") or "pending"
    current_node = data.get("current_node")
    progress_pct = int(data.get("progress_pct") or 0)
    warnings = list(data.get("warnings") or [])
    metadata = dict(data.get("metadata") or {})
    error = data.get("error")
    tb = data.get("traceback")
    created_at = float(data.get("created_at") or time.time())
    updated_at = float(data.get("updated_at") or created_at)

    result: Any = None
    if status == "completed":
        uri = data.get("result_gcs_uri")
        if isinstance(uri, str) and uri.strip():
            result = _download_json_from_gcs(uri)
        else:
            result = data.get("result")

    return RunRecord(
        status=status,
        current_node=current_node,
        progress_pct=progress_pct,
        warnings=warnings,
        metadata=metadata,
        error=error,
        traceback=tb,
        result=result,
        created_at=created_at,
        updated_at=updated_at,
    )

def update_run(
    run_id: str,
    *,
    status: Any = _UNSET,
    current_node: Any = _UNSET,
    progress_pct: Any = _UNSET,
    warnings: Any = _UNSET,
    metadata: Any = _UNSET,
    error: Any = _UNSET,
    traceback: Any = _UNSET,
    result: Any = _UNSET,
) -> None:
    if not _use_durable_store():
        with _LOCK:
            rec = _RUNS.get(run_id)
            if rec is None:
                return

            if status is not _UNSET:
                rec.status = status

            if current_node is not _UNSET:
                rec.current_node = current_node

            if progress_pct is not _UNSET:
                rec.progress_pct = max(rec.progress_pct, int(progress_pct))

            if warnings is not _UNSET:
                if warnings:
                    rec.warnings.extend([w for w in warnings if w])
                else:
                    rec.warnings = []

            if metadata is not _UNSET:
                if metadata is None:
                    rec.metadata = {}
                elif isinstance(metadata, dict):
                    rec.metadata.update(metadata)
                else:
                    rec.metadata = {"value": metadata}

            if error is not _UNSET:
                rec.error = error

            if traceback is not _UNSET:
                rec.traceback = traceback

            if result is not _UNSET:
                rec.result = result

            rec.updated_at = time.time()
        return

    doc_ref = _firestore_doc_ref(run_id)
    if doc_ref is None:
        return

    try:
        snap = doc_ref.get()
    except Exception:
        logger.exception("Failed to read run before update")
        return

    if not getattr(snap, "exists", False):
        return

    existing = snap.to_dict() or {}
    updates: Dict[str, Any] = {}

    if status is not _UNSET:
        updates["status"] = status

    if current_node is not _UNSET:
        updates["current_node"] = current_node

    if progress_pct is not _UNSET:
        try:
            existing_progress = int(existing.get("progress_pct") or 0)
        except Exception:
            existing_progress = 0
        updates["progress_pct"] = max(existing_progress, int(progress_pct))

    if warnings is not _UNSET:
        if warnings:
            current_warnings = list(existing.get("warnings") or [])
            current_warnings.extend([w for w in warnings if w])
            updates["warnings"] = current_warnings
        else:
            updates["warnings"] = []

    if metadata is not _UNSET:
        if metadata is None:
            updates["metadata"] = {}
        elif isinstance(metadata, dict):
            current_meta = dict(existing.get("metadata") or {})
            current_meta.update(metadata)
            updates["metadata"] = current_meta
        else:
            updates["metadata"] = {"value": metadata}

    if error is not _UNSET:
        updates["error"] = error

    if traceback is not _UNSET:
        updates["traceback"] = traceback

    if result is not _UNSET:
        uri = _upload_run_result(run_id, result)
        if uri:
            updates["result_gcs_uri"] = uri
            updates["result"] = None
        else:
            payload = _serialize_result_json_bytes(result)
            if payload is not None and len(payload) <= _INLINE_RESULT_MAX_BYTES:
                try:
                    updates["result"] = json.loads(payload.decode("utf-8"))
                    updates["result_gcs_uri"] = None
                except Exception:
                    logger.exception("Failed to store inline result during update")

    updates["updated_at"] = time.time()

    try:
        doc_ref.set(updates, merge=True)
    except Exception:
        logger.exception("Failed to update run in Firestore")

def update_run_progress(run_id: str, *, current_node: str, progress_pct: int) -> None:
    update_run(run_id, current_node=current_node, progress_pct=progress_pct)

def set_run_completed(run_id: str, *, result: Any) -> None:
    if not _use_durable_store():
        update_run(
            run_id,
            status="completed",
            current_node="END",
            progress_pct=100,
            result=result,
            error=None,
            traceback=None,
        )
        return

    uri = _upload_run_result(run_id, result)

    if uri:
        update_run(
            run_id,
            status="completed",
            current_node="END",
            progress_pct=100,
            error=None,
            traceback=None,
            result=_UNSET,
            metadata=_UNSET,
            warnings=_UNSET,
        )
        doc_ref = _firestore_doc_ref(run_id)
        if doc_ref is None:
            return
        try:
            doc_ref.set({"result_gcs_uri": uri, "result": None, "updated_at": time.time()}, merge=True)
        except Exception:
            logger.exception("Failed to set result_gcs_uri")
        return

    doc_ref = _firestore_doc_ref(run_id)
    if doc_ref is not None:
        inline_ok = _try_store_result_inline(doc_ref, run_id, result)
        if inline_ok:
            update_run(
                run_id,
                status="completed",
                current_node="END",
                progress_pct=100,
                error=None,
                traceback=None,
                result=_UNSET,
                metadata=_UNSET,
                warnings=_UNSET,
            )
            return

    set_run_failed(
        run_id,
        error="Result persistence failed (GCS upload failed and inline Firestore fallback was unavailable/too large)",
        traceback=None,
        current_node="END",
    )

def set_run_failed(
    run_id: str,
    *,
    error: str,
    traceback: Optional[str] = None,
    current_node: Optional[str] = None,
) -> None:
    update_run(
        run_id,
        status="failed",
        current_node=current_node,
        progress_pct=0,
        error=error,
        traceback=traceback,
    )

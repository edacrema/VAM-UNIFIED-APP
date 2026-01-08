from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional


RunStatus = Literal["pending", "running", "completed", "failed"]


_UNSET = object()


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


def create_run(run_id: str) -> None:
    with _LOCK:
        _RUNS[run_id] = RunRecord(status="pending", current_node=None, progress_pct=0, metadata={})


def get_run(run_id: str) -> Optional[RunRecord]:
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


def update_run_progress(run_id: str, *, current_node: str, progress_pct: int) -> None:
    update_run(run_id, current_node=current_node, progress_pct=progress_pct)


def set_run_completed(run_id: str, *, result: Any) -> None:
    update_run(
        run_id,
        status="completed",
        current_node="END",
        progress_pct=100,
        result=result,
        error=None,
        traceback=None,
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

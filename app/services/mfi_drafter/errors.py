"""Typed fail-closed errors for the live MFI generation pipeline."""

from __future__ import annotations

from typing import Any, Dict, Optional


class MFIGenerationBlockedError(RuntimeError):
    """Stop a live run before any report payload can be published."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        stage: str,
        status_code: int,
        artifact_type: Optional[str] = None,
        artifact_id: Optional[str] = None,
        field_name: Optional[str] = None,
        task_id: Optional[str] = None,
        batch_id: Optional[str] = None,
        call_id: Optional[str] = None,
        attempt: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.stage = stage
        self.status_code = status_code
        self.artifact_type = artifact_type
        self.artifact_id = artifact_id
        self.field_name = field_name
        self.task_id = task_id
        self.batch_id = batch_id
        self.call_id = call_id
        self.attempt = attempt

    @property
    def node(self) -> str:
        return self.stage

    def to_public_dict(self) -> Dict[str, Any]:
        return {
            key: value
            for key, value in {
                "code": self.code,
                "message": self.message,
                "stage": self.stage,
                "artifact_type": self.artifact_type,
                "artifact_id": self.artifact_id,
                "field_name": self.field_name,
                "task_id": self.task_id,
                "batch_id": self.batch_id,
                "call_id": self.call_id,
                "attempt": self.attempt,
            }.items()
            if value is not None
        }


def claim_identity_blocked(
    message: str,
    *,
    stage: str,
    artifact_type: Optional[str] = None,
    artifact_id: Optional[str] = None,
) -> MFIGenerationBlockedError:
    return MFIGenerationBlockedError(
        "mfi_claim_identity_contract_failed",
        message,
        stage=stage,
        status_code=500,
        artifact_type=artifact_type,
        artifact_id=artifact_id,
    )

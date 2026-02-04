"""Configurazione centralizzata LLM (Singleton)."""
import os
from dotenv import load_dotenv
from langchain_google_vertexai import ChatVertexAI
from langchain_core.language_models.chat_models import BaseChatModel

load_dotenv()

_model_instance: BaseChatModel | None = None


def _get_vertex_project_id() -> str:
    project_id = (os.getenv("VERTEX_PROJECT_ID") or "").strip()
    if project_id:
        return project_id

    for key in ("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT"):
        candidate = (os.getenv(key) or "").strip()
        if candidate:
            return candidate

    try:
        import google.auth  # type: ignore

        _, inferred_project_id = google.auth.default()
        if inferred_project_id:
            return str(inferred_project_id).strip()
    except Exception:
        pass

    raise RuntimeError(
        "Missing Vertex project id. Set VERTEX_PROJECT_ID (recommended) or ensure GOOGLE_CLOUD_PROJECT is set."
    )


def get_model() -> BaseChatModel:
    """Restituisce istanza singleton del modello."""
    global _model_instance
    if _model_instance is None:

        max_output_tokens_raw = (os.getenv("LLM_MAX_OUTPUT_TOKENS") or "").strip()
        max_output_tokens: int | None
        if max_output_tokens_raw:
            try:
                max_output_tokens = int(max_output_tokens_raw)
            except ValueError:
                max_output_tokens = None
        else:
            max_output_tokens = None

        _model_instance = ChatVertexAI(
            model_name=os.getenv("LLM_MODEL", "gemini-2.5-pro"),
            project=_get_vertex_project_id(),
            location=(os.getenv("VERTEX_LOCATION") or "us-central1").strip(),
            temperature=0,
            timeout=60,
            max_retries=2,
            max_output_tokens=max_output_tokens,
        )
    return _model_instance


def configure_model(provider: str = "google", model_name: str | None = None, api_key: str | None = None):
    """Riconfigura il modello (utile per test o switch provider)."""
    global _model_instance
    max_output_tokens_raw = (os.getenv("LLM_MAX_OUTPUT_TOKENS") or "").strip()

    max_output_tokens: int | None
    if max_output_tokens_raw:
        try:
            max_output_tokens = int(max_output_tokens_raw)
        except ValueError:
            max_output_tokens = None
    else:
        max_output_tokens = None

    _model_instance = ChatVertexAI(
        model_name=model_name or "gemini-2.5-flash",
        project=_get_vertex_project_id(),
        location=(os.getenv("VERTEX_LOCATION") or "us-central1").strip(),
        temperature=0,
        timeout=60,
        max_retries=2,
        max_output_tokens=max_output_tokens,
    )
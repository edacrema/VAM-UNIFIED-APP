"""Configurazione centralizzata LLM (Singleton)."""
import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.language_models.chat_models import BaseChatModel

load_dotenv()

_model_instance: BaseChatModel | None = None


def get_model() -> BaseChatModel:
    """Restituisce istanza singleton del modello."""
    global _model_instance
    if _model_instance is None:
        _model_instance = ChatGoogleGenerativeAI(
            model=os.getenv("LLM_MODEL", "gemini-2.5-flash"),
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0,
            timeout=60,
            max_retries=2,
        )
    return _model_instance


def configure_model(provider: str = "google", model_name: str | None = None, api_key: str | None = None):
    """Riconfigura il modello (utile per test o switch provider)."""
    global _model_instance
    _model_instance = ChatGoogleGenerativeAI(
        model=model_name or "gemini-2.5-flash",
        google_api_key=api_key or os.getenv("GOOGLE_API_KEY"),
        temperature=0,
        timeout=60,
        max_retries=2,
    )
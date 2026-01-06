"""Shared Module - Componenti condivisi tra i servizi."""
from .llm import get_model, configure_model
from .retrievers import GDELTRetriever, ReliefWebRetriever

__all__ = [
    "get_model",
    "configure_model",
    "GDELTRetriever",
    "ReliefWebRetriever"
]
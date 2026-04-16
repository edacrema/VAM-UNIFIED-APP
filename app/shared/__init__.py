"""Shared Module - Componenti condivisi tra i servizi."""

__all__ = [
    "get_model",
    "configure_model",
    "SeeristRetriever",
    "ReliefWebRetriever",
]


def __getattr__(name: str):
    if name in {"get_model", "configure_model"}:
        from .llm import configure_model, get_model

        return {"get_model": get_model, "configure_model": configure_model}[name]
    if name in {"SeeristRetriever", "ReliefWebRetriever"}:
        from .retrievers import ReliefWebRetriever, SeeristRetriever

        return {
            "SeeristRetriever": SeeristRetriever,
            "ReliefWebRetriever": ReliefWebRetriever,
        }[name]
    raise AttributeError(name)

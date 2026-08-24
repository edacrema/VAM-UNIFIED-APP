"""Deterministic narrative fixtures for offline tests and release regression only.

Nothing in the live graph, routers, dispatcher, report construction, or UI may import
this module.  These builders are intentionally non-publishable structural fixtures;
they are not a recovery path for a failed LLM generation.
"""

from .narrative import (
    fallback_dimension_narrative as build_offline_dimension_fixture,
    fallback_executive_narrative as build_offline_executive_fixture,
    fallback_market_narrative as build_offline_market_fixture,
)

__all__ = [
    "build_offline_dimension_fixture",
    "build_offline_executive_fixture",
    "build_offline_market_fixture",
]

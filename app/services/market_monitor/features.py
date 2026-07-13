"""Operational feature controls for the Market Monitor service."""
from __future__ import annotations

import os
from typing import Any, Mapping, Optional


SECOND_BASKET_FEATURE_ENV = "MARKET_MONITOR_SECOND_BASKET_ENABLED"
SECOND_BASKET_DISABLED_CODE = "second_basket_disabled"
SECOND_BASKET_DISABLED_MESSAGE = (
    "Second-basket configuration and selection are temporarily disabled. "
    "Submit a primary-only request or contact the service operator."
)
_FALSE_VALUES = {"0", "false", "no", "off"}


class SecondBasketFeatureDisabled(RuntimeError):
    """Raised when a request explicitly uses a disabled secondary basket."""

    status_code = 503
    code = SECOND_BASKET_DISABLED_CODE

    def __init__(self, message: str = SECOND_BASKET_DISABLED_MESSAGE):
        super().__init__(message)

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": str(self)}


def market_monitor_second_basket_enabled(
    environ: Optional[Mapping[str, str]] = None,
) -> bool:
    """Return the default-on operational state of the second-basket feature."""

    source = os.environ if environ is None else environ
    raw = source.get(SECOND_BASKET_FEATURE_ENV)
    if raw is None or not str(raw).strip():
        return True
    return str(raw).strip().lower() not in _FALSE_VALUES


def normalize_secondary_request(
    payload: Any,
    *,
    enabled: Optional[bool] = None,
) -> tuple[bool, Optional[str]]:
    """Apply the feature gate without silently changing explicit real-data use.

    Generation normally defaults secondary inclusion to true. When the feature
    is disabled, a legacy request that omits all secondary fields is normalized
    to primary-only. Explicit inclusion is rejected, while an ID remains ignored
    when inclusion is explicitly false. Mock runs never resolve persisted baskets.
    """

    include_secondary = bool(_payload_get(payload, "include_secondary_basket", False))
    secondary_id = _optional_identifier(_payload_get(payload, "secondary_basket_version_id"))
    effective_enabled = market_monitor_second_basket_enabled() if enabled is None else bool(enabled)
    if effective_enabled:
        return include_secondary, secondary_id

    if bool(_payload_get(payload, "use_mock_data", False)):
        return False, None
    if not include_secondary:
        return False, secondary_id

    explicit_include = _payload_field_is_explicit(payload, "include_secondary_basket")
    if explicit_include or secondary_id is not None:
        raise SecondBasketFeatureDisabled()
    return False, None


def second_basket_feature_metadata() -> dict[str, bool]:
    """Return the auditable feature state captured at request submission."""

    return {"second_food_basket_enabled": market_monitor_second_basket_enabled()}


def _payload_get(payload: Any, key: str, default: Any = None) -> Any:
    if isinstance(payload, Mapping):
        return payload.get(key, default)
    return getattr(payload, key, default)


def _payload_field_is_explicit(payload: Any, key: str) -> bool:
    if isinstance(payload, Mapping):
        return key in payload
    fields_set = getattr(payload, "model_fields_set", None)
    if fields_set is None:
        fields_set = getattr(payload, "__fields_set__", set())
    return key in set(fields_set or set())


def _optional_identifier(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    normalized = str(value).strip()
    return normalized or None

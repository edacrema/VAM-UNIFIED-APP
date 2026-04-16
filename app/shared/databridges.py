"""Lightweight requests-based connector for the WFP Databridges API."""
from __future__ import annotations

import logging
import os
import time
from collections.abc import Sequence
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.wfp.org/vam-data-bridges/7.0.0"
DEFAULT_TOKEN_URL = "https://api.wfp.org/token"


class DataBridgesAuth:
    """Client-credentials token helper with scope-aware caching."""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        token_url: str = DEFAULT_TOKEN_URL,
        timeout: int = 60,
        max_retries: int = 3,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not api_key or not api_secret:
            raise ValueError(
                "Databridges credentials are not configured. Set DATA_BRIDGES_KEY "
                "and DATA_BRIDGES_SECRET."
            )

        self.api_key = api_key
        self.api_secret = api_secret
        self.token_url = token_url
        self.timeout = timeout
        self.max_retries = max(1, max_retries)
        self.session = session or requests.Session()
        self._token_cache: dict[tuple[str, ...], dict[str, object]] = {}

    def get_token(self, scopes: Sequence[str]) -> str:
        scope_key = tuple(sorted(scope for scope in scopes if scope))
        now = time.time()
        cached = self._token_cache.get(scope_key)
        if cached and now < float(cached["expires_at"]):
            return str(cached["access_token"])

        payload = {
            "grant_type": "client_credentials",
            "scope": " ".join(scope_key),
        }

        response: Optional[requests.Response] = None
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(
                    self.token_url,
                    data=payload,
                    auth=(self.api_key, self.api_secret),
                    timeout=self.timeout,
                )
                response.raise_for_status()
                break
            except requests.exceptions.Timeout as exc:
                last_error = TimeoutError(
                    "Timed out while requesting a Databridges access token "
                    f"for scopes {scope_key} after {self.timeout}s."
                )
            except requests.exceptions.RequestException as exc:
                last_error = RuntimeError(
                    "Failed to request a Databridges access token "
                    f"for scopes {scope_key}: {exc}"
                )

            if attempt < self.max_retries:
                time.sleep(min(attempt, 5))

        if response is None:
            raise last_error or RuntimeError("Unknown Databridges token error.")

        token_payload = response.json()
        access_token = token_payload.get("access_token")
        if not access_token:
            raise RuntimeError("Databridges token response did not include access_token.")

        expires_in = int(token_payload.get("expires_in", 3600))
        granted_scopes = set(str(token_payload.get("scope", "")).split())
        if scope_key and granted_scopes and not set(scope_key).issubset(granted_scopes):
            raise ValueError(f"Could not acquire requested Databridges scopes: {scope_key}")

        self._token_cache[scope_key] = {
            "access_token": access_token,
            "expires_at": now + max(expires_in - 60, 1),
        }
        return str(access_token)


class DataBridgesClient:
    COMMODITIES_SCOPE = "vamdatabridges_commodities-list_get"
    MARKETS_SCOPE = "vamdatabridges_markets-list_get"
    MONTHLY_PRICES_SCOPE = "vamdatabridges_marketprices-pricemonthly_get"
    MFI_SURVEYS_SCOPE = "vamdatabridges_mfi-surveys_get"
    MFI_PROCESSED_SCOPE = "vamdatabridges_mfi-surveys-processeddata_get"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        token_url: str = DEFAULT_TOKEN_URL,
        timeout: int = 60,
        max_retries: int = 3,
        env: Optional[str] = None,
        session: Optional[requests.Session] = None,
        auth_provider: Optional[DataBridgesAuth] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.env = (env or "").strip() or None
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "UNIFIED_APP/DatabridgesConnector"})
        self.auth_provider = auth_provider or DataBridgesAuth(
            api_key,
            api_secret,
            token_url=token_url,
            timeout=timeout,
            max_retries=max_retries,
            session=self.session,
        )

    def list_commodities(
        self,
        country_code: str,
        commodity_name: Optional[str] = None,
        commodity_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"countryCode": country_code, "format": "json"}
        if commodity_name:
            params["commodityName"] = commodity_name
        if commodity_id is not None:
            params["commodityID"] = commodity_id
        return self._paginate(
            "/Commodities/List",
            params=params,
            scopes=[self.COMMODITIES_SCOPE],
        )

    def list_markets(self, country_code: str) -> list[dict[str, Any]]:
        return self._paginate(
            "/Markets/List",
            params={"countryCode": country_code, "format": "json"},
            scopes=[self.MARKETS_SCOPE],
        )

    def list_monthly_prices(
        self,
        country_code: str,
        commodity_id: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        latest_value_only: bool = False,
        price_flag: Optional[str] = None,
        price_type_name: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "countryCode": country_code,
            "latestValueOnly": _wire_bool(latest_value_only),
            "format": "json",
        }
        if commodity_id is not None:
            params["commodityID"] = commodity_id
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        if price_flag:
            params["priceFlag"] = price_flag
        if price_type_name:
            params["priceTypeName"] = price_type_name
        return self._paginate(
            "/MarketPrices/PriceMonthly",
            params=params,
            scopes=[self.MONTHLY_PRICES_SCOPE],
        )

    def list_mfi_surveys(
        self,
        adm0_code: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if adm0_code is not None:
            params["adm0Code"] = adm0_code
        if start_date:
            params["startDate"] = start_date
        if end_date:
            params["endDate"] = end_date
        return self._paginate(
            "/MFI/Surveys",
            params=params,
            scopes=[self.MFI_SURVEYS_SCOPE],
        )

    def list_mfi_processed_data(
        self,
        survey_id: int,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        params = {
            "surveyID": survey_id,
            "pageSize": page_size,
            "format": "json",
        }
        return self._paginate(
            "/MFI/Surveys/ProcessedData",
            params=params,
            scopes=[self.MFI_PROCESSED_SCOPE],
            page_size=page_size,
        )

    def _paginate(
        self,
        path: str,
        *,
        params: dict[str, Any],
        scopes: Sequence[str],
        page_size: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 1
        first_total_items: Optional[int] = None

        while True:
            payload = self._get(path, params={**params, "page": page}, scopes=scopes)
            page_items = _payload_items(payload)
            if not page_items:
                break

            items.extend(page_items)
            total_items = _payload_total(payload)
            if first_total_items is None and total_items is not None:
                first_total_items = total_items
            if first_total_items is not None and len(items) >= first_total_items:
                break
            if page_size is not None and len(page_items) < page_size:
                break

            page += 1
            if page > 1000:
                raise RuntimeError(f"Databridges pagination runaway detected for {path}.")

        return items

    def _get(
        self,
        path: str,
        *,
        params: dict[str, Any],
        scopes: Sequence[str],
    ) -> Any:
        request_params = {key: value for key, value in params.items() if value is not None}
        if self.env:
            request_params["env"] = self.env

        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(
                url,
                params=request_params,
                headers=self._auth_headers(scopes),
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise TimeoutError(
                f"Timed out while requesting Databridges {path} page "
                f"{request_params.get('page')} after {self.timeout}s."
            ) from exc
        except requests.exceptions.HTTPError as exc:
            detail = getattr(response, "text", "")[:500]
            raise RuntimeError(
                f"Databridges request failed for {path} page "
                f"{request_params.get('page')} with HTTP {response.status_code}: {detail}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise RuntimeError(
                f"Databridges request failed for {path} page "
                f"{request_params.get('page')}: {exc}"
            ) from exc

        try:
            return response.json()
        except ValueError as exc:
            raise RuntimeError(f"Databridges {path} returned non-JSON data.") from exc

    def _auth_headers(self, scopes: Sequence[str]) -> dict[str, str]:
        access_token = self.auth_provider.get_token(scopes)
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }


def get_databridges_client() -> DataBridgesClient:
    global _CLIENT
    if _CLIENT is None:
        timeout = int(os.getenv("DATA_BRIDGES_TIMEOUT", "60"))
        max_retries = int(os.getenv("DATA_BRIDGES_MAX_RETRIES", "3"))
        _CLIENT = DataBridgesClient(
            os.getenv("DATA_BRIDGES_KEY", ""),
            os.getenv("DATA_BRIDGES_SECRET", ""),
            base_url=os.getenv("DATA_BRIDGES_API_BASE_URL", DEFAULT_BASE_URL),
            token_url=os.getenv("DATA_BRIDGES_TOKEN_URL", DEFAULT_TOKEN_URL),
            timeout=timeout,
            max_retries=max_retries,
            env=os.getenv("DATA_BRIDGES_ENV"),
        )
    return _CLIENT


def reset_databridges_client_for_tests() -> None:
    global _CLIENT
    _CLIENT = None


def _wire_bool(value: bool) -> str:
    return "true" if value else "false"


def _payload_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    raw = payload.get("items")
    if raw is None:
        raw = payload.get("Items")
    if raw is None:
        raw = payload.get("data")
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, dict)]


def _payload_total(payload: Any) -> Optional[int]:
    if not isinstance(payload, dict):
        return None
    for key in ("totalItems", "total_items", "TotalItems", "total"):
        value = payload.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


_CLIENT: Optional[DataBridgesClient] = None

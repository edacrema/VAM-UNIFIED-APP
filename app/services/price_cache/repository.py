from __future__ import annotations

from datetime import date
from typing import List, Optional, Protocol, Sequence

from .schemas import (
    CacheStatus,
    CountryAvailability,
    CountryMetadata,
    CountryRecord,
    MonthlyPriceRecord,
)


class PriceCacheRepository(Protocol):
    def get_cache_status(self) -> CacheStatus:
        ...

    def get_active_version_id(self) -> Optional[str]:
        ...

    def get_active_version_id_for_country(self, country_iso3: str) -> Optional[str]:
        ...

    def list_countries(self) -> List[CountryRecord]:
        ...

    def get_country_metadata(self, country_iso3: str) -> Optional[CountryMetadata]:
        ...

    def get_country_availability(self, country_iso3: str) -> Optional[CountryAvailability]:
        ...

    def get_price_window(
        self,
        country_iso3: str,
        start_date: date | str,
        end_date: date | str,
        *,
        commodity_ids: Optional[Sequence[int]] = None,
        market_ids: Optional[Sequence[int]] = None,
        admin1_names: Optional[Sequence[str]] = None,
    ) -> List[MonthlyPriceRecord]:
        ...

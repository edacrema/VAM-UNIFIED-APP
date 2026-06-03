from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class CacheStatus:
    has_active_cache: bool
    active_version_id: Optional[str] = None
    status: Optional[str] = None
    activated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    rows_prices: int = 0
    rows_commodities: int = 0
    rows_markets: int = 0
    rows_countries: int = 0
    validation_summary: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


@dataclass(frozen=True)
class CountryRecord:
    cache_version_id: str
    country_iso3: str
    country_name: str
    currency_code: Optional[str] = None
    currency_name: Optional[str] = None
    latest_price_date: Optional[date] = None


@dataclass(frozen=True)
class CommodityRecord:
    cache_version_id: str
    country_iso3: str
    commodity_id: int
    commodity_name: str
    commodity_unit_id: Optional[int] = None
    commodity_unit_name: Optional[str] = None
    category_name: Optional[str] = None
    active: bool = True


@dataclass(frozen=True)
class UnitRecord:
    cache_version_id: str
    commodity_unit_id: int
    commodity_unit_name: str
    conversion_to_kg_l: Optional[float] = None
    active: bool = True


@dataclass(frozen=True)
class MarketRecord:
    cache_version_id: str
    country_iso3: str
    market_id: int
    market_name: str
    admin1_name: Optional[str] = None
    admin2_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    active: bool = True


@dataclass(frozen=True)
class CurrencyRecord:
    cache_version_id: str
    currency_id: int
    currency_code: Optional[str]
    currency_name: str


@dataclass(frozen=True)
class MonthlyPriceRecord:
    cache_version_id: str
    country_iso3: str
    commodity_id: int
    market_id: int
    price_date: date
    price: float
    currency_id: Optional[int] = None
    currency_code: Optional[str] = None
    currency_name: Optional[str] = None
    commodity_unit_id: Optional[int] = None
    commodity_unit_name: Optional[str] = None
    price_type_id: Optional[int] = None
    price_type_name: str = ""
    price_flag: str = ""
    original_frequency: Optional[str] = None
    observations: Optional[int] = None
    source_payload_hash: Optional[str] = None
    admin1_name: Optional[str] = None
    admin2_name: Optional[str] = None
    market_name: Optional[str] = None
    commodity_name: Optional[str] = None
    data_source: Optional[str] = None


@dataclass(frozen=True)
class CountryMetadata:
    country: CountryRecord
    commodities: List[CommodityRecord] = field(default_factory=list)
    units: List[UnitRecord] = field(default_factory=list)
    markets: List[MarketRecord] = field(default_factory=list)
    currencies: List[CurrencyRecord] = field(default_factory=list)

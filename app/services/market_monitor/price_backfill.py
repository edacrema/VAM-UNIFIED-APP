from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd


@dataclass(frozen=True)
class PriceRequirement:
    commodity_id: int
    commodity_name: str
    month: str
    hard: bool = False
    is_basket: bool = False
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "commodity_id": self.commodity_id,
            "commodity_name": self.commodity_name,
            "month": self.month,
            "hard": self.hard,
            "is_basket": self.is_basket,
            "reason": self.reason,
        }


@dataclass
class CommodityGapStatus:
    commodity_id: int
    commodity_name: str
    is_basket: bool
    missing_reference_month: bool = False
    missing_soft_months: list[str] = field(default_factory=list)
    source_status: str = "not_checked"
    latest_source_month: Optional[str] = None
    backfill_attempted: bool = False
    fetched_rows: int = 0
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "commodity_id": self.commodity_id,
            "commodity_name": self.commodity_name,
            "is_basket": self.is_basket,
            "missing_reference_month": self.missing_reference_month,
            "missing_soft_months": list(self.missing_soft_months),
            "source_status": self.source_status,
            "latest_source_month": self.latest_source_month,
            "backfill_attempted": self.backfill_attempted,
            "fetched_rows": self.fetched_rows,
            "error": self.error,
        }

    def hard_stop_reason(self, iso3: str, reference_month: str) -> str:
        prefix = f"{self.commodity_name} (commodity_id={self.commodity_id})"
        if self.source_status == "source_error":
            return f"{prefix}: {self.error or 'DataBridges could not be reached.'}"
        if self.source_status == "no_source_data":
            return (
                f"{prefix}: DataBridges returns no monthly price data for this commodity in {iso3}; "
                "this usually means the basket uses an unsupported country commodity id"
            )
        latest = self.latest_source_month or "an earlier month"
        return (
            f"{prefix}: DataBridges has monthly prices for {iso3} through {latest}, "
            f"but no usable price for {reference_month} yet; the reference month may not be published"
        )


@dataclass
class ReportPriceGapReport:
    country: str
    iso3: str
    time_period: str
    reference_month: str
    window_start: str
    window_end: str
    requirements: list[PriceRequirement] = field(default_factory=list)
    commodity_statuses: list[CommodityGapStatus] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    backfill_attempted: bool = False
    backfill_rows_fetched: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "country": self.country,
            "iso3": self.iso3,
            "time_period": self.time_period,
            "reference_month": self.reference_month,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "requirements": [item.to_dict() for item in self.requirements],
            "commodity_statuses": [item.to_dict() for item in self.commodity_statuses],
            "warnings": list(self.warnings),
            "backfill_attempted": self.backfill_attempted,
            "backfill_rows_fetched": self.backfill_rows_fetched,
            "hard_missing": [item.to_dict() for item in self.hard_missing_statuses()],
        }

    def hard_missing_statuses(self) -> list[CommodityGapStatus]:
        return [
            item
            for item in self.commodity_statuses
            if item.is_basket and item.missing_reference_month
        ]

    def soft_gap_statuses(self) -> list[CommodityGapStatus]:
        return [
            item
            for item in self.commodity_statuses
            if item.missing_soft_months
        ]

    def warning_messages(self) -> list[str]:
        messages = list(self.warnings)
        soft = self.soft_gap_statuses()
        if soft:
            parts = []
            for item in soft:
                role = "Basket commodity" if item.is_basket else "Non-basket selected commodity"
                months = ", ".join(item.missing_soft_months)
                parts.append(f"{role} {item.commodity_name} is missing {months}")
            messages.append(
                f"Price data warning: {self.country} {self.reference_month} has complete "
                "reference-month basket prices, but trailing-window data is incomplete. "
                + "; ".join(parts)
                + "; MoM, YoY, or trend values may show N/A."
            )
        return _dedupe_text(messages)


@dataclass
class ReportPriceDataResult:
    df_national: pd.DataFrame
    df_regional: pd.DataFrame
    raw_rows: pd.DataFrame
    warnings: list[str]
    gap_report: ReportPriceGapReport
    cache_metadata: dict[str, Any]
    exchange_rate_data: Optional[dict[str, Any]] = None
    df_history_national: pd.DataFrame = field(default_factory=pd.DataFrame)


class PriceDataGateError(RuntimeError):
    status_code = 409

    def __init__(self, gap_report: ReportPriceGapReport, *, status_code: Optional[int] = None) -> None:
        self.gap_report = gap_report
        if status_code is not None:
            self.status_code = status_code
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        hard_missing = self.gap_report.hard_missing_statuses()
        if hard_missing:
            reasons = "; ".join(
                item.hard_stop_reason(self.gap_report.iso3, self.gap_report.reference_month)
                for item in hard_missing
            )
            if any(item.source_status == "source_error" for item in hard_missing):
                return (
                    f"Cannot generate Price Bulletin for {self.gap_report.country} ({self.gap_report.iso3}) "
                    f"{self.gap_report.reference_month} because targeted DataBridges backfill could not "
                    "verify or obtain missing reference-month basket prices after adapter retries: "
                    f"{reasons}. No report was generated."
                )
            return (
                f"Cannot generate Price Bulletin for {self.gap_report.country} ({self.gap_report.iso3}) "
                f"{self.gap_report.reference_month} because the reference-month food basket is incomplete "
                "after targeted DataBridges backfill. Missing required basket commodity prices for "
                f"{self.gap_report.reference_month}: {reasons}. No report was generated."
            )
        return (
            f"Cannot generate Price Bulletin for {self.gap_report.country} ({self.gap_report.iso3}) "
            f"{self.gap_report.reference_month} because required price data could not be verified. "
            "No report was generated."
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": str(self),
            "status_code": self.status_code,
            "price_gap_report": self.gap_report.to_dict(),
        }


class BasketReferenceMonthMissing(PriceDataGateError):
    status_code = 409


class ReportPriceBackfillUnavailable(PriceDataGateError):
    status_code = 503


def _dedupe_text(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out

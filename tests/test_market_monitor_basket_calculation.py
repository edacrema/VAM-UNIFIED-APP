from types import SimpleNamespace

import pandas as pd
import pytest

from app.services.market_monitor import data_loader
from app.services.market_monitor.basket_calculation import (
    BasketCalculationSpec,
    BasketScopeValidationError,
    apply_primary_food_basket_aliases,
    calculate_basket_series,
    calculate_basket_statistics,
    target_coverage_gaps,
)


def _spec(role, *, quantity=1, scope="national", regions=None, items=None):
    return BasketCalculationSpec.from_snapshot(
        {
            "basket_role": role,
            "basket_version_id": f"{role}-v1",
            "basket_name": "MEB" if role == "primary" else "Pastoral basket",
            "short_description": "Test basket",
            "scope_type": scope,
            "regions": regions or [],
            "items": items
            or [
                {
                    "commodity_id": 1,
                    "commodity_name": "Maize",
                    "databridges_unit_id": 100,
                    "databridges_unit": "kg",
                    "weight_quantity": quantity,
                }
            ],
        }
    )


def _price_frame(months=("2024-02", "2025-01", "2025-02")):
    rows = []
    for month in months:
        for region, maize, beans in (("Region A", 10, 5), ("Region B", 20, 7)):
            rows.extend(
                [
                    {
                        "Commodity ID": 1,
                        "Commodity": "Maize",
                        "Price Date": pd.Timestamp(f"{month}-01"),
                        "Price": maize,
                        "Admin 1": region,
                        "Unit ID": 100,
                        "Unit": "kg",
                    },
                    {
                        "Commodity ID": 2,
                        "Commodity": "Beans",
                        "Price Date": pd.Timestamp(f"{month}-01"),
                        "Price": beans,
                        "Admin 1": region,
                        "Unit ID": 100,
                        "Unit": "kg",
                    },
                    {
                        "Commodity ID": 1,
                        "Commodity": "Maize",
                        "Price Date": pd.Timestamp(f"{month}-01"),
                        "Price": 999,
                        "Admin 1": region,
                        "Unit ID": 999,
                        "Unit": "bag",
                    },
                ]
            )
    return pd.DataFrame(rows)


def test_two_baskets_share_prices_but_keep_quantities_scopes_and_units_independent():
    frame = _price_frame()
    primary = _spec("primary", quantity=2)
    secondary = _spec(
        "secondary",
        scope="selected_regions",
        regions=["Region A", "Region B"],
        items=[
            {
                "commodity_id": 1,
                "commodity_name": "Maize",
                "databridges_unit_id": 100,
                "databridges_unit": "kg",
                "weight_quantity": 1,
            },
            {
                "commodity_id": 2,
                "commodity_name": "Beans",
                "databridges_unit_id": 100,
                "databridges_unit": "kg",
                "weight_quantity": 3,
            },
        ],
    )
    months = pd.DatetimeIndex([pd.Timestamp("2024-02-01"), pd.Timestamp("2025-01-01"), pd.Timestamp("2025-02-01")])

    result = calculate_basket_series(
        frame,
        [primary, secondary],
        full_date_index=months,
        run_regions=["Region A", "Region B"],
        available_regions=["Region A", "Region B"],
    )
    stats = calculate_basket_statistics(frame, [primary, secondary], result, target_date=pd.Timestamp("2025-02-01"))

    assert result.national["BasketRole"].unique().tolist() == ["primary"]
    assert stats["primary"]["current_cost"] == 30.0
    assert stats["primary"]["applicable_regions"] == ["Region A", "Region B"]
    assert stats["primary"]["component_contributions"][0]["by_region"] == [
        {"region": "Region A", "mean_price": 10.0, "absolute_contribution": 20.0},
        {"region": "Region B", "mean_price": 20.0, "absolute_contribution": 40.0},
    ]
    assert stats["secondary"]["regional_statistics"]["Region A"]["current_cost"] == 25.0
    assert stats["secondary"]["regional_statistics"]["Region B"]["current_cost"] == 41.0
    assert stats["secondary"]["current_cost"] == 33.0
    assert stats["secondary"]["scope_label"] == "Average across selected regions"
    assert stats["secondary"]["component_contributions"][0]["absolute_contribution"] == 15.0
    assert all(item["current_complete"] for item in stats["secondary"]["regional_statistics"].values())


def test_incomplete_components_never_create_partial_cost_or_comparison():
    frame = _price_frame()
    frame = frame[
        ~(
            (frame["Commodity ID"] == 2)
            & (frame["Admin 1"] == "Region B")
            & (frame["Price Date"] == pd.Timestamp("2025-02-01"))
        )
    ]
    secondary = _spec(
        "secondary",
        scope="selected_regions",
        regions=["Region A", "Region B"],
        items=[
            {"commodity_id": 1, "commodity_name": "Maize", "databridges_unit_id": 100, "weight_quantity": 1},
            {"commodity_id": 2, "commodity_name": "Beans", "databridges_unit_id": 100, "weight_quantity": 1},
        ],
    )
    months = pd.DatetimeIndex([pd.Timestamp("2024-02-01"), pd.Timestamp("2025-01-01"), pd.Timestamp("2025-02-01")])

    result = calculate_basket_series(
        frame,
        [secondary],
        full_date_index=months,
        run_regions=["Region A", "Region B"],
        available_regions=["Region A", "Region B"],
    )
    stats = calculate_basket_statistics(frame, [secondary], result, target_date=pd.Timestamp("2025-02-01"))["secondary"]
    target = result.summaries["secondary"].iloc[-1]

    assert target["Complete"] is False or not bool(target["Complete"])
    assert pd.isna(target["Cost"])
    assert stats["current_cost"] is None
    assert stats["mom_change_pct"] is None
    assert stats["missing_component_names"] == ["Beans"]
    assert target_coverage_gaps([secondary], result, target_date=pd.Timestamp("2025-02-01")) == [
        {
            "basket_role": "secondary",
            "basket_version_id": "secondary-v1",
            "basket_name": "Pastoral basket",
            "scope_type": "selected_regions",
            "scope_label": "Region B",
            "region": "Region B",
            "missing_component_names": ["Beans"],
        }
    ]


def test_primary_foodbasket_alias_is_strict_for_regional_breakdowns():
    frame = _price_frame(months=("2025-02",))
    frame = frame[~((frame["Commodity ID"] == 1) & (frame["Admin 1"] == "Region B"))]
    primary = _spec("primary", quantity=2)
    result = calculate_basket_series(
        frame,
        [primary],
        full_date_index=pd.DatetimeIndex([pd.Timestamp("2025-02-01")]),
        run_regions=["Region A", "Region B"],
        available_regions=["Region A", "Region B"],
    )
    national = pd.DataFrame(index=pd.DatetimeIndex([pd.Timestamp("2025-02-01")]))
    regional = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2025-02-01"), pd.Timestamp("2025-02-01")],
            "Region": ["Region A", "Region B"],
        }
    )

    national, regional = apply_primary_food_basket_aliases(national, regional, result)

    assert national.iloc[0]["FoodBasket"] == 20.0
    assert regional.set_index("Region").loc["Region A", "FoodBasket"] == 20.0
    assert pd.isna(regional.set_index("Region").loc["Region B", "FoodBasket"])


def test_selected_region_scope_requires_run_overlap():
    secondary = _spec("secondary", scope="selected_regions", regions=["Region A"])
    with pytest.raises(BasketScopeValidationError, match="no overlap"):
        calculate_basket_series(
            _price_frame(months=("2025-02",)),
            [secondary],
            full_date_index=pd.DatetimeIndex([pd.Timestamp("2025-02-01")]),
            run_regions=["Region B"],
            available_regions=["Region A", "Region B"],
        )


def test_unit_id_mismatch_falls_back_to_unit_name_and_warns():
    frame = _price_frame(months=("2025-02",))
    frame["Unit ID"] = None
    configured = _spec("primary")
    legacy = _spec(
        "secondary",
        items=[
            {
                "commodity_id": 1,
                "commodity_name": "Maize",
                "databridges_unit": "kg",
                "weight_quantity": 1,
            }
        ],
    )
    months = pd.DatetimeIndex([pd.Timestamp("2025-02-01")])

    result = calculate_basket_series(
        frame,
        [configured, legacy],
        full_date_index=months,
        run_regions=[],
        available_regions=["Region A", "Region B"],
    )

    configured_row = result.national[result.national["BasketRole"] == "primary"].iloc[0]
    legacy_row = result.national[result.national["BasketRole"] == "secondary"].iloc[0]
    # Unit-id match finds nothing, but the unit-name fallback still narrows to
    # the "kg" rows, so the month stays complete and the "bag" poison row stays
    # out of the cost.
    assert bool(configured_row["Complete"])
    assert configured_row["Cost"] == 15.0
    assert bool(legacy_row["Complete"])
    assert legacy_row["Cost"] == 15.0
    assert any("do not match the configured unit" in warning for warning in result.warnings)
    assert any("legacy component unit metadata" in warning for warning in result.warnings)


def test_unusable_unit_metadata_never_blocks_completeness():
    """Production regression: refreshed price rows carrying no usable unit
    metadata must not zero out reportable months (pre-second-basket semantics:
    any price for the commodity in the month keeps it complete)."""
    frame = _price_frame(months=("2025-02",))
    frame["Unit ID"] = None
    frame["Unit"] = ""
    configured = _spec("primary")
    months = pd.DatetimeIndex([pd.Timestamp("2025-02-01")])

    result = calculate_basket_series(
        frame,
        [configured],
        full_date_index=months,
        run_regions=[],
        available_regions=["Region A", "Region B"],
    )

    row = result.national[result.national["BasketRole"] == "primary"].iloc[0]
    assert bool(row["Complete"])
    assert row["Cost"] is not None and not pd.isna(row["Cost"])
    summary = result.summaries["primary"]
    assert summary.loc[summary["Complete"].astype(bool), "Date"].tolist() == [pd.Timestamp("2025-02-01")]
    assert any("do not match the configured unit" in warning for warning in result.warnings)


def test_matching_unit_rows_are_still_preferred_for_costs():
    frame = _price_frame(months=("2025-02",))
    configured = _spec("primary")
    months = pd.DatetimeIndex([pd.Timestamp("2025-02-01")])

    result = calculate_basket_series(
        frame,
        [configured],
        full_date_index=months,
        run_regions=[],
        available_regions=["Region A", "Region B"],
    )

    row = result.national[result.national["BasketRole"] == "primary"].iloc[0]
    # Unit-100 rows exist, so the 999/"bag" rows must stay excluded from the mean.
    assert bool(row["Complete"])
    assert row["Cost"] == 15.0
    assert not any("do not match the configured unit" in warning for warning in result.warnings)


def test_secondary_only_components_do_not_drive_optional_module_ordering():
    primary = _spec("primary")
    secondary = _spec(
        "secondary",
        items=[
            {"commodity_id": 2, "commodity_name": "Rice", "weight_quantity": 1},
            {"commodity_id": 1, "commodity_name": "Maize", "weight_quantity": 1},
        ],
    )

    assert data_loader._primary_driven_optional_module_names(
        ["Maize", "Rice", "Salt"],
        [primary, secondary],
    ) == ["Maize", "Salt"]


def test_vectorized_series_matches_per_cell_reference():
    """The grouped-aggregation index must reproduce _component_mean_price
    cell-by-cell across unit preference, name fallback, unit-agnostic
    fallback, NaN prices, and missing months/regions."""
    import numpy as np

    from app.services.market_monitor import basket_calculation as bc

    def _row(cid, name, month, region, price, unit_id, unit):
        return {
            "Commodity ID": cid,
            "Commodity": name,
            "Price Date": pd.Timestamp(f"{month}-01"),
            "Price": price,
            "Admin 1": region,
            "Unit ID": unit_id,
            "Unit": unit,
        }

    rows = []
    for month in ("2025-01", "2025-02", "2025-03"):
        rows.append(_row(1, "Maize", month, "Region A", 10, 100, "kg"))
        if month != "2025-03":
            rows.append(_row(1, "Maize", month, "Region B", 20, 100, "kg"))
        rows.append(_row(1, "Maize", month, "Region A", 999, 999, "bag"))
        rows.append(_row(1, "Maize", month, "Region B", 999, 999, "bag"))
        if month != "2025-02":
            rows.append(_row(2, "Beans", month, "Region A", 7, None, ""))
            rows.append(_row(2, "Beans", month, "Region B", 7, None, ""))
    rows.append(_row(3, "Oil", "2025-01", "Region A", 55, 77, "KG "))
    rows.append(_row(3, "Oil", "2025-01", "Region A", 500, 88, "bag"))
    rows.append(_row(3, "Oil", "2025-02", "Region A", 500, 88, "bag"))
    rows.append(_row(4, "Salt", "2025-01", "Region A", np.nan, 100, "kg"))
    rows.append(_row(4, "Salt", "2025-01", "Region A", 3, 999, "bag"))
    frame = pd.DataFrame(rows)

    primary = _spec(
        "primary",
        items=[
            {"commodity_id": 1, "commodity_name": "Maize", "databridges_unit_id": 100, "databridges_unit": "kg", "weight_quantity": 2},
            {"commodity_id": 2, "commodity_name": "Beans", "databridges_unit_id": 5, "databridges_unit": "lt", "weight_quantity": 1},
            {"commodity_id": 3, "commodity_name": "Oil", "databridges_unit": "kg", "weight_quantity": 1},
            {"commodity_id": 4, "commodity_name": "Salt", "databridges_unit_id": 100, "databridges_unit": "kg", "weight_quantity": 1},
        ],
    )
    secondary = _spec(
        "secondary",
        scope="selected_regions",
        regions=["Region A", "Region B"],
        items=[
            {"commodity_id": 1, "commodity_name": "Maize", "databridges_unit_id": 100, "databridges_unit": "kg", "weight_quantity": 3},
        ],
    )
    months = pd.DatetimeIndex([pd.Timestamp("2025-01-01"), pd.Timestamp("2025-02-01"), pd.Timestamp("2025-03-01")])

    result = calculate_basket_series(
        frame,
        [primary, secondary],
        full_date_index=months,
        run_regions=["Region A", "Region B"],
        available_regions=["Region A", "Region B"],
    )

    prepared = bc._prepare_price_frame(frame)
    specs_by_role = {"primary": primary, "secondary": secondary}
    checked = 0
    for source in (result.national, result.regional):
        for row in source.to_dict(orient="records"):
            spec = specs_by_role[row["BasketRole"]]
            region = row["Region"] if isinstance(row["Region"], str) else None
            expected_missing = []
            expected_contributions = []
            for item in spec.items:
                reference = bc._component_mean_price(prepared, item, pd.Timestamp(row["Date"]), region)
                if reference is None:
                    expected_missing.append(item.commodity_name)
                else:
                    expected_contributions.append(reference * item.quantity)
            expected_complete = bool(spec.items) and not expected_missing
            assert bool(row["Complete"]) == expected_complete, (row["BasketRole"], row["Date"], region)
            assert list(row["MissingComponentNames"]) == expected_missing, (row["BasketRole"], row["Date"], region)
            if expected_complete:
                assert row["Cost"] == round(float(sum(expected_contributions)), 2), (row["BasketRole"], row["Date"], region)
            else:
                assert pd.isna(row["Cost"])
            checked += 1
    assert checked == (3 + 6) + 6  # 3 national + 6 regional (primary) + 6 regional (secondary)


def test_large_series_completes_quickly():
    """Bangladesh-scale frames must not take minutes (regression for the
    per-cell quadratic scan that stalled the reportable-months endpoint)."""
    import time

    months = pd.date_range("2016-01-01", periods=120, freq="MS")
    regions = [f"Region {chr(65 + i)}" for i in range(8)]
    commodity_ids = list(range(1, 11))
    rows = []
    for cid in commodity_ids:
        for region in regions:
            for market in range(3):
                for month in months:
                    rows.append(
                        {
                            "Commodity ID": cid,
                            "Commodity": f"Commodity {cid}",
                            "Price Date": month,
                            "Price": 10.0 + cid + market,
                            "Admin 1": region,
                            "Unit ID": 100,
                            "Unit": "kg",
                        }
                    )
    frame = pd.DataFrame(rows)
    spec = _spec(
        "primary",
        items=[
            {"commodity_id": cid, "commodity_name": f"Commodity {cid}", "databridges_unit_id": 100, "databridges_unit": "kg", "weight_quantity": 1}
            for cid in commodity_ids
        ],
    )

    started = time.monotonic()
    result = calculate_basket_series(
        frame,
        [spec],
        full_date_index=months,
        run_regions=regions,
        available_regions=regions,
    )
    elapsed = time.monotonic() - started

    summary = result.summaries["primary"]
    assert summary["Complete"].astype(bool).all()
    assert len(summary) == len(months)
    assert elapsed < 15, f"basket series took {elapsed:.1f}s for a 28.8k-row frame"


def _monthly_row(commodity_id, name, month, region, price):
    return SimpleNamespace(
        price_flag="actual",
        price=price,
        price_date=pd.Timestamp(f"{month}-01").date(),
        commodity_name=name,
        commodity_id=commodity_id,
        price_type_name="Retail",
        admin1_name=region,
        admin2_name=None,
        market_name=region,
        market_id=commodity_id * 10 + (1 if region == "Region A" else 2),
        commodity_unit_id=100,
        commodity_unit_name="kg",
        currency_name="Currency",
        currency_code="CUR",
        observations=1,
        data_source="test",
    )


class _ReportabilityRepo:
    def __init__(self, rows):
        self.rows = rows

    def get_price_window(self, *_args, commodity_ids=None, **_kwargs):
        ids = set(commodity_ids or [])
        return [row for row in self.rows if row.commodity_id in ids]


def test_joint_reportability_uses_run_region_intersection_and_effective_aliases():
    rows = []
    for month in ("2025-01", "2025-02"):
        for region in ("Region A", "Region B"):
            rows.append(_monthly_row(1, "Maize", month, region, 10))
            if not (month == "2025-02" and region == "Region B"):
                rows.append(_monthly_row(2, "Beans", month, region, 5))
    primary = _spec("primary")
    secondary = _spec(
        "secondary",
        scope="selected_regions",
        regions=["Region A", "Region B"],
        items=[{"commodity_id": 2, "commodity_name": "Beans", "databridges_unit_id": 100, "weight_quantity": 1}],
    )
    availability = SimpleNamespace(
        cache_version_id="cache-v1",
        date_start=pd.Timestamp("2025-01-01").date(),
        date_end=pd.Timestamp("2025-02-01").date(),
        admin1_names=["Region A", "Region B"],
    )

    all_regions = data_loader._build_role_aware_reportable_months_payload(
        canonical="Testland",
        iso3="TST",
        repo=_ReportabilityRepo(rows),
        availability=availability,
        specs=[primary, secondary],
        secondary_included=True,
        run_regions=["Region A", "Region B"],
    )
    region_a = data_loader._build_role_aware_reportable_months_payload(
        canonical="Testland",
        iso3="TST",
        repo=_ReportabilityRepo(rows),
        availability=availability,
        specs=[primary, secondary],
        secondary_included=True,
        run_regions=["Region A"],
    )

    assert all_regions["primary_reportable_months"] == ["2025-01", "2025-02"]
    assert all_regions["secondary_reportable_months"] == ["2025-01"]
    assert all_regions["joint_reportable_months"] == ["2025-01"]
    assert all_regions["reportable_months"] == ["2025-01"]
    assert all_regions["missing_by_basket_and_month"]["secondary"]["months"]["2025-02"] == [
        {"region": "Region B", "scope_label": "Region B", "missing_component_names": ["Beans"]}
    ]
    assert region_a["joint_reportable_months"] == ["2025-01", "2025-02"]

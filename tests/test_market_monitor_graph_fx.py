import pandas as pd

from app.shared.report_blocks import build_market_monitor_report_blocks
from app.services.market_monitor import graph as market_graph


def _state_with_frames(df_national, df_history=None, df_regional=None):
    return {
        "country": "South Sudan",
        "time_period": "2026-02",
        "currency_code": "SSP",
        "cache_metadata": {"currency_code": "SSP"},
        "time_series_data_national": df_national.to_json(date_format="iso"),
        "time_series_history_national": (
            df_history.to_json(date_format="iso") if df_history is not None else None
        ),
        "time_series_data_regional": (
            df_regional.to_json(date_format="iso") if df_regional is not None else None
        ),
        "data_statistics": {"commodities": {"Maize": {"yoy_change_pct": 10}}},
    }


def test_currency_axis_label_uses_resolved_code():
    assert market_graph._currency_axis_label("Cost", "CDF") == "Cost (CDF)"
    assert market_graph._fx_axis_label("CDF") == "CDF per 1 USD"
    assert market_graph._fuel_axis_label("CDF") == "CDF/Litre"


def test_history_overlay_values_skip_gracefully_with_short_history():
    target = pd.date_range("2026-01-01", periods=2, freq="MS")
    history = pd.DataFrame({"FoodBasket": [10.0]}, index=pd.DatetimeIndex(["2025-01-01"]))

    prior, five_year_mean, low, high, counts = market_graph._history_overlay_values(history, target, "FoodBasket")

    assert prior.loc[pd.Timestamp("2026-01-01")] == 10.0
    assert pd.isna(prior.loc[pd.Timestamp("2026-02-01")])
    assert pd.isna(five_year_mean.loc[pd.Timestamp("2026-01-01")])
    assert pd.isna(low.loc[pd.Timestamp("2026-01-01")])
    assert pd.isna(high.loc[pd.Timestamp("2026-01-01")])
    assert counts.loc[pd.Timestamp("2026-01-01")] == 1


def test_history_overlay_values_require_five_prior_same_month_values_for_five_year_band():
    target = pd.date_range("2026-01-01", periods=1, freq="MS")
    history = pd.DataFrame(
        {"FoodBasket": [10.0, 11.0, 12.0, 13.0, 14.0]},
        index=pd.DatetimeIndex(["2021-01-01", "2022-01-01", "2023-01-01", "2024-01-01", "2025-01-01"]),
    )

    prior, five_year_mean, low, high, counts = market_graph._history_overlay_values(history, target, "FoodBasket")

    assert prior.loc[pd.Timestamp("2026-01-01")] == 14.0
    assert five_year_mean.loc[pd.Timestamp("2026-01-01")] == 12.0
    assert low.loc[pd.Timestamp("2026-01-01")] == 10.0
    assert high.loc[pd.Timestamp("2026-01-01")] == 14.0
    assert counts.loc[pd.Timestamp("2026-01-01")] == 5


def test_graph_designer_renders_fx_chart_and_skips_blank_regional_chart():
    dates = pd.date_range("2025-02-01", periods=13, freq="MS")
    df_national = pd.DataFrame(
        {
            "FoodBasket": range(100, 113),
            "Maize": range(10, 23),
            "ExchangeRate": range(1000, 1013),
            "ExchangeRateUnofficial": range(1100, 1113),
        },
        index=dates,
    )
    history_dates = pd.date_range("2021-03-01", periods=60, freq="MS")
    df_history = pd.DataFrame(
        {
            "FoodBasket": range(60),
            "Maize": range(100, 160),
        },
        index=history_dates,
    )
    df_regional = pd.DataFrame(
        {
            "Date": [dates[-1], dates[-1]],
            "Region": ["No Data", "Zero"],
            "FoodBasket": [None, 0],
        }
    )

    result = market_graph.node_graph_designer(_state_with_frames(df_national, df_history, df_regional))

    assert "food_basket_trend" in result["visualizations"]
    assert "commodity_trends" in result["visualizations"]
    assert "exchange_rate_trend" in result["visualizations"]
    assert "regional_comparison" not in result["visualizations"]


def test_graph_designer_applies_commodity_overlays_only_to_single_commodity_pages(monkeypatch):
    calls = []

    def fake_overlay(_ax, _history, _target_index, column, **kwargs):
        calls.append((column, kwargs.get("label_prefix")))

    monkeypatch.setattr(market_graph, "_plot_history_overlays", fake_overlay)
    dates = pd.date_range("2025-02-01", periods=13, freq="MS")
    history_dates = pd.date_range("2020-02-01", periods=73, freq="MS")
    df_history = pd.DataFrame(
        {
            "FoodBasket": range(73),
            "Maize": range(100, 173),
            "Sorghum": range(200, 273),
        },
        index=history_dates,
    )
    df_multi = pd.DataFrame(
        {
            "FoodBasket": range(100, 113),
            "Maize": range(10, 23),
            "Sorghum": range(20, 33),
        },
        index=dates,
    )

    market_graph.node_graph_designer(_state_with_frames(df_multi, df_history))

    assert calls == [("FoodBasket", None)]

    calls.clear()
    df_single = df_multi.drop(columns=["Sorghum"])

    market_graph.node_graph_designer(_state_with_frames(df_single, df_history))

    assert calls == [("FoodBasket", None), ("Maize", "Maize")]


def test_graph_designer_renders_fuel_chart_and_applies_overlays_for_two_or_fewer_series(monkeypatch):
    calls = []

    def fake_overlay(_ax, _history, _target_index, column, **kwargs):
        calls.append((column, kwargs.get("label_prefix")))

    monkeypatch.setattr(market_graph, "_plot_history_overlays", fake_overlay)
    dates = pd.date_range("2025-06-01", periods=13, freq="MS")
    df_national = pd.DataFrame(
        {
            "FoodBasket": range(100, 113),
            "Fuel (diesel)": range(200, 213),
            "Fuel (petrol/gasoline)": range(300, 313),
        },
        index=dates,
    )
    df_history = pd.DataFrame(
        {
            "FoodBasket": range(73),
            "Fuel (diesel)": range(1000, 1073),
            "Fuel (petrol/gasoline)": range(2000, 2073),
        },
        index=pd.date_range("2020-06-01", periods=73, freq="MS"),
    )
    state = _state_with_frames(df_national, df_history)
    state["fuel_energy_data"] = {
        "available": True,
        "series": [
            {"kind": "diesel", "label": "Diesel", "column_name": "Fuel (diesel)"},
            {"kind": "petrol_gasoline", "label": "Petrol/Gasoline", "column_name": "Fuel (petrol/gasoline)"},
        ],
    }

    result = market_graph.node_graph_designer(state)

    assert "fuel_prices" in result["visualizations"]
    assert ("Fuel (diesel)", "Diesel") in calls
    assert ("Fuel (petrol/gasoline)", "Petrol/Gasoline") in calls


def test_graph_designer_skips_fuel_overlays_when_chart_has_more_than_two_series(monkeypatch):
    calls = []

    def fake_overlay(_ax, _history, _target_index, column, **kwargs):
        calls.append((column, kwargs.get("label_prefix")))

    monkeypatch.setattr(market_graph, "_plot_history_overlays", fake_overlay)
    dates = pd.date_range("2025-06-01", periods=13, freq="MS")
    df_national = pd.DataFrame(
        {
            "FoodBasket": range(100, 113),
            "Fuel (diesel)": range(200, 213),
            "Fuel (petrol/gasoline)": range(300, 313),
            "Fuel (other)": range(400, 413),
        },
        index=dates,
    )
    df_history = pd.DataFrame({"FoodBasket": range(73)}, index=pd.date_range("2020-06-01", periods=73, freq="MS"))
    state = _state_with_frames(df_national, df_history)
    state["fuel_energy_data"] = {
        "available": True,
        "series": [
            {"kind": "diesel", "label": "Diesel", "column_name": "Fuel (diesel)"},
            {"kind": "petrol_gasoline", "label": "Petrol/Gasoline", "column_name": "Fuel (petrol/gasoline)"},
            {"kind": "other", "label": "Other", "column_name": "Fuel (other)"},
        ],
    }

    result = market_graph.node_graph_designer(state)

    assert "fuel_prices" in result["visualizations"]
    assert all(not column.startswith("Fuel") for column, _label in calls)


def test_graph_designer_renders_livestock_chart_and_applies_absolute_overlays(monkeypatch):
    calls = []

    def fake_overlay(_ax, _history, _target_index, column, **kwargs):
        calls.append((column, kwargs.get("label_prefix")))

    monkeypatch.setattr(market_graph, "_plot_history_overlays", fake_overlay)
    dates = pd.date_range("2025-06-01", periods=13, freq="MS")
    df_national = pd.DataFrame(
        {
            "FoodBasket": range(100, 113),
            "Animal Products - Meat (beef)": range(200, 213),
            "Animal Products - Meat (pork)": range(300, 313),
        },
        index=dates,
    )
    df_history = pd.DataFrame(
        {
            "FoodBasket": range(73),
            "Animal Products - Meat (beef)": range(1000, 1073),
            "Animal Products - Meat (pork)": range(2000, 2073),
        },
        index=pd.date_range("2020-06-01", periods=73, freq="MS"),
    )
    state = _state_with_frames(df_national, df_history)
    state["livestock_animal_products_data"] = {
        "available": True,
        "chart": {
            "mode": "absolute",
            "axis_label": "CDF/KG",
            "series": [
                {"column_name": "Animal Products - Meat (beef)", "label": "Meat (beef)", "unit": "KG"},
                {"column_name": "Animal Products - Meat (pork)", "label": "Meat (pork)", "unit": "KG"},
            ],
        },
        "series": [],
    }

    result = market_graph.node_graph_designer(state)

    assert "livestock_animal_products" in result["visualizations"]
    assert ("Animal Products - Meat (beef)", "Meat (beef)") in calls
    assert ("Animal Products - Meat (pork)", "Meat (pork)") in calls


def test_graph_designer_renders_livestock_indexed_chart_without_mixed_unit_overlays(monkeypatch):
    calls = []

    def fake_overlay(_ax, _history, _target_index, column, **kwargs):
        calls.append((column, kwargs.get("label_prefix")))

    monkeypatch.setattr(market_graph, "_plot_history_overlays", fake_overlay)
    dates = pd.date_range("2025-06-01", periods=13, freq="MS")
    df_national = pd.DataFrame(
        {
            "FoodBasket": range(100, 113),
            "Livestock - Livestock (Goat)": range(1000, 1013),
            "Animal Products - Milk (camel)": range(50, 63),
            "Animal Products - Meat (goat)": range(200, 213),
        },
        index=dates,
    )
    state = _state_with_frames(df_national, pd.DataFrame({"FoodBasket": range(73)}, index=pd.date_range("2020-06-01", periods=73, freq="MS")))
    state["livestock_animal_products_data"] = {
        "available": True,
        "chart": {
            "mode": "indexed",
            "axis_label": "Index (first month = 100)",
            "series": [
                {"column_name": "Livestock - Livestock (Goat)", "label": "Livestock (Goat)", "unit": "Head"},
                {"column_name": "Animal Products - Milk (camel)", "label": "Milk (camel)", "unit": "L"},
                {"column_name": "Animal Products - Meat (goat)", "label": "Meat (goat)", "unit": "KG"},
            ],
        },
        "series": [],
    }

    result = market_graph.node_graph_designer(state)

    assert "livestock_animal_products" in result["visualizations"]
    assert all(not column.startswith("Animal Products") and not column.startswith("Livestock") for column, _ in calls)


def test_graph_designer_renders_labour_market_chart(monkeypatch):
    calls = []

    def fake_overlay(_ax, _history, _target_index, column, **kwargs):
        calls.append((column, kwargs.get("label_prefix")))

    monkeypatch.setattr(market_graph, "_plot_history_overlays", fake_overlay)
    dates = pd.date_range("2025-06-01", periods=13, freq="MS")
    df_national = pd.DataFrame(
        {
            "FoodBasket": range(100, 113),
            "Labour - Purchasing power (Maize)": [20 + i * 0.1 for i in range(13)],
            "Labour - Casual wage": range(300, 313),
        },
        index=dates,
    )
    df_history = pd.DataFrame(
        {
            "FoodBasket": range(73),
            "Labour - Purchasing power (Maize)": [10 + i * 0.1 for i in range(73)],
            "Labour - Casual wage": range(1000, 1073),
        },
        index=pd.date_range("2020-06-01", periods=73, freq="MS"),
    )
    state = _state_with_frames(df_national, df_history)
    state["labour_market_data"] = {
        "available": True,
        "chart": {
            "mode": "purchasing_power",
            "axis_label": "kg of Maize per day's wage",
            "series": [
                {"column_name": "Labour - Purchasing power (Maize)", "label": "Purchasing power", "axis": "primary"},
                {"column_name": "Labour - Casual wage", "label": "Casual wage", "axis": "secondary", "axis_label": "AFN/day"},
            ],
        },
        "series": [],
    }

    result = market_graph.node_graph_designer(state)

    assert "labour_market" in result["visualizations"]
    assert ("Labour - Purchasing power (Maize)", "Purchasing power") in calls


def test_market_monitor_report_blocks_use_human_module_heading():
    blocks = build_market_monitor_report_blocks(
        {
            "country": "South Sudan",
            "time_period": "2026-02",
            "module_sections": {
                "exchange_rate": "Exchange rate narrative.",
                "fuel_energy": "Fuel narrative.",
                "livestock_animal_products": "Animal narrative.",
                "labour_market": "Labour narrative.",
            },
            "visualizations": {
                "fuel_prices": "base64",
                "livestock_animal_products": "base64",
                "labour_market": "base64",
            },
        }
    )

    headings = [block.text for block in blocks if block.type == "heading"]
    figures = [block.figure_id for block in blocks if block.type == "figure"]

    assert "Exchange Rate Analysis" in headings
    assert "Fuel & Energy" in headings
    assert "Livestock & Animal Products" in headings
    assert "Labour Market" in headings
    assert "EXCHANGE_RATE Analysis" not in headings
    assert "FUEL_ENERGY Analysis" not in headings
    assert "fuel_prices" in figures
    assert "livestock_animal_products" in figures
    assert "labour_market" in figures


def test_fuel_energy_module_fallback_narrative_contains_required_elements():
    module = market_graph.FuelEnergyModule()

    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise RuntimeError("offline")

    result = module.generate_section(
        {
            "country": "Somalia",
            "time_period": "2026-06",
            "fuel_energy_data": {
                "available": True,
                "unit": "SOS/Litre",
                "driver_hint": "The increase is consistent with higher fuel-market pressure.",
                "series": [
                    {
                        "kind": "diesel",
                        "label": "Diesel",
                        "current_price": 1120,
                        "mom_change_pct": 1.8,
                        "yoy_change_pct": 12.0,
                        "latest_month": "2026-06",
                    }
                ],
                "regional_disparities": [],
            },
        },
        FailingLLM(),
    )

    narrative = result["narrative"].lower()
    assert "diesel averaged 1120 sos/litre" in narrative
    assert "consistent with higher fuel-market pressure" in narrative
    assert "transport and distribution costs" in narrative


def test_livestock_module_fallback_narrative_uses_only_available_series():
    module = market_graph.LivestockAnimalProductsModule()

    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise RuntimeError("offline")

    result = module.generate_section(
        {
            "country": "Somalia",
            "time_period": "2026-06",
            "livestock_animal_products_data": {
                "available": True,
                "driver_hint": "The increase is consistent with seasonal demand.",
                "series": [
                    {
                        "group": "live_animal",
                        "label": "Livestock (Goat)",
                        "current_price": 1200,
                        "axis_unit": "SOS/Head",
                        "mom_change_pct": 2.0,
                        "yoy_change_pct": None,
                        "latest_month": "2026-06",
                    }
                ],
                "regional_disparities": [],
            },
        },
        FailingLLM(),
    )

    narrative = result["narrative"].lower()
    assert "livestock (goat) averaged 1200 sos/head" in narrative
    assert "animal-source protein affordability" in narrative
    assert "pastoralist income" in narrative
    assert "milk" not in narrative


def test_labour_module_fallback_narrative_contains_wage_and_purchasing_power():
    module = market_graph.LabourMarketModule()

    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise RuntimeError("offline")

    result = module.generate_section(
        {
            "country": "Afghanistan",
            "time_period": "2026-06",
            "labour_market_data": {
                "available": True,
                "driver_hint": "The wage increase is consistent with seasonal labour demand.",
                "series": [
                    {
                        "kind": "casual_unskilled",
                        "label": "Casual/unskilled wage",
                        "current_wage": 420,
                        "axis_unit": "AFN/day",
                        "mom_change_pct": 1.0,
                        "yoy_change_pct": 5.0,
                        "latest_month": "2026-06",
                    }
                ],
                "purchasing_power": {
                    "current_kg": 19.09,
                    "staple_name": "Maize",
                    "latest_month": "2026-06",
                    "mom_change_pct": -1.5,
                },
            },
        },
        FailingLLM(),
    )

    narrative = result["narrative"].lower()
    assert "casual/unskilled wage averaged 420 afn/day" in narrative
    assert "one day's wage bought about 19.09 kg of maize" in narrative
    assert "seasonal labour demand" in narrative


def test_exchange_rate_module_uses_existing_databridges_data_without_te(monkeypatch):
    module = market_graph.ExchangeRateModule(api_key=None)

    def fail_fetch(*_args, **_kwargs):
        raise AssertionError("TradingEconomics should not be called when DataBridges FX exists")

    monkeypatch.setattr(module, "_fetch_historical_series", fail_fetch)
    existing = {
        "currency_code": "SSP",
        "current_rate": 1000,
        "monthly_change_pct": 1.0,
        "yearly_change_pct": 5.0,
        "trend": "stable",
    }

    assert module.fetch_data({"exchange_rate_data": existing}) == {"exchange_rate_data": existing}


def test_exchange_rate_module_falls_back_to_te_when_state_has_no_fx(monkeypatch):
    module = market_graph.ExchangeRateModule(api_key="key")
    dates = pd.date_range("2024-01-01", "2025-02-28", freq="D")
    te_df = pd.DataFrame({"Close": range(1, len(dates) + 1)}, index=dates)
    monkeypatch.setattr(module, "_fetch_historical_series", lambda **_kwargs: te_df)

    result = module.fetch_data(
        {
            "exchange_rate_data": None,
            "currency_code": "SSP",
            "country": "South Sudan",
            "time_period": "2025-02",
        }
    )

    assert result["exchange_rate_data"]["current_rate"] == float(len(dates))
    assert result["exchange_rate_data"]["currency_code"] == "SSP"


def test_exchange_rate_module_skip_warning_when_no_databridges_fx_or_te(monkeypatch):
    monkeypatch.delenv("TE_API_KEY", raising=False)
    monkeypatch.setattr(market_graph, "get_model", lambda: None)

    result = market_graph.node_module_orchestrator(
        {
            "enabled_modules": ["exchange_rate"],
            "exchange_rate_data": None,
            "currency_code": "SSP",
            "country": "South Sudan",
            "time_period": "2026-02",
            "llm_calls": 0,
        }
    )

    assert result["module_sections"] == {}
    assert any("no exchange-rate source was available" in warning for warning in result["warnings"])

import pandas as pd

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


def test_history_overlay_values_skip_gracefully_with_short_history():
    target = pd.date_range("2026-01-01", periods=2, freq="MS")
    history = pd.DataFrame({"FoodBasket": [10.0]}, index=pd.DatetimeIndex(["2025-01-01"]))

    prior, five_year_mean, low, high = market_graph._history_overlay_values(history, target, "FoodBasket")

    assert prior.loc[pd.Timestamp("2026-01-01")] == 10.0
    assert pd.isna(prior.loc[pd.Timestamp("2026-02-01")])
    assert five_year_mean.loc[pd.Timestamp("2026-01-01")] == 10.0
    assert low.loc[pd.Timestamp("2026-01-01")] == 10.0
    assert high.loc[pd.Timestamp("2026-01-01")] == 10.0


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

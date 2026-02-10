from datetime import date, datetime

import streamlit as st

from streamlit_shared import (
    apply_wfp_theme,
    render_instructions_sidebar_button,
    quote_path_param,
    render_results_tabs,
    render_report_blocks,
    render_report_sections,
    render_visualizations,
    render_wfp_sidebar_logo,
    request_bytes,
    request_json,
    run_async_and_poll,
    safe_show_error,
)

st.set_page_config(page_title="Price Bulletin Drafter", layout="wide")
apply_wfp_theme()

with st.sidebar:
    render_wfp_sidebar_logo()
    render_instructions_sidebar_button(key="sidebar_instructions_price_bulletin")

st.title("Price Bulletin Drafter")
st.subheader("Generate Report")

countries = []
country_currency = {}

countries_resp = st.session_state.get("mm_countries_resp")
if countries_resp is None:
    try:
        countries_resp = request_json("GET", "/market-monitor/countries", timeout=30)
        st.session_state["mm_countries_resp"] = countries_resp
    except Exception:
        countries_resp = None
        st.session_state.pop("mm_countries_resp", None)

if isinstance(countries_resp, dict):
    countries_list = countries_resp.get("countries") or []
    if isinstance(countries_list, list):
        for c in countries_list:
            if isinstance(c, dict) and c.get("has_data"):
                name = c.get("name")
                if isinstance(name, str):
                    countries.append(name)
                    country_currency[name] = c.get("currency_code")

if countries:
    country = st.selectbox("Country", countries, index=0, key="mm_country")
else:
    country = st.text_input("Country", key="mm_country")

metadata = None
regions = []
commodities = []
default_commodities = []
time_period_options = []
default_time_period = None

if country:
    metadata_cache = st.session_state.setdefault("mm_country_metadata", {})
    metadata = metadata_cache.get(country)
    if metadata is None:
        try:
            metadata = request_json(
                "GET",
                f"/market-monitor/countries/{quote_path_param(country)}/metadata",
                timeout=30,
            )
            metadata_cache[country] = metadata
        except Exception:
            metadata = None

if isinstance(metadata, dict):
    regions = metadata.get("regions") or []
    commodities = metadata.get("commodities") or []
    default_commodities = metadata.get("default_commodities") or []

    date_range = metadata.get("date_range")
    if isinstance(date_range, dict):
        start_s = date_range.get("start")
        end_s = date_range.get("end")
        if isinstance(start_s, str) and isinstance(end_s, str):
            try:
                start_d = datetime.strptime(start_s, "%Y-%m-%d").date().replace(day=1)
                end_d = datetime.strptime(end_s, "%Y-%m-%d").date().replace(day=1)
                cur = start_d
                while cur <= end_d:
                    time_period_options.append(cur.strftime("%Y-%m"))
                    if cur.month == 12:
                        cur = date(cur.year + 1, 1, 1)
                    else:
                        cur = date(cur.year, cur.month + 1, 1)
                default_time_period = end_d.strftime("%Y-%m") if time_period_options else None
            except Exception:
                time_period_options = []
                default_time_period = None

if not time_period_options:
    today_month = date.today().replace(day=1)
    months = []
    cur = today_month
    for _ in range(36):
        months.append(cur.strftime("%Y-%m"))
        if cur.month == 1:
            cur = date(cur.year - 1, 12, 1)
        else:
            cur = date(cur.year, cur.month - 1, 1)
    time_period_options = list(reversed(months))
    default_time_period = today_month.strftime("%Y-%m")

with st.form("market_monitor_form"):
    time_period_index = 0
    if default_time_period in time_period_options:
        time_period_index = time_period_options.index(default_time_period)
    time_period = st.selectbox(
        "Time Period (YYYY-MM)",
        options=time_period_options,
        index=time_period_index,
        key=f"mm_time_period_{country}" if isinstance(country, str) and country else "mm_time_period",
    )

    use_news_dates = st.checkbox("Use News Dates", value=False, key="mm_use_news_dates")
    news_start_date_date = st.date_input(
        "News Start Date",
        value=date.today(),
        disabled=not use_news_dates,
        key="mm_news_start_date",
    )
    news_end_date_date = st.date_input(
        "News End Date",
        value=date.today(),
        disabled=not use_news_dates,
        key="mm_news_end_date",
    )

    if use_news_dates and news_end_date_date < news_start_date_date:
        st.error("News End Date must be on or after News Start Date.")

    news_start_date = news_start_date_date.strftime("%Y-%m-%d") if use_news_dates else ""
    news_end_date = news_end_date_date.strftime("%Y-%m-%d") if use_news_dates else ""

    use_mock_data = st.checkbox("Use Mock Data", value=False)

    valid_commodities = [c for c in commodities if isinstance(c, str)]
    default_candidates = [c for c in default_commodities if c in valid_commodities]
    commodity_list = st.multiselect(
        "Commodities",
        options=valid_commodities,
        default=default_candidates or valid_commodities,
        key=f"mm_commodities_{country}" if isinstance(country, str) and country else "mm_commodities",
    )

    admin1_list = st.multiselect(
        "Regions (Admin1)",
        options=[r for r in regions if isinstance(r, str)],
        default=[r for r in regions if isinstance(r, str)],
        key=f"mm_regions_{country}" if isinstance(country, str) and country else "mm_regions",
    )

    currency_default = (country_currency.get(country) or "USD") if isinstance(country, str) else "USD"
    currency_code = st.text_input("Currency Code", value=str(currency_default))

    enabled_modules = st.multiselect("Enabled Modules", options=["exchange_rate"], default=[])

    previous_report_text = st.text_area("Previous Report Text (optional)", value="", height=120)

    submitted = st.form_submit_button("Run")

if submitted:
    try:
        payload = {
            "country": country,
            "time_period": time_period,
            "commodity_list": commodity_list,
            "admin1_list": admin1_list,
            "currency_code": currency_code,
            "enabled_modules": enabled_modules,
            "news_start_date": news_start_date or None,
            "news_end_date": news_end_date or None,
            "previous_report_text": previous_report_text or "",
            "use_mock_data": use_mock_data,
        }

        run_id, final_status, result = run_async_and_poll(
            start_method="POST",
            start_path="/market-monitor/generate-async",
            status_path_template="/market-monitor/status/{run_id}",
            result_path_template="/market-monitor/result/{run_id}",
            start_json=payload,
            poll_interval_seconds=2.0,
            timeout_seconds=3600,
        )
        st.session_state["mm_last_result"] = result
        st.session_state["mm_last_run_id"] = run_id

    except Exception as e:
        safe_show_error(e)

result = st.session_state.get("mm_last_result")
run_id = st.session_state.get("mm_last_run_id")

if isinstance(result, dict):

    display_run_id = str(run_id or result.get("run_id") or "")

    def _summary() -> None:
        cols = st.columns(4)
        cols[0].metric("Run ID", display_run_id)
        cols[1].metric("Country", str(result.get("country") or ""))
        cols[2].metric("Time Period", str(result.get("time_period") or ""))
        cols[3].metric("LLM Calls", str(result.get("llm_calls") or 0))

        render_report_blocks(result.get("report_blocks"), visualizations=result.get("visualizations"))

    def _visuals() -> None:
        with st.expander("Report Sections", expanded=False):
            render_report_sections(result.get("report_sections"))

        st.subheader("Visualizations")
        render_visualizations(result.get("visualizations"))

        with st.expander("Data Statistics", expanded=False):
            st.json(result.get("data_statistics"))

    def _export() -> None:
        if not run_id:
            st.info("Export is available for asynchronous runs only.")
            return

        docx_bytes = None
        if st.session_state.get("mm_docx_run_id") == run_id:
            docx_bytes = st.session_state.get("mm_docx_bytes")

        if docx_bytes is None:
            with st.spinner("Preparing DOCX..."):
                try:
                    docx_bytes = request_bytes(
                        "POST",
                        f"/market-monitor/export-docx/{run_id}",
                        json_body={},
                        timeout=300,
                    )
                except Exception as e:
                    safe_show_error(e)
                    return

            st.session_state["mm_docx_bytes"] = docx_bytes
            st.session_state["mm_docx_run_id"] = run_id

        if docx_bytes:
            st.download_button(
                "Generate & Download DOCX",
                data=docx_bytes,
                file_name=f"market-monitor-{run_id}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=f"mm_download_docx_{run_id}",
            )

    render_results_tabs(summary=_summary, json_data=result, visuals=_visuals, export=_export)

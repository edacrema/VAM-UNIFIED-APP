import streamlit as st

from streamlit_shared import (
    quote_path_param,
    get_backend_base_url,
    render_results_tabs,
    render_report_blocks,
    render_report_sections,
    render_visualizations,
    request_bytes,
    request_json,
    run_async_and_poll,
    safe_show_error,
    set_backend_base_url,
)

st.set_page_config(page_title="Market Monitor", layout="wide")

with st.sidebar:
    backend_url = st.text_input("Backend Base URL", value=get_backend_base_url())
    set_backend_base_url(backend_url)

st.title("Market Monitor")

st.subheader("Dataset")

dataset_col1, dataset_col2 = st.columns(2)

with dataset_col1:
    if st.button("Refresh Dataset Status"):
        st.session_state["mm_dataset_status"] = None

    try:
        if st.session_state.get("mm_dataset_status") is None:
            st.session_state["mm_dataset_status"] = request_json("GET", "/market-monitor/dataset/status", timeout=20)
        st.json(st.session_state.get("mm_dataset_status"))
    except Exception as e:
        safe_show_error(e)

with dataset_col2:
    uploaded_dataset = st.file_uploader("Upload price_data.csv", type=["csv"], key="mm_dataset_upload")
    if st.button("Upload Dataset"):
        if uploaded_dataset is None:
            st.error("Please upload a CSV")
        else:
            try:
                files = {
                    "file": (
                        uploaded_dataset.name,
                        uploaded_dataset.getvalue(),
                        uploaded_dataset.type or "text/csv",
                    )
                }
                resp = request_json("POST", "/market-monitor/dataset/upload", files=files, timeout=120)
                st.success("Uploaded")
                st.json(resp)
                st.session_state["mm_dataset_status"] = None
            except Exception as e:
                safe_show_error(e)

st.divider()

st.subheader("Generate Report")

countries = []
country_currency = {}

try:
    countries_resp = request_json("GET", "/market-monitor/countries", timeout=30)
    if isinstance(countries_resp, dict):
        countries_list = countries_resp.get("countries") or []
        if isinstance(countries_list, list):
            for c in countries_list:
                if isinstance(c, dict) and c.get("has_data"):
                    name = c.get("name")
                    if isinstance(name, str):
                        countries.append(name)
                        country_currency[name] = c.get("currency_code")
except Exception:
    countries = []

mode = st.radio("Mode", ["Synchronous", "Asynchronous"], horizontal=True)

with st.form("market_monitor_form"):
    if countries:
        country = st.selectbox("Country", countries, index=0)
    else:
        country = st.text_input("Country")

    time_period = st.text_input("Time Period (YYYY-MM)", value="2025-01")

    news_start_date = st.text_input("News Start Date (optional, YYYY-MM-DD)", value="")
    news_end_date = st.text_input("News End Date (optional, YYYY-MM-DD)", value="")

    use_mock_data = st.checkbox("Use Mock Data", value=False)

    metadata = None
    regions = []
    commodities = []
    default_commodities = []

    if country:
        try:
            metadata = request_json(
                "GET",
                f"/market-monitor/countries/{quote_path_param(country)}/metadata",
                timeout=30,
            )
        except Exception:
            metadata = None

    if isinstance(metadata, dict):
        regions = metadata.get("regions") or []
        commodities = metadata.get("commodities") or []
        default_commodities = metadata.get("default_commodities") or []

    commodity_list = st.multiselect(
        "Commodities",
        options=[c for c in commodities if isinstance(c, str)],
        default=[c for c in default_commodities if isinstance(c, str)],
    )

    admin1_list = st.multiselect(
        "Regions (Admin1)",
        options=[r for r in regions if isinstance(r, str)],
        default=[],
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

        if mode == "Synchronous":
            result = request_json("POST", "/market-monitor/generate", json_body=payload, timeout=1800)
            st.session_state["mm_last_result"] = result
            st.session_state["mm_last_run_id"] = None
        else:
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

        if st.button("Build DOCX", key=f"mm_build_docx_{run_id}"):
            try:
                docx_bytes = request_bytes(
                    "POST",
                    f"/market-monitor/export-docx/{run_id}",
                    json_body={},
                    timeout=300,
                )
                st.session_state["mm_docx_bytes"] = docx_bytes
                st.session_state["mm_docx_run_id"] = run_id
            except Exception as e:
                safe_show_error(e)
                return

        docx_bytes = None
        if st.session_state.get("mm_docx_run_id") == run_id:
            docx_bytes = st.session_state.get("mm_docx_bytes")

        if docx_bytes:
            st.download_button(
                "Download DOCX",
                data=docx_bytes,
                file_name=f"market-monitor-{run_id}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=f"mm_download_docx_{run_id}",
            )

    render_results_tabs(summary=_summary, json_data=result, visuals=_visuals, export=_export)

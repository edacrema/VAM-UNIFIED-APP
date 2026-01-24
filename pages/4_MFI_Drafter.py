import streamlit as st

from streamlit_shared import (
    get_backend_base_url,
    render_report_blocks,
    render_results_tabs,
    request_bytes,
    request_json,
    run_async_and_poll,
    safe_show_error,
    set_backend_base_url,
)

st.set_page_config(page_title="MFI Drafter", layout="wide")

with st.sidebar:
    backend_url = st.text_input("Backend Base URL", value=get_backend_base_url())
    set_backend_base_url(backend_url)

st.title("MFI Report Generator")

mode = st.radio("Input Mode", ["Manual", "Upload CSV"], horizontal=True)
run_mode = st.radio("Run Mode", ["Synchronous", "Asynchronous"], horizontal=True)

sample_markets = {}
try:
    sample_markets = request_json("GET", "/mfi-drafter/sample-markets", timeout=20)
except Exception:
    sample_markets = {}

if mode == "Manual":
    with st.form("mfi_drafter_manual"):
        country = st.text_input("Country")
        data_collection_start = st.text_input("Data Collection Start (YYYY-MM-DD)")
        data_collection_end = st.text_input("Data Collection End (YYYY-MM-DD)")

        defaults = []
        if isinstance(sample_markets, dict) and country in sample_markets:
            payload = sample_markets.get(country)
            if isinstance(payload, dict):
                defaults = payload.get("markets") or []

        markets_text = st.text_area(
            "Markets (one per line)",
            value="\n".join([m for m in defaults if isinstance(m, str)]),
            height=150,
        )

        submitted = st.form_submit_button("Run")

    if submitted:
        try:
            markets = [m.strip() for m in markets_text.splitlines() if m.strip()]
            payload = {
                "country": country,
                "data_collection_start": data_collection_start,
                "data_collection_end": data_collection_end,
                "markets": markets,
            }

            if run_mode == "Synchronous":
                result = request_json("POST", "/mfi-drafter/generate", json_body=payload, timeout=1800)
                st.session_state["mfi_last_result"] = result
                st.session_state["mfi_last_run_id"] = None
            else:
                run_id, final_status, result = run_async_and_poll(
                    start_method="POST",
                    start_path="/mfi-drafter/generate-async",
                    status_path_template="/mfi-drafter/status/{run_id}",
                    result_path_template="/mfi-drafter/result/{run_id}",
                    start_json=payload,
                    poll_interval_seconds=2.0,
                    timeout_seconds=3600,
                )
                st.session_state["mfi_last_result"] = result
                st.session_state["mfi_last_run_id"] = run_id

        except Exception as e:
            safe_show_error(e)

else:
    with st.form("mfi_drafter_csv"):
        uploaded = st.file_uploader("Processed MFI CSV", type=["csv"], key="mfi_csv_file")
        country_override = st.text_input("Country Override (optional)")
        start_override = st.text_input("Start Override (optional)")
        end_override = st.text_input("End Override (optional)")

        validate = st.form_submit_button("Validate CSV")
        run = st.form_submit_button("Run")

    if validate:
        try:
            if uploaded is None:
                st.error("Please upload a CSV")
            else:
                files = {
                    "file": (
                        uploaded.name,
                        uploaded.getvalue(),
                        uploaded.type or "text/csv",
                    )
                }
                resp = request_json("POST", "/mfi-drafter/validate-csv", files=files, timeout=120)
                st.json(resp)
        except Exception as e:
            safe_show_error(e)

    if run:
        try:
            if uploaded is None:
                st.error("Please upload a CSV")
            else:
                files = {
                    "file": (
                        uploaded.name,
                        uploaded.getvalue(),
                        uploaded.type or "text/csv",
                    )
                }
                data = {
                    "country_override": country_override or "",
                    "data_collection_start_override": start_override or "",
                    "data_collection_end_override": end_override or "",
                }

                if run_mode == "Synchronous":
                    result = request_json(
                        "POST",
                        "/mfi-drafter/generate-from-csv",
                        data=data,
                        files=files,
                        timeout=1800,
                    )
                    st.session_state["mfi_last_result"] = result
                    st.session_state["mfi_last_run_id"] = None
                else:
                    run_id, final_status, result = run_async_and_poll(
                        start_method="POST",
                        start_path="/mfi-drafter/generate-from-csv-async",
                        status_path_template="/mfi-drafter/status/{run_id}",
                        result_path_template="/mfi-drafter/result/{run_id}",
                        start_data=data,
                        start_files=files,
                        poll_interval_seconds=2.0,
                        timeout_seconds=3600,
                    )
                    st.session_state["mfi_last_result"] = result
                    st.session_state["mfi_last_run_id"] = run_id

        except Exception as e:
            safe_show_error(e)

result = st.session_state.get("mfi_last_result")
run_id = st.session_state.get("mfi_last_run_id")

if isinstance(result, dict):

    display_run_id = str(run_id or result.get("run_id") or "")

    def _summary() -> None:
        cols = st.columns(4)
        cols[0].metric("Run ID", display_run_id)
        cols[1].metric("Country", str(result.get("country") or ""))
        cols[2].metric("National MFI", str(result.get("national_mfi") or ""))
        cols[3].metric("LLM Calls", str(result.get("llm_calls") or 0))

        render_report_blocks(result.get("report_blocks"), visualizations=result.get("visualizations"))

    def _visuals() -> None:
        with st.expander("Risk Distribution", expanded=False):
            st.json(result.get("risk_distribution"))

        with st.expander("Markets Data", expanded=False):
            st.json(result.get("markets_data"))

    def _export() -> None:
        if not run_id:
            st.info("Export is available for asynchronous runs only.")
            return

        if st.button("Build DOCX", key=f"mfi_build_docx_{run_id}"):
            try:
                docx_bytes = request_bytes(
                    "POST",
                    f"/mfi-drafter/export-docx/{run_id}",
                    json_body={},
                    timeout=300,
                )
                st.session_state["mfi_docx_bytes"] = docx_bytes
                st.session_state["mfi_docx_run_id"] = run_id
            except Exception as e:
                safe_show_error(e)
                return

        docx_bytes = None
        if st.session_state.get("mfi_docx_run_id") == run_id:
            docx_bytes = st.session_state.get("mfi_docx_bytes")

        if docx_bytes:
            st.download_button(
                "Download DOCX",
                data=docx_bytes,
                file_name=f"mfi-drafter-{run_id}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=f"mfi_download_docx_{run_id}",
            )

    render_results_tabs(summary=_summary, json_data=result, visuals=_visuals, export=_export)

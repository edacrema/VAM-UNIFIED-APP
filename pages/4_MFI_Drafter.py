import streamlit as st

from streamlit_shared import (
    apply_wfp_theme,
    render_bug_report_header_link,
    render_bug_report_sidebar_link,
    render_instructions_sidebar_button,
    render_onboarding_sidebar_button,
    render_report_blocks,
    render_results_tabs,
    render_wfp_sidebar_logo,
    request_bytes,
    request_json,
    run_async_and_poll,
    safe_show_error,
)

st.set_page_config(page_title="MFI Drafter", layout="wide")
apply_wfp_theme()

with st.sidebar:
    render_wfp_sidebar_logo()
    render_onboarding_sidebar_button(key="sidebar_onboarding_mfi_drafter")
    render_instructions_sidebar_button(key="sidebar_instructions_mfi_drafter")
    render_bug_report_sidebar_link()

title_col, bug_col = st.columns([3, 1])
with title_col:
    st.title("MFI Report Generator")
with bug_col:
    render_bug_report_header_link()

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

            st.session_state.pop("mfi_docx_bytes", None)
            st.session_state.pop("mfi_docx_run_id", None)

            if isinstance(final_status, dict) and final_status.get("status") == "failed":
                st.error(final_status.get("error") or "failed")

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

        docx_bytes = None
        if st.session_state.get("mfi_docx_run_id") == run_id:
            docx_bytes = st.session_state.get("mfi_docx_bytes")

        if docx_bytes is None:
            with st.spinner("Preparing DOCX..."):
                try:
                    docx_bytes = request_bytes(
                        "POST",
                        f"/mfi-drafter/export-docx/{run_id}",
                        json_body={},
                        timeout=300,
                    )
                except Exception as e:
                    safe_show_error(e)
                    return

            st.session_state["mfi_docx_bytes"] = docx_bytes
            st.session_state["mfi_docx_run_id"] = run_id

        if docx_bytes:
            st.download_button(
                "Generate & Download DOCX",
                data=docx_bytes,
                file_name=f"mfi-drafter-{run_id}.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                key=f"mfi_download_docx_{run_id}",
            )

    render_results_tabs(summary=_summary, json_data=result, visuals=_visuals, export=_export)

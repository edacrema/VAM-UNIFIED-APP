import streamlit as st

from streamlit_shared import (
    get_backend_base_url,
    render_results_tabs,
    request_json,
    run_async_and_poll,
    safe_show_error,
    set_backend_base_url,
)

st.set_page_config(page_title="MFI Validator", layout="wide")

with st.sidebar:
    backend_url = st.text_input("Backend Base URL", value=get_backend_base_url())
    set_backend_base_url(backend_url)

st.title("RAW MFI Dataset Validator")

tab_sync, tab_async = st.tabs(["Synchronous", "Asynchronous"])

with tab_sync:
    with st.form("mfi_validator_sync"):
        uploaded = st.file_uploader("RAW MFI CSV", type=["csv"], key="mfi_val_file")
        survey_type = st.selectbox("Survey Type", ["full mfi", "reduced mfi"], index=0)
        template = st.file_uploader("Template (optional)", type=["csv", "json"], key="mfi_val_template")
        submitted = st.form_submit_button("Validate")

    if submitted:
        try:
            if uploaded is None:
                st.error("Please upload a CSV file")
            else:
                files = {
                    "file": (
                        uploaded.name,
                        uploaded.getvalue(),
                        uploaded.type or "text/csv",
                    )
                }
                if template is not None:
                    files["template"] = (
                        template.name,
                        template.getvalue(),
                        template.type or "application/octet-stream",
                    )

                data = {"survey_type": survey_type}
                result = request_json(
                    "POST",
                    "/mfi-validator/validate-file",
                    data=data,
                    files=files,
                    timeout=600,
                )

                if isinstance(result, dict):
                    def _summary() -> None:
                        cols = st.columns(4)
                        cols[0].metric("Success", str(result.get("success")))
                        cols[1].metric("Country", str(result.get("country") or ""))
                        cols[2].metric("Survey Period", str(result.get("survey_period") or ""))
                        cols[3].metric("LLM Calls", str(result.get("llm_calls") or 0))

                        st.subheader("Final Report")
                        st.markdown(result.get("final_report") or "")

                        with st.expander("Layer Results", expanded=False):
                            st.json(result.get("layer_results"))

                    render_results_tabs(summary=_summary, json_data=result)
                else:
                    st.write(result)
        except Exception as e:
            safe_show_error(e)

with tab_async:
    with st.form("mfi_validator_async"):
        uploaded = st.file_uploader("RAW MFI CSV", type=["csv"], key="mfi_val_file_async")
        survey_type = st.selectbox("Survey Type", ["full mfi", "reduced mfi"], index=0, key="mfi_val_survey_type_async")
        template = st.file_uploader("Template (optional)", type=["csv", "json"], key="mfi_val_template_async")
        submitted = st.form_submit_button("Start Async Validation")

    if submitted:
        try:
            if uploaded is None:
                st.error("Please upload a CSV file")
            else:
                files = {
                    "file": (
                        uploaded.name,
                        uploaded.getvalue(),
                        uploaded.type or "text/csv",
                    )
                }
                if template is not None:
                    files["template"] = (
                        template.name,
                        template.getvalue(),
                        template.type or "application/octet-stream",
                    )

                data = {"survey_type": survey_type}

                run_id, final_status, result = run_async_and_poll(
                    start_method="POST",
                    start_path="/mfi-validator/validate-file-async",
                    status_path_template="/mfi-validator/status/{run_id}",
                    result_path_template="/mfi-validator/result/{run_id}",
                    start_data=data,
                    start_files=files,
                    poll_interval_seconds=2.0,
                    timeout_seconds=1800,
                )

                if isinstance(final_status, dict) and final_status.get("status") == "failed":
                    st.error(final_status.get("error") or "failed")

                if isinstance(result, dict):
                    def _summary() -> None:
                        cols = st.columns(5)
                        cols[0].metric("Run ID", str(run_id))
                        cols[1].metric("Success", str(result.get("success")))
                        cols[2].metric("Country", str(result.get("country") or ""))
                        cols[3].metric("Survey Period", str(result.get("survey_period") or ""))
                        cols[4].metric("LLM Calls", str(result.get("llm_calls") or 0))

                        st.subheader("Final Report")
                        st.markdown(result.get("final_report") or "")

                        with st.expander("Layer Results", expanded=False):
                            st.json(result.get("layer_results"))

                    render_results_tabs(summary=_summary, json_data=result)
                else:
                    st.write(result)

        except Exception as e:
            safe_show_error(e)

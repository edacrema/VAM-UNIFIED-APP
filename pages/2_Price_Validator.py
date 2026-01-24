import streamlit as st

from streamlit_shared import (
    get_backend_base_url,
    render_results_tabs,
    request_json,
    run_async_and_poll,
    safe_show_error,
    set_backend_base_url,
)

st.set_page_config(page_title="Price Validator", layout="wide")

with st.sidebar:
    backend_url = st.text_input("Backend Base URL", value=get_backend_base_url())
    set_backend_base_url(backend_url)

st.title("Price Data Validator")

tab_sync, tab_async = st.tabs(["Synchronous", "Asynchronous"])

with tab_sync:
    with st.form("price_validator_sync"):
        uploaded = st.file_uploader("Price Data (CSV/XLSX)", type=["csv", "xlsx", "xls"], key="price_val_file")
        template = st.file_uploader("Template (optional)", type=["csv", "xlsx", "xls"], key="price_val_template")
        submitted = st.form_submit_button("Validate")

    if submitted:
        try:
            if uploaded is None:
                st.error("Please upload a file")
            else:
                files = {
                    "file": (
                        uploaded.name,
                        uploaded.getvalue(),
                        uploaded.type or "application/octet-stream",
                    )
                }
                if template is not None:
                    files["template"] = (
                        template.name,
                        template.getvalue(),
                        template.type or "application/octet-stream",
                    )

                result = request_json(
                    "POST",
                    "/price-validator/validate-file",
                    files=files,
                    timeout=600,
                )

                if isinstance(result, dict):
                    def _summary() -> None:
                        cols = st.columns(4)
                        cols[0].metric("Success", str(result.get("success")))
                        cols[1].metric("Country", str(result.get("country") or ""))
                        cols[2].metric("File Type", str(result.get("file_type") or ""))
                        cols[3].metric("LLM Calls", str(result.get("llm_calls") or 0))

                        st.subheader("Final Report")
                        st.markdown(result.get("final_report") or "")

                        with st.expander("Layer Results", expanded=False):
                            st.json(result.get("layer_results"))

                        with st.expander("Product Classifications", expanded=False):
                            st.json(result.get("product_classifications"))

                    render_results_tabs(summary=_summary, json_data=result)
                else:
                    st.write(result)
        except Exception as e:
            safe_show_error(e)

with tab_async:
    with st.form("price_validator_async"):
        uploaded = st.file_uploader("Price Data (CSV/XLSX)", type=["csv", "xlsx", "xls"], key="price_val_file_async")
        template = st.file_uploader("Template (optional)", type=["csv", "xlsx", "xls"], key="price_val_template_async")
        submitted = st.form_submit_button("Start Async Validation")

    if submitted:
        try:
            if uploaded is None:
                st.error("Please upload a file")
            else:
                files = {
                    "file": (
                        uploaded.name,
                        uploaded.getvalue(),
                        uploaded.type or "application/octet-stream",
                    )
                }
                if template is not None:
                    files["template"] = (
                        template.name,
                        template.getvalue(),
                        template.type or "application/octet-stream",
                    )

                run_id, final_status, result = run_async_and_poll(
                    start_method="POST",
                    start_path="/price-validator/validate-file-async",
                    status_path_template="/price-validator/status/{run_id}",
                    result_path_template="/price-validator/result/{run_id}",
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
                        cols[3].metric("File Type", str(result.get("file_type") or ""))
                        cols[4].metric("LLM Calls", str(result.get("llm_calls") or 0))

                        st.subheader("Final Report")
                        st.markdown(result.get("final_report") or "")

                        with st.expander("Layer Results", expanded=False):
                            st.json(result.get("layer_results"))

                        with st.expander("Product Classifications", expanded=False):
                            st.json(result.get("product_classifications"))

                    render_results_tabs(summary=_summary, json_data=result)
                else:
                    st.write(result)

        except Exception as e:
            safe_show_error(e)

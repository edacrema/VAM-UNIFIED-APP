import streamlit as st

from streamlit_shared import (
    apply_wfp_theme,
    render_bug_report_header_link,
    render_bug_report_sidebar_link,
    render_instructions_sidebar_button,
    render_onboarding_sidebar_button,
    render_results_tabs,
    render_wfp_sidebar_logo,
    run_async_and_poll,
    safe_show_error,
)

st.set_page_config(page_title="Price Validator", layout="wide")
apply_wfp_theme()

with st.sidebar:
    render_wfp_sidebar_logo()
    render_onboarding_sidebar_button(key="sidebar_onboarding_price_validator")
    render_instructions_sidebar_button(key="sidebar_instructions_price_validator")
    render_bug_report_sidebar_link()

title_col, bug_col = st.columns([3, 1])
with title_col:
    st.title("Price Data Validator")
with bug_col:
    render_bug_report_header_link()

with st.form("price_validator_async"):
    uploaded = st.file_uploader("Price Data (.xlsx)", type=["xlsx"], key="price_val_file_async")
    template = st.file_uploader("Template (.xlsx)", type=["xlsx"], key="price_val_template_async")
    submitted = st.form_submit_button("Validate")

if submitted:
    try:
        if uploaded is None or template is None:
            st.error("Please upload both the dataset and template")
        else:
            files = {
                "file": (
                    uploaded.name,
                    uploaded.getvalue(),
                    uploaded.type or "application/octet-stream",
                )
            }
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
                display_run_id = str(run_id or result.get("run_id") or "")

                def _summary() -> None:
                    cols = st.columns(6)
                    cols[0].metric("Run ID", display_run_id)
                    cols[1].metric("Success", str(result.get("success")))
                    cols[2].metric("Country", str(result.get("country") or ""))
                    cols[3].metric("File Type", str(result.get("file_type") or ""))
                    cols[4].metric("LLM Calls", str(result.get("llm_calls") or 0))
                    cols[5].metric(
                        "Commodity Suggestions",
                        str(len(result.get("product_classifications") or [])),
                    )

                    st.subheader("Final Report")
                    st.markdown(result.get("final_report") or "")

                    with st.expander("Layer Results", expanded=False):
                        st.json(result.get("layer_results"))

                    with st.expander("Detected Columns", expanded=False):
                        st.json(result.get("column_roles"))

                    with st.expander("Commodity Suggestions", expanded=False):
                        st.json(result.get("product_classifications"))

                render_results_tabs(summary=_summary, json_data=result)
            else:
                st.write(result)

    except Exception as e:
        safe_show_error(e)

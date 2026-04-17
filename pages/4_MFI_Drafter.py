from datetime import date

import streamlit as st

from streamlit_shared import (
    apply_wfp_theme,
    render_bug_report_header_link,
    render_bug_report_sidebar_link,
    render_instructions_sidebar_button,
    render_onboarding_sidebar_button,
    quote_path_param,
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

def _survey_label(survey: dict) -> str:
    survey_id = survey.get("survey_id")
    start = survey.get("survey_start_date") or "?"
    end = survey.get("survey_end_date") or "?"
    name = survey.get("survey_name") or survey.get("survey_original_filename") or survey.get("xls_form_name") or "MFI survey"
    return f"{survey_id} | {start} to {end} | {name}"

input_source = st.radio(
    "Input Source",
    ["Processed CSV Upload", "Databridges Survey"],
    horizontal=True,
    key="mfi_input_source",
)

if input_source == "Processed CSV Upload":
    st.caption(
        "Upload the final processed/elaborated MFI CSV produced by Databridges. "
        "Optional overrides let you replace the country or collection period when the file metadata is incomplete."
    )

    with st.form("mfi_drafter_csv"):
        uploaded = st.file_uploader("Processed MFI CSV", type=["csv"], key="mfi_drafter_csv_file")
        country_override = st.text_input("Country Override (optional)", key="mfi_country_override")
        use_date_override = st.checkbox(
            "Override Collection Dates",
            value=False,
            key="mfi_use_date_override",
        )
        override_cols = st.columns(2)
        with override_cols[0]:
            start_override = st.date_input(
                "Collection Start Override",
                value=date.today().replace(month=1, day=1),
                disabled=not use_date_override,
                key="mfi_csv_start_override",
            )
        with override_cols[1]:
            end_override = st.date_input(
                "Collection End Override",
                value=date.today(),
                disabled=not use_date_override,
                key="mfi_csv_end_override",
            )
        run_csv = st.form_submit_button("Run")

    if run_csv:
        try:
            if uploaded is None:
                st.error("Please upload a processed MFI CSV file")
            else:
                files = {
                    "file": (
                        uploaded.name,
                        uploaded.getvalue(),
                        uploaded.type or "text/csv",
                    )
                }
                data = {}
                if country_override.strip():
                    data["country_override"] = country_override.strip()
                if use_date_override:
                    data["data_collection_start_override"] = start_override.strftime("%Y-%m-%d")
                    data["data_collection_end_override"] = end_override.strftime("%Y-%m-%d")

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
else:
    countries = []
    countries_resp = st.session_state.get("mfi_countries_resp")
    if countries_resp is None:
        try:
            countries_resp = request_json("GET", "/mfi-drafter/countries", timeout=60)
            st.session_state["mfi_countries_resp"] = countries_resp
        except Exception:
            countries_resp = None
            st.session_state.pop("mfi_countries_resp", None)

    if isinstance(countries_resp, dict):
        for item in countries_resp.get("countries") or []:
            if isinstance(item, dict) and isinstance(item.get("name"), str):
                countries.append(item["name"])

    if countries:
        country = st.selectbox("Country", countries, key="mfi_country")
    else:
        country = st.text_input("Country", key="mfi_country")

    use_date_filter = st.checkbox("Filter Surveys by Date Range", value=False, key="mfi_use_date_filter")
    cols = st.columns(2)
    with cols[0]:
        survey_start = st.date_input(
            "Survey Start",
            value=date.today().replace(month=1, day=1),
            disabled=not use_date_filter,
            key="mfi_survey_start",
        )
    with cols[1]:
        survey_end = st.date_input(
            "Survey End",
            value=date.today(),
            disabled=not use_date_filter,
            key="mfi_survey_end",
        )

    surveys = []
    if country:
        params = {}
        if use_date_filter:
            params = {
                "start_date": survey_start.strftime("%Y-%m-%d"),
                "end_date": survey_end.strftime("%Y-%m-%d"),
            }
        try:
            surveys_resp = request_json(
                "GET",
                f"/mfi-drafter/countries/{quote_path_param(country)}/surveys",
                params=params,
                timeout=60,
            )
            if isinstance(surveys_resp, dict):
                surveys = [item for item in surveys_resp.get("surveys") or [] if isinstance(item, dict)]
        except Exception as e:
            safe_show_error(e)

    survey_options = {_survey_label(survey): survey for survey in surveys if survey.get("survey_id") is not None}
    survey_labels = list(survey_options.keys()) or ["No surveys available"]

    with st.form("mfi_drafter_survey"):
        selected_label = st.selectbox(
            "MFI Survey",
            options=survey_labels,
            key=f"mfi_survey_{country}" if isinstance(country, str) and country else "mfi_survey",
            disabled=not survey_options,
        )
        run = st.form_submit_button("Run", disabled=not survey_options)

    if run:
        try:
            selected_survey = survey_options.get(selected_label)
            if not selected_survey:
                st.error("Please select a survey")
            else:
                run_id, final_status, result = run_async_and_poll(
                    start_method="POST",
                    start_path="/mfi-drafter/generate-from-survey-async",
                    status_path_template="/mfi-drafter/status/{run_id}",
                    result_path_template="/mfi-drafter/result/{run_id}",
                    start_json={"survey_id": selected_survey["survey_id"]},
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

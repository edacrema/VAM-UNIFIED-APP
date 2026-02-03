import base64
import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import quote

import pandas as pd
import streamlit as st

from app.streamlit_backend.dispatcher import dispatch_request

WFP_LOGO_URL = "https://upload.wikimedia.org/wikipedia/commons/thumb/5/5f/WFP_Logo.svg/512px-WFP_Logo.svg.png"
WFP_PRIMARY = "#0072BC"
WFP_PRIMARY_DARK = "#005A9C"
WFP_NAVY = "#003A5D"
WFP_LIGHT = "#E6F1FA"
WFP_BG = "#EEF4FA"
WFP_TEXT = "#0C1E2E"


def apply_wfp_theme() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700&display=swap');
        :root {
            --wfp-primary: #0072BC;
            --wfp-primary-dark: #005A9C;
            --wfp-navy: #003A5D;
            --wfp-light: #E6F1FA;
            --wfp-bg: #EEF4FA;
            --wfp-surface: #F1F6FB;
            --wfp-surface-strong: #E1ECF7;
            --wfp-border: rgba(0, 58, 93, 0.18);
            --wfp-text: #0C1E2E;
            --wfp-muted: #4B5F73;
        }
        html, body, [class*="css"] {
            font-family: 'Montserrat', sans-serif;
        }
        .stApp {
            background: radial-gradient(1200px 700px at 0% 0%, #E1ECF7 0%, #F1F6FB 45%, #E7F1FA 100%);
            color: var(--wfp-text);
        }
        main .block-container {
            padding-top: 2.5rem;
        }
        main h1, main h2, main h3, main h4, main h5, main h6 {
            color: var(--wfp-navy);
        }
        main label {
            color: var(--wfp-text);
            font-weight: 600;
        }
        label[data-testid="stWidgetLabel"],
        label[data-testid="stWidgetLabel"] * {
            color: var(--wfp-text) !important;
            opacity: 1 !important;
        }
        label[data-testid="stWidgetLabel"] p {
            color: var(--wfp-text) !important;
            opacity: 1 !important;
            font-weight: 600 !important;
        }
        div[data-testid="stMetricLabel"],
        div[data-testid="stMetricLabel"] * {
            color: rgba(0, 58, 93, 0.9) !important;
            opacity: 1 !important;
            font-weight: 700 !important;
        }
        div[data-testid="stMetricValue"],
        div[data-testid="stMetricValue"] * {
            color: var(--wfp-navy) !important;
            opacity: 1 !important;
            font-weight: 700 !important;
        }
        div[data-testid="stMetricDelta"],
        div[data-testid="stMetricDelta"] * {
            opacity: 1 !important;
        }
        div[data-testid="stSpinner"],
        div[data-testid="stSpinner"] * {
            color: var(--wfp-text) !important;
            opacity: 1 !important;
        }
        main p, main .stMarkdown {
            color: var(--wfp-text);
        }
        main .stCaption, main small {
            color: var(--wfp-muted);
        }
        div[data-testid="stForm"] {
            background: linear-gradient(160deg, var(--wfp-surface) 0%, var(--wfp-light) 100%);
            border: 1px solid var(--wfp-border);
            border-radius: 18px;
            padding: 1.5rem 1.5rem 0.75rem 1.5rem;
            box-shadow: 0 18px 36px rgba(0, 58, 93, 0.08);
        }
        section[data-testid="stSidebar"], .stSidebar {
            background: linear-gradient(180deg, var(--wfp-navy) 0%, var(--wfp-primary) 100%);
            color: #FFFFFF;
        }
        section[data-testid="stSidebar"] .stTextInput label,
        section[data-testid="stSidebar"] label,
        section[data-testid="stSidebar"] span,
        section[data-testid="stSidebar"] p {
            color: #E6F1FA;
        }
        .stButton > button,
        .stDownloadButton > button,
        div[data-testid="stFormSubmitButton"] button,
        div[data-testid^="baseButton-"] button {
            background: var(--wfp-primary) !important;
            color: #FFFFFF !important;
            border-radius: 999px !important;
            border: none !important;
            padding: 0.55rem 1.25rem !important;
            font-weight: 600 !important;
        }
        .stButton > button:hover,
        .stDownloadButton > button:hover,
        div[data-testid="stFormSubmitButton"] button:hover,
        div[data-testid^="baseButton-"] button:hover {
            background: var(--wfp-primary-dark) !important;
            color: #FFFFFF !important;
        }
        .stTextInput input,
        .stTextArea textarea,
        .stNumberInput input,
        .stDateInput input,
        .stTimeInput input,
        .stSelectbox div[data-baseweb="select"] > div,
        .stMultiSelect div[data-baseweb="select"] > div {
            background-color: var(--wfp-surface);
            color: var(--wfp-text);
            border: 1px solid var(--wfp-border);
            border-radius: 8px;
        }
        .stTextInput input:focus,
        .stTextArea textarea:focus,
        .stNumberInput input:focus,
        .stDateInput input:focus,
        .stTimeInput input:focus,
        .stSelectbox div[data-baseweb="select"]:focus-within,
        .stMultiSelect div[data-baseweb="select"]:focus-within {
            border-color: var(--wfp-primary);
            box-shadow: 0 0 0 2px rgba(0, 114, 188, 0.18);
        }
        .stSelectbox div[data-baseweb="select"] span,
        .stMultiSelect div[data-baseweb="select"] span {
            color: var(--wfp-text) !important;
        }
        .stTextInput input::placeholder,
        .stTextArea textarea::placeholder,
        .stNumberInput input::placeholder,
        .stDateInput input::placeholder,
        .stTimeInput input::placeholder {
            color: #5A6B7B;
        }
        div[role="listbox"] {
            background-color: var(--wfp-surface);
            color: var(--wfp-text);
        }
        div[role="option"] {
            color: var(--wfp-text);
        }
        .stMultiSelect div[data-baseweb="tag"] {
            background: var(--wfp-surface-strong);
            color: var(--wfp-navy);
            border: 1px solid rgba(0, 58, 93, 0.2);
        }
        .stMultiSelect div[data-baseweb="tag"] span {
            color: var(--wfp-navy);
        }
        .stMultiSelect div[data-baseweb="tag"] svg {
            color: var(--wfp-navy);
        }
        div[data-baseweb="checkbox"] > div {
            background-color: var(--wfp-surface);
            border-color: var(--wfp-border);
        }
        div[data-testid="stCheckbox"] label,
        div[data-testid="stCheckbox"] span,
        div[data-testid="stCheckbox"] p {
            color: var(--wfp-text) !important;
            opacity: 1 !important;
            font-weight: 600 !important;
        }
        div[data-testid="stCheckbox"] small {
            color: var(--wfp-muted) !important;
            opacity: 1 !important;
        }
        section[data-testid="stSidebar"] div[data-testid="stCheckbox"] * {
            color: #E6F1FA !important;
        }
        /* File uploader: override dark dropzone + low-contrast labels */
        div[data-testid="stFileUploader"] label {
            color: var(--wfp-text) !important;
            opacity: 1 !important;
            font-weight: 600;
        }
        div[data-testid="stFileUploader"] small,
        div[data-testid="stFileUploader"] p,
        div[data-testid="stFileUploader"] span {
            color: var(--wfp-muted) !important;
            opacity: 1 !important;
        }
        section[data-testid="stFileUploaderDropzone"],
        div[data-testid="stFileUploader"] section {
            background: linear-gradient(160deg, var(--wfp-surface) 0%, var(--wfp-light) 100%) !important;
            border: 1px dashed rgba(0, 114, 188, 0.45) !important;
            border-radius: 14px !important;
            box-shadow: 0 10px 22px rgba(0, 58, 93, 0.08) !important;
        }
        section[data-testid="stFileUploaderDropzone"] *,
        div[data-testid="stFileUploader"] section * {
            color: var(--wfp-text) !important;
            opacity: 1 !important;
        }
        div[data-testid="stFileUploader"] button,
        section[data-testid="stFileUploaderDropzone"] button {
            background: var(--wfp-primary) !important;
            color: #FFFFFF !important;
            border: none !important;
            border-radius: 999px !important;
            font-weight: 600 !important;
        }
        div[data-testid="stFileUploader"] button:hover,
        section[data-testid="stFileUploaderDropzone"] button:hover {
            background: var(--wfp-primary-dark) !important;
            color: #FFFFFF !important;
        }
        .stTabs [data-baseweb="tab"] {
            font-weight: 600;
        }
        .wfp-sidebar-logo {
            display: block;
            margin: 0.5rem auto 1rem auto;
            max-width: 180px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_wfp_sidebar_logo() -> None:
    base_dir = Path(__file__).resolve().parent
    logo_candidates = [
        base_dir / "assets" / "wfp_logo.png",
        base_dir / "app" / "shared" / "assets" / "wfp_logo.png",
    ]
    logo_path = next((p for p in logo_candidates if p.exists()), None)
    if logo_path is not None:
        st.image(str(logo_path), width="stretch")
    else:
        st.image(WFP_LOGO_URL, width="stretch")
    st.markdown("---")


def quote_path_param(value: Any) -> str:
    return quote(str(value), safe="")


def request_json(
    method: str,
    path: str,
    *,
    params: Optional[dict] = None,
    json_body: Any = None,
    data: Optional[dict] = None,
    files: Optional[dict] = None,
    timeout: int = 60,
) -> Any:
    resp = dispatch_request(method, path, params=params, json_body=json_body, data=data, files=files)
    if resp.status_code >= 400:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise RuntimeError(f"{method} {path} failed ({resp.status_code}): {detail}")

    if not resp.content:
        return None

    try:
        return resp.json()
    except Exception:
        return resp.text


def request_bytes(method: str, path: str, *, json_body: Any = None, timeout: int = 120) -> bytes:
    resp = dispatch_request(method, path, json_body=json_body)
    if resp.status_code >= 400:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise RuntimeError(f"{method} {path} failed ({resp.status_code}): {detail}")
    return resp.content


def safe_show_error(err: Exception) -> None:
    st.error(str(err))


def decode_base64_data(value: Any) -> Optional[bytes]:
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    if s.startswith("data:") and "base64," in s:
        s = s.split("base64,", 1)[1]
    try:
        return base64.b64decode(s)
    except Exception:
        return None


def render_retriever_traces(traces: Any, *, max_items: int = 200) -> None:
    if not isinstance(traces, list) or not traces:
        st.write("No logs")
        return

    lines = []
    for item in traces[-max_items:]:
        if isinstance(item, (dict, list)):
            try:
                lines.append(json.dumps(item, ensure_ascii=False))
            except Exception:
                lines.append(str(item))
        else:
            lines.append(str(item))

    st.code("\n".join(lines))


def render_results_tabs(
    *,
    summary: Callable[[], None],
    json_data: Any,
    visuals: Optional[Callable[[], None]] = None,
    export: Optional[Callable[[], None]] = None,
) -> None:
    tab_summary, tab_json, tab_visuals, tab_export = st.tabs(["Summary", "JSON", "Visuals", "Export"])

    with tab_summary:
        summary()

    with tab_json:
        if json_data is None:
            st.write("No data")
        else:
            st.json(json_data)

    with tab_visuals:
        if visuals is None:
            st.write("No visuals")
        else:
            visuals()

    with tab_export:
        if export is None:
            st.write("No export available")
        else:
            export()


def render_run_status(status: Any) -> None:
    if not isinstance(status, dict):
        st.write(status)
        return

    progress = int(status.get("progress_pct") or 0)
    progress = min(max(progress, 0), 100)

    cols = st.columns(3)
    cols[0].metric("Status", str(status.get("status") or ""))
    cols[1].metric("Node", str(status.get("current_node") or ""))
    cols[2].metric("Progress", f"{progress}%")

    st.progress(progress / 100)

    warnings = status.get("warnings") or []
    if isinstance(warnings, list) and warnings:
        with st.expander("Warnings", expanded=False):
            for w in warnings:
                st.write(f"- {w}")

    metadata = status.get("metadata")
    if isinstance(metadata, dict) and metadata:
        retriever_traces = metadata.get("retriever_traces")
        if isinstance(retriever_traces, list) and retriever_traces:
            with st.expander("Logs", expanded=False):
                render_retriever_traces(retriever_traces)

        with st.expander("Metadata", expanded=False):
            st.json(metadata)

    if status.get("error"):
        st.error(str(status.get("error")))

    if status.get("traceback"):
        with st.expander("Traceback", expanded=False):
            st.code(str(status.get("traceback")))


def run_async_and_poll(
    *,
    start_method: str,
    start_path: str,
    status_path_template: str,
    result_path_template: str,
    start_json: Any = None,
    start_data: Optional[dict] = None,
    start_files: Optional[dict] = None,
    poll_interval_seconds: float = 2.0,
    timeout_seconds: int = 1800,
) -> Tuple[str, Any, Any]:
    start_resp = request_json(
        start_method,
        start_path,
        json_body=start_json,
        data=start_data,
        files=start_files,
        timeout=60,
    )

    if not isinstance(start_resp, dict) or "run_id" not in start_resp:
        raise RuntimeError(f"Unexpected start response: {start_resp}")

    run_id = str(start_resp.get("run_id"))

    status_placeholder = st.empty()
    started = time.time()
    last_status: Any = None

    while True:
        status = request_json("GET", status_path_template.format(run_id=run_id), timeout=30)
        last_status = status
        with status_placeholder.container():
            render_run_status(status)

        if isinstance(status, dict) and status.get("status") in {"completed", "failed"}:
            break

        if time.time() - started > timeout_seconds:
            raise RuntimeError("Polling timeout")

        time.sleep(poll_interval_seconds)

    result: Any = None
    if isinstance(last_status, dict) and last_status.get("status") == "completed":
        result = request_json("GET", result_path_template.format(run_id=run_id), timeout=120)

    return run_id, last_status, result


def render_visualizations(visualizations: Any) -> None:
    if not isinstance(visualizations, dict) or not visualizations:
        st.write("No visualizations")
        return

    ids = [k for k in visualizations.keys() if isinstance(k, str)]
    ids.sort()

    for fig_id in ids:
        img_b64 = visualizations.get(fig_id)
        img_bytes = decode_base64_data(img_b64)
        if img_bytes is None:
            continue
        st.subheader(fig_id)
        st.image(img_bytes, width="stretch")


def render_report_sections(sections: Any) -> None:
    if not isinstance(sections, dict) or not sections:
        st.write("No report sections")
        return

    keys = [k for k in sections.keys() if isinstance(k, str)]
    keys.sort()

    for k in keys:
        content = sections.get(k)
        with st.expander(k, expanded=False):
            if isinstance(content, str):
                st.markdown(content)
            else:
                st.write(content)


def render_report_blocks(blocks: Any, visualizations: Any = None) -> None:
    if not isinstance(blocks, list):
        st.write(blocks)
        return

    viz = visualizations if isinstance(visualizations, dict) else {}

    for idx, block in enumerate(blocks):
        if not isinstance(block, dict):
            st.write(block)
            continue

        btype = block.get("type")

        if btype == "heading":
            text = str(block.get("text") or "")
            level = int(block.get("level") or 2)
            if level <= 1:
                st.title(text)
            elif level == 2:
                st.header(text)
            elif level == 3:
                st.subheader(text)
            else:
                st.markdown(f"**{text}**")
            continue

        if btype == "paragraph":
            text = str(block.get("text") or "")
            if text.strip():
                st.markdown(text)
            continue

        if btype == "figure":
            fig_id = block.get("figure_id")
            caption = block.get("caption")
            if isinstance(fig_id, str) and fig_id in viz:
                img_bytes = decode_base64_data(viz.get(fig_id))
                if img_bytes is not None:
                    st.image(img_bytes, caption=caption, width="stretch")
                else:
                    st.write({"figure_id": fig_id})
            else:
                st.write({"figure_id": fig_id})
            continue

        if btype == "references":
            refs = block.get("references")
            if isinstance(refs, list) and refs:
                for r in refs:
                    if isinstance(r, dict):
                        title = r.get("title") or r.get("doc_title") or r.get("doc_id") or "Reference"
                        url = r.get("url")
                        source = r.get("source")
                        date = r.get("date")
                        parts = [p for p in [source, date] if p]
                        label = title if not parts else f"{title} ({', '.join([str(p) for p in parts])})"
                        if url:
                            st.markdown(f"- [{label}]({url})")
                        else:
                            st.markdown(f"- {label}")
                    else:
                        st.markdown(f"- {r}")
            continue

        if btype == "definition_box":
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                st.info(text)
            else:
                st.write(block)
            continue

        if btype == "table":
            meta = block.get("meta")
            if isinstance(meta, dict) and meta.get("table_kind") == "mfi_overview":
                dims = meta.get("dimensions") or []
                rows = meta.get("rows") or []

                if isinstance(dims, list) and isinstance(rows, list) and rows:
                    flat_rows = []
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        dim_scores = r.get("dimension_scores")
                        if not isinstance(dim_scores, dict):
                            dim_scores = {}
                        flat = {
                            "market_name": r.get("market_name", ""),
                            "region": r.get("region", ""),
                            "overall_mfi": r.get("overall_mfi", 0),
                        }
                        for d in dims:
                            if isinstance(d, str):
                                flat[d] = dim_scores.get(d, 0)
                        flat_rows.append(flat)

                    df = pd.DataFrame(flat_rows)
                    edited = st.data_editor(
                        df,
                        width="stretch",
                        key=f"report_table_{idx}",
                    )
                    try:
                        csv_bytes = edited.to_csv(index=False).encode("utf-8")
                    except Exception:
                        csv_bytes = df.to_csv(index=False).encode("utf-8")

                    st.download_button(
                        "Download CSV",
                        data=csv_bytes,
                        file_name="mfi_overview.csv",
                        mime="text/csv",
                        key=f"report_table_download_{idx}",
                    )
                    continue

            if isinstance(meta, dict):
                st.json(meta)
            else:
                st.write(meta)
            continue

        st.write(block)

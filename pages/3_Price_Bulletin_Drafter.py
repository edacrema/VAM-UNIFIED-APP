from datetime import date, datetime

import streamlit as st

from streamlit_shared import (
    apply_wfp_theme,
    render_bug_report_header_link,
    render_bug_report_sidebar_link,
    render_instructions_sidebar_button,
    render_onboarding_sidebar_button,
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


def _dedupe_text(values):
    seen = set()
    items = []
    for value in values or []:
        text = str(value or "").strip()
        key = text.lower()
        if text and key not in seen:
            seen.add(key)
            items.append(text)
    return items


def _basket_item_name(item):
    if not isinstance(item, dict):
        return ""
    return str(item.get("commodity_name_snapshot") or item.get("commodity_name") or "").strip()


def _short_id(value):
    text = str(value or "")
    return text[:8] if text else "n/a"


st.set_page_config(page_title="Price Bulletin Drafter", layout="wide")
apply_wfp_theme()

with st.sidebar:
    render_wfp_sidebar_logo()
    render_onboarding_sidebar_button(key="sidebar_onboarding_price_bulletin")
    render_instructions_sidebar_button(key="sidebar_instructions_price_bulletin")
    render_bug_report_sidebar_link()

title_col, bug_col = st.columns([3, 1])
with title_col:
    st.title("Price Bulletin Drafter")
with bug_col:
    render_bug_report_header_link()
st.subheader("Generate Report")

countries = []
country_currency = {}
cache_status = {}

cache_status_resp = st.session_state.get("mm_cache_status_resp")
if cache_status_resp is None:
    try:
        cache_status_resp = request_json("GET", "/market-monitor/cache/status", timeout=30)
        st.session_state["mm_cache_status_resp"] = cache_status_resp
    except Exception as e:
        cache_status_resp = None
        st.session_state.pop("mm_cache_status_resp", None)
        safe_show_error(e)

if isinstance(cache_status_resp, dict):
    cache_status = cache_status_resp

st.markdown("#### Price Cache")
status_cols = st.columns(4)
status_cols[0].metric("Status", str(cache_status.get("status") or "inactive"))
status_cols[1].metric("Countries", str(cache_status.get("active_country_count") or 0))
status_cols[2].metric("Price Rows", str(cache_status.get("rows_prices") or 0))
active_version = str(cache_status.get("active_version_id") or "")
status_cols[3].metric("Version", active_version[:8] if active_version else "none")

if cache_status.get("completed_at") or cache_status.get("activated_at"):
    st.caption(
        "Completed: "
        f"{cache_status.get('completed_at') or 'n/a'} | Activated: {cache_status.get('activated_at') or 'n/a'}"
    )

cache_warnings = cache_status.get("warnings") or []
if cache_warnings:
    with st.expander("Cache warnings", expanded=False):
        for warning in cache_warnings:
            st.warning(str(warning))

if not cache_status.get("has_active_cache"):
    st.warning("No active price cache is available. Run a cache refresh before drafting a Price Bulletin.")
    st.stop()

countries_resp = st.session_state.get("mm_countries_resp")
if countries_resp is None:
    try:
        countries_resp = request_json("GET", "/market-monitor/countries", timeout=30)
        st.session_state["mm_countries_resp"] = countries_resp
    except Exception:
        countries_resp = None
        st.session_state.pop("mm_countries_resp", None)

if isinstance(countries_resp, dict):
    if not cache_status and isinstance(countries_resp.get("cache_status"), dict):
        cache_status = countries_resp["cache_status"]
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
    st.warning("The active cache does not contain any countries with monthly price data.")
    st.stop()

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
        except Exception as e:
            metadata = None
            safe_show_error(e)

if not isinstance(metadata, dict):
    st.warning("Cached metadata is not available for the selected country.")
    st.stop()

regions = metadata.get("regions") or []
raw_commodities = metadata.get("commodities") or []
commodities = [
    item.get("name")
    for item in raw_commodities
    if isinstance(item, dict) and isinstance(item.get("name"), str)
]
if not commodities:
    commodities = [item for item in raw_commodities if isinstance(item, str)]
default_commodities = metadata.get("default_commodities") or []

selected_latest = metadata.get("latest_cached_date")
selected_version = str(metadata.get("cache_version_id") or "")
st.caption(
    "Selected country cache: "
    f"latest date {selected_latest or 'n/a'} | version {selected_version[:8] if selected_version else 'n/a'}"
)
for warning in metadata.get("warnings") or []:
    st.warning(str(warning))

operator_warnings = metadata.get("operator_warnings") or []
if operator_warnings:
    with st.expander("Cache processing notes (for administrators)", expanded=False):
        for warning in operator_warnings:
            st.info(str(warning))

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
    st.warning("The selected country has no cached monthly price date range.")
    st.stop()

if not commodities:
    st.warning("The selected country has no cached priced commodities.")
    st.stop()

basket_resp = None
basket_cache = st.session_state.setdefault("mm_country_basket", {})
if country:
    basket_resp = basket_cache.get(country)
    if basket_resp is None:
        try:
            basket_resp = request_json(
                "GET",
                f"/market-monitor/countries/{quote_path_param(country)}/basket",
                timeout=30,
            )
            basket_cache[country] = basket_resp
        except Exception as e:
            basket_resp = None
            safe_show_error(e)

active_basket = None
if isinstance(basket_resp, dict):
    active_basket = basket_resp.get("active_basket")
    if not isinstance(active_basket, dict):
        active_basket = None

basket_items = active_basket.get("items") if isinstance(active_basket, dict) else []
if not isinstance(basket_items, list):
    basket_items = []
basket_item_names = _dedupe_text(_basket_item_name(item) for item in basket_items)
basket_items_by_id = {}
for item in basket_items:
    if isinstance(item, dict):
        try:
            basket_items_by_id[int(item.get("commodity_id"))] = item
        except Exception:
            pass

st.markdown("#### Country Food Basket")
if active_basket:
    basket_cols = st.columns(4)
    basket_cols[0].metric("Basket Version", str(active_basket.get("version_number") or "n/a"))
    basket_cols[1].metric("Items", str(len(basket_items)))
    basket_cols[2].metric("Created By", str(active_basket.get("created_by_user_id") or "unknown"))
    basket_cols[3].metric("Cache Version", _short_id(active_basket.get("cache_version_id_at_creation")))
    st.caption(
        "Created: "
        f"{active_basket.get('created_at') or 'n/a'} | "
        f"Basket ID: {_short_id(active_basket.get('basket_version_id'))}"
    )
    summary_rows = [
        {
            "Commodity": _basket_item_name(item),
            "Quantity": item.get("weight_quantity"),
            "Unit": item.get("databridges_unit") or item.get("unit"),
            "Note": item.get("item_note") or "",
        }
        for item in basket_items
        if isinstance(item, dict)
    ]
    if summary_rows:
        st.dataframe(summary_rows, hide_index=True, use_container_width=True)
else:
    st.warning("No active food basket exists for this country. Save a basket before generating a bulletin.")

editor_rows = []
for commodity in raw_commodities:
    if not isinstance(commodity, dict):
        continue
    commodity_id = commodity.get("id")
    if commodity_id in (None, ""):
        continue
    try:
        commodity_id_int = int(commodity_id)
    except Exception:
        continue
    saved_item = basket_items_by_id.get(commodity_id_int) or {}
    editor_rows.append(
        {
            "Include": bool(saved_item),
            "Commodity ID": commodity_id_int,
            "Commodity": str(commodity.get("name") or ""),
            "Unit": str(commodity.get("unit") or commodity.get("unit_name") or ""),
            "Quantity": float(saved_item.get("weight_quantity") or 0.0),
            "Note": str(saved_item.get("item_note") or ""),
        }
    )

with st.expander("Edit country food basket", expanded=not bool(active_basket)):
    st.caption("Saved baskets are shared across users and publish immediately.")
    column_config = {}
    if hasattr(st, "column_config"):
        column_config = {
            "Include": st.column_config.CheckboxColumn("Include"),
            "Quantity": st.column_config.NumberColumn("Quantity", min_value=0.0, step=0.1, format="%.3f"),
            "Note": st.column_config.TextColumn("Note"),
        }
    edited_rows = st.data_editor(
        editor_rows,
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        disabled=["Commodity ID", "Commodity", "Unit"],
        column_config=column_config,
        key=f"mm_basket_editor_{country}_{active_basket.get('basket_version_id') if active_basket else 'new'}",
    )
    change_note = st.text_input(
        "Change note",
        value="",
        key=f"mm_basket_change_note_{country}",
        placeholder="Optional note for this published basket version",
    )
    if st.button("Save food basket", type="primary", key=f"mm_save_food_basket_{country}"):
        save_items = []
        invalid_rows = []
        for row in edited_rows or []:
            if not isinstance(row, dict) or not row.get("Include"):
                continue
            try:
                quantity = float(row.get("Quantity") or 0)
                commodity_id = int(row.get("Commodity ID"))
            except Exception:
                invalid_rows.append(str(row.get("Commodity") or row.get("Commodity ID") or "Unknown"))
                continue
            if quantity <= 0:
                invalid_rows.append(str(row.get("Commodity") or commodity_id))
                continue
            save_items.append(
                {
                    "commodity_id": commodity_id,
                    "weight_quantity": quantity,
                    "item_note": str(row.get("Note") or "").strip() or None,
                }
            )

        if invalid_rows:
            st.error("Selected commodities need a positive quantity: " + ", ".join(invalid_rows))
        elif not save_items:
            st.error("Select at least one commodity for the basket.")
        else:
            try:
                basket_resp = request_json(
                    "POST",
                    f"/market-monitor/countries/{quote_path_param(country)}/basket",
                    json_body={
                        "items": save_items,
                        "change_note": change_note or None,
                        "created_by_user_id": "streamlit",
                    },
                    timeout=60,
                )
                basket_cache[country] = basket_resp
                active_basket = basket_resp.get("active_basket") if isinstance(basket_resp, dict) else None
                basket_items = active_basket.get("items") if isinstance(active_basket, dict) else []
                basket_item_names = _dedupe_text(_basket_item_name(item) for item in basket_items)
                st.success("Food basket saved and published.")
            except Exception as e:
                safe_show_error(e)

if not active_basket:
    st.info("Report generation is disabled until the country food basket is saved.")

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

    valid_commodities = [c for c in commodities if isinstance(c, str)]
    default_candidates = [c for c in default_commodities if c in valid_commodities]
    locked_basket_names = [name for name in basket_item_names if name]
    additional_options = [name for name in valid_commodities if name not in locked_basket_names]
    default_additional = [name for name in default_candidates if name in additional_options]
    if locked_basket_names:
        st.caption("Basket commodities are locked into every run: " + ", ".join(locked_basket_names))
    additional_commodities = st.multiselect(
        "Additional commodities",
        options=additional_options,
        default=default_additional,
        key=f"mm_additional_commodities_{country}" if isinstance(country, str) and country else "mm_additional_commodities",
    )
    commodity_list = _dedupe_text(locked_basket_names + additional_commodities)

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

    submitted = st.form_submit_button("Run", disabled=not bool(active_basket))

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
            "use_mock_data": False,
            "basket_version_id": active_basket.get("basket_version_id") if isinstance(active_basket, dict) else None,
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

        cache_metadata = result.get("cache_metadata")
        if isinstance(cache_metadata, dict) and cache_metadata:
            with st.expander("Cache metadata", expanded=False):
                st.json(cache_metadata)

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

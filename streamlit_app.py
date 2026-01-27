import streamlit as st

from streamlit_shared import (
    apply_wfp_theme,
    get_backend_base_url,
    render_wfp_sidebar_logo,
    set_backend_base_url,
)

st.set_page_config(page_title="VAM LLM Testing App", layout="wide")
apply_wfp_theme()

with st.sidebar:
    st.markdown("## VAM LLM Testing App")
    render_wfp_sidebar_logo()
    backend_url = st.text_input("Backend Base URL", value=get_backend_base_url())
    set_backend_base_url(backend_url)

st.title("VAM LLM Testing App")


def _go_to(page_path: str) -> None:
    if hasattr(st, "switch_page"):
        st.switch_page(page_path)
        return
    st.info("Use the pages in the left sidebar to access each service.")


st.markdown(
    """
    <style>
    .landing-menu {
        max-width: 760px;
        margin: 2.5rem auto 0 auto;
    }
    .landing-menu [data-testid="stMarkdownContainer"] h3 {
        text-align: center;
        margin-bottom: 1.5rem;
    }
    .landing-menu .stButton > button,
    .landing-menu div[data-testid^="baseButton-"] button,
    .landing-menu div[data-testid="stFormSubmitButton"] button,
    .landing-menu .stDownloadButton > button {
        width: 100% !important;
        height: 78px !important;
        font-size: 1.05rem !important;
        padding: 0.85rem 1.1rem !important;
        justify-content: center !important;
        margin-bottom: 1rem !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="landing-menu">', unsafe_allow_html=True)

st.markdown("### Select a service")
left, right = st.columns(2)

with left:
    if st.button(
        "MFI Dataset Validator",
        type="primary",
        key="nav_mfi_validator",
        use_container_width=True,
    ):
        _go_to("pages/1_MFI_Validator.py")

    if st.button(
        "Price Bulletin Drafter",
        type="primary",
        key="nav_price_bulletin",
        use_container_width=True,
    ):
        _go_to("pages/3_Price_Bulletin_Drafter.py")

with right:
    if st.button(
        "Price Data Validator",
        type="primary",
        key="nav_price_validator",
        use_container_width=True,
    ):
        _go_to("pages/2_Price_Validator.py")

    if st.button(
        "MFI Report Generator",
        type="primary",
        key="nav_mfi_drafter",
        use_container_width=True,
    ):
        _go_to("pages/4_MFI_Drafter.py")

st.markdown("</div>", unsafe_allow_html=True)

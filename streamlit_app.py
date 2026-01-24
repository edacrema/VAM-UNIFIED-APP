import streamlit as st

from streamlit_shared import (
    get_backend_base_url,
    request_json,
    safe_show_error,
    set_backend_base_url,
)

st.set_page_config(page_title="VAM Unified App", layout="wide")

with st.sidebar:
    backend_url = st.text_input("Backend Base URL", value=get_backend_base_url())
    set_backend_base_url(backend_url)

st.title("VAM Unified App")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Backend Health")
    try:
        health = request_json("GET", "/health", timeout=10)
        st.json(health)
    except Exception as e:
        safe_show_error(e)

with col2:
    st.subheader("Services")
    try:
        root = request_json("GET", "/", timeout=10)
        st.json(root)
    except Exception as e:
        safe_show_error(e)

st.info("Use the pages in the left sidebar to access each service.")

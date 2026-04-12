from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="KR Quant Studio",
    layout="wide",
)

pages = [
    st.Page(
        "pages/screener.py",
        title="Sector Screener",
        icon=":material/search:",
        default=True,
    ),
    st.Page(
        "pages/operating_leverage.py",
        title="Operating Leverage",
        icon=":material/monitoring:",
    ),
    st.Page(
        "pages/gpm_regression.py",
        title="GPM Regression",
        icon=":material/scatter_plot:",
    ),
]

st.navigation(pages).run()

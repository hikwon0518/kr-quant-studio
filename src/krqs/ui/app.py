from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="KR Quant Studio",
    layout="wide",
)

pages = [
    st.Page(
        "pages/operating_leverage.py",
        title="Operating Leverage",
        icon=":material/monitoring:",
        default=True,
    ),
    st.Page(
        "pages/gpm_regression.py",
        title="GPM Regression",
        icon=":material/scatter_plot:",
    ),
]

st.navigation(pages).run()

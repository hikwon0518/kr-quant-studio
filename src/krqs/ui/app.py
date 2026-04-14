from __future__ import annotations

import streamlit as st

from krqs.ui.state import get_db

st.set_page_config(
    page_title="KR Quant Studio",
    page_icon="https://em-content.zobj.net/source/twitter/408/chart-increasing_1f4c8.png",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    /* Global typography */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    /* Sidebar polish */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0E1117 0%, #141924 100%);
    }
    section[data-testid="stSidebar"] hr { border-color: #2A3040; }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1A1F2E 0%, #1E2536 100%);
        border: 1px solid #2A3040;
        border-radius: 10px;
        padding: 14px 18px;
    }
    div[data-testid="stMetric"] label { color: #8B95A5 !important; font-size: 0.78rem; }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-weight: 700; font-size: 1.5rem;
    }

    /* Dataframe styling */
    .stDataFrame { border-radius: 8px; overflow: hidden; }

    /* Buttons */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #00D4AA 0%, #00B896 100%);
        border: none; color: #0E1117; font-weight: 600;
        border-radius: 8px; transition: all 0.2s;
    }
    .stButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #00E8BB 0%, #00D4AA 100%);
        box-shadow: 0 4px 12px rgba(0, 212, 170, 0.3);
    }

    /* Download button */
    .stDownloadButton > button {
        border: 1px solid #2A3040; border-radius: 8px;
        background: transparent; color: #E6EDF3;
    }
    .stDownloadButton > button:hover { border-color: #00D4AA; color: #00D4AA; }

    /* Expander headers */
    .streamlit-expanderHeader { font-weight: 600; }

    /* Hide default hamburger + footer */
    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }

    /* Hide anchor icon next to page titles */
    [data-testid="stHeaderActionElements"] {display: none;}

    /* Brand header */
    .brand-header {
        display: flex; align-items: center; gap: 10px;
        padding: 4px 0 12px 0; margin-bottom: 8px;
        border-bottom: 1px solid #2A3040;
    }
    .brand-header h3 { margin: 0; color: #00D4AA; font-weight: 700; letter-spacing: -0.02em; }
    .brand-header span { color: #5A6577; font-size: 0.8rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown(
        '<div class="brand-header">'
        '<h3>KR Quant Studio</h3>'
        '<span>v0.1</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    try:
        con = get_db()
        row = con.execute(
            "SELECT MAX(source_updated_at) AS latest FROM financials_quarterly"
        ).fetchone()
        if row and row[0]:
            st.caption(f"최종 데이터: {row[0]}")
    except Exception:
        pass

pages = [
    st.Page(
        "views/growth_valuation.py",
        title="Growth x Valuation",
        icon=":material/trending_up:",
        default=True,
    ),
    st.Page(
        "views/screener.py",
        title="Sector Screener",
        icon=":material/search:",
    ),
    st.Page(
        "views/operating_leverage.py",
        title="Operating Leverage",
        icon=":material/monitoring:",
    ),
    st.Page(
        "views/gpm_regression.py",
        title="GPM Regression",
        icon=":material/scatter_plot:",
    ),
    st.Page(
        "views/log_analysis.py",
        title="Log-Scale Analysis",
        icon=":material/show_chart:",
    ),
    st.Page(
        "views/sanity_check.py",
        title="Sanity Check",
        icon=":material/fact_check:",
    ),
    st.Page(
        "views/guide.py",
        title="Guide",
        icon=":material/menu_book:",
    ),
]

st.navigation(pages).run()

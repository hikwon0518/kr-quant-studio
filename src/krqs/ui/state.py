from __future__ import annotations

import streamlit as st

from krqs.data.db.connection import get_connection, initialize_schema, load_seed_data

BN = 100_000_000


@st.cache_resource
def get_db():
    con = get_connection()
    initialize_schema(con)
    load_seed_data(con)
    return con


def init_simulator_state() -> None:
    defaults = {
        "revenue_bn": 50_000.0,
        "cogs_bn": 40_000.0,
        "sga_bn": 3_000.0,
        "interest_bn": 500.0,
        "gpm_low": 0.12,
        "gpm_mid": 0.20,
        "gpm_high": 0.28,
        "selected_corp_code": None,
        "selected_corp_label": "",
        "selected_fiscal_year": None,
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)

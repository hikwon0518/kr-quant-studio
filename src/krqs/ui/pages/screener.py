from __future__ import annotations

import streamlit as st

from krqs.services.screener_service import get_available_years, screen_companies
from krqs.ui.state import BN, get_db

con = get_db()

st.title("Sector Screener")
st.caption("상장 기업 재무 지표 필터 및 정렬")

# ── Sidebar: filters ────────────────────────────────────────────────
with st.sidebar:
    st.header("필터 설정")

    available_years = get_available_years(con)
    if not available_years:
        st.warning("DB에 재무 데이터가 없습니다. 먼저 데이터를 동기화하세요.")
        st.stop()

    fiscal_year = st.selectbox("회계연도", available_years, index=0)

    st.subheader("수익성")
    opm_range = st.slider(
        "OPM (%)",
        min_value=0.0,
        max_value=100.0,
        value=(0.0, 100.0),
        step=1.0,
        format="%.0f%%",
        key="filter_opm",
    )
    min_gpm = st.slider(
        "GPM 최소 (%)",
        min_value=0.0,
        max_value=100.0,
        value=0.0,
        step=1.0,
        format="%.0f%%",
        key="filter_gpm",
    )
    min_roe = st.slider(
        "ROE 최소 (%)",
        min_value=-100.0,
        max_value=100.0,
        value=-100.0,
        step=1.0,
        format="%.0f%%",
        key="filter_roe",
    )
    min_ebitda_margin = st.slider(
        "EBITDA Margin 최소 (%)",
        min_value=0.0,
        max_value=100.0,
        value=0.0,
        step=1.0,
        format="%.0f%%",
        key="filter_ebitda_margin",
    )

    st.subheader("안정성")
    max_debt_ratio = st.slider(
        "부채비율 최대 (%)",
        min_value=0.0,
        max_value=100.0,
        value=100.0,
        step=1.0,
        format="%.0f%%",
        key="filter_debt_ratio",
    )

    st.subheader("규모")
    min_revenue_bn = st.number_input(
        "매출 최소 (억원)",
        min_value=0,
        value=0,
        step=100,
        key="filter_revenue",
    )

    st.divider()
    st.subheader("정렬")

    sort_options = {
        "OPM": "opm",
        "GPM": "gpm",
        "ROE": "roe",
        "부채비율": "debt_ratio",
        "매출": "revenue",
        "영업이익": "operating_income",
        "순이익": "net_income",
        "EBITDA": "ebitda",
        "EBITDA Margin": "ebitda_margin",
        "총자산": "total_assets",
        "자기자본": "total_equity",
    }
    sort_label = st.selectbox("정렬 기준", list(sort_options.keys()), index=0)
    sort_by = sort_options[sort_label]
    sort_desc = st.radio("정렬 방향", ["내림차순", "오름차순"], horizontal=True) == "내림차순"

    limit = st.number_input("최대 표시 종목 수", min_value=10, max_value=1000, value=100, step=10)

    run_search = st.button("검색", type="primary", use_container_width=True)

# ── Resolve filter values ────────────────────────────────────────────
# Convert percentage UI values to ratios (0-1 scale) for the DB query.
# Only apply a filter when the user has moved it from the default "no filter" position.
min_opm = opm_range[0] / 100.0 if opm_range[0] > 0.0 else None
max_opm = opm_range[1] / 100.0 if opm_range[1] < 100.0 else None
min_gpm_val = min_gpm / 100.0 if min_gpm > 0.0 else None
min_roe_val = min_roe / 100.0 if min_roe > -100.0 else None
max_debt_val = max_debt_ratio / 100.0 if max_debt_ratio < 100.0 else None
min_ebitda_val = min_ebitda_margin / 100.0 if min_ebitda_margin > 0.0 else None
min_rev_won = int(min_revenue_bn * BN) if min_revenue_bn > 0 else None

# Always run on first load; re-run when user clicks search.
if "screener_ran" not in st.session_state:
    st.session_state["screener_ran"] = True
    run_search = True

if not run_search:
    st.info("사이드바에서 필터를 설정한 뒤 **검색** 버튼을 누르세요.")
    st.stop()

# ── Query ────────────────────────────────────────────────────────────
df = screen_companies(
    con,
    fiscal_year=fiscal_year,
    min_opm=min_opm,
    max_opm=max_opm,
    min_gpm=min_gpm_val,
    min_roe=min_roe_val,
    max_debt_ratio=max_debt_val,
    min_revenue=min_rev_won,
    min_ebitda_margin=min_ebitda_val,
    sort_by=sort_by,
    sort_desc=sort_desc,
    limit=limit,
)

# ── Display results ──────────────────────────────────────────────────
st.subheader(f"{len(df):,}개 종목 발견  (FY{fiscal_year})")

if df.empty:
    st.warning("조건에 맞는 종목이 없습니다. 필터를 완화해보세요.")
    st.stop()

# Build a display copy with readable formatting.
display = df.copy()

# Money columns: convert from won to 억원.
money_cols = ["revenue", "operating_income", "net_income", "total_assets", "total_equity", "ebitda"]
for col in money_cols:
    if col in display.columns:
        display[col] = display[col].apply(lambda v: round(v / BN) if v is not None else None)

# Ratio columns: format as percentages.
ratio_cols = ["gpm", "opm", "roe", "debt_ratio", "ebitda_margin"]
for col in ratio_cols:
    if col in display.columns:
        display[col] = display[col].apply(lambda v: f"{v:.1%}" if v is not None else "-")

# Korean column names for display.
col_rename = {
    "corp_name": "종목명",
    "stock_code": "종목코드",
    "market": "시장",
    "fiscal_year": "회계연도",
    "revenue": "매출(억)",
    "operating_income": "영업이익(억)",
    "net_income": "순이익(억)",
    "total_assets": "총자산(억)",
    "total_equity": "자기자본(억)",
    "gpm": "GPM",
    "opm": "OPM",
    "roe": "ROE",
    "debt_ratio": "부채비율",
    "ebitda": "EBITDA(억)",
    "ebitda_margin": "EBITDA Margin",
}
display = display.rename(columns=col_rename)

st.dataframe(display, width="stretch", hide_index=True)

# ── CSV download ─────────────────────────────────────────────────────
csv_bytes = display.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="CSV 다운로드",
    data=csv_bytes,
    file_name=f"screener_FY{fiscal_year}.csv",
    mime="text/csv",
    use_container_width=True,
)

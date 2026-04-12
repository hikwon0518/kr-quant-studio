from __future__ import annotations

import pandas as pd
import streamlit as st

from krqs.services.screener_service import get_available_years, screen_companies
from krqs.ui.state import BN, get_db

con = get_db()

st.title("Sector Screener")
st.caption("DART 공시 기반 상장 기업 재무 스크리닝")

# ── Sidebar: filters ────────────────────────────────────────────────
with st.sidebar:
    available_years = get_available_years(con)
    if not available_years:
        st.warning("DB에 재무 데이터가 없습니다. 먼저 데이터를 동기화하세요.")
        st.stop()

    fiscal_year = st.selectbox("회계연도", available_years, index=0)

    with st.expander("수익성 필터", expanded=True):
        opm_range = st.slider(
            "OPM (%)", 0.0, 100.0, (0.0, 100.0), 1.0,
            format="%.0f%%", key="filter_opm",
        )
        min_gpm = st.slider(
            "GPM 최소 (%)", 0.0, 100.0, 0.0, 1.0,
            format="%.0f%%", key="filter_gpm",
        )
        min_roe = st.slider(
            "ROE 최소 (%)", -100.0, 100.0, -100.0, 1.0,
            format="%.0f%%", key="filter_roe",
        )
        min_ebitda_margin = st.slider(
            "EBITDA Margin 최소 (%)", 0.0, 100.0, 0.0, 1.0,
            format="%.0f%%", key="filter_ebitda_margin",
        )

    with st.expander("안정성 필터"):
        max_debt_ratio = st.slider(
            "부채비율 최대 (%)", 0.0, 100.0, 100.0, 1.0,
            format="%.0f%%", key="filter_debt_ratio",
        )

    with st.expander("규모 필터"):
        min_revenue_bn = st.number_input(
            "매출 최소 (억원)", min_value=0, value=0, step=100,
            key="filter_revenue",
        )

    st.divider()

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
    sort_desc = st.radio(
        "정렬 방향", ["내림차순", "오름차순"], horizontal=True
    ) == "내림차순"

    limit = st.number_input(
        "최대 표시 종목 수", min_value=10, max_value=1000, value=100, step=10
    )

    run_search = st.button("검색", type="primary", use_container_width=True)

# ── Build active filters ────────────────────────────────────────────
min_opm = opm_range[0] / 100.0 if opm_range[0] > 0.0 else None
max_opm = opm_range[1] / 100.0 if opm_range[1] < 100.0 else None
min_gpm_val = min_gpm / 100.0 if min_gpm > 0.0 else None
min_roe_val = min_roe / 100.0 if min_roe > -100.0 else None
max_debt_val = max_debt_ratio / 100.0 if max_debt_ratio < 100.0 else None
min_ebitda_val = min_ebitda_margin / 100.0 if min_ebitda_margin > 0.0 else None
min_rev_won = int(min_revenue_bn * BN) if min_revenue_bn > 0 else None

active_filters: list[str] = []
if min_opm is not None:
    active_filters.append(f"OPM >= {opm_range[0]:.0f}%")
if max_opm is not None:
    active_filters.append(f"OPM <= {opm_range[1]:.0f}%")
if min_gpm_val is not None:
    active_filters.append(f"GPM >= {min_gpm:.0f}%")
if min_roe_val is not None:
    active_filters.append(f"ROE >= {min_roe:.0f}%")
if max_debt_val is not None:
    active_filters.append(f"D/A <= {max_debt_ratio:.0f}%")
if min_ebitda_val is not None:
    active_filters.append(f"EBITDA M >= {min_ebitda_margin:.0f}%")
if min_rev_won is not None:
    active_filters.append(f"매출 >= {min_revenue_bn:,}억")

# Auto-run on first load
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

# ── Active filter chips ─────────────────────────────────────────────
if active_filters:
    chips = " ".join(
        f'<span style="background:#1E2536;border:1px solid #2A3040;border-radius:6px;'
        f'padding:3px 10px;font-size:0.78rem;color:#00D4AA;margin-right:4px;">'
        f'{f}</span>'
        for f in active_filters
    )
    st.markdown(chips, unsafe_allow_html=True)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ── KPI summary cards ───────────────────────────────────────────────
st.subheader(f"{len(df):,}개 종목 발견  ·  FY{fiscal_year}")

if df.empty:
    st.markdown(
        '<div style="text-align:center;padding:60px 0;color:#5A6577;">'
        '<p style="font-size:2rem;margin-bottom:8px;">0</p>'
        '<p>조건에 맞는 종목이 없습니다</p>'
        '<p style="font-size:0.85rem;">필터를 완화하거나 회계연도를 변경해보세요</p>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.stop()

# Summary metrics from results
c1, c2, c3, c4, c5 = st.columns(5)
_opm = pd.to_numeric(df["opm"], errors="coerce")
_roe = pd.to_numeric(df["roe"], errors="coerce")
_debt = pd.to_numeric(df["debt_ratio"], errors="coerce")
_rev = pd.to_numeric(df["revenue"], errors="coerce")
_gpm = pd.to_numeric(df["gpm"], errors="coerce")

c1.metric("평균 OPM", f"{_opm.mean():.1%}" if _opm.notna().any() else "-")
c2.metric("평균 ROE", f"{_roe.mean():.1%}" if _roe.notna().any() else "-")
c3.metric("중앙 GPM", f"{_gpm.median():.1%}" if _gpm.notna().any() else "-")
c4.metric("평균 부채비율", f"{_debt.mean():.1%}" if _debt.notna().any() else "-")
c5.metric("합산 매출", f"{_rev.sum() / BN:,.0f}억" if _rev.notna().any() else "-")

st.divider()

# ── Results table ────────────────────────────────────────────────────
display = df.copy()

money_cols = [
    "revenue", "operating_income", "net_income",
    "total_assets", "total_equity", "ebitda",
]
for col in money_cols:
    if col in display.columns:
        display[col] = pd.to_numeric(display[col], errors="coerce")
        display[col] = (display[col] / BN).round(0).astype("Int64")

ratio_cols = ["gpm", "opm", "roe", "debt_ratio", "ebitda_margin"]
for col in ratio_cols:
    if col in display.columns:
        display[col] = pd.to_numeric(display[col], errors="coerce")
        display[col] = display[col].apply(
            lambda v: f"{v:.1%}" if pd.notna(v) else "-"
        )

col_rename = {
    "corp_name": "종목명",
    "stock_code": "종목코드",
    "market": "시장",
    "fiscal_year": "FY",
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
    "ebitda_margin": "EBITDA%",
}
display = display.rename(columns=col_rename)

st.dataframe(display, use_container_width=True, hide_index=True, height=500)

# ── Download ─────────────────────────────────────────────────────────
csv_bytes = display.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="CSV 다운로드",
    data=csv_bytes,
    file_name=f"screener_FY{fiscal_year}.csv",
    mime="text/csv",
    use_container_width=True,
)

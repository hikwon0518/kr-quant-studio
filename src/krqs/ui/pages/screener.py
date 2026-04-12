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

csv_bytes = display.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="CSV 다운로드",
    data=csv_bytes,
    file_name=f"screener_FY{fiscal_year}.csv",
    mime="text/csv",
    use_container_width=True,
)

# ══════════════════════════════════════════════════════════════════════
# TREND TAB: multi-year metric trends
# ══════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("연도별 추세 분석")
st.caption("여러 해에 걸친 지표 변화를 추적합니다. 성장률의 방향(로그 추세)이 핵심입니다.")

from krqs.services.screener_service import get_trend_data

tcol1, tcol2, tcol3, tcol4 = st.columns(4)

trend_metric_options = {
    "OPM": "opm",
    "GPM": "gpm",
    "ROE": "roe",
    "매출": "revenue",
    "영업이익": "operating_income",
    "EBITDA Margin": "ebitda_margin",
    "부채비율": "debt_ratio",
}
trend_label = tcol1.selectbox("추세 지표", list(trend_metric_options.keys()), key="trend_metric")
trend_metric = trend_metric_options[trend_label]

trend_years = tcol2.multiselect(
    "비교 연도",
    sorted(available_years, reverse=True),
    default=sorted(available_years, reverse=True)[:3],
    key="trend_years",
)
only_improving = tcol3.checkbox("연속 상승만", key="trend_improving")
trend_sort = tcol4.selectbox(
    "정렬", ["최신값 높은순", "YoY 변화 높은순"], key="trend_sort"
)

if len(trend_years) < 2:
    st.info("추세 분석에는 최소 2개 연도가 필요합니다.")
else:
    trend_df = get_trend_data(
        con,
        years=sorted(trend_years),
        metric=trend_metric,
        min_years=2,
        only_improving=only_improving,
        sort_by="yoy_change" if "YoY" in trend_sort else "latest",
        limit=100,
    )

    if trend_df.empty:
        st.warning("조건에 맞는 추세 데이터가 없습니다.")
    else:
        st.caption(f"{len(trend_df)}개 종목 · {trend_label} 추세")

        # Format the trend table
        trend_display = trend_df.copy()
        year_cols = sorted([c for c in trend_display.columns if isinstance(c, int)])
        is_ratio = trend_metric in ("opm", "gpm", "roe", "debt_ratio", "ebitda_margin")

        for col in year_cols + ["latest"]:
            if col in trend_display.columns:
                if is_ratio:
                    trend_display[col] = pd.to_numeric(trend_display[col], errors="coerce").apply(
                        lambda v: f"{v:.1%}" if pd.notna(v) else "-"
                    )
                else:
                    trend_display[col] = pd.to_numeric(trend_display[col], errors="coerce").apply(
                        lambda v: f"{v / BN:,.0f}" if pd.notna(v) else "-"
                    )

        if "yoy_change" in trend_display.columns:
            if is_ratio:
                trend_display["yoy_change"] = pd.to_numeric(
                    trend_df["yoy_change"], errors="coerce"
                ).apply(lambda v: f"{v:+.1%}p" if pd.notna(v) else "-")
            else:
                trend_display["yoy_change"] = pd.to_numeric(
                    trend_df["yoy_change"], errors="coerce"
                ).apply(lambda v: f"{v / BN:+,.0f}" if pd.notna(v) else "-")

        # Rename columns
        rename_map = {
            "corp_name": "종목명",
            "stock_code": "종목코드",
            "market": "시장",
            "data_years": "데이터(년)",
            "latest": f"최신({trend_label})",
            "yoy_change": "YoY 변화",
        }
        for y in year_cols:
            rename_map[y] = f"FY{y}"

        show_cols = ["corp_name", "stock_code"] + year_cols + ["yoy_change"]
        show_cols = [c for c in show_cols if c in trend_display.columns]
        trend_display = trend_display[show_cols].rename(columns=rename_map)

        st.dataframe(trend_display, use_container_width=True, hide_index=True, height=400)

        trend_csv = trend_display.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="추세 CSV 다운로드",
            data=trend_csv,
            file_name=f"trend_{trend_metric}.csv",
            mime="text/csv",
            use_container_width=True,
        )

# ══════════════════════════════════════════════════════════════════════
# GROWTH ANALYSIS: CAGR, acceleration, earnings growth
# ══════════════════════════════════════════════════════════════════════
st.divider()
st.subheader("성장 분석")
st.caption(
    "CAGR(복합성장률), 성장 가속도(이계도함수), 이익 성장률(PEG의 G)을 계산합니다. "
    "로그 추세가 돌파하면 something special."
)

from krqs.services.screener_service import get_growth_analysis

gcol1, gcol2, gcol3 = st.columns(3)
growth_years = gcol1.multiselect(
    "분석 기간",
    sorted(available_years, reverse=True),
    default=sorted(available_years, reverse=True)[:4],
    key="growth_years",
)
growth_sort_options = {
    "매출 CAGR": "rev_cagr",
    "영업이익 CAGR": "op_cagr",
    "이익 성장률": "earnings_growth",
    "성장 가속도": "accel",
    "최신 매출": "latest_rev",
    "최신 OPM": "latest_opm",
}
growth_sort_label = gcol2.selectbox("정렬", list(growth_sort_options.keys()), key="growth_sort")
growth_limit = gcol3.number_input("표시 종목 수", 10, 500, 100, 10, key="growth_limit")

if len(growth_years) < 3:
    st.info("성장 분석에는 최소 3개 연도가 필요합니다.")
else:
    growth_df = get_growth_analysis(
        con,
        years=sorted(growth_years),
        min_years=3,
        sort_by=growth_sort_options[growth_sort_label],
        limit=growth_limit,
    )

    if growth_df.empty:
        st.warning("3개년 이상 데이터가 있는 종목이 없습니다.")
    else:
        st.caption(f"{len(growth_df)}개 종목 · {growth_sort_label} 순")

        gd = growth_df.copy()
        gd["latest_rev"] = pd.to_numeric(gd["latest_rev"], errors="coerce").apply(
            lambda v: f"{v / BN:,.0f}" if pd.notna(v) else "-"
        )
        gd["latest_opm"] = pd.to_numeric(gd["latest_opm"], errors="coerce").apply(
            lambda v: f"{v:.1%}" if pd.notna(v) else "-"
        )
        for col in ["rev_cagr", "op_cagr", "earnings_growth"]:
            gd[col] = pd.to_numeric(gd[col], errors="coerce").apply(
                lambda v: f"{v:+.1%}" if pd.notna(v) else "-"
            )
        gd["accel"] = pd.to_numeric(gd["accel"], errors="coerce").apply(
            lambda v: f"{v:+.1%}p" if pd.notna(v) else "-"
        )

        gd = gd.rename(columns={
            "corp_name": "종목명",
            "stock_code": "코드",
            "market": "시장",
            "years": "기간",
            "data_points": "N",
            "latest_rev": "최신매출(억)",
            "latest_opm": "최신OPM",
            "rev_cagr": "매출CAGR",
            "op_cagr": "OP CAGR",
            "earnings_growth": "이익성장률",
            "accel": "성장가속도",
        })

        st.dataframe(gd, use_container_width=True, hide_index=True, height=400)

        growth_csv = gd.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="성장분석 CSV 다운로드",
            data=growth_csv,
            file_name="growth_analysis.csv",
            mime="text/csv",
            use_container_width=True,
        )

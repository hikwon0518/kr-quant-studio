from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from krqs.services.growth_valuation_service import get_growth_valuation_table
from krqs.services.screener_service import get_available_years
from krqs.ui.formatters import format_krw
from krqs.ui.state import BN, get_db

con = get_db()

st.title("Growth x Valuation")
st.caption(
    "성장률과 밸류에이션을 결합한 핵심 스크리닝. "
    "PEG < 1은 성장 대비 저평가, PEG > 2는 고평가를 시사합니다."
)

# ── Sidebar: filters ────────────────────────────────────────────────
with st.sidebar:
    available_years = get_available_years(con)
    if not available_years:
        st.warning("DB에 재무 데이터가 없습니다. 먼저 데이터를 동기화하세요.")
        st.stop()

    fiscal_year = st.selectbox(
        "회계연도", available_years, index=0, key="gv_fiscal_year",
    )

    with st.expander("성장 필터", expanded=True):
        min_years = st.slider(
            "최소 데이터 연수", 2, 5, 2, key="gv_min_years",
        )

    with st.expander("밸류에이션 필터", expanded=True):
        max_per = st.slider(
            "최대 PER", 5, 100, 100, 1,
            help="100 = 필터 없음",
            key="gv_max_per",
        )
        max_peg = st.slider(
            "최대 PEG", 0.0, 5.0, 5.0, 0.1,
            help="5.0 = 필터 없음",
            key="gv_max_peg",
        )

    st.divider()

    sort_options = {
        "PEG": "peg",
        "매출CAGR": "rev_cagr",
        "이익성장률": "earnings_growth",
        "PER": "per",
        "성장가속도": "accel",
    }
    sort_label = st.selectbox(
        "정렬 기준", list(sort_options.keys()), index=0, key="gv_sort",
    )
    sort_by = sort_options[sort_label]

    limit = st.number_input(
        "최대 표시 종목 수", min_value=10, max_value=500, value=50, step=10,
        key="gv_limit",
    )

# ── Query ────────────────────────────────────────────────────────────
df = get_growth_valuation_table(
    con,
    fiscal_year=fiscal_year,
    min_years=min_years,
    limit=500,  # fetch more, filter/sort in Python
)

if df.empty:
    st.markdown(
        '<div style="text-align:center;padding:60px 0;color:#5A6577;">'
        '<p style="font-size:2rem;margin-bottom:8px;">0</p>'
        '<p>성장 + 가격 데이터가 있는 종목이 없습니다</p>'
        '<p style="font-size:0.85rem;">데이터를 동기화하거나 필터를 완화해보세요</p>'
        "</div>",
        unsafe_allow_html=True,
    )
    st.stop()

# ── Apply numeric coercion ──────────────────────────────────────────
for col in ["per", "pbr", "peg", "rev_cagr", "op_cagr",
            "earnings_growth", "accel", "close", "marcap",
            "latest_rev", "latest_opm"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Apply valuation filters ─────────────────────────────────────────
active_filters: list[str] = []

if max_per < 100:
    df = df[df["per"].isna() | (df["per"] <= max_per)]
    active_filters.append(f"PER <= {max_per}")

if max_peg < 5.0:
    df = df[df["peg"].isna() | (df["peg"] <= max_peg)]
    active_filters.append(f"PEG <= {max_peg:.1f}")

# ── Sort ─────────────────────────────────────────────────────────────
if sort_by in df.columns:
    ascending = sort_by in ("peg", "per")  # lower is better for PEG/PER
    df = df.sort_values(sort_by, ascending=ascending, na_position="last")

df = df.head(limit).reset_index(drop=True)

# ── Filter chips ────────────────────────────────────────────────────
if active_filters:
    chips = " ".join(
        f'<span style="background:#1E2536;border:1px solid #2A3040;border-radius:6px;'
        f'padding:3px 10px;font-size:0.78rem;color:#00D4AA;margin-right:4px;">'
        f"{f}</span>"
        for f in active_filters
    )
    st.markdown(chips, unsafe_allow_html=True)
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

# ── KPI row ─────────────────────────────────────────────────────────
st.subheader(f"{len(df):,}개 종목  ·  FY{fiscal_year}")

c1, c2, c3, c4 = st.columns(4)

_peg = df["peg"].dropna()
_per = df["per"].dropna()
_eg = df["earnings_growth"].dropna()

c1.metric("중앙 PEG", f"{_peg.median():.2f}" if not _peg.empty else "-")
c2.metric("중앙 PER", f"{_per.median():.1f}배" if not _per.empty else "-")
c3.metric(
    "중앙 이익성장률",
    f"{_eg.median():.1%}" if not _eg.empty else "-",
)

# Breakout count: PEG < 1 and earnings_growth > 0
breakout_mask = (df["peg"] < 1) & (df["earnings_growth"] > 0)
breakout_count = int(breakout_mask.sum())
c4.metric("PEG<1 종목", f"{breakout_count}개")

st.divider()

# ══════════════════════════════════════════════════════════════════════
# SCATTER PLOT: The key visual
# ══════════════════════════════════════════════════════════════════════
st.subheader("Growth vs. Valuation Scatter")
st.caption("X = 이익성장률, Y = PER, 색상 = PEG 구간, 크기 = 시총")

scatter_df = df.dropna(subset=["earnings_growth", "per"]).copy()

if scatter_df.empty:
    st.info("이익성장률과 PER 데이터가 모두 있는 종목이 없습니다.")
else:
    # PEG bucket for colour
    def _peg_bucket(val: float | None) -> str:
        if pd.isna(val):
            return "N/A"
        if val < 1:
            return "PEG < 1 (저평가)"
        if val <= 2:
            return "PEG 1~2 (적정)"
        return "PEG > 2 (고평가)"

    scatter_df["peg_bucket"] = scatter_df["peg"].apply(_peg_bucket)

    # Marcap in 억 for size
    scatter_df["marcap_eok"] = pd.to_numeric(
        scatter_df["marcap"], errors="coerce"
    ) / BN

    # Earnings growth as % for axis
    scatter_df["eg_pct"] = scatter_df["earnings_growth"] * 100

    color_scale = alt.Scale(
        domain=["PEG < 1 (저평가)", "PEG 1~2 (적정)", "PEG > 2 (고평가)", "N/A"],
        range=["#00D4AA", "#FFD700", "#FF4B4B", "#5A6577"],
    )

    points = (
        alt.Chart(scatter_df)
        .mark_circle(opacity=0.75, stroke="#0E1117", strokeWidth=0.5)
        .encode(
            x=alt.X("eg_pct:Q", title="이익성장률 (%)"),
            y=alt.Y("per:Q", title="PER (배)", scale=alt.Scale(zero=False)),
            color=alt.Color(
                "peg_bucket:N",
                title="PEG 구간",
                scale=color_scale,
                legend=alt.Legend(orient="bottom"),
            ),
            size=alt.Size(
                "marcap_eok:Q",
                title="시총 (억)",
                scale=alt.Scale(range=[30, 600]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("corp_name:N", title="종목명"),
                alt.Tooltip("stock_code:N", title="종목코드"),
                alt.Tooltip("eg_pct:Q", title="이익성장률(%)", format=".1f"),
                alt.Tooltip("per:Q", title="PER", format=".1f"),
                alt.Tooltip("peg:Q", title="PEG", format=".2f"),
                alt.Tooltip("marcap_eok:Q", title="시총(억)", format=",.0f"),
                alt.Tooltip("rev_cagr:Q", title="매출CAGR", format=".1%"),
                alt.Tooltip("accel:Q", title="가속도", format="+.1%"),
            ],
        )
    )

    # PEG = 1 diagonal reference line: PER = earnings_growth(%) * 1
    # => per = eg_pct * 1.0 (when PEG=1)
    eg_min = max(scatter_df["eg_pct"].min(), 0.1)
    eg_max = scatter_df["eg_pct"].max()
    if eg_max > eg_min:
        line_df = pd.DataFrame({
            "eg_pct": [eg_min, eg_max],
            "per_ref": [eg_min * 1.0, eg_max * 1.0],
        })

        peg_line = (
            alt.Chart(line_df)
            .mark_line(color="#FF6B6B", strokeDash=[6, 4], strokeWidth=1.5)
            .encode(
                x=alt.X("eg_pct:Q"),
                y=alt.Y("per_ref:Q"),
            )
        )

        # Label for PEG=1 line
        label_df = pd.DataFrame({
            "eg_pct": [eg_max],
            "per_ref": [eg_max * 1.0],
            "label": ["PEG = 1"],
        })
        peg_label = (
            alt.Chart(label_df)
            .mark_text(
                align="left", dx=5, dy=-8,
                color="#FF6B6B", fontSize=11, fontWeight="bold",
            )
            .encode(
                x=alt.X("eg_pct:Q"),
                y=alt.Y("per_ref:Q"),
                text="label:N",
            )
        )

        scatter_chart = alt.layer(points, peg_line, peg_label)
    else:
        scatter_chart = points

    scatter_chart = (
        scatter_chart
        .properties(height=480)
        .configure_axis(
            gridColor="#1E2536",
            labelColor="#8B95A5",
            titleColor="#8B95A5",
        )
        .configure_title(color="#E6EDF3")
        .configure_view(strokeWidth=0)
        .configure_legend(
            labelColor="#8B95A5",
            titleColor="#8B95A5",
        )
    )

    st.altair_chart(scatter_chart, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════
# RESULTS TABLE
# ══════════════════════════════════════════════════════════════════════
st.subheader("종목 리스트")

display = df.copy()

# Format money columns to 억
for col in ["latest_rev", "marcap"]:
    if col in display.columns:
        display[col] = pd.to_numeric(display[col], errors="coerce")
        display[col] = (display[col] / BN).round(0).astype("Int64")

# Format close price
if "close" in display.columns:
    display["close"] = pd.to_numeric(display["close"], errors="coerce").apply(
        lambda v: f"{int(v):,}" if pd.notna(v) else "-"
    )

# Format OPM and growth rates as percentages
for col in ["latest_opm", "rev_cagr", "op_cagr", "earnings_growth"]:
    if col in display.columns:
        display[col] = pd.to_numeric(df[col], errors="coerce").apply(
            lambda v: f"{v:+.1%}" if pd.notna(v) else "-"
        )

# Format accel as %p
if "accel" in display.columns:
    display["accel"] = pd.to_numeric(df["accel"], errors="coerce").apply(
        lambda v: f"{v:+.1%}p" if pd.notna(v) else "-"
    )

# Format PER as X.X배
if "per" in display.columns:
    display["per"] = pd.to_numeric(df["per"], errors="coerce").apply(
        lambda v: f"{v:.1f}배" if pd.notna(v) else "-"
    )

# Format PBR as X.XX
if "pbr" in display.columns:
    display["pbr"] = pd.to_numeric(df["pbr"], errors="coerce").apply(
        lambda v: f"{v:.2f}" if pd.notna(v) else "-"
    )

# Format PEG as X.XX
if "peg" in display.columns:
    display["peg"] = pd.to_numeric(df["peg"], errors="coerce").apply(
        lambda v: f"{v:.2f}" if pd.notna(v) else "-"
    )

# Format marcap for display
if "marcap" in display.columns:
    display["marcap"] = pd.to_numeric(display["marcap"], errors="coerce").apply(
        lambda v: format_krw(v) if pd.notna(v) else "-"
    )

col_rename = {
    "corp_name": "종목명",
    "stock_code": "종목코드",
    "market": "시장",
    "latest_rev": "매출(억)",
    "latest_opm": "OPM",
    "rev_cagr": "매출CAGR",
    "op_cagr": "OP CAGR",
    "earnings_growth": "이익성장률",
    "accel": "성장가속도",
    "close": "현재가",
    "marcap": "시총",
    "per": "PER",
    "pbr": "PBR",
    "peg": "PEG",
}
display = display.rename(columns=col_rename)

st.dataframe(display, use_container_width=True, hide_index=True, height=500)

# ── CSV download ────────────────────────────────────────────────────
csv_bytes = display.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="CSV 다운로드",
    data=csv_bytes,
    file_name=f"growth_valuation_FY{fiscal_year}.csv",
    mime="text/csv",
    use_container_width=True,
)

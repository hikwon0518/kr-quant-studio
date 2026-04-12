from __future__ import annotations

from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from krqs.data.db.repositories.prices import (
    get_latest_price,
    get_price_range,
    get_valuation,
)
from krqs.domain.valuation import compute_implied_growth
from krqs.services.simulator_service import search_corporations
from krqs.ui.formatters import format_krw
from krqs.ui.state import BN, get_db

con = get_db()

st.title("Sanity Check Dashboard")
st.caption("이미 가격에 반영됐는가? — 근데 보통 가격은 저걸 다 싹다 미리 감안해서 반영하는게 문제")

# ── Sidebar ────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("종목 검색")

    query = st.text_input(
        "종목명 또는 코드",
        placeholder="예: 삼성전자, 005930",
        key="sc_corp_query",
    )

    matches = search_corporations(con, query) if query else []
    if query and not matches:
        st.warning(f"'{query}'에 해당하는 종목이 없습니다.")

    selected = None
    if matches:
        labels = [m.display for m in matches]
        selected_idx = st.selectbox(
            "검색 결과",
            range(len(matches)),
            format_func=lambda i: labels[i],
            key="sc_corp_match_idx",
        )
        selected = matches[selected_idx]

    st.divider()

    comp_period_map = {"1년": 1, "2년": 2, "3년": 3}
    comp_period_label = st.selectbox(
        "비교 기간",
        list(comp_period_map.keys()),
        index=0,
        key="sc_comp_period",
    )
    comp_years = comp_period_map[comp_period_label]

# ── Main area ──────────────────────────────────────────────────────────
if selected is None or selected.stock_code is None:
    st.info("사이드바에서 종목을 검색하세요.")
    st.stop()

stock_code = selected.stock_code
corp_code = selected.corp_code
corp_name = selected.corp_name

st.subheader(f"{corp_name} ({stock_code})")

# ── Check data availability ────────────────────────────────────────────
latest_price = get_latest_price(con, stock_code)
if latest_price is None:
    st.warning(
        f"{corp_name}의 가격 데이터가 없습니다. "
        "먼저 가격 데이터를 동기화하세요."
    )
    st.stop()

# ── Fetch financials (annual, Q4) ──────────────────────────────────────
fin_rows = con.execute(
    """
    SELECT fiscal_year, revenue, operating_income, net_income,
           total_equity, marcap_proxy
    FROM (
        SELECT f.fiscal_year, f.revenue, f.operating_income, f.net_income,
               f.total_equity,
               (SELECT p.marcap FROM price_daily p
                WHERE p.stock_code = ?
                  AND p.trade_date = (
                      SELECT MAX(p2.trade_date) FROM price_daily p2
                      WHERE p2.stock_code = ?
                        AND EXTRACT(YEAR FROM p2.trade_date) = f.fiscal_year
                  )
               ) AS marcap_proxy
        FROM financials_quarterly f
        WHERE f.corp_code = ?
          AND f.fiscal_quarter = 4
        ORDER BY f.fiscal_year DESC
    ) sub
    ORDER BY fiscal_year ASC
    """,
    [stock_code, stock_code, corp_code],
).fetchall()

fin_cols = [
    "fiscal_year", "revenue", "operating_income", "net_income",
    "total_equity", "marcap_proxy",
]
fin_df = pd.DataFrame(fin_rows, columns=fin_cols)

for col in ["revenue", "operating_income", "net_income", "total_equity", "marcap_proxy"]:
    fin_df[col] = pd.to_numeric(fin_df[col], errors="coerce")

if fin_df.empty or len(fin_df) < 2:
    st.warning(
        f"{corp_name}의 재무 데이터가 부족합니다 (최소 2개 연도 필요). "
        "재무 데이터를 동기화하세요."
    )
    st.stop()

# ── Price data for comparison period ───────────────────────────────────
end_date_str = str(latest_price["trade_date"])
start_date_str = str(
    date.fromisoformat(end_date_str) - timedelta(days=comp_years * 365)
)
price_rows = get_price_range(con, stock_code, start_date_str, end_date_str)
price_df = pd.DataFrame(price_rows)
price_df["trade_date"] = pd.to_datetime(price_df["trade_date"])
price_df["close"] = pd.to_numeric(price_df["close"], errors="coerce")
price_df = price_df.dropna(subset=["close"]).sort_values("trade_date").reset_index(drop=True)

if price_df.empty or len(price_df) < 2:
    st.warning("비교 기간의 가격 데이터가 부족합니다.")
    st.stop()

st.divider()

# ══════════════════════════════════════════════════════════════════════
# SECTION 1: Fundamental vs Price comparison
# ══════════════════════════════════════════════════════════════════════
st.subheader("Fundamental vs Price")
st.caption("펀더멘털 성장률과 주가 수익률을 비교합니다.")

# Use financials spanning the comparison period
latest_fy = int(fin_df["fiscal_year"].max())
earliest_fy_target = latest_fy - comp_years
fin_comparison = fin_df[fin_df["fiscal_year"] >= earliest_fy_target].copy()

if len(fin_comparison) >= 2:
    earliest = fin_comparison.iloc[0]
    latest_fin = fin_comparison.iloc[-1]

    def _safe_growth(latest_val, earliest_val):
        if pd.notna(latest_val) and pd.notna(earliest_val) and earliest_val != 0:
            return (latest_val / earliest_val) - 1.0
        return None

    rev_growth = _safe_growth(latest_fin["revenue"], earliest["revenue"])
    op_growth = _safe_growth(latest_fin["operating_income"], earliest["operating_income"])
    ni_growth = _safe_growth(latest_fin["net_income"], earliest["net_income"])

    # Price return over the same period
    earliest_close = float(price_df.iloc[0]["close"])
    latest_close = float(price_df.iloc[-1]["close"])
    price_return = (latest_close / earliest_close - 1.0) if earliest_close > 0 else None

    g1, g2, g3, g4 = st.columns(4)
    g1.metric(
        "매출 성장률",
        f"{rev_growth:+.1%}" if rev_growth is not None else "-",
        help=f"FY{int(earliest['fiscal_year'])} -> FY{int(latest_fin['fiscal_year'])}",
    )
    g2.metric(
        "영업이익 성장률",
        f"{op_growth:+.1%}" if op_growth is not None else "-",
    )
    g3.metric(
        "순이익 성장률",
        f"{ni_growth:+.1%}" if ni_growth is not None else "-",
    )
    g4.metric(
        "주가 수익률",
        f"{price_return:+.1%}" if price_return is not None else "-",
        help=f"{comp_period_label} 기준",
    )

    # Auto comment
    growth_vals = [
        g for g in [rev_growth, op_growth, ni_growth] if g is not None
    ]
    if growth_vals and price_return is not None:
        max_growth = max(growth_vals)
        min_growth = min(growth_vals)
        if price_return > max_growth:
            st.warning(
                "주가가 펀더멘털보다 빠르게 반영됨 (과열?) -- "
                f"주가 수익률 {price_return:+.1%} > 최대 펀더멘털 성장 {max_growth:+.1%}"
            )
        elif price_return < min_growth:
            st.success(
                "펀더멘털 대비 주가 underperform (기회?) -- "
                f"주가 수익률 {price_return:+.1%} < 최소 펀더멘털 성장 {min_growth:+.1%}"
            )
        else:
            st.info(
                "주가와 펀더멘털이 비슷한 속도로 움직이는 중 -- "
                f"주가 {price_return:+.1%}, 펀더멘털 {min_growth:+.1%} ~ {max_growth:+.1%}"
            )
else:
    st.info("비교 기간에 해당하는 재무 데이터가 부족합니다.")

st.divider()

# ══════════════════════════════════════════════════════════════════════
# SECTION 2: Multiple re-rating vs earnings growth decomposition
# ══════════════════════════════════════════════════════════════════════
st.subheader("주가 변동 분해: EPS 성장 vs 멀티플 리레이팅")
st.caption("지난 N년간 주가 변동을 이익 성장 기여분과 멀티플 변화 기여분으로 분해합니다.")

valuation = get_valuation(con, stock_code)

if valuation and len(fin_comparison) >= 2:
    current_marcap = latest_price.get("marcap")
    current_ni = latest_fin["net_income"]

    earliest_ni = earliest["net_income"]
    earliest_marcap = earliest.get("marcap_proxy")

    # Current PER
    current_per = None
    if (
        current_marcap is not None
        and current_ni is not None
        and pd.notna(current_ni)
        and current_ni > 0
    ):
        current_per = current_marcap / current_ni

    # Prior PER
    prior_per = None
    if (
        earliest_marcap is not None
        and pd.notna(earliest_marcap)
        and earliest_ni is not None
        and pd.notna(earliest_ni)
        and earliest_ni > 0
    ):
        prior_per = earliest_marcap / earliest_ni

    if current_per is not None and prior_per is not None and prior_per > 0:
        # Decomposition: total return = eps_change * multiple_change
        eps_change = current_ni / earliest_ni if earliest_ni > 0 else 1.0
        multiple_change = current_per / prior_per

        total_return_pct = (eps_change * multiple_change - 1.0) * 100
        eps_contribution_pct = (eps_change - 1.0) * 100
        multiple_contribution_pct = (multiple_change - 1.0) * 100

        st.markdown(
            f"""
<div style="background:#1E2536; border:1px solid #2A3040; border-radius:10px;
            padding:20px; margin:10px 0;">
<p style="font-size:1.1rem; color:#E6EDF3; margin-bottom:12px;">
    지난 <b>{comp_years}년</b> 주가 변동 분해
</p>
<p style="font-size:1.4rem; color:#00D4AA; margin-bottom:4px;">
    주가 {total_return_pct:+.1f}%
</p>
<p style="font-size:1.0rem; color:#8B95A5; margin-left:20px;">
    ├─ EPS 성장 기여: <span style="color:#FFD700;">{eps_contribution_pct:+.1f}%</span>
    (PER 기준 {prior_per:.1f}배 → {current_per:.1f}배)
</p>
<p style="font-size:1.0rem; color:#8B95A5; margin-left:20px;">
    └─ 멀티플 리레이팅: <span style="color:#FFD700;">{multiple_contribution_pct:+.1f}%</span>
    (멀티플 {multiple_change:.2f}배 변화)
</p>
</div>
            """,
            unsafe_allow_html=True,
        )

        # Interpretation
        if multiple_contribution_pct > eps_contribution_pct and multiple_contribution_pct > 0:
            st.caption(
                "멀티플 확장이 이익 성장보다 큰 기여 -- "
                "시장 기대가 이미 높게 반영되어 있을 수 있습니다."
            )
        elif eps_contribution_pct > 0 and multiple_contribution_pct < 0:
            st.caption(
                "이익은 성장했으나 멀티플은 축소 -- "
                "시장이 향후 성장 둔화를 반영하고 있을 수 있습니다."
            )
    else:
        st.info(
            "PER 분해에 필요한 데이터가 부족합니다 "
            "(순이익이 양수여야 하며 과거 시총 데이터가 필요합니다)."
        )
else:
    st.info("밸류에이션 분해에 필요한 데이터가 부족합니다.")

st.divider()

# ══════════════════════════════════════════════════════════════════════
# SECTION 3: Implied growth rate
# ══════════════════════════════════════════════════════════════════════
st.subheader("시장 내포 성장 프리미엄")
st.caption("현재 PER이 과거 평균 대비 얼마나 높은지로 시장이 기대하는 프리미엄을 역산합니다.")

# Compute historical PER from annual financials + price
per_history = []
for _, frow in fin_df.iterrows():
    fy = int(frow["fiscal_year"])
    fy_ni = frow["net_income"]
    fy_marcap = frow.get("marcap_proxy")
    if (
        pd.notna(fy_ni) and fy_ni > 0
        and pd.notna(fy_marcap) and fy_marcap > 0
    ):
        per_history.append({
            "fiscal_year": fy,
            "per": fy_marcap / fy_ni,
            "net_income": fy_ni,
            "marcap": fy_marcap,
        })

# Also include current PER from live data
if valuation:
    current_per_val = valuation.get("per")
    if current_per_val is not None and pd.notna(current_per_val) and current_per_val > 0:
        # Check if we already have the latest year
        existing_years = [p["fiscal_year"] for p in per_history]
        current_label = latest_fy + 1  # label as "current" year
        per_history.append({
            "fiscal_year": current_label,
            "per": float(current_per_val),
            "net_income": valuation.get("net_income"),
            "marcap": valuation.get("marcap"),
        })

if len(per_history) >= 2:
    per_hist_df = pd.DataFrame(per_history)
    per_hist_df["per"] = pd.to_numeric(per_hist_df["per"], errors="coerce")

    # Filter out extreme outliers (PER > 200 or < 0)
    per_hist_df = per_hist_df[
        (per_hist_df["per"] > 0) & (per_hist_df["per"] < 200)
    ].copy()

    if len(per_hist_df) >= 2:
        avg_per = float(per_hist_df["per"].iloc[:-1].mean())  # average excluding current
        current_per_display = float(per_hist_df["per"].iloc[-1])

        implied_growth = compute_implied_growth(current_per_display, avg_per)

        i1, i2, i3 = st.columns(3)
        i1.metric("현재 PER", f"{current_per_display:.1f}배")
        i2.metric("과거 평균 PER", f"{avg_per:.1f}배")
        i3.metric(
            "내포 프리미엄",
            f"{implied_growth:+.1%}",
            delta=f"{implied_growth:+.1%}",
            delta_color="normal",
        )

        if implied_growth > 0:
            st.info(
                f"시장이 현재 가격에 내포하는 프리미엄: **{implied_growth:+.1%}** -- "
                "시장이 과거 평균보다 낙관적으로 평가하고 있습니다."
            )
        elif implied_growth < 0:
            st.success(
                f"시장이 현재 가격에 내포하는 디스카운트: **{implied_growth:+.1%}** -- "
                "시장이 과거 평균보다 보수적으로 평가하고 있습니다."
            )
        else:
            st.info("현재 PER이 과거 평균 수준과 유사합니다.")

        st.divider()

        # ══════════════════════════════════════════════════════════════
        # SECTION 4: Historical PER band chart
        # ══════════════════════════════════════════════════════════════
        st.subheader("Historical PER Band")
        st.caption("연도별 PER 추이. 현재 PER 수준을 과거와 비교합니다.")

        chart_df = per_hist_df.copy()
        chart_df["fiscal_year"] = chart_df["fiscal_year"].astype(str)

        # Bar chart for historical PER
        bars = (
            alt.Chart(chart_df)
            .mark_bar(
                cornerRadiusTopLeft=4,
                cornerRadiusTopRight=4,
                opacity=0.85,
            )
            .encode(
                x=alt.X(
                    "fiscal_year:N",
                    title="회계연도",
                    axis=alt.Axis(labelAngle=0),
                ),
                y=alt.Y(
                    "per:Q",
                    title="PER (배)",
                    scale=alt.Scale(zero=True),
                ),
                color=alt.condition(
                    alt.datum.fiscal_year == str(chart_df["fiscal_year"].iloc[-1]),
                    alt.value("#00D4AA"),
                    alt.value("#2A3040"),
                ),
                tooltip=[
                    alt.Tooltip("fiscal_year:N", title="연도"),
                    alt.Tooltip("per:Q", title="PER", format=".1f"),
                ],
            )
        )

        # Average PER line
        avg_line_df = pd.DataFrame({
            "avg_per": [avg_per],
        })
        avg_rule = (
            alt.Chart(avg_line_df)
            .mark_rule(color="#FFD700", strokeWidth=2, strokeDash=[6, 4])
            .encode(
                y=alt.Y("avg_per:Q"),
            )
        )

        # Label for average
        avg_label_df = pd.DataFrame({
            "fiscal_year": [chart_df["fiscal_year"].iloc[-1]],
            "avg_per": [avg_per],
            "label": [f"평균 {avg_per:.1f}배"],
        })
        avg_label = (
            alt.Chart(avg_label_df)
            .mark_text(
                align="right", dx=-5, dy=-10,
                color="#FFD700", fontSize=11, fontWeight="bold",
            )
            .encode(
                x=alt.X("fiscal_year:N"),
                y=alt.Y("avg_per:Q"),
                text="label:N",
            )
        )

        per_chart = (
            alt.layer(bars, avg_rule, avg_label)
            .properties(height=380)
            .configure_axis(
                gridColor="#1E2536",
                labelColor="#8B95A5",
                titleColor="#8B95A5",
            )
            .configure_title(color="#E6EDF3")
            .configure_view(strokeWidth=0)
        )

        st.altair_chart(per_chart, use_container_width=True)

    else:
        st.info("PER 밴드 차트를 위한 데이터가 부족합니다 (유효 PER 2개 이상 필요).")
else:
    st.info(
        "Historical PER 분석에 필요한 데이터가 부족합니다 "
        "(순이익이 양수인 연도가 2개 이상 필요)."
    )

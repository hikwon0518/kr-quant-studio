from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from krqs.data.db.repositories.financials import get_history
from krqs.domain.gpm_regression import fit_gpm_vs_revenue
from krqs.services.simulator_service import search_corporations
from krqs.ui.state import BN, get_db, init_simulator_state

init_simulator_state()
con = get_db()

st.title("GPM-Revenue Regression")
st.caption("과거 데이터로 GPM 밴드 자동 추정")

with st.sidebar:
    st.header("종목 선택")
    query = st.text_input(
        "종목명 또는 코드", placeholder="예: 삼성전자", key="gpm_corp_query"
    )

    selected = None
    if query:
        matches = search_corporations(con, query)
        if matches:
            idx = st.selectbox(
                "검색 결과",
                range(len(matches)),
                format_func=lambda i: matches[i].display,
                key="gpm_match_idx",
            )
            selected = matches[idx]
        else:
            st.warning("검색 결과 없음")

    st.header("회귀 옵션")
    remove_outliers = st.checkbox("IQR 이상치 제거", value=True)
    confidence = st.slider("신뢰수준", 0.80, 0.99, 0.95, 0.01)

    st.header("수동 입력")
    manual_mode = st.checkbox("DB 대신 수동 데이터 입력", value=(selected is None))

history_df: pd.DataFrame | None = None
data_source_label = ""

if selected and not manual_mode:
    history = get_history(con, selected.corp_code, quarter=4)
    valid = [
        h
        for h in history
        if h.get("revenue") is not None and h.get("gpm") is not None
    ]
    if not valid:
        st.warning(f"{selected.corp_name}의 재무 이력이 DB에 없습니다.")
    else:
        history_df = pd.DataFrame(valid)
        data_source_label = f"{selected.corp_name} · {len(valid)}개년"

if manual_mode:
    st.subheader("과거 데이터 입력")
    st.caption("revenue는 원 단위, gpm은 0~1 사이 비율로 입력")
    default_data = pd.DataFrame(
        {
            "fiscal_year": [2018, 2019, 2020, 2021, 2022],
            "revenue": [
                1_000_000_000_000,
                1_500_000_000_000,
                2_000_000_000_000,
                3_000_000_000_000,
                4_000_000_000_000,
            ],
            "gpm": [0.15, 0.18, 0.22, 0.25, 0.28],
        }
    )
    edited = st.data_editor(
        default_data,
        num_rows="dynamic",
        width="stretch",
        key="gpm_manual_editor",
    )
    history_df = edited
    data_source_label = "수동 입력"

if history_df is None or len(history_df) < 3:
    st.info(
        "회귀분석을 위해 3개 이상의 (revenue, gpm) 데이터가 필요합니다. "
        "종목을 선택하거나 수동 입력 모드를 활성화하세요."
    )
    st.stop()

records = history_df.to_dict(orient="records")
result = fit_gpm_vs_revenue(
    records,
    remove_outliers=remove_outliers,
    confidence=confidence,
)

if result is None:
    st.error("회귀분석 실패. 데이터에 유효한 (revenue, gpm) 쌍이 3개 이상인지 확인하세요.")
    st.stop()

st.info(f"데이터 소스: **{data_source_label}**")

c1, c2, c3, c4 = st.columns(4)
c1.metric("관측치", f"{result.observations}")
c2.metric("이상치 제거", f"{result.outliers_removed}")
c3.metric("R²", f"{result.r_squared:.3f}")
slope_per_trillion = result.slope * 1_000_000_000_000
c4.metric("Slope (매출 1조당)", f"{slope_per_trillion:+.4f}")

st.divider()

col_chart, col_band = st.columns([3, 1])

with col_chart:
    st.subheader("Scatter + 회귀선 + 신뢰구간")
    fitted_df = result.fitted_df.copy()
    fitted_df["revenue_bn"] = fitted_df["revenue"] / BN

    base = alt.Chart(fitted_df).encode(
        x=alt.X("revenue_bn:Q", title="매출액 (억원)"),
    )
    band = base.mark_area(opacity=0.2, color="#4c78a8").encode(
        y=alt.Y("lower:Q", title="GPM"),
        y2="upper:Q",
    )
    line = base.mark_line(color="#e45756", strokeWidth=2).encode(y="fitted:Q")
    points = base.mark_circle(size=100, color="#4c78a8").encode(
        y=alt.Y("gpm:Q", title="GPM"),
        tooltip=[
            alt.Tooltip("revenue_bn:Q", title="매출(억)", format=",.0f"),
            alt.Tooltip("gpm:Q", title="GPM", format=".1%"),
        ],
    )
    chart = (band + line + points).properties(height=420).interactive()
    st.altair_chart(chart, width="stretch")

with col_band:
    st.subheader("제안 GPM 밴드")
    st.metric("Low", f"{result.predicted_gpm_low:.1%}")
    st.metric("Mid", f"{result.predicted_gpm_mid:.1%}")
    st.metric("High", f"{result.predicted_gpm_high:.1%}")

    st.caption("이 밴드는 가장 높은 매출 데이터 기준 예측 신뢰구간입니다.")

    if st.button(
        "시뮬레이터에 적용", type="primary", width="stretch"
    ):
        st.session_state["gpm_low"] = max(0.0, round(result.predicted_gpm_low, 4))
        st.session_state["gpm_mid"] = round(result.predicted_gpm_mid, 4)
        st.session_state["gpm_high"] = round(result.predicted_gpm_high, 4)
        st.success(
            "Operating Leverage 페이지로 이동하면 이 밴드가 반영됩니다."
        )

if result.outliers_removed > 0:
    with st.expander(f"제거된 이상치 ({result.outliers_removed}건)"):
        st.dataframe(result.outlier_df, width="stretch")

with st.expander("적합 데이터 원본"):
    st.dataframe(result.fitted_df, width="stretch")

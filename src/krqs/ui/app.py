from __future__ import annotations

import pandas as pd
import streamlit as st

from krqs.data.db.connection import get_connection, initialize_schema
from krqs.data.db.repositories.corps import count_listed
from krqs.domain.operating_leverage import (
    DEFAULT_SGA_YOY_GROWTH,
    DEFAULT_TAX_RATE,
    BaselineInputs,
    GpmBand,
    build_scenario_matrix,
)
from krqs.services.simulator_service import (
    load_corp_baseline,
    search_corporations,
    suggest_gpm_band,
)

BN = 100_000_000  # 1억원

st.set_page_config(
    page_title="KR Quant Studio — Operating Leverage",
    layout="wide",
)


@st.cache_resource
def get_db():
    con = get_connection()
    initialize_schema(con)
    return con


def _init_state() -> None:
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
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


_init_state()
con = get_db()
listed_count = count_listed(con)

st.title("Operating Leverage Simulator")
st.caption("매출 성장률 × GPM 밴드 시나리오 매트릭스")

with st.sidebar:
    st.header("종목 검색")

    if listed_count == 0:
        st.info(
            "DB가 비어있습니다. 터미널에서 다음 명령을 실행하세요:\n\n"
            "```bash\n"
            "python -m uv run python scripts/sync_corp_codes.py\n"
            "python -m uv run python scripts/sync_financials.py --corp 에코프로비엠\n"
            "```"
        )
    else:
        st.caption(f"DB 내 상장기업 {listed_count:,}개")

    query = st.text_input(
        "종목명 또는 코드", placeholder="예: 삼성전자, 005930", key="corp_query"
    )

    matches = search_corporations(con, query) if query else []
    if query and not matches:
        st.warning(f"'{query}'에 해당하는 종목이 없습니다.")

    if matches:
        labels = [m.display for m in matches]
        selected_idx = st.selectbox(
            "검색 결과",
            range(len(matches)),
            format_func=lambda i: labels[i],
            key="corp_match_idx",
        )
        selected = matches[selected_idx]

        if st.button("자동 채우기", type="primary", use_container_width=True):
            loaded = load_corp_baseline(con, selected.corp_code)
            if loaded is None:
                st.error(
                    f"{selected.corp_name}의 재무 데이터가 DB에 없습니다.\n\n"
                    f"`python -m uv run python scripts/sync_financials.py "
                    f"--corp {selected.corp_name}` 를 먼저 실행하세요."
                )
            else:
                st.session_state["revenue_bn"] = loaded.baseline.revenue / BN
                st.session_state["cogs_bn"] = loaded.baseline.cogs / BN
                st.session_state["sga_bn"] = loaded.baseline.sga / BN
                st.session_state["interest_bn"] = (
                    loaded.baseline.interest_expense / BN
                )

                suggested = suggest_gpm_band(loaded.historical_gpm)
                if suggested is not None:
                    st.session_state["gpm_low"] = float(suggested.low)
                    st.session_state["gpm_mid"] = float(suggested.mid)
                    st.session_state["gpm_high"] = float(suggested.high)

                st.session_state["selected_corp_code"] = loaded.corp_code
                st.session_state["selected_corp_label"] = (
                    f"{loaded.corp_name} · FY{loaded.fiscal_year}"
                )
                st.rerun()

    st.divider()
    st.header("직전 연도 실적 (억원)")
    st.number_input("매출액", step=1000.0, min_value=0.0, key="revenue_bn")
    st.number_input("매출원가", step=1000.0, min_value=0.0, key="cogs_bn")
    st.number_input("판관비", step=100.0, min_value=0.0, key="sga_bn")
    st.number_input("이자비용", step=50.0, min_value=0.0, key="interest_bn")

    st.header("GPM 밴드")
    st.slider("GPM Low", 0.0, 0.6, step=0.01, format="%.2f", key="gpm_low")
    st.slider("GPM Mid", 0.0, 0.6, step=0.01, format="%.2f", key="gpm_mid")
    st.slider("GPM High", 0.0, 0.6, step=0.01, format="%.2f", key="gpm_high")

    st.header("가정")
    tax_rate = st.slider(
        "법인세율", 0.0, 0.5, DEFAULT_TAX_RATE, 0.01, format="%.2f"
    )
    sga_growth = st.slider(
        "판관비 YoY 증가율",
        -0.10,
        0.20,
        DEFAULT_SGA_YOY_GROWTH,
        0.01,
        format="%.2f",
    )

    st.header("성장률 범위")
    growth_range = st.slider(
        "매출 성장률 (%)",
        min_value=0,
        max_value=100,
        value=(10, 70),
        step=5,
    )

if not (
    st.session_state["gpm_low"]
    <= st.session_state["gpm_mid"]
    <= st.session_state["gpm_high"]
):
    st.error("GPM 밴드는 low ≤ mid ≤ high 순서여야 합니다.")
    st.stop()

baseline = BaselineInputs(
    revenue=int(st.session_state["revenue_bn"] * BN),
    cogs=int(st.session_state["cogs_bn"] * BN),
    sga=int(st.session_state["sga_bn"] * BN),
    interest_expense=int(st.session_state["interest_bn"] * BN),
)
gpm_band = GpmBand(
    low=st.session_state["gpm_low"],
    mid=st.session_state["gpm_mid"],
    high=st.session_state["gpm_high"],
)

growth_rates = tuple(
    round(x / 100, 3) for x in range(growth_range[0], growth_range[1] + 1, 5)
)
if not growth_rates:
    st.warning("성장률 범위를 선택해주세요.")
    st.stop()

matrix = build_scenario_matrix(
    baseline,
    gpm_band,
    growth_rates=growth_rates,
    tax_rate=tax_rate,
    sga_yoy_growth=sga_growth,
)

if st.session_state["selected_corp_label"]:
    st.info(f"선택된 종목: **{st.session_state['selected_corp_label']}**")

baseline_gross = baseline.revenue - baseline.cogs
baseline_gpm = baseline_gross / baseline.revenue if baseline.revenue else 0.0
baseline_op_income = baseline_gross - baseline.sga
baseline_opm = baseline_op_income / baseline.revenue if baseline.revenue else 0.0

col1, col2, col3, col4 = st.columns(4)
col1.metric("매출액", f"{st.session_state['revenue_bn']:,.0f}억")
col2.metric("실적 GPM", f"{baseline_gpm:.1%}")
col3.metric("실적 영업이익", f"{baseline_op_income / BN:,.0f}억")
col4.metric("실적 OPM", f"{baseline_opm:.1%}")

st.divider()
st.subheader("시나리오 매트릭스")

display = matrix.copy()
money_cols = [
    "revenue",
    "gross_profit",
    "sga",
    "operating_income",
    "interest_expense",
    "pretax_income",
    "net_income",
]
for col in money_cols:
    display[col] = (display[col] / BN).round(0).astype(int)

display["growth_rate"] = display["growth_rate"].apply(lambda x: f"{x:+.0%}")
display["gpm"] = display["gpm"].apply(lambda x: f"{x:.1%}")
display["opm"] = display["opm"].apply(lambda x: f"{x:.1%}")

display = display.rename(
    columns={
        "growth_rate": "성장률",
        "gpm_scenario": "GPM",
        "gpm": "GPM값",
        "revenue": "매출(억)",
        "gross_profit": "매출총이익(억)",
        "sga": "판관비(억)",
        "operating_income": "영업이익(억)",
        "interest_expense": "이자비용(억)",
        "pretax_income": "세전이익(억)",
        "net_income": "순이익(억)",
        "opm": "OPM",
        "is_insolvent": "적자전환",
    }
)


def highlight_insolvent(row: pd.Series) -> list[str]:
    if row["적자전환"]:
        return ["background-color: #ffe0e0"] * len(row)
    return [""] * len(row)


styled = display.style.apply(highlight_insolvent, axis=1)
st.dataframe(styled, use_container_width=True, hide_index=True)

st.divider()
st.subheader("영업이익 곡선 (성장률 × GPM 시나리오)")

chart_data = matrix[["growth_rate", "gpm_scenario", "operating_income"]].copy()
chart_data["영업이익(억)"] = (chart_data["operating_income"] / BN).round(0)
chart_pivot = chart_data.pivot(
    index="growth_rate", columns="gpm_scenario", values="영업이익(억)"
)[["low", "mid", "high"]]
chart_pivot.index = [f"{x:+.0%}" for x in chart_pivot.index]

st.line_chart(chart_pivot)

with st.expander("원본 매트릭스 (원 단위)"):
    st.dataframe(matrix, use_container_width=True)

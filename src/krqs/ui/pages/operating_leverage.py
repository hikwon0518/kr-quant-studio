from __future__ import annotations

import pandas as pd
import streamlit as st

from krqs.data.db.repositories.corps import count_listed
from krqs.domain.operating_leverage import (
    DEFAULT_SGA_YOY_GROWTH,
    DEFAULT_TAX_RATE,
    BaselineInputs,
    GpmBand,
    build_scenario_matrix,
)
try:
    from krqs.services.data_sync_service import (
        sync_corp_codes,
        sync_corp_financials,
    )
    _SYNC_AVAILABLE = True
except ImportError:
    _SYNC_AVAILABLE = False
from krqs.services.simulator_service import (
    load_corp_baseline,
    search_corporations,
    suggest_gpm_band,
)
from krqs.ui.state import BN, get_db, init_simulator_state

init_simulator_state()
con = get_db()
listed_count = count_listed(con)

st.title("Operating Leverage Simulator")
st.caption("매출 성장률 × GPM 밴드 시나리오 매트릭스")

with st.sidebar:
    with st.expander("데이터 동기화", expanded=(listed_count == 0)):
        if not _SYNC_AVAILABLE:
            st.info("DART 동기화 기능은 로컬 환경에서만 사용 가능합니다.")
        elif listed_count == 0:
            st.warning("DB가 비어있습니다. 먼저 기업코드를 갱신하세요.")
        else:
            st.caption(f"DB 내 상장기업 {listed_count:,}개")

        if _SYNC_AVAILABLE and st.button("기업코드 갱신 (DART)", width="stretch"):
            with st.status("DART에서 기업코드 다운로드 중...", expanded=True) as status:
                try:
                    result = sync_corp_codes(con)
                    status.update(
                        label=f"기업코드 갱신 완료 · 상장 {result.listed_total_in_db:,}개",
                        state="complete",
                    )
                    st.caption(
                        f"다운로드 {result.downloaded_bytes / 1024:.0f} KB · "
                        f"전체 {result.parsed_total:,} · 상장 {result.listed_upserted:,}"
                    )
                except Exception as e:
                    status.update(label=f"실패: {e}", state="error")
            st.rerun()

    st.header("종목 검색")

    query = st.text_input(
        "종목명 또는 코드",
        placeholder="예: 삼성전자, 005930",
        key="corp_query",
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

        sync_col, load_col = st.columns(2)
        if _SYNC_AVAILABLE and sync_col.button("재무 동기화", width="stretch", help="DART에서 최근 5년치 재무 받아오기"):
            with st.status(
                f"{selected.corp_name} 재무 동기화 중...", expanded=True
            ) as status:
                progress_bar = st.progress(0.0)

                def _progress(i, n, outcome):
                    progress_bar.progress(i / n, text=f"{outcome.year}: {outcome.status}")

                try:
                    result = sync_corp_financials(
                        con, selected.corp_code, years=5, progress_callback=_progress
                    )
                    status.update(
                        label=f"동기화 완료 · {result.success_count}/5년 성공",
                        state="complete" if result.success_count > 0 else "error",
                    )
                    for outcome in result.outcomes:
                        if outcome.status == "ok":
                            rev_bn = (outcome.revenue or 0) / BN
                            op_bn = (outcome.operating_income or 0) / BN
                            st.caption(
                                f"{outcome.year}: 매출 {rev_bn:,.0f}억 · 영업이익 {op_bn:,.0f}억"
                            )
                        else:
                            st.caption(
                                f"{outcome.year}: {outcome.status}"
                                + (f" ({outcome.message})" if outcome.message else "")
                            )
                except Exception as e:
                    status.update(label=f"실패: {e}", state="error")

        if load_col.button("자동 채우기", type="primary", width="stretch"):
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
                st.session_state["selected_fiscal_year"] = loaded.fiscal_year
                st.rerun()

    st.divider()
    with st.expander("기본 입력", expanded=True):
        st.number_input("매출액", step=1000.0, min_value=0.0, key="revenue_bn")
        st.number_input("매출원가", step=1000.0, min_value=0.0, key="cogs_bn")
        st.number_input("판관비", step=100.0, min_value=0.0, key="sga_bn")
        st.number_input("이자비용", step=50.0, min_value=0.0, key="interest_bn")

    with st.expander("GPM 밴드", expanded=False):
        st.slider("GPM Low", 0.0, 0.6, step=0.01, format="%.2f", key="gpm_low")
        st.slider("GPM Mid", 0.0, 0.6, step=0.01, format="%.2f", key="gpm_mid")
        st.slider("GPM High", 0.0, 0.6, step=0.01, format="%.2f", key="gpm_high")

    with st.expander("가정", expanded=False):
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

    with st.expander("성장률 범위", expanded=False):
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

# Keep raw OPM for conditional formatting, store formatted version separately
_opm_raw = display["opm"].copy()
display["opm"] = display["opm"].apply(lambda x: f"{x:.1%}")

# Replace boolean 적자전환 with colored text labels
display["is_insolvent"] = display["is_insolvent"].apply(
    lambda x: "위험" if x else "안전"
)

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


def _opm_bg(opm_val: float) -> str:
    """Return background-color CSS for an OPM value (as a decimal, e.g. 0.05 = 5%)."""
    if opm_val < 0:
        return "background-color: #ffcccc"  # red
    if opm_val < 0.05:
        return "background-color: #fff3cd"  # yellow
    if opm_val < 0.10:
        return "background-color: #d4edda"  # light green
    return "background-color: #28a745; color: white"  # dark green


def _style_row(row: pd.Series) -> list[str]:
    """Apply row-level and cell-level conditional formatting."""
    styles = [""] * len(row)
    col_list = list(row.index)

    # OPM column gradient
    if "OPM" in col_list:
        opm_idx = col_list.index("OPM")
        raw_idx = row.name  # DataFrame integer index
        styles[opm_idx] = _opm_bg(_opm_raw.iloc[raw_idx])

    # 적자전환 column colored text
    if "적자전환" in col_list:
        ins_idx = col_list.index("적자전환")
        if row["적자전환"] == "위험":
            styles[ins_idx] = "color: #dc3545; font-weight: bold"  # red
        else:
            styles[ins_idx] = "color: #28a745; font-weight: bold"  # green

    # Insolvent row background (keep existing behaviour for full-row tint)
    if row.get("적자전환") == "위험":
        base_bg = "background-color: #ffe0e0"
        styles = [
            f"{s}; {base_bg}" if s else base_bg for s in styles
        ]

    return styles


styled = display.style.apply(_style_row, axis=1)
st.dataframe(styled, width="stretch", hide_index=True)

st.divider()
st.subheader("영업이익 곡선 (성장률 × GPM 시나리오)")

chart_data = matrix[["growth_rate", "gpm_scenario", "operating_income"]].copy()
chart_data["영업이익(억)"] = (chart_data["operating_income"] / BN).round(0)
chart_pivot = chart_data.pivot(
    index="growth_rate", columns="gpm_scenario", values="영업이익(억)"
)[["low", "mid", "high"]]
chart_pivot.index = [f"{x:+.0%}" for x in chart_pivot.index]

st.line_chart(chart_pivot)

st.divider()
st.subheader("리서치 리포트")

if st.button("HTML 리포트 생성", type="primary", width="stretch"):
    from krqs.services.report_service import build_operating_leverage_report

    artifact = build_operating_leverage_report(
        baseline,
        gpm_band,
        matrix,
        tax_rate=tax_rate,
        sga_growth=sga_growth,
        corp_label=st.session_state["selected_corp_label"] or None,
        fiscal_year=st.session_state["selected_fiscal_year"],
        data_source="DART OpenAPI" if st.session_state["selected_corp_code"] else "수동 입력",
        corp_code=st.session_state["selected_corp_code"] or None,
        db_con=con,
    )

    st.success(f"리포트 ID: `{artifact.report_id}`")
    col_hash, col_snap = st.columns(2)
    col_hash.caption(f"Parameter hash: `{artifact.param_hash[:16]}…`")
    if artifact.snapshot_path:
        col_snap.caption(f"DB 스냅샷: `{artifact.snapshot_path.name}`")
    else:
        col_snap.caption("DB 스냅샷: 미저장 (DB 잠김 또는 미존재)")

    st.download_button(
        label="HTML 다운로드",
        data=artifact.html,
        file_name=f"{artifact.report_id}.html",
        mime="text/html",
        width="stretch",
    )
    st.caption(
        "브라우저로 열어 Ctrl+P → PDF로 저장하면 인쇄 품질 PDF를 얻을 수 있습니다."
    )

with st.expander("원본 매트릭스 (원 단위)"):
    st.dataframe(matrix, width="stretch")

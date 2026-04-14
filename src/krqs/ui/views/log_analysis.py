from __future__ import annotations

from datetime import date, timedelta

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from krqs.data.db.repositories.prices import get_latest_price, get_price_range
from krqs.domain.log_trend import (
    compute_growth_acceleration,
    detect_signal,
    fit_log_trend,
)
from krqs.services.simulator_service import search_corporations
from krqs.ui.components.price_chart import render_price_chart
from krqs.ui.formatters import format_krw
from krqs.ui.state import BN, get_db

con = get_db()

st.title("Log-Scale Price Analysis")
st.caption("로그스케일로 보면 성장속도가 보인다.")

# ── Sidebar ────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("종목 검색")

    query = st.text_input(
        "종목명 또는 코드",
        placeholder="예: 삼성전자, 005930",
        key="log_corp_query",
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
            key="log_corp_match_idx",
        )
        selected = matches[selected_idx]

    st.divider()

    period_map = {"1년": 365, "2년": 730, "3년": 1095, "5년": 1825}
    period_label = st.selectbox(
        "분석 기간",
        list(period_map.keys()),
        index=2,
        key="log_period",
    )
    period_days = period_map[period_label]

    log_scale = st.checkbox("로그 스케일", value=True, key="log_scale")
    show_trend = st.checkbox("추세선 표시", value=True, key="log_show_trend")

# ── Main area ──────────────────────────────────────────────────────────
if selected is None or selected.stock_code is None:
    st.info("사이드바에서 종목을 검색하세요.")
    st.stop()

stock_code = selected.stock_code
corp_name = selected.corp_name

st.subheader(f"{corp_name} ({stock_code})")

# ── Fetch price data ───────────────────────────────────────────────────
latest = get_latest_price(con, stock_code)
if latest is None:
    st.warning(
        f"{corp_name}의 가격 데이터가 없습니다. "
        "먼저 가격 데이터를 동기화하세요."
    )
    st.stop()

end_date = str(latest["trade_date"])
start_date = str(date.fromisoformat(end_date) - timedelta(days=period_days))

rows = get_price_range(con, stock_code, start_date, end_date)
if not rows:
    st.warning("선택한 기간에 가격 데이터가 없습니다.")
    st.stop()

df = pd.DataFrame(rows)
df["trade_date"] = pd.to_datetime(df["trade_date"])
df["close"] = pd.to_numeric(df["close"], errors="coerce")
df = df.dropna(subset=["close"]).sort_values("trade_date").reset_index(drop=True)

if df.empty:
    st.warning("유효한 가격 데이터가 없습니다.")
    st.stop()

# ── KPI cards: current price info ──────────────────────────────────────
current_close = int(latest["close"])
current_marcap = latest.get("marcap")

# 52-week high/low from the most recent 252 trading days
recent_252 = df.tail(252)
high_52w = int(recent_252["high"].max()) if "high" in recent_252.columns and not recent_252["high"].isna().all() else None
low_52w = int(recent_252["low"].min()) if "low" in recent_252.columns and not recent_252["low"].isna().all() else None

c1, c2, c3, c4 = st.columns(4)
c1.metric("현재가", f"{current_close:,}원")
c2.metric(
    "시총",
    format_krw(current_marcap / BN) if current_marcap else "-",
)
c3.metric("52주 고가", f"{high_52w:,}원" if high_52w else "-")
c4.metric("52주 저가", f"{low_52w:,}원" if low_52w else "-")

st.divider()

# ── Log trend fitting ──────────────────────────────────────────────────
prices = df["close"].to_numpy(dtype=float)
dates = np.arange(len(prices), dtype=float)

log_result = fit_log_trend(dates, prices)

if log_result is not None:
    df["fitted"] = log_result.fitted_values
    df["upper"] = log_result.upper_band
    df["lower"] = log_result.lower_band

# ── Price chart ────────────────────────────────────────────────────────
render_price_chart(
    df,
    log_scale=log_scale,
    show_trend=show_trend and log_result is not None,
    title=f"{corp_name} 주가 ({period_label})",
    height=450,
)

# ── Signal badge ───────────────────────────────────────────────────────
if log_result is not None:
    signal = detect_signal(log_result)
    if signal == "something_special":
        st.success(
            "Something Special -- "
            "로그추세 상향 돌파 (성장률 가속 중)"
        )
    elif signal == "something_wrong":
        st.error(
            "Something Wrong -- "
            "로그추세 하향 이탈 (성장률 둔화 경고)"
        )
    else:
        st.info("추세 유지 중 -- 로그 추세 밴드 내 정상 범위")

    st.divider()

    # ── Log trend metrics ──────────────────────────────────────────────
    st.subheader("로그 추세 지표")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric(
        "연환산 수익률",
        f"{log_result.annualized_return:.1%}",
    )
    m2.metric(
        "R² (적합도)",
        f"{log_result.r_squared:.4f}",
    )
    m3.metric(
        "현재 이탈도",
        f"{log_result.current_deviation:+.2f}σ",
    )
    m4.metric(
        "관측치 수",
        f"{log_result.observations:,}일",
    )
else:
    st.warning(
        "로그 추세 분석에는 최소 30개의 관측치가 필요합니다. "
        f"현재 {len(df)}개."
    )

st.divider()

# ── Growth acceleration ────────────────────────────────────────────────
st.subheader("성장 가속도 분석")

accel = compute_growth_acceleration(df["close"])

a1, a2, a3, a4 = st.columns(4)

recent_cagr = accel["recent_slope"]
prior_cagr = accel["prior_slope"]
acceleration = accel["acceleration"]
accel_signal = accel["signal"]

a1.metric(
    "최근 반기 CAGR",
    f"{recent_cagr:.1%}",
)
a2.metric(
    "이전 반기 CAGR",
    f"{prior_cagr:.1%}",
)
a3.metric(
    "가속도",
    f"{acceleration:+.1%}p",
    delta=f"{acceleration:+.1%}p",
    delta_color="normal",
)

signal_label_map = {
    "accelerating": "가속 중",
    "decelerating": "감속 중",
    "steady": "보합",
}
a4.metric("시그널", signal_label_map.get(accel_signal, accel_signal))

st.divider()

# ── Top movers: stocks with highest 1yr returns ───────────────────────
st.subheader("로그로 봐야 할 종목")
st.caption("최근 1년 수익률이 100%를 넘는 종목 — 로그 스케일 분석이 특히 유효합니다.")

try:
    top_movers_df = con.execute(
        """
        WITH latest AS (
            SELECT stock_code, MAX(trade_date) AS max_date
            FROM price_daily
            GROUP BY stock_code
        ),
        current_prices AS (
            SELECT p.stock_code, p.close AS current_close, p.marcap,
                   l.max_date
            FROM price_daily p
            JOIN latest l
              ON p.stock_code = l.stock_code AND p.trade_date = l.max_date
        ),
        year_ago AS (
            SELECT p.stock_code, p.close AS prev_close
            FROM price_daily p
            JOIN latest l ON p.stock_code = l.stock_code
            WHERE p.trade_date = (
                SELECT MAX(trade_date)
                FROM price_daily p2
                WHERE p2.stock_code = p.stock_code
                  AND p2.trade_date <= l.max_date - INTERVAL '252' DAY
            )
        )
        SELECT
            c.corp_name AS 종목명,
            cp.stock_code AS 종목코드,
            ya.prev_close AS "1년전 종가",
            cp.current_close AS 현재가,
            ROUND((cp.current_close * 1.0 / ya.prev_close - 1) * 100, 1) AS "1년 수익률(%)",
            ROUND(cp.marcap * 1.0 / 100000000, 0) AS "시총(억)"
        FROM current_prices cp
        JOIN year_ago ya ON cp.stock_code = ya.stock_code
        JOIN corps c ON cp.stock_code = c.stock_code
        WHERE ya.prev_close > 0
          AND (cp.current_close * 1.0 / ya.prev_close - 1) > 1.0
        ORDER BY (cp.current_close * 1.0 / ya.prev_close) DESC
        LIMIT 10
        """
    ).fetchdf()

    if top_movers_df.empty:
        st.info("1년 수익률 100% 이상인 종목이 없습니다.")
    else:
        # Format display columns
        display_movers = top_movers_df.copy()
        for col in ["1년전 종가", "현재가"]:
            if col in display_movers.columns:
                display_movers[col] = pd.to_numeric(
                    display_movers[col], errors="coerce"
                ).apply(lambda v: f"{int(v):,}" if pd.notna(v) else "-")
        if "시총(억)" in display_movers.columns:
            display_movers["시총(억)"] = pd.to_numeric(
                display_movers["시총(억)"], errors="coerce"
            ).apply(lambda v: format_krw(v) if pd.notna(v) else "-")
        if "1년 수익률(%)" in display_movers.columns:
            display_movers["1년 수익률(%)"] = pd.to_numeric(
                display_movers["1년 수익률(%)"], errors="coerce"
            ).apply(lambda v: f"+{v:.1f}%" if pd.notna(v) else "-")

        st.dataframe(display_movers, use_container_width=True, hide_index=True)
except Exception:
    st.info("1년 수익률 상위 종목 조회를 건너뜁니다. 가격 데이터가 충분하지 않을 수 있습니다.")

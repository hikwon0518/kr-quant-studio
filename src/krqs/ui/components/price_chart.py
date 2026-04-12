from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st


def render_price_chart(
    df: pd.DataFrame,
    *,
    log_scale: bool = False,
    show_trend: bool = False,
    title: str = "",
    height: int = 400,
) -> None:
    """Render an Altair price chart with optional log-scale and trend bands.

    Parameters
    ----------
    df : pd.DataFrame
        Must have ``trade_date`` and ``close`` columns.
        If *show_trend* is True and the DataFrame contains ``fitted``,
        ``upper``, ``lower`` columns, trend bands are drawn.
    log_scale : bool
        Use logarithmic Y axis.
    show_trend : bool
        Overlay fitted trend line with upper/lower bands.
    title : str
        Chart title.
    height : int
        Chart height in pixels.
    """
    if df.empty:
        st.info("차트 데이터가 없습니다.")
        return

    plot_df = df.copy()
    plot_df["trade_date"] = pd.to_datetime(plot_df["trade_date"])
    plot_df["close"] = pd.to_numeric(plot_df["close"], errors="coerce")

    y_scale = alt.Scale(type="log") if log_scale else alt.Scale(zero=False)

    base = alt.Chart(plot_df).encode(
        x=alt.X("trade_date:T", title="날짜"),
    )

    # Price line
    price_line = base.mark_line(color="#00D4AA", strokeWidth=1.5).encode(
        y=alt.Y("close:Q", title="종가", scale=y_scale),
        tooltip=[
            alt.Tooltip("trade_date:T", title="날짜"),
            alt.Tooltip("close:Q", title="종가", format=",.0f"),
        ],
    )

    layers = [price_line]

    # Trend bands
    has_trend = show_trend and all(
        c in plot_df.columns for c in ("fitted", "upper", "lower")
    )
    if has_trend:
        plot_df["fitted"] = pd.to_numeric(plot_df["fitted"], errors="coerce")
        plot_df["upper"] = pd.to_numeric(plot_df["upper"], errors="coerce")
        plot_df["lower"] = pd.to_numeric(plot_df["lower"], errors="coerce")

        trend_base = alt.Chart(plot_df).encode(
            x=alt.X("trade_date:T"),
        )

        band = trend_base.mark_area(opacity=0.15, color="#FFD700").encode(
            y=alt.Y("lower:Q", scale=y_scale),
            y2=alt.Y2("upper:Q"),
        )

        fitted_line = trend_base.mark_line(
            color="#FFD700", strokeWidth=1.2, strokeDash=[4, 3],
        ).encode(
            y=alt.Y("fitted:Q", scale=y_scale),
        )

        layers = [band] + layers + [fitted_line]

    chart = (
        alt.layer(*layers)
        .properties(
            title=title,
            height=height,
        )
        .configure_axis(
            gridColor="#1E2536",
            labelColor="#8B95A5",
            titleColor="#8B95A5",
        )
        .configure_title(color="#E6EDF3")
        .configure_view(strokeWidth=0)
    )

    st.altair_chart(chart, use_container_width=True)

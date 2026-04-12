from __future__ import annotations

import math
from typing import Sequence

import duckdb
import numpy as np
import pandas as pd


def get_available_years(con: duckdb.DuckDBPyConnection) -> list[int]:
    """Return distinct fiscal years that have annual (Q4) data, descending."""
    rows = con.execute(
        """
        SELECT DISTINCT fiscal_year
        FROM financials_quarterly
        WHERE fiscal_quarter = 4
        ORDER BY fiscal_year DESC
        """
    ).fetchall()
    return [r[0] for r in rows]


# Columns that are valid sort targets.
_SORTABLE_COLUMNS = {
    "opm",
    "gpm",
    "roe",
    "debt_ratio",
    "revenue",
    "operating_income",
    "net_income",
    "ebitda",
    "ebitda_margin",
    "total_assets",
    "total_equity",
}


def screen_companies(
    con: duckdb.DuckDBPyConnection,
    *,
    fiscal_year: int | None = None,
    min_opm: float | None = None,
    max_opm: float | None = None,
    min_gpm: float | None = None,
    min_roe: float | None = None,
    max_debt_ratio: float | None = None,
    min_revenue: int | None = None,
    min_operating_income: int | None = None,
    min_ebitda_margin: float | None = None,
    sort_by: str = "opm",
    sort_desc: bool = True,
    limit: int = 100,
) -> pd.DataFrame:
    """Query DuckDB to filter and sort companies by financial metrics.

    All filters are optional -- when *None*, that filter is skipped.
    Returns a DataFrame ready for display.
    """
    # Resolve fiscal year to the latest available if not specified.
    if fiscal_year is None:
        years = get_available_years(con)
        if not years:
            return pd.DataFrame()
        fiscal_year = years[0]

    # Validate sort column to prevent injection.
    if sort_by not in _SORTABLE_COLUMNS:
        sort_by = "opm"

    # Build WHERE clause dynamically with parameterised placeholders.
    conditions: list[str] = [
        "f.fiscal_quarter = 4",
        "f.fiscal_year = ?",
    ]
    params: list[object] = [fiscal_year]

    _filter_map: list[tuple[str, str, object | None]] = [
        ("f.opm >= ?", ">=", min_opm),
        ("f.opm <= ?", "<=", max_opm),
        ("f.gpm >= ?", ">=", min_gpm),
        ("f.roe >= ?", ">=", min_roe),
        ("f.debt_ratio <= ?", "<=", max_debt_ratio),
        ("f.revenue >= ?", ">=", min_revenue),
        ("f.operating_income >= ?", ">=", min_operating_income),
        ("f.ebitda_margin >= ?", ">=", min_ebitda_margin),
    ]

    for clause, _op, value in _filter_map:
        if value is not None:
            conditions.append(clause)
            params.append(value)

    where = " AND ".join(conditions)
    direction = "DESC" if sort_desc else "ASC"

    sql = f"""
        SELECT
            c.corp_name,
            c.stock_code,
            c.market,
            f.fiscal_year,
            f.revenue,
            f.operating_income,
            f.net_income,
            f.total_assets,
            f.total_equity,
            f.gpm,
            f.opm,
            f.roe,
            f.debt_ratio,
            f.ebitda,
            f.ebitda_margin
        FROM financials_quarterly f
        JOIN corps c ON c.corp_code = f.corp_code
        WHERE {where}
          AND c.stock_code IS NOT NULL
        ORDER BY f.{sort_by} {direction} NULLS LAST
        LIMIT ?
    """  # noqa: S608 -- sort_by is validated against allowlist above
    params.append(limit)

    result = con.execute(sql, params)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    return pd.DataFrame(rows, columns=columns)


def get_trend_data(
    con: duckdb.DuckDBPyConnection,
    *,
    years: Sequence[int] | None = None,
    metric: str = "opm",
    min_years: int = 2,
    only_improving: bool = False,
    sort_by: str = "latest",
    limit: int = 200,
) -> pd.DataFrame:
    """Get multi-year trend data for a metric across all companies.

    Returns a pivoted DataFrame: rows=companies, columns=years, values=metric.
    If only_improving=True, filter to companies where metric increased every year.
    """
    if metric not in _SORTABLE_COLUMNS:
        metric = "opm"

    if years is None:
        years = get_available_years(con)
    if len(years) < min_years:
        return pd.DataFrame()

    placeholders = ", ".join("?" for _ in years)
    sql = f"""
        SELECT c.corp_code, c.corp_name, c.stock_code, c.market,
               f.fiscal_year, f.{metric}
        FROM financials_quarterly f
        JOIN corps c ON c.corp_code = f.corp_code
        WHERE f.fiscal_quarter = 4
          AND f.fiscal_year IN ({placeholders})
          AND c.stock_code IS NOT NULL
          AND f.{metric} IS NOT NULL
    """
    df = con.execute(sql, list(years)).fetchdf()
    if df.empty:
        return pd.DataFrame()

    pivot = df.pivot_table(
        index=["corp_code", "corp_name", "stock_code", "market"],
        columns="fiscal_year",
        values=metric,
    ).reset_index()

    year_cols = sorted([c for c in pivot.columns if isinstance(c, int)])
    if len(year_cols) < min_years:
        return pd.DataFrame()

    # Count how many years each company has data for
    pivot["data_years"] = pivot[year_cols].notna().sum(axis=1)
    pivot = pivot[pivot["data_years"] >= min_years]

    if only_improving and len(year_cols) >= 2:
        def _is_improving(row):
            vals = [row[y] for y in year_cols if pd.notna(row[y])]
            if len(vals) < 2:
                return False
            return all(vals[i] < vals[i + 1] for i in range(len(vals) - 1))
        pivot = pivot[pivot.apply(_is_improving, axis=1)]

    # Add latest value and YoY change
    latest_year = year_cols[-1]
    prev_year = year_cols[-2] if len(year_cols) >= 2 else None
    pivot["latest"] = pivot[latest_year]
    if prev_year is not None:
        pivot["yoy_change"] = pivot[latest_year] - pivot[prev_year]
    else:
        pivot["yoy_change"] = None

    # Sort
    if sort_by == "yoy_change" and "yoy_change" in pivot.columns:
        pivot = pivot.sort_values("yoy_change", ascending=False, na_position="last")
    else:
        pivot = pivot.sort_values("latest", ascending=False, na_position="last")

    return pivot.head(limit).reset_index(drop=True)


def get_growth_analysis(
    con: duckdb.DuckDBPyConnection,
    *,
    years: Sequence[int] | None = None,
    min_years: int = 3,
    sort_by: str = "rev_cagr",
    limit: int = 200,
) -> pd.DataFrame:
    """Compute CAGR, growth acceleration, and earnings growth for all companies.

    Returns DataFrame with: corp info, revenue CAGR, OP CAGR, growth acceleration,
    earnings growth rate (the 'G' in PEG).
    """
    if years is None:
        years = get_available_years(con)
    years = sorted(years)
    if len(years) < min_years:
        return pd.DataFrame()

    placeholders = ", ".join("?" for _ in years)
    sql = f"""
        SELECT c.corp_code, c.corp_name, c.stock_code, c.market,
               f.fiscal_year, f.revenue, f.operating_income, f.net_income, f.opm
        FROM financials_quarterly f
        JOIN corps c ON c.corp_code = f.corp_code
        WHERE f.fiscal_quarter = 4
          AND f.fiscal_year IN ({placeholders})
          AND c.stock_code IS NOT NULL
          AND f.revenue IS NOT NULL
          AND f.revenue > 0
    """
    df = con.execute(sql, list(years)).fetchdf()
    if df.empty:
        return pd.DataFrame()

    results = []
    for (corp_code, corp_name, stock_code, market), grp in df.groupby(
        ["corp_code", "corp_name", "stock_code", "market"]
    ):
        grp = grp.sort_values("fiscal_year")
        if len(grp) < min_years:
            continue

        revs = grp["revenue"].to_numpy(dtype=float)
        ops = grp["operating_income"].to_numpy(dtype=float)
        nets = grp["net_income"].to_numpy(dtype=float)
        opms = grp["opm"].to_numpy(dtype=float)
        yrs = grp["fiscal_year"].to_numpy()
        n = len(yrs)
        span = int(yrs[-1] - yrs[0])
        if span == 0:
            continue

        # 1. CAGR
        rev_cagr = (revs[-1] / revs[0]) ** (1.0 / span) - 1 if revs[0] > 0 else None
        op_cagr = None
        if ops[0] > 0 and ops[-1] > 0:
            op_cagr = (ops[-1] / ops[0]) ** (1.0 / span) - 1

        # 2. Growth Acceleration (이계도함수)
        #    Compare recent growth rate vs earlier growth rate.
        #    If recent > earlier, acceleration is positive.
        accel = None
        if n >= 3:
            yoy_rates = []
            for i in range(1, n):
                if revs[i - 1] > 0:
                    yoy_rates.append(revs[i] / revs[i - 1] - 1)
            if len(yoy_rates) >= 2:
                mid = len(yoy_rates) // 2
                early_avg = np.mean(yoy_rates[:mid])
                late_avg = np.mean(yoy_rates[mid:])
                accel = late_avg - early_avg

        # 3. Earnings Growth Rate (PEG의 'G')
        #    CAGR of net_income (for companies with positive earnings both ends)
        earnings_growth = None
        if nets[0] > 0 and nets[-1] > 0:
            earnings_growth = (nets[-1] / nets[0]) ** (1.0 / span) - 1

        # Latest values
        latest_rev = revs[-1]
        latest_opm = opms[-1] if pd.notna(opms[-1]) else None

        results.append({
            "corp_name": corp_name,
            "stock_code": stock_code,
            "market": market,
            "years": f"{int(yrs[0])}-{int(yrs[-1])}",
            "data_points": n,
            "latest_rev": latest_rev,
            "latest_opm": latest_opm,
            "rev_cagr": rev_cagr,
            "op_cagr": op_cagr,
            "earnings_growth": earnings_growth,
            "accel": accel,
        })

    out = pd.DataFrame(results)
    if out.empty:
        return out

    valid_sorts = {"rev_cagr", "op_cagr", "earnings_growth", "accel", "latest_rev", "latest_opm"}
    if sort_by not in valid_sorts:
        sort_by = "rev_cagr"
    out = out.sort_values(sort_by, ascending=False, na_position="last")
    return out.head(limit).reset_index(drop=True)

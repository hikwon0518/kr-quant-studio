from __future__ import annotations

import duckdb
import pandas as pd

from krqs.data.db.repositories.prices import get_latest_prices_bulk
from krqs.services.screener_service import get_available_years, get_growth_analysis


def get_growth_valuation_table(
    con: duckdb.DuckDBPyConnection,
    *,
    fiscal_year: int | None = None,
    min_years: int = 2,
    limit: int = 200,
) -> pd.DataFrame:
    """Merge growth data + latest prices + valuation.

    Steps
    -----
    1. Get growth analysis from screener_service.get_growth_analysis()
    2. Get latest prices via prices.get_latest_prices_bulk()
    3. Join on stock_code
    4. Compute PER = marcap / net_income, PBR = marcap / total_equity
    5. Compute PEG = PER / (earnings_growth * 100) where earnings_growth > 0

    Returns a DataFrame with: corp_name, stock_code, market, latest_rev,
    latest_opm, rev_cagr, op_cagr, earnings_growth, accel, close, marcap,
    per, pbr, peg.
    """
    # Determine the years to use for growth analysis
    available = get_available_years(con)
    if not available:
        return pd.DataFrame()

    if fiscal_year is not None:
        # Use fiscal_year as the latest, include enough prior years
        years = [y for y in available if y <= fiscal_year]
    else:
        years = available

    if len(years) < min_years:
        return pd.DataFrame()

    # 1. Growth analysis from screener_service
    growth_df = get_growth_analysis(
        con,
        years=years,
        min_years=min_years,
        sort_by="rev_cagr",
        limit=limit * 3,  # fetch extra to allow filtering later
    )
    if growth_df.empty:
        return pd.DataFrame()

    # 2. Latest prices
    stock_codes = growth_df["stock_code"].dropna().tolist()
    prices_df = get_latest_prices_bulk(con, stock_codes=stock_codes)
    if prices_df.empty:
        return growth_df.head(limit).reset_index(drop=True)

    # 3. PER / PBR from SQL: latest price joined with latest annual financials
    valuation_df = _get_valuation_bulk(con, fiscal_year=fiscal_year)

    # 4. Merge growth + prices
    merged = growth_df.merge(
        prices_df[["stock_code", "close", "marcap"]],
        on="stock_code",
        how="left",
    )

    # Merge valuation (PER, PBR)
    if not valuation_df.empty:
        merged = merged.merge(
            valuation_df[["stock_code", "per", "pbr"]],
            on="stock_code",
            how="left",
        )
    else:
        merged["per"] = None
        merged["pbr"] = None

    # 5. Compute PEG = PER / (earnings_growth * 100) where earnings_growth > 0
    merged["per"] = pd.to_numeric(merged["per"], errors="coerce")
    merged["earnings_growth"] = pd.to_numeric(merged["earnings_growth"], errors="coerce")
    merged["peg"] = merged.apply(_compute_peg_row, axis=1)

    # Select and order columns
    keep_cols = [
        "corp_name", "stock_code", "market",
        "latest_rev", "latest_opm",
        "rev_cagr", "op_cagr", "earnings_growth", "accel",
        "close", "marcap",
        "per", "pbr", "peg",
    ]
    for col in keep_cols:
        if col not in merged.columns:
            merged[col] = None

    return merged[keep_cols].head(limit).reset_index(drop=True)


def _compute_peg_row(row: pd.Series) -> float | None:
    """Compute PEG for a single row."""
    per = row.get("per")
    eg = row.get("earnings_growth")
    if pd.isna(per) or pd.isna(eg) or eg <= 0:
        return None
    return per / (eg * 100.0)


def _get_valuation_bulk(
    con: duckdb.DuckDBPyConnection,
    *,
    fiscal_year: int | None = None,
) -> pd.DataFrame:
    """Bulk PER/PBR via SQL: latest price x latest annual financials."""
    fy_filter = ""
    params: list[object] = []
    if fiscal_year is not None:
        fy_filter = "AND f.fiscal_year = ?"
        params.append(fiscal_year)

    sql = f"""
        WITH latest_price AS (
            SELECT stock_code, close, marcap, shares_out,
                   ROW_NUMBER() OVER (
                       PARTITION BY stock_code ORDER BY trade_date DESC
                   ) AS rn
            FROM price_daily
        ),
        latest_fin AS (
            SELECT c.stock_code,
                   f.net_income,
                   f.total_equity,
                   f.fiscal_year,
                   ROW_NUMBER() OVER (
                       PARTITION BY c.stock_code ORDER BY f.fiscal_year DESC
                   ) AS rn
            FROM financials_quarterly f
            JOIN corps c ON f.corp_code = c.corp_code
            WHERE f.fiscal_quarter = 4
              AND c.stock_code IS NOT NULL
              {fy_filter}
        )
        SELECT
            lp.stock_code,
            lp.close,
            lp.marcap,
            lf.net_income,
            lf.total_equity,
            CASE WHEN lf.net_income > 0
                 THEN lp.marcap * 1.0 / lf.net_income
                 ELSE NULL END AS per,
            CASE WHEN lf.total_equity > 0
                 THEN lp.marcap * 1.0 / lf.total_equity
                 ELSE NULL END AS pbr
        FROM latest_price lp
        JOIN latest_fin lf ON lp.stock_code = lf.stock_code
        WHERE lp.rn = 1 AND lf.rn = 1
    """

    try:
        result = con.execute(sql, params)
        cols = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()

from __future__ import annotations

import duckdb
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

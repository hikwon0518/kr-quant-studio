from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import duckdb
import pandas as pd


def upsert_prices(
    con: duckdb.DuckDBPyConnection,
    stock_code: str,
    df: pd.DataFrame,
    *,
    now: datetime | None = None,
) -> int:
    """Upsert OHLCV rows from a DataFrame with DateTimeIndex.

    Expected columns: Open, High, Low, Close, Volume, and optionally
    Change, Marcap.  Computes shares_out = marcap / close when marcap
    is available.  Returns number of rows upserted.
    """
    if df is None or df.empty:
        return 0

    timestamp = now or datetime.now(timezone.utc)

    rows: list[tuple] = []
    for trade_date, row in df.iterrows():
        close_val = int(row["Close"]) if pd.notna(row.get("Close")) else None
        open_val = int(row["Open"]) if pd.notna(row.get("Open")) else None
        high_val = int(row["High"]) if pd.notna(row.get("High")) else None
        low_val = int(row["Low"]) if pd.notna(row.get("Low")) else None
        volume_val = int(row["Volume"]) if pd.notna(row.get("Volume")) else None

        marcap_val: int | None = None
        if "Marcap" in row.index and pd.notna(row.get("Marcap")):
            marcap_val = int(row["Marcap"])

        shares_out: int | None = None
        if marcap_val and close_val and close_val > 0:
            shares_out = marcap_val // close_val

        td = pd.Timestamp(trade_date)
        rows.append((
            stock_code,
            td.strftime("%Y-%m-%d"),
            open_val,
            high_val,
            low_val,
            close_val,
            volume_val,
            marcap_val,
            shares_out,
        ))

    if not rows:
        return 0

    con.executemany(
        """
        INSERT INTO price_daily (
            stock_code, trade_date,
            open, high, low, close, volume,
            marcap, shares_out
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (stock_code, trade_date) DO UPDATE SET
            open       = excluded.open,
            high       = excluded.high,
            low        = excluded.low,
            close      = excluded.close,
            volume     = excluded.volume,
            marcap     = excluded.marcap,
            shares_out = excluded.shares_out
        """,
        rows,
    )
    return len(rows)


def get_price_range(
    con: duckdb.DuckDBPyConnection,
    stock_code: str,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    """Return price rows for a stock between start_date and end_date."""
    result = con.execute(
        """
        SELECT stock_code, trade_date, open, high, low, close,
               volume, marcap, shares_out
        FROM price_daily
        WHERE stock_code = ? AND trade_date >= ? AND trade_date <= ?
        ORDER BY trade_date
        """,
        [stock_code, start_date, end_date],
    ).fetchall()
    cols = [
        "stock_code", "trade_date", "open", "high", "low", "close",
        "volume", "marcap", "shares_out",
    ]
    return [dict(zip(cols, r)) for r in result]


def get_latest_price(
    con: duckdb.DuckDBPyConnection,
    stock_code: str,
) -> dict[str, Any] | None:
    """Return the most recent price row for a stock, or None."""
    row = con.execute(
        """
        SELECT stock_code, trade_date, open, high, low, close,
               volume, marcap, shares_out
        FROM price_daily
        WHERE stock_code = ?
        ORDER BY trade_date DESC
        LIMIT 1
        """,
        [stock_code],
    ).fetchone()
    if row is None:
        return None
    cols = [
        "stock_code", "trade_date", "open", "high", "low", "close",
        "volume", "marcap", "shares_out",
    ]
    return dict(zip(cols, row))


def get_latest_prices_bulk(
    con: duckdb.DuckDBPyConnection,
    stock_codes: list[str] | None = None,
) -> pd.DataFrame:
    """Return a DataFrame with the latest price per stock.

    If stock_codes is None, returns latest prices for all stocks.
    """
    if stock_codes is not None and len(stock_codes) == 0:
        return pd.DataFrame()

    base_query = """
        SELECT p.stock_code, p.trade_date, p.open, p.high, p.low,
               p.close, p.volume, p.marcap, p.shares_out
        FROM price_daily p
        INNER JOIN (
            SELECT stock_code, MAX(trade_date) AS max_date
            FROM price_daily
            {where_clause}
            GROUP BY stock_code
        ) latest
        ON p.stock_code = latest.stock_code AND p.trade_date = latest.max_date
        ORDER BY p.stock_code
    """

    if stock_codes is not None:
        placeholders = ", ".join(["?"] * len(stock_codes))
        where_clause = f"WHERE stock_code IN ({placeholders})"
        query = base_query.format(where_clause=where_clause)
        result = con.execute(query, stock_codes).fetchall()
    else:
        query = base_query.format(where_clause="")
        result = con.execute(query).fetchall()

    cols = [
        "stock_code", "trade_date", "open", "high", "low", "close",
        "volume", "marcap", "shares_out",
    ]
    return pd.DataFrame(result, columns=cols)


def get_valuation(
    con: duckdb.DuckDBPyConnection,
    stock_code: str,
) -> dict[str, Any] | None:
    """JOIN price_daily with financials_quarterly to compute PER/PBR."""
    row = con.execute(
        """
        SELECT p.close, p.marcap, p.shares_out,
               f.net_income, f.total_equity, f.operating_income,
               CASE WHEN f.net_income > 0
                    THEN p.marcap * 1.0 / f.net_income
                    ELSE NULL END AS per,
               CASE WHEN f.total_equity > 0
                    THEN p.marcap * 1.0 / f.total_equity
                    ELSE NULL END AS pbr
        FROM (
            SELECT * FROM price_daily
            WHERE stock_code = ?
            ORDER BY trade_date DESC LIMIT 1
        ) p
        CROSS JOIN (
            SELECT * FROM financials_quarterly
            WHERE corp_code = (
                SELECT corp_code FROM corps WHERE stock_code = ?
            )
            AND fiscal_quarter = 4
            ORDER BY fiscal_year DESC LIMIT 1
        ) f
        """,
        [stock_code, stock_code],
    ).fetchone()
    if row is None:
        return None
    cols = [
        "close", "marcap", "shares_out",
        "net_income", "total_equity", "operating_income",
        "per", "pbr",
    ]
    return dict(zip(cols, row))

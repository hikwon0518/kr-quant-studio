from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Callable

import duckdb

from krqs.data.fdr.client import fetch_ohlcv, fetch_stock_listing
from krqs.data.db.repositories.corps import get_all_listed
from krqs.data.db.repositories.prices import get_latest_price, upsert_prices


@dataclass(frozen=True)
class PriceSyncResult:
    stock_code: str
    status: str  # ok | no_data | error
    rows_synced: int = 0
    message: str | None = None


def sync_stock_prices(
    con: duckdb.DuckDBPyConnection,
    stock_code: str,
    years: int = 3,
) -> PriceSyncResult:
    """Sync price history for a single stock."""
    today = date.today()
    start = f"{today.year - years}-01-01"
    end = today.strftime("%Y-%m-%d")

    try:
        df = fetch_ohlcv(stock_code, start, end)
    except Exception as e:
        return PriceSyncResult(
            stock_code=stock_code,
            status="error",
            message=repr(e),
        )

    if df is None or df.empty:
        return PriceSyncResult(
            stock_code=stock_code,
            status="no_data",
        )

    rows = upsert_prices(con, stock_code, df)
    return PriceSyncResult(
        stock_code=stock_code,
        status="ok",
        rows_synced=rows,
    )


def _has_recent_prices(
    con: duckdb.DuckDBPyConnection,
    stock_code: str,
) -> bool:
    """Check whether we already have price data within the last 7 days."""
    latest = get_latest_price(con, stock_code)
    if latest is None:
        return False
    trade_date = latest["trade_date"]
    # DuckDB returns date objects
    if isinstance(trade_date, str):
        from datetime import datetime
        trade_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
    delta = date.today() - trade_date
    return delta.days <= 7


def sync_all_prices(
    con: duckdb.DuckDBPyConnection,
    years: int = 3,
    limit: int | None = None,
    force: bool = False,
    progress_callback: Callable[
        [int, int, PriceSyncResult], None
    ] | None = None,
) -> list[PriceSyncResult]:
    """Batch sync prices for all listed stocks.

    Args:
        con: DuckDB connection.
        years: Number of years of history to fetch.
        limit: Sync only first N stocks (for testing; None = all).
        force: Re-sync even if recent data already exists.
        progress_callback: Called after each stock with (seq, total, result).

    Returns:
        List of PriceSyncResult for each stock processed.
    """
    corps = get_all_listed(con)
    if limit is not None and limit > 0:
        corps = corps[:limit]

    results: list[PriceSyncResult] = []
    total = len(corps)

    for idx, corp in enumerate(corps):
        stock_code = corp["stock_code"]
        seq = idx + 1

        # Resume support: skip if recent data exists
        if not force and _has_recent_prices(con, stock_code):
            result = PriceSyncResult(
                stock_code=stock_code,
                status="skipped",
            )
        else:
            result = sync_stock_prices(con, stock_code, years=years)

        results.append(result)
        if progress_callback is not None:
            progress_callback(seq, total, result)

    return results

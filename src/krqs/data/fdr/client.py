from __future__ import annotations

import time

import FinanceDataReader as fdr
import pandas as pd


def fetch_ohlcv(stock_code: str, start: str, end: str) -> pd.DataFrame:
    """Fetch OHLCV for one stock. Returns df with DateTimeIndex, columns: Open/High/Low/Close/Volume/Change."""
    return fdr.DataReader(stock_code, start, end)


def fetch_stock_listing() -> pd.DataFrame:
    """Fetch all KRX listed stocks with Marcap data."""
    return fdr.StockListing("KRX")


def fetch_ohlcv_batch(
    stock_codes: list[str],
    start: str,
    end: str,
    delay: float = 0.5,
    progress_callback=None,
) -> dict[str, pd.DataFrame]:
    """Batch fetch with rate limiting. Returns {stock_code: df}."""
    results: dict[str, pd.DataFrame] = {}
    for i, code in enumerate(stock_codes):
        try:
            df = fdr.DataReader(code, start, end)
            if df is not None and not df.empty:
                results[code] = df
        except Exception:
            pass  # skip failed stocks
        if i > 0:
            time.sleep(delay)
        if progress_callback:
            progress_callback(i + 1, len(stock_codes), code, code in results)
    return results

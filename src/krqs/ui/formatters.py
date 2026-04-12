from __future__ import annotations

import pandas as pd


def format_krw(amount_eok: float | int | None) -> str:
    """Format amount in 억원 to human-readable Korean currency.

    - None/NaN → "-"
    - >= 10000억 (1조) → "X.X조"
    - < 10000억 → "X,XXX억"
    """
    if amount_eok is None or not pd.notna(amount_eok):
        return "-"
    if amount_eok >= 10_000:
        return f"{amount_eok / 10_000:.1f}조"
    return f"{amount_eok:,.0f}억"

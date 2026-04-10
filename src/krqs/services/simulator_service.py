from __future__ import annotations

from dataclasses import dataclass

import duckdb

from krqs.data.db.repositories.corps import find_by_name, get_by_corp_code
from krqs.data.db.repositories.financials import get_history, get_latest_annual
from krqs.domain.operating_leverage import BaselineInputs, GpmBand


@dataclass(frozen=True)
class CorpSearchResult:
    corp_code: str
    corp_name: str
    stock_code: str | None

    @property
    def display(self) -> str:
        code = self.stock_code or "-"
        return f"{self.corp_name} ({code})"


@dataclass(frozen=True)
class CorpBaseline:
    corp_code: str
    corp_name: str
    stock_code: str | None
    fiscal_year: int
    baseline: BaselineInputs
    historical_gpm: list[float]


def search_corporations(
    con: duckdb.DuckDBPyConnection, query: str, limit: int = 20
) -> list[CorpSearchResult]:
    query = query.strip()
    if not query:
        return []
    rows = find_by_name(con, query, limit=limit)
    return [
        CorpSearchResult(
            corp_code=str(r["corp_code"]),
            corp_name=str(r["corp_name"]),
            stock_code=r.get("stock_code"),
        )
        for r in rows
    ]


def load_corp_baseline(
    con: duckdb.DuckDBPyConnection, corp_code: str
) -> CorpBaseline | None:
    corp = get_by_corp_code(con, corp_code)
    if corp is None:
        return None
    latest = get_latest_annual(con, corp_code)
    if latest is None or latest.get("revenue") is None:
        return None

    baseline = BaselineInputs(
        revenue=int(latest["revenue"] or 0),
        cogs=int(latest["cogs"] or 0),
        sga=int(latest["sga"] or 0),
        interest_expense=int(latest["interest_expense"] or 0),
    )

    history = get_history(con, corp_code, quarter=4)
    historical_gpm = [
        float(h["gpm"]) for h in history if h.get("gpm") is not None
    ]

    return CorpBaseline(
        corp_code=corp_code,
        corp_name=str(corp.get("corp_name") or ""),
        stock_code=corp.get("stock_code"),
        fiscal_year=int(latest["fiscal_year"]),
        baseline=baseline,
        historical_gpm=historical_gpm,
    )


def suggest_gpm_band(historical_gpm: list[float]) -> GpmBand | None:
    if len(historical_gpm) < 2:
        return None
    sorted_gpm = sorted(historical_gpm)
    low = sorted_gpm[0]
    high = sorted_gpm[-1]
    mid = sorted_gpm[len(sorted_gpm) // 2]
    return GpmBand(
        low=round(low, 4),
        mid=round(mid, 4),
        high=round(high, 4),
    )

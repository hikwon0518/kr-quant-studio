from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Callable

import duckdb

from krqs.data.dart.client import DartAPIError, DartClient
from krqs.data.dart.corp_code import filter_listed, parse_corp_code_zip
from krqs.data.dart.parsers import parse_fnltt_single_acnt_all
from krqs.data.db.repositories.corps import count_listed, upsert_corps
from krqs.data.db.repositories.financials import upsert_financials, upsert_raw_response


@dataclass(frozen=True)
class CorpCodeSyncResult:
    downloaded_bytes: int
    parsed_total: int
    listed_upserted: int
    listed_total_in_db: int


@dataclass(frozen=True)
class YearSyncOutcome:
    year: int
    status: str  # ok | no_data | parse_failed | error
    revenue: int | None = None
    operating_income: int | None = None
    message: str | None = None


@dataclass(frozen=True)
class FinancialsSyncResult:
    corp_code: str
    years_requested: list[int]
    outcomes: list[YearSyncOutcome] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return sum(1 for o in self.outcomes if o.status == "ok")


def sync_corp_codes(con: duckdb.DuckDBPyConnection) -> CorpCodeSyncResult:
    with DartClient() as client:
        zip_bytes = client.fetch_corp_code_zip()

    entries = parse_corp_code_zip(zip_bytes)
    listed = filter_listed(entries)
    upserted = upsert_corps(con, listed)
    total = count_listed(con)

    return CorpCodeSyncResult(
        downloaded_bytes=len(zip_bytes),
        parsed_total=len(entries),
        listed_upserted=upserted,
        listed_total_in_db=total,
    )


def sync_corp_financials(
    con: duckdb.DuckDBPyConnection,
    corp_code: str,
    years: int = 5,
    *,
    fs_div: str = "CFS",
    progress_callback: Callable[[int, int, YearSyncOutcome], None] | None = None,
) -> FinancialsSyncResult:
    current_year = date.today().year
    target_years = list(range(current_year - years, current_year))
    outcomes: list[YearSyncOutcome] = []

    with DartClient() as client:
        for idx, year in enumerate(target_years):
            outcome = _fetch_and_upsert_one_year(con, client, corp_code, year, fs_div)
            outcomes.append(outcome)
            if progress_callback is not None:
                progress_callback(idx + 1, len(target_years), outcome)

    return FinancialsSyncResult(
        corp_code=corp_code,
        years_requested=target_years,
        outcomes=outcomes,
    )


def _fetch_and_upsert_one_year(
    con: duckdb.DuckDBPyConnection,
    client: DartClient,
    corp_code: str,
    year: int,
    fs_div: str,
) -> YearSyncOutcome:
    try:
        response = client.fetch_single_company_financials(
            corp_code=corp_code,
            bsns_year=year,
            reprt_code="11011",
            fs_div=fs_div,
        )
    except DartAPIError as e:
        return YearSyncOutcome(year=year, status="error", message=str(e))
    except Exception as e:  # httpx, timeout, etc.
        return YearSyncOutcome(year=year, status="error", message=repr(e))

    if str(response.get("status")) == "013":
        return YearSyncOutcome(year=year, status="no_data")

    # 원본 JSON 저장 (L3 감사 로그)
    upsert_raw_response(
        con,
        corp_code=corp_code,
        report_code="11011",
        bsns_year=year,
        endpoint="fnlttSinglAcntAll",
        raw_json=response,
    )

    parsed = parse_fnltt_single_acnt_all(response)
    if parsed is None:
        return YearSyncOutcome(year=year, status="parse_failed")

    upsert_financials(con, parsed)
    return YearSyncOutcome(
        year=year,
        status="ok",
        revenue=parsed.revenue,
        operating_income=parsed.operating_income,
    )

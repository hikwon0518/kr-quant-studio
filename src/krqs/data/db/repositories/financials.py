from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import duckdb

from krqs.data.dart.parsers import ParsedFinancials

REPRT_CODE_TO_QUARTER: dict[str, int] = {
    "11013": 1,  # 1분기보고서
    "11012": 2,  # 반기보고서
    "11014": 3,  # 3분기보고서
    "11011": 4,  # 사업보고서 (연간)
}


def upsert_financials(
    con: duckdb.DuckDBPyConnection,
    parsed: ParsedFinancials,
    *,
    now: datetime | None = None,
) -> None:
    timestamp = now or datetime.now(timezone.utc)
    quarter = REPRT_CODE_TO_QUARTER.get(parsed.reprt_code, 4)

    gpm: float | None = None
    if parsed.gross_profit is not None and parsed.revenue:
        gpm = parsed.gross_profit / parsed.revenue

    opm: float | None = None
    if parsed.operating_income is not None and parsed.revenue:
        opm = parsed.operating_income / parsed.revenue

    roe: float | None = None
    if parsed.net_income is not None and parsed.total_equity:
        roe = parsed.net_income / parsed.total_equity

    debt_ratio: float | None = None
    if parsed.total_liabilities is not None and parsed.total_assets:
        debt_ratio = parsed.total_liabilities / parsed.total_assets

    ebitda: int | None = None
    if parsed.operating_income is not None and parsed.depreciation is not None:
        ebitda = parsed.operating_income + parsed.depreciation
    elif parsed.operating_income is not None:
        ebitda = parsed.operating_income

    ebitda_margin: float | None = None
    if ebitda is not None and parsed.revenue:
        ebitda_margin = ebitda / parsed.revenue

    con.execute(
        """
        INSERT INTO financials_quarterly (
            corp_code, fiscal_year, fiscal_quarter, period_end,
            revenue, cogs, gross_profit, sga,
            operating_income, interest_expense, net_income,
            total_assets, cash_and_equivalents,
            short_term_investments, total_debt,
            total_equity, total_liabilities, depreciation, ppe, retained_earnings,
            gpm, opm, roe, debt_ratio, ebitda, ebitda_margin,
            revenue_yoy, opm_yoy, source_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (corp_code, fiscal_year, fiscal_quarter) DO UPDATE SET
            revenue              = excluded.revenue,
            cogs                 = excluded.cogs,
            gross_profit         = excluded.gross_profit,
            sga                  = excluded.sga,
            operating_income     = excluded.operating_income,
            interest_expense     = excluded.interest_expense,
            net_income           = excluded.net_income,
            total_assets         = excluded.total_assets,
            cash_and_equivalents = excluded.cash_and_equivalents,
            total_equity         = excluded.total_equity,
            total_liabilities    = excluded.total_liabilities,
            depreciation         = excluded.depreciation,
            ppe                  = excluded.ppe,
            retained_earnings    = excluded.retained_earnings,
            gpm                  = excluded.gpm,
            opm                  = excluded.opm,
            roe                  = excluded.roe,
            debt_ratio           = excluded.debt_ratio,
            ebitda               = excluded.ebitda,
            ebitda_margin        = excluded.ebitda_margin,
            source_updated_at    = excluded.source_updated_at
        """,
        [
            parsed.corp_code,
            parsed.bsns_year,
            quarter,
            None,
            parsed.revenue,
            parsed.cogs,
            parsed.gross_profit,
            parsed.sga,
            parsed.operating_income,
            parsed.interest_expense,
            parsed.net_income,
            parsed.total_assets,
            parsed.cash_and_equivalents,
            None,
            None,
            parsed.total_equity,
            parsed.total_liabilities,
            parsed.depreciation,
            parsed.ppe,
            parsed.retained_earnings,
            gpm,
            opm,
            roe,
            debt_ratio,
            ebitda,
            ebitda_margin,
            None,
            None,
            timestamp,
        ],
    )


def get_latest_annual(
    con: duckdb.DuckDBPyConnection, corp_code: str
) -> dict[str, object] | None:
    result = con.execute(
        """
        SELECT corp_code, fiscal_year, fiscal_quarter,
               revenue, cogs, gross_profit, sga,
               operating_income, interest_expense, net_income,
               total_assets, cash_and_equivalents, gpm, opm
        FROM financials_quarterly
        WHERE corp_code = ? AND fiscal_quarter = 4
        ORDER BY fiscal_year DESC
        LIMIT 1
        """,
        [corp_code],
    ).fetchone()
    if result is None:
        return None
    cols = [
        "corp_code",
        "fiscal_year",
        "fiscal_quarter",
        "revenue",
        "cogs",
        "gross_profit",
        "sga",
        "operating_income",
        "interest_expense",
        "net_income",
        "total_assets",
        "cash_and_equivalents",
        "gpm",
        "opm",
    ]
    return dict(zip(cols, result))


def get_history(
    con: duckdb.DuckDBPyConnection, corp_code: str, quarter: int = 4
) -> list[dict[str, object]]:
    rows = con.execute(
        """
        SELECT fiscal_year, revenue, cogs, gross_profit, sga,
               operating_income, gpm, opm
        FROM financials_quarterly
        WHERE corp_code = ? AND fiscal_quarter = ?
        ORDER BY fiscal_year
        """,
        [corp_code, quarter],
    ).fetchall()
    cols = [
        "fiscal_year",
        "revenue",
        "cogs",
        "gross_profit",
        "sga",
        "operating_income",
        "gpm",
        "opm",
    ]
    return [dict(zip(cols, r)) for r in rows]


def upsert_raw_response(
    con: duckdb.DuckDBPyConnection,
    corp_code: str,
    report_code: str,
    bsns_year: int,
    endpoint: str,
    raw_json: dict[str, Any],
    *,
    now: datetime | None = None,
) -> None:
    timestamp = now or datetime.now(timezone.utc)
    con.execute(
        """
        INSERT INTO dart_raw_responses
            (corp_code, report_code, bsns_year, endpoint, fetched_at, raw_json)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (corp_code, report_code, bsns_year, endpoint) DO UPDATE SET
            fetched_at = excluded.fetched_at,
            raw_json   = excluded.raw_json
        """,
        [corp_code, report_code, bsns_year, endpoint, timestamp,
         json.dumps(raw_json, ensure_ascii=False)],
    )


def get_dart_source_metadata(
    con: duckdb.DuckDBPyConnection,
    corp_code: str,
) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT bsns_year, report_code, endpoint, fetched_at,
               raw_json->>'$.list[0].rcept_no' AS rcept_no
        FROM dart_raw_responses
        WHERE corp_code = ?
        ORDER BY bsns_year DESC
        """,
        [corp_code],
    ).fetchall()
    cols = ["bsns_year", "report_code", "endpoint", "fetched_at", "rcept_no"]
    return [dict(zip(cols, r)) for r in rows]

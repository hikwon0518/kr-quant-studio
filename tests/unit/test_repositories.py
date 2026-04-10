from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from krqs.data.dart.corp_code import CorpCodeEntry
from krqs.data.dart.parsers import ParsedFinancials
from krqs.data.db.connection import initialize_schema
from krqs.data.db.repositories.corps import (
    count_listed,
    find_by_name,
    get_by_corp_code,
    upsert_corps,
)
from krqs.data.db.repositories.financials import (
    get_history,
    get_latest_annual,
    upsert_financials,
)


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    schema = (
        Path(__file__).parent.parent.parent
        / "src"
        / "krqs"
        / "data"
        / "db"
        / "schema.sql"
    )
    initialize_schema(c, schema)
    yield c
    c.close()


class TestCorpsRepository:
    def test_upsert_and_find(self, con):
        entries = [
            CorpCodeEntry("00126380", "삼성전자", "005930", "20240101"),
            CorpCodeEntry("00164779", "SK하이닉스", "000660", "20240101"),
        ]
        count = upsert_corps(con, entries)
        assert count == 2

        matches = find_by_name(con, "삼성")
        assert len(matches) == 1
        assert matches[0]["corp_code"] == "00126380"

    def test_upsert_updates_existing(self, con):
        entries = [CorpCodeEntry("00126380", "삼성전자", "005930", "20240101")]
        upsert_corps(con, entries)

        updated = [
            CorpCodeEntry("00126380", "삼성전자 주식회사", "005930", "20250101")
        ]
        upsert_corps(con, updated)

        result = get_by_corp_code(con, "00126380")
        assert result["corp_name"] == "삼성전자 주식회사"

    def test_count_listed(self, con):
        entries = [
            CorpCodeEntry("00001", "상장A", "111111", "20240101"),
            CorpCodeEntry("00002", "상장B", "222222", "20240101"),
            CorpCodeEntry("00003", "비상장", None, "20240101"),
        ]
        upsert_corps(con, entries)
        assert count_listed(con) == 2

    def test_find_by_name_exact_match_first(self, con):
        entries = [
            CorpCodeEntry("001", "하이닉스시스템아이씨", "100001", "20240101"),
            CorpCodeEntry("002", "SK하이닉스", "000660", "20240101"),
        ]
        upsert_corps(con, entries)
        matches = find_by_name(con, "SK하이닉스")
        assert matches[0]["corp_code"] == "002"


class TestFinancialsRepository:
    def _make_parsed(self, year: int, revenue: int, op_income: int) -> ParsedFinancials:
        return ParsedFinancials(
            corp_code="00126380",
            bsns_year=year,
            reprt_code="11011",
            revenue=revenue,
            cogs=int(revenue * 0.6),
            gross_profit=int(revenue * 0.4),
            sga=int(revenue * 0.15),
            operating_income=op_income,
            interest_expense=100_000_000_000,
            net_income=int(op_income * 0.7),
            total_assets=revenue * 2,
            cash_and_equivalents=int(revenue * 0.1),
        )

    def test_upsert_and_get_latest(self, con):
        parsed = self._make_parsed(2022, 1_000_000_000_000, 100_000_000_000)
        upsert_financials(con, parsed)

        latest = get_latest_annual(con, "00126380")
        assert latest is not None
        assert latest["fiscal_year"] == 2022
        assert latest["revenue"] == 1_000_000_000_000
        assert latest["operating_income"] == 100_000_000_000
        assert latest["gpm"] == pytest.approx(0.4)
        assert latest["opm"] == pytest.approx(0.1)

    def test_latest_returns_most_recent(self, con):
        for year, rev in [(2020, 800e9), (2021, 900e9), (2022, 1000e9)]:
            upsert_financials(
                con, self._make_parsed(year, int(rev), int(rev * 0.1))
            )
        latest = get_latest_annual(con, "00126380")
        assert latest["fiscal_year"] == 2022

    def test_upsert_is_idempotent(self, con):
        p = self._make_parsed(2022, 1_000_000_000_000, 100_000_000_000)
        upsert_financials(con, p)
        upsert_financials(con, p)
        count = con.execute(
            "SELECT COUNT(*) FROM financials_quarterly WHERE corp_code='00126380'"
        ).fetchone()[0]
        assert count == 1

    def test_get_history(self, con):
        for year, rev in [(2020, 800e9), (2021, 900e9), (2022, 1000e9)]:
            upsert_financials(
                con, self._make_parsed(year, int(rev), int(rev * 0.1))
            )
        history = get_history(con, "00126380", quarter=4)
        assert len(history) == 3
        assert [h["fiscal_year"] for h in history] == [2020, 2021, 2022]

    def test_missing_corp_returns_none(self, con):
        assert get_latest_annual(con, "99999999") is None

    def test_null_revenue_handled(self, con):
        parsed = ParsedFinancials(
            corp_code="00000000",
            bsns_year=2022,
            reprt_code="11011",
            revenue=None,
            cogs=None,
            gross_profit=None,
            sga=None,
            operating_income=None,
            interest_expense=None,
            net_income=None,
            total_assets=None,
            cash_and_equivalents=None,
        )
        upsert_financials(con, parsed)
        latest = get_latest_annual(con, "00000000")
        assert latest is not None
        assert latest["revenue"] is None
        assert latest["gpm"] is None

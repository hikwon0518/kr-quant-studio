from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from krqs.data.dart.corp_code import CorpCodeEntry
from krqs.data.dart.parsers import ParsedFinancials
from krqs.data.db.connection import initialize_schema
from krqs.data.db.repositories.corps import upsert_corps
from krqs.data.db.repositories.financials import upsert_financials
from krqs.services.simulator_service import (
    load_corp_baseline,
    search_corporations,
    suggest_gpm_band,
)

_SCHEMA = (
    Path(__file__).parent.parent.parent
    / "src"
    / "krqs"
    / "data"
    / "db"
    / "schema.sql"
)


@pytest.fixture
def con() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect(":memory:")
    initialize_schema(c, _SCHEMA)
    yield c
    c.close()


@pytest.fixture
def seeded_con(con) -> duckdb.DuckDBPyConnection:
    upsert_corps(
        con,
        [
            CorpCodeEntry("00126380", "삼성전자", "005930", "20240101"),
            CorpCodeEntry("00164779", "SK하이닉스", "000660", "20240101"),
        ],
    )
    for year, rev, op in [
        (2020, 200_000_000_000_000, 35_000_000_000_000),
        (2021, 279_000_000_000_000, 51_000_000_000_000),
        (2022, 302_000_000_000_000, 43_000_000_000_000),
    ]:
        upsert_financials(
            con,
            ParsedFinancials(
                corp_code="00126380",
                bsns_year=year,
                reprt_code="11011",
                revenue=rev,
                cogs=int(rev * 0.6),
                gross_profit=int(rev * 0.4),
                sga=int(rev * 0.15),
                operating_income=op,
                interest_expense=674_000_000_000,
                net_income=int(op * 0.7),
                total_assets=rev * 2,
                cash_and_equivalents=int(rev * 0.1),
                total_equity=int(rev * 0.8),
                total_liabilities=int(rev * 1.2),
                depreciation=int(rev * 0.05),
                ppe=int(rev * 0.3),
                retained_earnings=int(rev * 0.5),
            ),
        )
    return con


class TestSuggestGpmBand:
    def test_returns_none_for_insufficient_data(self):
        assert suggest_gpm_band([]) is None
        assert suggest_gpm_band([0.25]) is None

    def test_band_covers_range(self):
        band = suggest_gpm_band([0.15, 0.20, 0.25, 0.30])
        assert band is not None
        assert band.low == pytest.approx(0.15)
        assert band.high == pytest.approx(0.30)
        assert band.low <= band.mid <= band.high

    def test_handles_unsorted_input(self):
        band = suggest_gpm_band([0.30, 0.15, 0.25, 0.20])
        assert band.low == pytest.approx(0.15)
        assert band.high == pytest.approx(0.30)


class TestSearchCorporations:
    def test_empty_query_returns_empty(self, seeded_con):
        assert search_corporations(seeded_con, "") == []
        assert search_corporations(seeded_con, "   ") == []

    def test_finds_by_korean_name(self, seeded_con):
        results = search_corporations(seeded_con, "삼성")
        assert len(results) == 1
        assert results[0].corp_code == "00126380"
        assert results[0].stock_code == "005930"

    def test_display_format(self, seeded_con):
        results = search_corporations(seeded_con, "삼성전자")
        assert "삼성전자" in results[0].display
        assert "005930" in results[0].display


class TestLoadCorpBaseline:
    def test_returns_none_for_unknown_corp(self, seeded_con):
        assert load_corp_baseline(seeded_con, "99999999") is None

    def test_returns_none_for_corp_without_financials(self, seeded_con):
        assert load_corp_baseline(seeded_con, "00164779") is None

    def test_loads_latest_fiscal_year(self, seeded_con):
        loaded = load_corp_baseline(seeded_con, "00126380")
        assert loaded is not None
        assert loaded.corp_name == "삼성전자"
        assert loaded.fiscal_year == 2022
        assert loaded.baseline.revenue == 302_000_000_000_000

    def test_historical_gpm_populated(self, seeded_con):
        loaded = load_corp_baseline(seeded_con, "00126380")
        assert len(loaded.historical_gpm) == 3
        for g in loaded.historical_gpm:
            assert 0.39 < g < 0.41

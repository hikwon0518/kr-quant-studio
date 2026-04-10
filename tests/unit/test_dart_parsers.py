import json
from pathlib import Path

import pytest

from krqs.data.dart.parsers import (
    _parse_amount,
    parse_fnltt_single_acnt_all,
)

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sample_single_acnt.json"


@pytest.fixture
def sample_response() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


class TestParseAmount:
    def test_plain_number(self):
        assert _parse_amount("123456") == 123456

    def test_comma_separated(self):
        assert _parse_amount("302,231,360,000,000") == 302_231_360_000_000

    def test_empty_string(self):
        assert _parse_amount("") is None

    def test_dash(self):
        assert _parse_amount("-") is None

    def test_none(self):
        assert _parse_amount(None) is None

    def test_negative(self):
        assert _parse_amount("-1,000") == -1000

    def test_invalid(self):
        assert _parse_amount("abc") is None


class TestParseFinancials:
    def test_parses_sample_response(self, sample_response):
        result = parse_fnltt_single_acnt_all(sample_response)
        assert result is not None
        assert result.corp_code == "00126380"
        assert result.bsns_year == 2022
        assert result.reprt_code == "11011"

    def test_income_statement_fields(self, sample_response):
        r = parse_fnltt_single_acnt_all(sample_response)
        assert r.revenue == 302_231_360_000_000
        assert r.cogs == 190_041_770_000_000
        assert r.gross_profit == 112_189_590_000_000
        assert r.sga == 68_812_960_000_000
        assert r.operating_income == 43_376_630_000_000
        assert r.interest_expense == 674_500_000_000
        assert r.net_income == 55_654_077_000_000

    def test_balance_sheet_fields(self, sample_response):
        r = parse_fnltt_single_acnt_all(sample_response)
        assert r.total_assets == 448_424_507_000_000
        assert r.cash_and_equivalents == 49_680_278_000_000

    def test_empty_response_returns_none(self):
        assert parse_fnltt_single_acnt_all({"status": "013", "list": []}) is None
        assert parse_fnltt_single_acnt_all({"status": "000"}) is None

    def test_computes_gross_profit_when_missing(self):
        response = {
            "status": "000",
            "list": [
                {
                    "corp_code": "12345678",
                    "bsns_year": "2022",
                    "reprt_code": "11011",
                    "sj_div": "IS",
                    "account_id": "ifrs-full_Revenue",
                    "thstrm_amount": "1,000",
                },
                {
                    "corp_code": "12345678",
                    "bsns_year": "2022",
                    "reprt_code": "11011",
                    "sj_div": "IS",
                    "account_id": "ifrs-full_CostOfSales",
                    "thstrm_amount": "400",
                },
            ],
        }
        r = parse_fnltt_single_acnt_all(response)
        assert r.revenue == 1000
        assert r.cogs == 400
        assert r.gross_profit == 600

    def test_handles_korean_name_fallback(self):
        response = {
            "status": "000",
            "list": [
                {
                    "corp_code": "99999999",
                    "bsns_year": "2023",
                    "reprt_code": "11011",
                    "sj_div": "IS",
                    "account_id": "some-unknown-id",
                    "account_nm": "매출액",
                    "thstrm_amount": "5,000",
                },
                {
                    "corp_code": "99999999",
                    "bsns_year": "2023",
                    "reprt_code": "11011",
                    "sj_div": "IS",
                    "account_id": "",
                    "account_nm": "영업이익",
                    "thstrm_amount": "500",
                },
            ],
        }
        r = parse_fnltt_single_acnt_all(response)
        assert r.revenue == 5000
        assert r.operating_income == 500

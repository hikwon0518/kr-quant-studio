import zipfile
from io import BytesIO

import pytest

from krqs.data.dart.corp_code import (
    CorpCodeEntry,
    filter_listed,
    parse_corp_code_zip,
)


SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<result>
  <list>
    <corp_code>00126380</corp_code>
    <corp_name>삼성전자</corp_name>
    <stock_code>005930</stock_code>
    <modify_date>20240829</modify_date>
  </list>
  <list>
    <corp_code>00164779</corp_code>
    <corp_name>SK하이닉스</corp_name>
    <stock_code>000660</stock_code>
    <modify_date>20240712</modify_date>
  </list>
  <list>
    <corp_code>00555555</corp_code>
    <corp_name>비상장기업</corp_name>
    <stock_code> </stock_code>
    <modify_date>20230101</modify_date>
  </list>
  <list>
    <corp_code></corp_code>
    <corp_name>이름없음</corp_name>
  </list>
</result>
"""


@pytest.fixture
def sample_zip_bytes() -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("CORPCODE.xml", SAMPLE_XML.encode("utf-8"))
    return buf.getvalue()


class TestParseCorpCodeZip:
    def test_returns_entries(self, sample_zip_bytes):
        entries = parse_corp_code_zip(sample_zip_bytes)
        assert len(entries) == 3  # 빈 corp_code 행 제외

    def test_entry_fields(self, sample_zip_bytes):
        entries = parse_corp_code_zip(sample_zip_bytes)
        samsung = next(e for e in entries if e.corp_name == "삼성전자")
        assert samsung.corp_code == "00126380"
        assert samsung.stock_code == "005930"
        assert samsung.modify_date == "20240829"

    def test_unlisted_has_none_stock_code(self, sample_zip_bytes):
        entries = parse_corp_code_zip(sample_zip_bytes)
        unlisted = next(e for e in entries if e.corp_name == "비상장기업")
        assert unlisted.stock_code is None


class TestFilterListed:
    def test_excludes_unlisted(self):
        entries = [
            CorpCodeEntry("A", "listed", "005930", "20240101"),
            CorpCodeEntry("B", "unlisted", None, "20240101"),
        ]
        listed = filter_listed(entries)
        assert len(listed) == 1
        assert listed[0].corp_name == "listed"

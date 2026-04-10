from __future__ import annotations

import zipfile
from dataclasses import dataclass
from io import BytesIO
from xml.etree import ElementTree as ET


@dataclass(frozen=True)
class CorpCodeEntry:
    corp_code: str
    corp_name: str
    stock_code: str | None
    modify_date: str


def parse_corp_code_zip(zip_bytes: bytes) -> list[CorpCodeEntry]:
    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        xml_name = next(
            name for name in zf.namelist() if name.lower().endswith(".xml")
        )
        xml_bytes = zf.read(xml_name)

    root = ET.fromstring(xml_bytes)
    entries: list[CorpCodeEntry] = []

    for item in root.findall("list"):
        corp_code = (item.findtext("corp_code") or "").strip()
        corp_name = (item.findtext("corp_name") or "").strip()
        stock_code_raw = (item.findtext("stock_code") or "").strip()
        modify_date = (item.findtext("modify_date") or "").strip()

        if not corp_code or not corp_name:
            continue

        entries.append(
            CorpCodeEntry(
                corp_code=corp_code,
                corp_name=corp_name,
                stock_code=stock_code_raw or None,
                modify_date=modify_date,
            )
        )

    return entries


def filter_listed(entries: list[CorpCodeEntry]) -> list[CorpCodeEntry]:
    return [e for e in entries if e.stock_code]

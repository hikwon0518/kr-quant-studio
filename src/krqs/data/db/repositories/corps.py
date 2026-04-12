from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import duckdb

from krqs.data.dart.corp_code import CorpCodeEntry


def upsert_corps(
    con: duckdb.DuckDBPyConnection,
    entries: Iterable[CorpCodeEntry],
    *,
    now: datetime | None = None,
) -> int:
    timestamp = now or datetime.now(timezone.utc)
    rows = [
        (e.corp_code, e.stock_code, e.corp_name, None, None, None, timestamp)
        for e in entries
    ]
    if not rows:
        return 0
    con.executemany(
        """
        INSERT INTO corps (
            corp_code, stock_code, corp_name,
            sector_krx, sector_custom, market, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (corp_code) DO UPDATE SET
            stock_code   = excluded.stock_code,
            corp_name    = excluded.corp_name,
            last_updated = excluded.last_updated
        """,
        rows,
    )
    return len(rows)


def find_by_name(
    con: duckdb.DuckDBPyConnection, query: str, limit: int = 20
) -> list[dict[str, str | None]]:
    rows = con.execute(
        """
        SELECT corp_code, stock_code, corp_name
        FROM corps
        WHERE corp_name LIKE ?
        ORDER BY
            CASE WHEN corp_name = ? THEN 0 ELSE 1 END,
            corp_name
        LIMIT ?
        """,
        [f"%{query}%", query, limit],
    ).fetchall()
    return [
        {"corp_code": r[0], "stock_code": r[1], "corp_name": r[2]}
        for r in rows
    ]


def get_by_corp_code(
    con: duckdb.DuckDBPyConnection, corp_code: str
) -> dict[str, str | None] | None:
    row = con.execute(
        """
        SELECT corp_code, stock_code, corp_name, sector_krx, market
        FROM corps WHERE corp_code = ?
        """,
        [corp_code],
    ).fetchone()
    if row is None:
        return None
    return {
        "corp_code": row[0],
        "stock_code": row[1],
        "corp_name": row[2],
        "sector_krx": row[3],
        "market": row[4],
    }


def get_all_listed(
    con: duckdb.DuckDBPyConnection,
) -> list[dict[str, str]]:
    """Return all corps with a non-null stock_code (i.e. listed companies)."""
    rows = con.execute(
        """
        SELECT corp_code, stock_code, corp_name
        FROM corps
        WHERE stock_code IS NOT NULL
        ORDER BY corp_code
        """,
    ).fetchall()
    return [
        {"corp_code": r[0], "stock_code": r[1], "corp_name": r[2]}
        for r in rows
    ]


def count_listed(con: duckdb.DuckDBPyConnection) -> int:
    return con.execute(
        "SELECT COUNT(*) FROM corps WHERE stock_code IS NOT NULL"
    ).fetchone()[0]

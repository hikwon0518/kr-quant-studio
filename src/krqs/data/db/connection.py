from __future__ import annotations

from pathlib import Path

import duckdb

from krqs.config.settings import get_settings

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_connection(db_path: Path | str | None = None) -> duckdb.DuckDBPyConnection:
    if db_path is None:
        db_path = get_settings().db_path
    path = Path(db_path)
    if str(path) != ":memory:":
        path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    return con


def initialize_schema(
    con: duckdb.DuckDBPyConnection,
    schema_path: Path | None = None,
) -> None:
    path = schema_path or _SCHEMA_PATH
    con.execute(path.read_text(encoding="utf-8"))

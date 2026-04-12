from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from krqs.config.settings import get_settings

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def _find_seed_dir() -> Path:
    """Find seed data directory. Works both in dev (source tree) and deployed (pip install)."""
    # 1. Current working directory (Streamlit Cloud runs from repo root)
    cwd_seed = Path.cwd() / "data" / "seed"
    if cwd_seed.exists():
        return cwd_seed
    # 2. Relative to this file (local dev, running from source)
    for n in range(3, 7):
        candidate = Path(__file__).parents[n] / "data" / "seed"
        if candidate.exists():
            return candidate
    return cwd_seed  # fallback
_logger = logging.getLogger(__name__)


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


def load_seed_data(con: duckdb.DuckDBPyConnection) -> int:
    """Load seed parquet files into empty tables. Returns rows loaded."""
    total = 0
    seed_dir = _find_seed_dir()
    seeds = {
        "corps": seed_dir / "seed_corps.parquet",
        "financials_quarterly": seed_dir / "seed_financials.parquet",
        "price_daily": seed_dir / "seed_prices.parquet",
    }
    _logger.info("Seed directory: %s (exists=%s)", seed_dir, seed_dir.exists())
    for table, path in seeds.items():
        if not path.exists():
            _logger.info("Seed file not found: %s", path)
            continue
        try:
            count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            if count > 0:
                _logger.info("Table %s already has %d rows, skipping seed", table, count)
                continue
            ppath = str(path).replace("\\", "/")
            con.execute(
                f"INSERT INTO {table} BY NAME SELECT * FROM read_parquet('{ppath}')"
            )
            rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            _logger.info("Loaded %d rows into %s from seed", rows, table)
            total += rows
        except Exception as e:
            _logger.error("Failed to load seed for %s: %s", table, e)
    return total

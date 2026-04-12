from __future__ import annotations

import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import duckdb

from krqs.config.settings import get_settings


def _print_result(result: duckdb.DuckDBPyConnection) -> None:
    rows = result.fetchall()
    if not rows:
        print("(no rows)")
        return
    columns = [desc[0] for desc in result.description]
    widths = [len(c) for c in columns]
    str_rows: list[list[str]] = []
    for row in rows:
        cells = [str(v) if v is not None else "NULL" for v in row]
        str_rows.append(cells)
        for i, cell in enumerate(cells):
            widths[i] = max(widths[i], len(cell))

    header = "  ".join(c.ljust(w) for c, w in zip(columns, widths))
    separator = "  ".join("-" * w for w in widths)
    print(header)
    print(separator)
    for cells in str_rows:
        print("  ".join(c.ljust(w) for c, w in zip(cells, widths)))
    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")


def main() -> int:
    settings = get_settings()
    db_path = settings.db_path

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        print("Run `make sync-corps` first to create the database.")
        return 1

    print(f"DuckDB shell -- connected to {db_path}")
    print("Type SQL queries, or .tables / .schema <table> / .quit")
    print()

    con = duckdb.connect(str(db_path), read_only=True)

    try:
        buffer = ""
        while True:
            try:
                prompt = "sql> " if not buffer else " ... "
                line = input(prompt)
            except (EOFError, KeyboardInterrupt):
                print()
                break

            stripped = line.strip()

            if not buffer:
                if stripped in (".quit", ".exit", ".q"):
                    break
                if stripped == ".tables":
                    try:
                        _print_result(
                            con.execute(
                                "SELECT table_name FROM information_schema.tables "
                                "WHERE table_schema = 'main' ORDER BY table_name"
                            )
                        )
                    except duckdb.Error as exc:
                        print(f"ERROR: {exc}")
                    continue
                if stripped.startswith(".schema"):
                    parts = stripped.split(maxsplit=1)
                    table = parts[1] if len(parts) > 1 else None
                    try:
                        if table:
                            _print_result(
                                con.execute(
                                    "SELECT column_name, data_type, is_nullable "
                                    "FROM information_schema.columns "
                                    "WHERE table_name = ? ORDER BY ordinal_position",
                                    [table],
                                )
                            )
                        else:
                            _print_result(
                                con.execute(
                                    "SELECT table_name, column_name, data_type "
                                    "FROM information_schema.columns "
                                    "WHERE table_schema = 'main' "
                                    "ORDER BY table_name, ordinal_position"
                                )
                            )
                    except duckdb.Error as exc:
                        print(f"ERROR: {exc}")
                    continue

            buffer = f"{buffer} {line}" if buffer else line
            if ";" not in buffer:
                continue

            sql = buffer.strip()
            buffer = ""
            if not sql:
                continue

            try:
                result = con.execute(sql)
                if result.description:
                    _print_result(result)
                else:
                    print("OK")
            except duckdb.Error as exc:
                print(f"ERROR: {exc}")
    finally:
        con.close()

    print("Bye.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

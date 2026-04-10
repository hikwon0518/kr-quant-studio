from __future__ import annotations

from krqs.data.dart.client import DartClient
from krqs.data.dart.corp_code import filter_listed, parse_corp_code_zip
from krqs.data.db.connection import get_connection, initialize_schema
from krqs.data.db.repositories.corps import count_listed, upsert_corps


def main() -> int:
    print("Downloading corp_code.zip from DART...")
    with DartClient() as client:
        zip_bytes = client.fetch_corp_code_zip()
    print(f"  downloaded {len(zip_bytes):,} bytes")

    entries = parse_corp_code_zip(zip_bytes)
    listed = filter_listed(entries)
    print(f"  parsed {len(entries):,} entries ({len(listed):,} listed)")

    con = get_connection()
    initialize_schema(con)
    upserted = upsert_corps(con, listed)
    total_listed = count_listed(con)
    con.close()

    print(f"Upserted {upserted:,} listed corps.")
    print(f"Total listed in DB: {total_listed:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

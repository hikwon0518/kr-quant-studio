from __future__ import annotations

import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from krqs.data.db.connection import get_connection, initialize_schema
from krqs.services.data_sync_service import sync_corp_codes


def main() -> int:
    print("Downloading corp_code.zip from DART...")
    con = get_connection()
    initialize_schema(con)
    try:
        result = sync_corp_codes(con)
    finally:
        con.close()

    print(f"  downloaded {result.downloaded_bytes:,} bytes")
    print(f"  parsed {result.parsed_total:,} entries")
    print(f"Upserted {result.listed_upserted:,} listed corps.")
    print(f"Total listed in DB: {result.listed_total_in_db:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

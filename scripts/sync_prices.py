from __future__ import annotations

import argparse
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from krqs.data.db.connection import get_connection, initialize_schema
from krqs.services.price_sync_service import (
    PriceSyncResult,
    sync_all_prices,
    sync_stock_prices,
)


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = int(seconds) // 60
    secs = int(seconds) % 60
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m {secs}s"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync daily OHLCV price data from FinanceDataReader"
    )
    parser.add_argument(
        "--stock",
        type=str,
        default=None,
        help="Single stock code to sync (e.g. 005930)",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help="Number of years of history to fetch (default: 3)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Sync only first N stocks (for testing; 0 = all)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-sync even if recent data already exists",
    )
    args = parser.parse_args()

    con = get_connection()
    initialize_schema(con)

    # Single stock mode
    if args.stock:
        print(f"Syncing prices for {args.stock}  |  Years: {args.years}")
        print("-" * 60)
        result = sync_stock_prices(con, args.stock, years=args.years)
        if result.status == "ok":
            print(f"  OK   {args.stock}: {result.rows_synced} rows synced")
        elif result.status == "no_data":
            print(f"  --   {args.stock}: no data returned")
        else:
            print(f"  FAIL {args.stock}: {result.message}")
        con.close()
        return 0 if result.status == "ok" else 1

    # Bulk mode
    limit = args.limit if args.limit > 0 else None
    print(f"Years: {args.years}  |  Limit: {limit or 'all'}  |  Force: {args.force}")
    print("-" * 60)

    synced = 0
    skipped = 0
    failed = 0
    no_data = 0
    start_time = time.monotonic()

    def progress(seq: int, total: int, result: PriceSyncResult) -> None:
        nonlocal synced, skipped, failed, no_data

        elapsed = time.monotonic() - start_time
        processed = synced + failed + no_data + 1
        remaining = total - seq
        if processed > 0:
            avg = elapsed / processed
            eta = remaining * avg
            eta_str = _fmt_duration(eta)
        else:
            eta_str = "..."

        if result.status == "ok":
            synced += 1
            print(
                f"  [{seq}/{total}] OK   {result.stock_code} "
                f"{result.rows_synced} rows  (ETA {eta_str})"
            )
        elif result.status == "skipped":
            skipped += 1
            if skipped <= 5 or skipped % 100 == 0:
                print(
                    f"  [{seq}/{total}] SKIP {result.stock_code} "
                    f"(already synced)"
                )
        elif result.status == "no_data":
            no_data += 1
            if no_data <= 10 or no_data % 100 == 0:
                print(
                    f"  [{seq}/{total}] --   {result.stock_code} "
                    f"no data  (ETA {eta_str})"
                )
        else:
            failed += 1
            msg = result.message or ""
            print(
                f"  [{seq}/{total}] FAIL {result.stock_code} "
                f"{msg}  (ETA {eta_str})"
            )

    sync_all_prices(
        con,
        years=args.years,
        limit=limit,
        force=args.force,
        progress_callback=progress,
    )

    con.close()

    total_time = time.monotonic() - start_time
    print()
    print("=" * 60)
    print("Price sync complete")
    print(f"  Synced:   {synced}")
    print(f"  Skipped:  {skipped}  (already had recent data)")
    print(f"  No data:  {no_data}")
    print(f"  Failed:   {failed}")
    print(f"  Time:     {_fmt_duration(total_time)}")
    print("=" * 60)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

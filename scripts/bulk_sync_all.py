from __future__ import annotations

import argparse
import sys
import time

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from datetime import date

from krqs.data.dart.client import DartClient
from krqs.data.db.connection import get_connection, initialize_schema
from krqs.data.db.repositories.corps import get_all_listed
from krqs.services.data_sync_service import _fetch_and_upsert_one_year


def _has_data_for_year(con, corp_code: str, year: int) -> bool:
    """Check whether financials_quarterly already has annual data for this corp/year."""
    row = con.execute(
        """
        SELECT 1 FROM financials_quarterly
        WHERE corp_code = ? AND fiscal_year = ? AND fiscal_quarter = 4
        LIMIT 1
        """,
        [corp_code, year],
    ).fetchone()
    return row is not None


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
        description="Bulk-sync annual financial data for ALL listed companies from DART"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=date.today().year - 1,
        help="Target fiscal year (default: previous year)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-sync even if data already exists for the target year",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Sync only first N companies (for testing; 0 = all)",
    )
    parser.add_argument(
        "--fs-div",
        choices=["CFS", "OFS"],
        default="CFS",
        help="CFS=consolidated, OFS=separate (default: CFS)",
    )
    args = parser.parse_args()

    con = get_connection()
    initialize_schema(con)

    corps = get_all_listed(con)
    if args.limit > 0:
        corps = corps[: args.limit]

    total = len(corps)
    print(f"Target year: {args.year}  |  Companies: {total}  |  Force: {args.force}")
    print(f"FS division: {args.fs_div}")
    print("-" * 60)

    synced = 0
    skipped = 0
    failed = 0
    no_data = 0
    start_time = time.monotonic()

    with DartClient() as client:
        for idx, corp in enumerate(corps):
            corp_code = corp["corp_code"]
            corp_name = corp["corp_name"]
            seq = idx + 1

            # Resume support: skip if data already exists
            if not args.force and _has_data_for_year(con, corp_code, args.year):
                skipped += 1
                if skipped <= 5 or skipped % 100 == 0:
                    print(f"  [{seq}/{total}] SKIP {corp_name} (already synced)")
                continue

            outcome = _fetch_and_upsert_one_year(
                con, client, corp_code, args.year, args.fs_div
            )

            elapsed = time.monotonic() - start_time
            processed = synced + failed + no_data + 1
            remaining_corps = total - seq
            if processed > 0:
                avg_per_corp = elapsed / processed
                eta = remaining_corps * avg_per_corp
                eta_str = _fmt_duration(eta)
            else:
                eta_str = "..."

            BN = 100_000_000
            if outcome.status == "ok":
                synced += 1
                rev_bn = (outcome.revenue or 0) / BN
                print(
                    f"  [{seq}/{total}] OK   {corp_name}: "
                    f"revenue={rev_bn:,.0f}B  (ETA {eta_str})"
                )
            elif outcome.status == "no_data":
                no_data += 1
                if no_data <= 10 or no_data % 100 == 0:
                    print(
                        f"  [{seq}/{total}] --   {corp_name}: "
                        f"no data  (ETA {eta_str})"
                    )
            else:
                failed += 1
                msg = outcome.message or ""
                print(
                    f"  [{seq}/{total}] FAIL {corp_name}: "
                    f"{outcome.status} {msg}  (ETA {eta_str})"
                )

    con.close()

    total_time = time.monotonic() - start_time
    print()
    print("=" * 60)
    print(f"Bulk sync complete for fiscal year {args.year}")
    print(f"  Synced:   {synced}")
    print(f"  Skipped:  {skipped}  (already had data)")
    print(f"  No data:  {no_data}  (DART status 013)")
    print(f"  Failed:   {failed}")
    print(f"  Total:    {total}")
    print(f"  Time:     {_fmt_duration(total_time)}")
    print("=" * 60)

    return 0 if failed < total else 1


if __name__ == "__main__":
    raise SystemExit(main())

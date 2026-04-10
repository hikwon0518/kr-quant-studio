from __future__ import annotations

import argparse
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from krqs.data.db.connection import get_connection, initialize_schema
from krqs.data.db.repositories.corps import find_by_name
from krqs.services.data_sync_service import sync_corp_financials


def _resolve_corp(con, query: str) -> tuple[str, str] | None:
    if query.isdigit() and len(query) == 8:
        return query, f"(corp_code={query})"
    matches = find_by_name(con, query)
    if not matches:
        return None
    if len(matches) > 1:
        print(f"Multiple matches for '{query}':")
        for m in matches[:10]:
            print(
                f"  {m['corp_code']}  {m['corp_name']}  (stock={m['stock_code']})"
            )
        return None
    m = matches[0]
    return str(m["corp_code"]), str(m["corp_name"])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync annual financial data from DART"
    )
    parser.add_argument(
        "--corp", required=True, help="종목명 또는 corp_code (8자리)"
    )
    parser.add_argument(
        "--years", type=int, default=5, help="최근 N년치 (기본 5)"
    )
    parser.add_argument(
        "--fs-div",
        choices=["CFS", "OFS"],
        default="CFS",
        help="CFS=연결, OFS=개별 (기본 CFS)",
    )
    args = parser.parse_args()

    con = get_connection()
    initialize_schema(con)

    resolved = _resolve_corp(con, args.corp)
    if resolved is None:
        print(
            f"Could not resolve '{args.corp}'. "
            "Run `make sync-corps` first or provide an 8-digit corp_code."
        )
        con.close()
        return 1
    corp_code, corp_name = resolved
    print(f"Syncing {corp_name} ({corp_code}) - last {args.years} years")

    BN = 100_000_000

    def _progress(i, n, outcome):
        if outcome.status == "ok":
            rev_bn = (outcome.revenue or 0) / BN
            op_bn = (outcome.operating_income or 0) / BN
            print(
                f"  [{i}/{n}] {outcome.year}: "
                f"revenue={rev_bn:,.0f}억  operating_income={op_bn:,.0f}억"
            )
        else:
            msg = outcome.message or ""
            print(f"  [{i}/{n}] {outcome.year}: {outcome.status} {msg}")

    try:
        result = sync_corp_financials(
            con,
            corp_code,
            years=args.years,
            fs_div=args.fs_div,
            progress_callback=_progress,
        )
    finally:
        con.close()

    print(f"Done. {result.success_count}/{args.years} years synced.")
    return 0 if result.success_count > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

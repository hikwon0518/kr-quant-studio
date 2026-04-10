from __future__ import annotations

import argparse
from datetime import date

from krqs.data.dart.client import DartAPIError, DartClient
from krqs.data.dart.parsers import parse_fnltt_single_acnt_all
from krqs.data.db.connection import get_connection, initialize_schema
from krqs.data.db.repositories.corps import find_by_name
from krqs.data.db.repositories.financials import upsert_financials


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
        "--years", type=int, default=5, help="최근 N년치 동기화 (기본 5)"
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
            "Run `make sync-corps` first or provide an exact 8-digit corp_code."
        )
        return 1
    corp_code, corp_name = resolved
    print(f"Syncing {corp_name} ({corp_code}) — last {args.years} years")

    current_year = date.today().year
    success = 0
    with DartClient() as client:
        for year in range(current_year - args.years, current_year):
            try:
                resp = client.fetch_single_company_financials(
                    corp_code=corp_code,
                    bsns_year=year,
                    reprt_code="11011",
                    fs_div=args.fs_div,
                )
            except DartAPIError as e:
                print(f"  {year}: API error — {e}")
                continue
            except Exception as e:
                print(f"  {year}: unexpected error — {e}")
                continue

            if str(resp.get("status")) == "013":
                print(f"  {year}: no data (status 013)")
                continue

            parsed = parse_fnltt_single_acnt_all(resp)
            if parsed is None:
                print(f"  {year}: parse failed")
                continue

            upsert_financials(con, parsed)
            rev_bn = (parsed.revenue or 0) / 100_000_000
            opi_bn = (parsed.operating_income or 0) / 100_000_000
            print(
                f"  {year}: revenue={rev_bn:,.0f}억  "
                f"operating_income={opi_bn:,.0f}억"
            )
            success += 1

    con.close()
    print(f"Done. {success}/{args.years} years synced.")
    return 0 if success > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

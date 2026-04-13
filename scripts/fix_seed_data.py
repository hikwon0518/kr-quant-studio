"""Fix seed parquet files to populate missing fields.

Fixes:
1. corps: market field (KOSPI/KOSDAQ/KONEX) from FinanceDataReader
2. prices: marcap and shares_out from FDR StockListing + computation
3. financials: multi-year data and depreciation from DART API

Usage:
    .venv/Scripts/python.exe scripts/fix_seed_data.py [--skip-financials] [--fin-limit N]
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import FinanceDataReader as fdr
import pandas as pd

SEED_DIR = Path("data/seed")


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


# ---------------------------------------------------------------------------
# 1. Fix market field in corps
# ---------------------------------------------------------------------------
def fix_corps_market() -> pd.DataFrame:
    print("=" * 60)
    print("[1/3] Fixing corps market field")
    print("=" * 60)

    corps = pd.read_parquet(SEED_DIR / "seed_corps.parquet")
    print(f"  Loaded {len(corps)} corps, market null: {corps.market.isna().sum()}")

    # Get KRX listing with Market info
    krx = fdr.StockListing("KRX")
    market_map = dict(zip(krx["Code"], krx["Market"]))
    print(f"  FDR KRX listing: {len(krx)} stocks")

    # Map market by stock_code
    corps["market"] = corps["stock_code"].map(market_map)
    filled = corps.market.notna().sum()
    still_null = corps.market.isna().sum()
    print(f"  Mapped market for {filled}/{len(corps)} corps from active listing")

    # Fill remaining from delisted stocks
    if still_null > 0:
        delist = fdr.StockListing("KRX-DELISTING")
        delist_map = dict(zip(delist["Symbol"], delist["Market"]))
        mask = corps.market.isna()
        corps.loc[mask, "market"] = corps.loc[mask, "stock_code"].map(delist_map)
        filled2 = corps.market.notna().sum() - filled
        print(f"  Mapped {filled2} more from delisted stocks")

    print(f"  KOSPI: {(corps.market == 'KOSPI').sum()}")
    print(f"  KOSDAQ: {(corps.market == 'KOSDAQ').sum()}")
    print(f"  KONEX: {(corps.market == 'KONEX').sum()}")
    print(f"  Still null: {corps.market.isna().sum()}")

    corps.to_parquet(SEED_DIR / "seed_corps.parquet", index=False)
    print(f"  Saved seed_corps.parquet")
    return corps


# ---------------------------------------------------------------------------
# 2. Fix marcap and shares_out in prices
# ---------------------------------------------------------------------------
def fix_prices_marcap() -> None:
    print()
    print("=" * 60)
    print("[2/3] Fixing prices marcap and shares_out")
    print("=" * 60)

    prices = pd.read_parquet(SEED_DIR / "seed_prices.parquet")
    print(f"  Loaded {len(prices)} price rows")
    print(f"  marcap null: {prices.marcap.isna().sum()}, shares_out null: {prices.shares_out.isna().sum()}")

    # Get current shares outstanding from FDR (active + delisted)
    krx = fdr.StockListing("KRX")
    shares_map = dict(zip(krx["Code"], krx["Stocks"]))
    print(f"  FDR active shares data: {len(shares_map)} stocks")

    # Also get from delisted stocks
    delist = fdr.StockListing("KRX-DELISTING")
    delist_shares = delist[delist["ListingShares"].notna()]
    for _, row in delist_shares.iterrows():
        code = row["Symbol"]
        if code not in shares_map:
            shares_map[code] = int(row["ListingShares"])
    print(f"  Total shares data (incl delisted): {len(shares_map)} stocks")

    # Map shares_out for each stock
    prices["shares_out"] = prices["stock_code"].map(shares_map)

    # Compute marcap = close * shares_out (where both are available)
    mask = prices["shares_out"].notna() & prices["close"].notna()
    prices.loc[mask, "marcap"] = (
        prices.loc[mask, "close"] * prices.loc[mask, "shares_out"]
    )

    # Ensure integer types (nullable)
    prices["shares_out"] = prices["shares_out"].astype("Int64")
    prices["marcap"] = prices["marcap"].astype("Int64")

    filled_shares = prices.shares_out.notna().sum()
    filled_marcap = prices.marcap.notna().sum()
    print(f"  After fix: shares_out filled={filled_shares}/{len(prices)}, marcap filled={filled_marcap}/{len(prices)}")

    prices.to_parquet(SEED_DIR / "seed_prices.parquet", index=False)
    print(f"  Saved seed_prices.parquet")


# ---------------------------------------------------------------------------
# 3. Fix financials: multi-year + depreciation from DART
# ---------------------------------------------------------------------------
def fix_financials(limit: int | None = None) -> None:
    print()
    print("=" * 60)
    print("[3/3] Fixing financials: multi-year data from DART")
    print("=" * 60)

    from krqs.data.dart.client import DartAPIError, DartClient
    from krqs.data.dart.parsers import parse_fnltt_single_acnt_all

    fin = pd.read_parquet(SEED_DIR / "seed_financials.parquet")
    corps = pd.read_parquet(SEED_DIR / "seed_corps.parquet")

    # Get all listed corps with stock codes
    listed = corps[corps.stock_code.notna()].copy()
    print(f"  Listed corps: {len(listed)}")
    print(f"  Current financials: {len(fin)} rows, {fin.corp_code.nunique()} corps")

    # Target years: FY2021 through FY2025
    target_years = [2021, 2022, 2023, 2024, 2025]

    # Build set of existing (corp_code, year) combos
    existing = set(zip(fin.corp_code, fin.fiscal_year))
    print(f"  Existing (corp, year) combos: {len(existing)}")

    # Find what's missing
    all_corps = listed.corp_code.tolist()
    if limit:
        all_corps = all_corps[:limit]

    missing_pairs = []
    for cc in all_corps:
        for yr in target_years:
            if (cc, yr) not in existing:
                missing_pairs.append((cc, yr))

    print(f"  Missing (corp, year) pairs to fetch: {len(missing_pairs)}")
    if not missing_pairs:
        print("  Nothing to fetch!")
        return

    # Also re-fetch existing rows where depreciation is NULL
    refetch_pairs = []
    for _, row in fin[fin.depreciation.isna()].iterrows():
        pair = (row.corp_code, int(row.fiscal_year))
        if pair not in [(cc, yr) for cc, yr in missing_pairs]:
            refetch_pairs.append(pair)

    print(f"  Re-fetch for depreciation: {len(refetch_pairs)} pairs")

    all_fetch = missing_pairs + refetch_pairs
    total = len(all_fetch)

    # Fetch from DART
    new_rows = []
    synced = 0
    no_data = 0
    failed = 0
    start = time.monotonic()

    # Build a quick corp_code -> corp_name map for logging
    name_map = dict(zip(corps.corp_code, corps.corp_name))

    with DartClient() as client:
        for idx, (corp_code, year) in enumerate(all_fetch):
            seq = idx + 1
            try:
                response = client.fetch_single_company_financials(
                    corp_code=corp_code,
                    bsns_year=year,
                    reprt_code="11011",
                    fs_div="CFS",
                )
            except DartAPIError as e:
                failed += 1
                if failed <= 10:
                    print(f"  [{seq}/{total}] FAIL {name_map.get(corp_code, corp_code)} {year}: {e}")
                continue
            except Exception as e:
                failed += 1
                if failed <= 10:
                    print(f"  [{seq}/{total}] ERR  {name_map.get(corp_code, corp_code)} {year}: {e}")
                continue

            if str(response.get("status")) == "013":
                no_data += 1
                if no_data <= 10 or no_data % 200 == 0:
                    print(f"  [{seq}/{total}] --   {name_map.get(corp_code, corp_code)} {year}: no data")
                continue

            parsed = parse_fnltt_single_acnt_all(response)
            if parsed is None:
                no_data += 1
                continue

            # Compute derived fields
            gpm = None
            if parsed.gross_profit is not None and parsed.revenue:
                gpm = parsed.gross_profit / parsed.revenue
            opm = None
            if parsed.operating_income is not None and parsed.revenue:
                opm = parsed.operating_income / parsed.revenue
            roe = None
            if parsed.net_income is not None and parsed.total_equity:
                roe = parsed.net_income / parsed.total_equity
            debt_ratio = None
            if parsed.total_liabilities is not None and parsed.total_assets:
                debt_ratio = parsed.total_liabilities / parsed.total_assets
            ebitda = None
            if parsed.operating_income is not None and parsed.depreciation is not None:
                ebitda = parsed.operating_income + parsed.depreciation
            elif parsed.operating_income is not None:
                ebitda = parsed.operating_income
            ebitda_margin = None
            if ebitda is not None and parsed.revenue:
                ebitda_margin = ebitda / parsed.revenue

            new_rows.append({
                "corp_code": parsed.corp_code,
                "fiscal_year": parsed.bsns_year,
                "fiscal_quarter": 4,
                "period_end": None,
                "revenue": parsed.revenue,
                "cogs": parsed.cogs,
                "gross_profit": parsed.gross_profit,
                "sga": parsed.sga,
                "operating_income": parsed.operating_income,
                "interest_expense": parsed.interest_expense,
                "net_income": parsed.net_income,
                "total_assets": parsed.total_assets,
                "cash_and_equivalents": parsed.cash_and_equivalents,
                "short_term_investments": None,
                "total_debt": None,
                "total_equity": parsed.total_equity,
                "total_liabilities": parsed.total_liabilities,
                "depreciation": parsed.depreciation,
                "ppe": parsed.ppe,
                "retained_earnings": parsed.retained_earnings,
                "gpm": gpm,
                "opm": opm,
                "roe": roe,
                "debt_ratio": debt_ratio,
                "ebitda": ebitda,
                "ebitda_margin": ebitda_margin,
                "revenue_yoy": None,
                "opm_yoy": None,
                "source_updated_at": datetime.now(timezone.utc),
            })

            synced += 1
            if synced <= 20 or synced % 100 == 0:
                elapsed = time.monotonic() - start
                processed = synced + no_data + failed
                if processed > 0:
                    eta = (total - seq) * (elapsed / processed)
                    eta_str = _fmt_duration(eta)
                else:
                    eta_str = "..."
                BN = 100_000_000
                rev_bn = (parsed.revenue or 0) / BN
                dep_str = f"dep={parsed.depreciation}" if parsed.depreciation else "dep=NULL"
                print(
                    f"  [{seq}/{total}] OK   {name_map.get(corp_code, corp_code)} {year}: "
                    f"rev={rev_bn:,.0f}B {dep_str}  (ETA {eta_str})"
                )

    elapsed = time.monotonic() - start
    print(f"\n  DART fetch done: synced={synced}, no_data={no_data}, failed={failed}, time={_fmt_duration(elapsed)}")

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        # Merge with existing: new rows override existing for same (corp_code, fiscal_year, fiscal_quarter)
        key_cols = ["corp_code", "fiscal_year", "fiscal_quarter"]

        # Remove existing rows that will be replaced
        new_keys = set(zip(new_df.corp_code, new_df.fiscal_year, new_df.fiscal_quarter))
        keep_mask = ~pd.Series(
            list(zip(fin.corp_code, fin.fiscal_year, fin.fiscal_quarter))
        ).apply(lambda x: x in new_keys)
        fin_kept = fin[keep_mask.values]

        # Ensure column alignment
        all_cols = list(fin.columns)
        for col in all_cols:
            if col not in new_df.columns:
                new_df[col] = None
        new_df = new_df[all_cols]

        merged = pd.concat([fin_kept, new_df], ignore_index=True)
        merged = merged.sort_values(["corp_code", "fiscal_year"]).reset_index(drop=True)

        print(f"  Before: {len(fin)} rows, After: {len(merged)} rows")
        print(f"  Depreciation non-null: {merged.depreciation.notna().sum()}/{len(merged)}")
        print(f"  Unique corps: {merged.corp_code.nunique()}")
        year_counts = merged.groupby("corp_code").fiscal_year.nunique()
        print(f"  Corps with 2+ years: {(year_counts >= 2).sum()}")
        print(f"  Corps with 3+ years: {(year_counts >= 3).sum()}")

        merged.to_parquet(SEED_DIR / "seed_financials.parquet", index=False)
        print(f"  Saved seed_financials.parquet")
    else:
        print("  No new rows to add")


def main():
    parser = argparse.ArgumentParser(description="Fix seed parquet data")
    parser.add_argument("--skip-financials", action="store_true",
                        help="Skip DART financial data fetch (slow)")
    parser.add_argument("--fin-limit", type=int, default=None,
                        help="Limit corps for financial sync (for testing)")
    args = parser.parse_args()

    fix_corps_market()
    fix_prices_marcap()
    if not args.skip_financials:
        fix_financials(limit=args.fin_limit)
    else:
        print("\n  [3/3] Skipped financials (--skip-financials)")

    print()
    print("=" * 60)
    print("Seed data fix complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

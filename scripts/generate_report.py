from __future__ import annotations

import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from krqs.data.db.connection import get_connection, initialize_schema
from krqs.data.db.repositories.corps import find_by_name
from krqs.domain.operating_leverage import (
    DEFAULT_SGA_YOY_GROWTH,
    DEFAULT_TAX_RATE,
    GpmBand,
    build_scenario_matrix,
)
from krqs.services.report_service import build_operating_leverage_report
from krqs.services.simulator_service import load_corp_baseline, suggest_gpm_band


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
        description="Generate an Operating Leverage HTML report"
    )
    parser.add_argument(
        "--corp", required=True, help="종목명 또는 corp_code (8자리)"
    )
    parser.add_argument(
        "--growth-min",
        type=float,
        default=0.10,
        help="최소 매출 성장률 (기본 0.10)",
    )
    parser.add_argument(
        "--growth-max",
        type=float,
        default=0.70,
        help="최대 매출 성장률 (기본 0.70)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="출력 HTML 파일 경로 (기본: 자동 생성)",
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
    print(f"Generating report for {corp_name} ({corp_code})...")

    loaded = load_corp_baseline(con, corp_code)
    if loaded is None:
        print(
            f"No financial data found for {corp_name}. "
            "Run `uv run python scripts/sync_financials.py --corp <name>` first."
        )
        con.close()
        return 1

    baseline = loaded.baseline
    fiscal_year = loaded.fiscal_year

    gpm_band = suggest_gpm_band(loaded.historical_gpm)
    if gpm_band is None:
        latest_gpm = (
            (baseline.revenue - baseline.cogs) / baseline.revenue
            if baseline.revenue
            else 0.2
        )
        gpm_band = GpmBand(
            low=round(latest_gpm - 0.02, 4),
            mid=round(latest_gpm, 4),
            high=round(latest_gpm + 0.02, 4),
        )
        print(f"  (insufficient history; using fallback GPM band around {latest_gpm:.1%})")

    BN = 100_000_000
    growth_rates = tuple(
        round(args.growth_min + i * 0.05, 2)
        for i in range(int((args.growth_max - args.growth_min) / 0.05) + 1)
    )

    print(f"  Baseline: revenue={baseline.revenue / BN:,.0f}억  sga={baseline.sga / BN:,.0f}억")
    print(f"  GPM band: low={gpm_band.low:.1%}  mid={gpm_band.mid:.1%}  high={gpm_band.high:.1%}")
    print(f"  Growth rates: {growth_rates[0]:+.0%} ~ {growth_rates[-1]:+.0%} ({len(growth_rates)} steps)")

    matrix = build_scenario_matrix(baseline, gpm_band, growth_rates=growth_rates)

    artifact = build_operating_leverage_report(
        baseline=baseline,
        gpm_band=gpm_band,
        matrix=matrix,
        tax_rate=DEFAULT_TAX_RATE,
        sga_growth=DEFAULT_SGA_YOY_GROWTH,
        corp_label=corp_name,
        fiscal_year=fiscal_year,
        data_source="DART OpenAPI",
        corp_code=corp_code,
        db_con=con,
    )
    con.close()

    output_path = args.output or Path(f"{artifact.report_id}.html")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(artifact.html, encoding="utf-8")

    print(f"\nReport saved to {output_path}")
    print(f"  Report ID: {artifact.report_id}")
    print(f"  Param hash: {artifact.param_hash[:16]}...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

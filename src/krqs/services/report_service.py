from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from krqs.config.settings import get_settings
from krqs.domain.operating_leverage import BaselineInputs, GpmBand
from krqs.reports.renderer import render_html

_TEMPLATE_OPERATING_LEVERAGE = "operating_leverage.html.j2"


@dataclass(frozen=True)
class ReportArtifact:
    report_id: str
    param_hash: str
    generated_at: datetime
    html: str
    snapshot_path: Path | None
    metadata: dict[str, Any] = field(default_factory=dict)


def _hash_parameters(payload: dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _generate_observations(
    matrix: pd.DataFrame,
    baseline_op_income: float,
    gpm_band: GpmBand,
) -> list[str]:
    notes: list[str] = []

    mid = matrix[matrix["gpm_scenario"] == "mid"].copy()
    if not mid.empty:
        low_growth_row = mid.iloc[0]
        high_growth_row = mid.iloc[-1]
        if low_growth_row["operating_income"] > 0:
            uplift = (
                high_growth_row["operating_income"]
                / low_growth_row["operating_income"]
                - 1
            )
            rev_uplift = (
                high_growth_row["revenue"] / low_growth_row["revenue"] - 1
            )
            notes.append(
                f"매출 성장률 {low_growth_row['growth_rate']:+.0%} → "
                f"{high_growth_row['growth_rate']:+.0%} 구간에서 "
                f"영업이익이 {uplift:+.0%} 변동 "
                f"(매출 변동 {rev_uplift:+.0%} 대비 영업레버리지 "
                f"{(uplift / rev_uplift):.2f}배, mid GPM 기준)."
            )

    insolvent_count = int(matrix["is_insolvent"].sum())
    total = len(matrix)
    if insolvent_count > 0:
        worst = matrix[matrix["is_insolvent"]].sort_values("growth_rate").iloc[0]
        notes.append(
            f"시나리오 {total}개 중 {insolvent_count}개에서 "
            f"이자비용이 영업이익을 초과하여 세전 적자 전환. "
            f"가장 낮은 적자전환 임계: 성장률 {worst['growth_rate']:+.0%}, "
            f"GPM {worst['gpm_scenario']} ({worst['gpm']:.1%})."
        )
    else:
        notes.append(
            "모든 시나리오에서 영업이익이 이자비용을 상회 — 재무적 여유 확보."
        )

    notes.append(
        f"가정된 GPM 밴드 폭: {gpm_band.low:.1%} ~ {gpm_band.high:.1%} "
        f"(mid {gpm_band.mid:.1%})."
    )

    return notes


def _snapshot_warehouse(
    report_id: str,
    snapshots_dir: Path,
    source_db_path: Path | None = None,
) -> Path | None:
    source = (
        source_db_path if source_db_path is not None else get_settings().db_path
    )
    if not source.exists():
        return None
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    destination = snapshots_dir / f"{report_id}__warehouse.duckdb"
    try:
        shutil.copy2(source, destination)
    except (PermissionError, OSError):
        return None
    return destination


def build_operating_leverage_report(
    baseline: BaselineInputs,
    gpm_band: GpmBand,
    matrix: pd.DataFrame,
    *,
    tax_rate: float,
    sga_growth: float,
    corp_label: str | None = None,
    fiscal_year: int | None = None,
    data_source: str | None = None,
    snapshots_dir: Path | None = None,
    source_db_path: Path | None = None,
    now: datetime | None = None,
) -> ReportArtifact:
    generated_at = now or datetime.now(timezone.utc)
    settings = get_settings()
    target_snapshots_dir = snapshots_dir or settings.snapshots_dir

    payload = {
        "baseline": {
            "revenue": baseline.revenue,
            "cogs": baseline.cogs,
            "sga": baseline.sga,
            "interest_expense": baseline.interest_expense,
        },
        "gpm_band": {
            "low": gpm_band.low,
            "mid": gpm_band.mid,
            "high": gpm_band.high,
        },
        "tax_rate": tax_rate,
        "sga_growth": sga_growth,
        "growth_rates": sorted(matrix["growth_rate"].unique().tolist()),
    }
    param_hash = _hash_parameters(payload)
    report_id = (
        f"OL_{generated_at.strftime('%Y%m%d_%H%M%S')}_{param_hash[:8]}"
    )

    gross = baseline.revenue - baseline.cogs
    baseline_gpm = gross / baseline.revenue if baseline.revenue else 0.0
    baseline_op_income = gross - baseline.sga
    baseline_opm = (
        baseline_op_income / baseline.revenue if baseline.revenue else 0.0
    )

    growth_rates_sorted = sorted(matrix["growth_rate"].unique().tolist())
    if growth_rates_sorted:
        rate_range = (
            f"{growth_rates_sorted[0]:+.0%} ~ {growth_rates_sorted[-1]:+.0%}"
        )
    else:
        rate_range = "-"

    rows = matrix.to_dict(orient="records")
    observations = _generate_observations(
        matrix, baseline_op_income, gpm_band
    )

    context: dict[str, Any] = {
        "title": f"Operating Leverage Report — {corp_label or 'Manual'}",
        "corp_label": corp_label,
        "fiscal_year": fiscal_year,
        "data_source": data_source,
        "report_id": report_id,
        "param_hash": param_hash,
        "generated_at": generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "baseline": {
            "revenue": baseline.revenue,
            "operating_income": baseline_op_income,
            "gpm": baseline_gpm,
            "opm": baseline_opm,
            "interest_expense": baseline.interest_expense,
        },
        "gpm_band": {
            "low": gpm_band.low,
            "mid": gpm_band.mid,
            "high": gpm_band.high,
        },
        "tax_rate": tax_rate,
        "sga_growth": sga_growth,
        "growth_rate_range": rate_range,
        "rows": rows,
        "observations": observations,
    }

    html = render_html(_TEMPLATE_OPERATING_LEVERAGE, context)
    snapshot_path = _snapshot_warehouse(
        report_id, target_snapshots_dir, source_db_path
    )

    return ReportArtifact(
        report_id=report_id,
        param_hash=param_hash,
        generated_at=generated_at,
        html=html,
        snapshot_path=snapshot_path,
        metadata={"corp_label": corp_label, "fiscal_year": fiscal_year},
    )

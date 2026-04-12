from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import logging

import duckdb
import numpy as np
import pandas as pd

from krqs.config.settings import get_settings
from krqs.data.db.repositories.financials import get_dart_source_metadata
from krqs.domain.gpm_regression import RegressionResult
from krqs.domain.operating_leverage import BaselineInputs, GpmBand
from krqs.reports.renderer import render_html

_TEMPLATE_OPERATING_LEVERAGE = "operating_leverage.html.j2"
_TEMPLATE_GPM_REGRESSION = "gpm_regression.html.j2"


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
        if (
            low_growth_row["operating_income"] > 0
            and low_growth_row["revenue"] > 0
            and low_growth_row["revenue"] != high_growth_row["revenue"]
        ):
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


_logger = logging.getLogger(__name__)


def _collect_dart_provenance(
    db_con: duckdb.DuckDBPyConnection | None,
    corp_code: str | None,
) -> list[dict[str, Any]]:
    if db_con is None or not corp_code:
        return []
    try:
        return get_dart_source_metadata(db_con, corp_code)
    except Exception:
        _logger.debug("DART provenance lookup failed for %s", corp_code, exc_info=True)
        return []


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
    corp_code: str | None = None,
    db_con: duckdb.DuckDBPyConnection | None = None,
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

    dart_sources = _collect_dart_provenance(db_con, corp_code)
    rcept_nos = list({
        str(s["rcept_no"])
        for s in dart_sources
        if s.get("rcept_no")
    })
    fetch_dates = sorted({
        s["fetched_at"].strftime("%Y-%m-%d %H:%M")
        if hasattr(s["fetched_at"], "strftime")
        else str(s["fetched_at"])[:16]
        for s in dart_sources
        if s.get("fetched_at")
    })

    context: dict[str, Any] = {
        "title": f"Operating Leverage Report — {corp_label or 'Manual'}",
        "corp_label": corp_label,
        "fiscal_year": fiscal_year,
        "data_source": data_source,
        "report_id": report_id,
        "param_hash": param_hash,
        "generated_at": generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "dart_rcept_nos": rcept_nos,
        "dart_fetch_dates": fetch_dates,
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


def _gpm_observations(result: RegressionResult, confidence: float) -> list[str]:
    notes: list[str] = []
    if result.r_squared >= 0.7:
        notes.append(
            f"R²={result.r_squared:.3f} — 매출과 GPM 사이에 강한 선형 관계가 존재합니다."
        )
    elif result.r_squared >= 0.4:
        notes.append(
            f"R²={result.r_squared:.3f} — 매출과 GPM 사이에 중간 수준의 선형 관계가 있으나, "
            "다른 변수의 영향도 클 수 있습니다."
        )
    else:
        notes.append(
            f"R²={result.r_squared:.3f} — 매출과 GPM 사이의 선형 관계가 약합니다. "
            "GPM 밴드 예측 시 주의가 필요합니다."
        )

    if result.slope > 0:
        notes.append(
            "양의 기울기 — 매출 증가 시 GPM이 개선되는 경향 (규모의 경제 또는 믹스 개선)."
        )
    elif result.slope < 0:
        notes.append(
            "음의 기울기 — 매출 증가 시 GPM이 하락하는 경향 (저마진 물량 확대 가능성)."
        )

    band_width = result.predicted_gpm_high - result.predicted_gpm_low
    notes.append(
        f"예측 GPM 밴드 폭: {band_width:.1%}p "
        f"(Low {result.predicted_gpm_low:.1%} ~ High {result.predicted_gpm_high:.1%}, "
        f"{confidence:.0%} 신뢰수준)."
    )
    return notes


def _build_svg_geometry(
    fitted_df: pd.DataFrame,
) -> dict[str, Any]:
    """Convert regression data to SVG coordinate strings."""
    if fitted_df.empty:
        return {"scatter_points": [], "line_points": "", "band_points": "",
                "x_ticks": [], "y_ticks": []}

    BN = 1e8
    rev = fitted_df["revenue"].to_numpy() / BN
    gpm = fitted_df["gpm"].to_numpy()
    fitted = fitted_df["fitted"].to_numpy()
    lower = fitted_df["lower"].to_numpy()
    upper = fitted_df["upper"].to_numpy()

    # chart area: x=[60..570], y=[20..300]
    x_min, x_max = float(rev.min()), float(rev.max())
    y_min = float(min(gpm.min(), lower.min())) - 0.01
    y_max = float(max(gpm.max(), upper.max())) + 0.01
    if x_max == x_min:
        x_max = x_min + 1
    if y_max == y_min:
        y_max = y_min + 0.01

    def sx(v: float) -> float:
        return 60 + (v - x_min) / (x_max - x_min) * 510

    def sy(v: float) -> float:
        return 300 - (v - y_min) / (y_max - y_min) * 280

    order = rev.argsort()
    rev_s, fitted_s, lower_s, upper_s = rev[order], fitted[order], lower[order], upper[order]

    scatter_points = []
    for i in range(len(rev)):
        fy = fitted_df["fiscal_year"].iloc[i] if "fiscal_year" in fitted_df.columns else ""
        scatter_points.append({
            "x": f"{sx(rev[i]):.1f}",
            "y": f"{sy(gpm[i]):.1f}",
            "label": f"{fy} rev={rev[i]:,.0f}억 GPM={gpm[i]:.1%}",
        })

    line_pts = " ".join(f"{sx(rev_s[i]):.1f},{sy(fitted_s[i]):.1f}" for i in range(len(rev_s)))
    band_upper = " ".join(f"{sx(rev_s[i]):.1f},{sy(upper_s[i]):.1f}" for i in range(len(rev_s)))
    band_lower = " ".join(f"{sx(rev_s[i]):.1f},{sy(lower_s[i]):.1f}" for i in range(len(rev_s) - 1, -1, -1))
    band_pts = f"{band_upper} {band_lower}"

    x_ticks = []
    for v in np.linspace(x_min, x_max, 5):
        x_ticks.append({"x": f"{sx(v):.1f}", "label": f"{v:,.0f}"})
    y_ticks = []
    for v in np.linspace(y_min, y_max, 5):
        y_ticks.append({"y": f"{sy(v):.1f}", "label": f"{v:.1%}"})

    return {
        "scatter_points": scatter_points,
        "line_points": line_pts,
        "band_points": band_pts,
        "x_ticks": x_ticks,
        "y_ticks": y_ticks,
    }


def build_gpm_regression_report(
    result: RegressionResult,
    *,
    confidence: float = 0.95,
    remove_outliers: bool = True,
    corp_label: str | None = None,
    data_source: str | None = None,
    snapshots_dir: Path | None = None,
    source_db_path: Path | None = None,
    now: datetime | None = None,
) -> ReportArtifact:
    generated_at = now or datetime.now(timezone.utc)
    settings = get_settings()
    target_snapshots_dir = snapshots_dir or settings.snapshots_dir

    payload = {
        "slope": result.slope,
        "intercept": result.intercept,
        "observations": result.observations,
        "confidence": confidence,
        "remove_outliers": remove_outliers,
        "predicted_gpm_low": result.predicted_gpm_low,
        "predicted_gpm_mid": result.predicted_gpm_mid,
        "predicted_gpm_high": result.predicted_gpm_high,
    }
    param_hash = _hash_parameters(payload)
    report_id = f"GPM_{generated_at.strftime('%Y%m%d_%H%M%S')}_{param_hash[:8]}"

    observations = _gpm_observations(result, confidence)
    svg_geo = _build_svg_geometry(result.fitted_df)
    data_rows = result.fitted_df.to_dict(orient="records")

    context: dict[str, Any] = {
        "title": f"GPM Regression Report — {corp_label or 'Manual'}",
        "corp_label": corp_label,
        "data_source": data_source,
        "report_id": report_id,
        "param_hash": param_hash,
        "generated_at": generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "dart_rcept_nos": [],
        "dart_fetch_dates": [],
        "result": result,
        "confidence": confidence,
        "remove_outliers": remove_outliers,
        "data_rows": data_rows,
        "observations": observations,
        **svg_geo,
    }

    html = render_html(_TEMPLATE_GPM_REGRESSION, context)
    snapshot_path = _snapshot_warehouse(
        report_id, target_snapshots_dir, source_db_path
    )

    return ReportArtifact(
        report_id=report_id,
        param_hash=param_hash,
        generated_at=generated_at,
        html=html,
        snapshot_path=snapshot_path,
        metadata={"corp_label": corp_label, "type": "gpm_regression"},
    )

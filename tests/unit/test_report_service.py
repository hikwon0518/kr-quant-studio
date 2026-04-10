from datetime import datetime, timezone
from pathlib import Path

import pytest

from krqs.domain.operating_leverage import (
    BaselineInputs,
    GpmBand,
    build_scenario_matrix,
)
from krqs.services.report_service import (
    _hash_parameters,
    build_operating_leverage_report,
)


@pytest.fixture
def sample_baseline() -> BaselineInputs:
    return BaselineInputs(
        revenue=5_000_000_000_000,
        cogs=4_000_000_000_000,
        sga=300_000_000_000,
        interest_expense=50_000_000_000,
    )


@pytest.fixture
def sample_gpm_band() -> GpmBand:
    return GpmBand(low=0.12, mid=0.20, high=0.28)


@pytest.fixture
def sample_matrix(sample_baseline, sample_gpm_band):
    return build_scenario_matrix(
        sample_baseline,
        sample_gpm_band,
        growth_rates=(0.10, 0.30, 0.50),
    )


class TestHashParameters:
    def test_deterministic(self):
        payload = {"a": 1, "b": [1, 2, 3], "c": {"nested": True}}
        assert _hash_parameters(payload) == _hash_parameters(payload)

    def test_different_for_different_input(self):
        a = _hash_parameters({"x": 1})
        b = _hash_parameters({"x": 2})
        assert a != b

    def test_order_independent(self):
        a = _hash_parameters({"x": 1, "y": 2})
        b = _hash_parameters({"y": 2, "x": 1})
        assert a == b


class TestBuildOperatingLeverageReport:
    def test_returns_artifact(
        self, sample_baseline, sample_gpm_band, sample_matrix, tmp_path
    ):
        now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
        artifact = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            corp_label="삼성전자",
            fiscal_year=2022,
            snapshots_dir=tmp_path,
            now=now,
        )
        assert artifact.report_id.startswith("OL_20260410_120000_")
        assert len(artifact.param_hash) == 64
        assert artifact.generated_at == now

    def test_html_contains_key_sections(
        self, sample_baseline, sample_gpm_band, sample_matrix, tmp_path
    ):
        artifact = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            corp_label="삼성전자",
            fiscal_year=2022,
            snapshots_dir=tmp_path,
        )
        html = artifact.html
        assert "Operating Leverage Analysis" in html
        assert "삼성전자" in html
        assert "2022" in html
        assert "param_hash" not in html  # 실제 값만 렌더링
        assert artifact.param_hash in html

    def test_matrix_rows_rendered(
        self, sample_baseline, sample_gpm_band, sample_matrix, tmp_path
    ):
        artifact = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            snapshots_dir=tmp_path,
        )
        assert artifact.html.count("<tr") >= len(sample_matrix) + 1

    def test_observations_generated(
        self, sample_baseline, sample_gpm_band, sample_matrix, tmp_path
    ):
        artifact = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            snapshots_dir=tmp_path,
        )
        assert "영업레버리지" in artifact.html or "영업이익" in artifact.html

    def test_reproducible_hash(
        self, sample_baseline, sample_gpm_band, sample_matrix, tmp_path
    ):
        a = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            snapshots_dir=tmp_path,
        )
        b = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            snapshots_dir=tmp_path,
        )
        assert a.param_hash == b.param_hash

    def test_different_tax_rate_changes_hash(
        self, sample_baseline, sample_gpm_band, sample_matrix, tmp_path
    ):
        a = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            snapshots_dir=tmp_path,
        )
        b = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.25,
            sga_growth=0.03,
            snapshots_dir=tmp_path,
        )
        assert a.param_hash != b.param_hash

    def test_no_snapshot_when_db_missing(
        self, sample_baseline, sample_gpm_band, sample_matrix, tmp_path
    ):
        missing = tmp_path / "nonexistent.duckdb"
        artifact = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            snapshots_dir=tmp_path,
            source_db_path=missing,
        )
        assert artifact.snapshot_path is None

    def test_snapshot_created_when_db_exists(
        self, sample_baseline, sample_gpm_band, sample_matrix, tmp_path
    ):
        fake_db = tmp_path / "warehouse.duckdb"
        fake_db.write_bytes(b"fake duckdb content")
        snapshots = tmp_path / "snapshots"
        artifact = build_operating_leverage_report(
            sample_baseline,
            sample_gpm_band,
            sample_matrix,
            tax_rate=0.22,
            sga_growth=0.03,
            snapshots_dir=snapshots,
            source_db_path=fake_db,
        )
        assert artifact.snapshot_path is not None
        assert artifact.snapshot_path.exists()
        assert artifact.snapshot_path.read_bytes() == b"fake duckdb content"


class TestReportRendererHtml:
    def test_render_html_direct(self):
        from krqs.reports.renderer import render_html

        context = {
            "title": "Test",
            "corp_label": "TestCo",
            "fiscal_year": 2022,
            "data_source": None,
            "report_id": "OL_TEST",
            "param_hash": "0" * 64,
            "generated_at": "2026-04-10 00:00:00 UTC",
            "baseline": {
                "revenue": 1000,
                "operating_income": 100,
                "gpm": 0.2,
                "opm": 0.1,
                "interest_expense": 50,
            },
            "gpm_band": {"low": 0.1, "mid": 0.2, "high": 0.3},
            "tax_rate": 0.22,
            "sga_growth": 0.03,
            "growth_rate_range": "+10% ~ +50%",
            "rows": [],
            "observations": ["테스트 관찰"],
        }
        html = render_html("operating_leverage.html.j2", context)
        assert "<!DOCTYPE html>" in html
        assert "TestCo" in html
        assert "테스트 관찰" in html

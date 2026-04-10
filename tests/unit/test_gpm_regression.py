import pandas as pd
import pytest

from krqs.domain.gpm_regression import (
    fit_gpm_vs_revenue,
    remove_outliers_iqr,
)


class TestRemoveOutliersIqr:
    def test_removes_extreme_value(self):
        df = pd.DataFrame({"gpm": [0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.90]})
        kept, removed = remove_outliers_iqr(df, "gpm")
        assert 0.90 not in kept["gpm"].values
        assert 0.90 in removed["gpm"].values

    def test_no_outliers_returns_all(self):
        df = pd.DataFrame({"gpm": [0.19, 0.20, 0.21, 0.20, 0.22]})
        kept, removed = remove_outliers_iqr(df, "gpm")
        assert len(kept) == 5
        assert len(removed) == 0

    def test_both_extremes_removed(self):
        df = pd.DataFrame(
            {"gpm": [-0.5, 0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 1.0]}
        )
        kept, removed = remove_outliers_iqr(df, "gpm")
        assert -0.5 not in kept["gpm"].values
        assert 1.0 not in kept["gpm"].values
        assert len(removed) == 2


class TestFitGpmVsRevenue:
    def test_returns_none_for_insufficient_data(self):
        assert fit_gpm_vs_revenue([]) is None
        assert (
            fit_gpm_vs_revenue(
                [
                    {"revenue": 100, "gpm": 0.2},
                    {"revenue": 200, "gpm": 0.25},
                ]
            )
            is None
        )

    def test_perfect_positive_correlation(self):
        history = [
            {"revenue": 100.0, "gpm": 0.20},
            {"revenue": 200.0, "gpm": 0.25},
            {"revenue": 300.0, "gpm": 0.30},
            {"revenue": 400.0, "gpm": 0.35},
        ]
        result = fit_gpm_vs_revenue(history, remove_outliers=False)
        assert result is not None
        assert result.r_squared == pytest.approx(1.0, abs=1e-6)
        assert result.slope > 0
        assert result.observations == 4

    def test_band_ordering(self):
        history = [
            {"revenue": 100, "gpm": 0.20},
            {"revenue": 200, "gpm": 0.22},
            {"revenue": 300, "gpm": 0.24},
            {"revenue": 400, "gpm": 0.26},
            {"revenue": 500, "gpm": 0.28},
        ]
        result = fit_gpm_vs_revenue(history, remove_outliers=False)
        assert result.predicted_gpm_low <= result.predicted_gpm_mid
        assert result.predicted_gpm_mid <= result.predicted_gpm_high

    def test_ignores_null_rows(self):
        history = [
            {"revenue": 100, "gpm": 0.20},
            {"revenue": 200, "gpm": None},
            {"revenue": 300, "gpm": 0.25},
            {"revenue": 400, "gpm": 0.30},
            {"revenue": None, "gpm": 0.15},
        ]
        result = fit_gpm_vs_revenue(history, remove_outliers=False)
        assert result is not None
        assert result.observations == 3

    def test_outlier_removed(self):
        history = [
            {"revenue": 100, "gpm": 0.20},
            {"revenue": 200, "gpm": 0.21},
            {"revenue": 300, "gpm": 0.22},
            {"revenue": 400, "gpm": 0.23},
            {"revenue": 500, "gpm": 0.24},
            {"revenue": 600, "gpm": 0.25},
            {"revenue": 700, "gpm": 0.90},
        ]
        result = fit_gpm_vs_revenue(history, remove_outliers=True)
        assert result.outliers_removed >= 1
        assert result.observations == 7 - result.outliers_removed

    def test_target_revenue_prediction(self):
        history = [
            {"revenue": 100.0, "gpm": 0.20},
            {"revenue": 200.0, "gpm": 0.22},
            {"revenue": 300.0, "gpm": 0.24},
            {"revenue": 400.0, "gpm": 0.26},
        ]
        # 회귀선: gpm ≈ 0.18 + 0.0002 × revenue → 500에서 0.28
        result = fit_gpm_vs_revenue(
            history, target_revenue=500.0, remove_outliers=False
        )
        assert result is not None
        assert result.predicted_gpm_mid == pytest.approx(0.28, abs=0.005)

    def test_fitted_df_has_required_columns(self):
        history = [
            {"revenue": 100, "gpm": 0.20},
            {"revenue": 200, "gpm": 0.22},
            {"revenue": 300, "gpm": 0.24},
            {"revenue": 400, "gpm": 0.26},
        ]
        result = fit_gpm_vs_revenue(history, remove_outliers=False)
        expected = {"revenue", "gpm", "fitted", "lower", "upper"}
        assert expected.issubset(set(result.fitted_df.columns))

    def test_r_squared_in_unit_interval(self):
        history = [
            {"revenue": 100, "gpm": 0.20},
            {"revenue": 200, "gpm": 0.30},
            {"revenue": 300, "gpm": 0.10},
            {"revenue": 400, "gpm": 0.40},
            {"revenue": 500, "gpm": 0.15},
        ]
        result = fit_gpm_vs_revenue(history, remove_outliers=False)
        assert 0.0 <= result.r_squared <= 1.0

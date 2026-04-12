import numpy as np
import pandas as pd
import pytest

from krqs.domain.log_trend import (
    LogTrendResult,
    compute_growth_acceleration,
    detect_signal,
    fit_log_trend,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _exponential_prices(
    n: int, daily_rate: float = 0.001, base: float = 10_000.0
) -> np.ndarray:
    """Generate perfectly exponential prices: base * exp(rate * t)."""
    t = np.arange(n, dtype=float)
    return base * np.exp(daily_rate * t)


# ---------------------------------------------------------------------------
# fit_log_trend
# ---------------------------------------------------------------------------


class TestFitLogTrendGuards:
    def test_fit_returns_none_for_insufficient_data(self):
        prices = np.array([100.0] * 10)
        dates = np.arange(10, dtype=float)
        assert fit_log_trend(dates, prices) is None

    def test_fit_returns_none_for_zero_prices(self):
        n = 50
        prices = np.ones(n, dtype=float)
        prices[25] = 0.0
        dates = np.arange(n, dtype=float)
        assert fit_log_trend(dates, prices) is None

    def test_fit_returns_none_for_negative_prices(self):
        n = 50
        prices = np.ones(n, dtype=float) * 100.0
        prices[10] = -1.0
        dates = np.arange(n, dtype=float)
        assert fit_log_trend(dates, prices) is None


class TestPerfectExponentialGrowth:
    def test_perfect_exponential_growth(self):
        daily_rate = 0.002
        prices = _exponential_prices(100, daily_rate=daily_rate)
        dates = np.arange(100, dtype=float)

        result = fit_log_trend(dates, prices)
        assert result is not None
        assert result.slope == pytest.approx(daily_rate, rel=1e-6)
        assert result.r_squared == pytest.approx(1.0, abs=1e-8)
        assert result.observations == 100

    def test_annualized_return_calculation(self):
        daily_rate = 0.001
        prices = _exponential_prices(252, daily_rate=daily_rate)
        dates = np.arange(252, dtype=float)

        result = fit_log_trend(dates, prices)
        assert result is not None
        expected_ann = np.exp(daily_rate * 252) - 1
        assert result.annualized_return == pytest.approx(expected_ann, rel=1e-6)


class TestBreakoutDetection:
    def test_breakout_detection(self):
        """Price well *above* the trend triggers trend_breakout."""
        n = 100
        daily_rate = 0.001
        prices = _exponential_prices(n, daily_rate=daily_rate)
        # Spike the last price far above the trend
        prices[-1] = prices[-1] * 3.0
        dates = np.arange(n, dtype=float)

        result = fit_log_trend(dates, prices)
        assert result is not None
        assert result.trend_breakout is True
        assert result.trend_breakdown is False
        assert result.current_deviation > 2.0

    def test_breakout_signal(self):
        result = LogTrendResult(
            slope=0.001, intercept=9.0, r_squared=0.95,
            residual_std=0.01, current_deviation=2.5,
            annualized_return=0.28, trend_breakout=True,
            trend_breakdown=False, observations=100,
            fitted_values=np.array([]), upper_band=np.array([]),
            lower_band=np.array([]),
        )
        assert detect_signal(result) == "something_special"


class TestBreakdownDetection:
    def test_breakdown_detection(self):
        """Price well *below* the trend triggers trend_breakdown."""
        n = 100
        daily_rate = 0.001
        prices = _exponential_prices(n, daily_rate=daily_rate)
        # Collapse the last price far below the trend
        prices[-1] = prices[-1] * 0.3
        dates = np.arange(n, dtype=float)

        result = fit_log_trend(dates, prices)
        assert result is not None
        assert result.trend_breakdown is True
        assert result.trend_breakout is False
        assert result.current_deviation < -2.0

    def test_breakdown_signal(self):
        result = LogTrendResult(
            slope=0.001, intercept=9.0, r_squared=0.95,
            residual_std=0.01, current_deviation=-2.5,
            annualized_return=0.28, trend_breakout=False,
            trend_breakdown=True, observations=100,
            fitted_values=np.array([]), upper_band=np.array([]),
            lower_band=np.array([]),
        )
        assert detect_signal(result) == "something_wrong"


class TestSteadyState:
    def test_steady_state(self):
        """Clean exponential data => deviation near zero, no breakout/breakdown."""
        prices = _exponential_prices(100, daily_rate=0.001)
        dates = np.arange(100, dtype=float)

        result = fit_log_trend(dates, prices)
        assert result is not None
        assert result.trend_breakout is False
        assert result.trend_breakdown is False
        assert abs(result.current_deviation) < 2.0  # within 2σ = steady

    def test_steady_signal(self):
        result = LogTrendResult(
            slope=0.001, intercept=9.0, r_squared=0.95,
            residual_std=0.01, current_deviation=0.3,
            annualized_return=0.28, trend_breakout=False,
            trend_breakdown=False, observations=100,
            fitted_values=np.array([]), upper_band=np.array([]),
            lower_band=np.array([]),
        )
        assert detect_signal(result) == "steady"


# ---------------------------------------------------------------------------
# compute_growth_acceleration
# ---------------------------------------------------------------------------


class TestGrowthAcceleration:
    def test_growth_acceleration_positive(self):
        """Recent half grows faster => accelerating."""
        half = 126
        slow = _exponential_prices(half, daily_rate=0.0005)
        fast_base = slow[-1]
        fast = fast_base * np.exp(0.003 * np.arange(half))
        prices = pd.Series(np.concatenate([slow, fast]))

        acc = compute_growth_acceleration(prices, half_window=half)
        assert acc["signal"] == "accelerating"
        assert acc["acceleration"] > 0

    def test_growth_acceleration_negative(self):
        """Recent half grows slower => decelerating."""
        half = 126
        fast = _exponential_prices(half, daily_rate=0.003)
        slow_base = fast[-1]
        slow = slow_base * np.exp(0.0005 * np.arange(half))
        prices = pd.Series(np.concatenate([fast, slow]))

        acc = compute_growth_acceleration(prices, half_window=half)
        assert acc["signal"] == "decelerating"
        assert acc["acceleration"] < 0

    def test_returns_steady_for_short_series(self):
        """Series shorter than 2*half_window => steady with zero slopes."""
        prices = pd.Series([100.0] * 10)
        acc = compute_growth_acceleration(prices, half_window=126)
        assert acc["signal"] == "steady"
        assert acc["acceleration"] == 0.0

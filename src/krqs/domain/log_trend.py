from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

_MIN_OBSERVATIONS: int = 30
_TRADING_DAYS_PER_YEAR: int = 252


@dataclass(frozen=True)
class LogTrendResult:
    slope: float
    intercept: float
    r_squared: float
    residual_std: float
    current_deviation: float
    annualized_return: float
    trend_breakout: bool
    trend_breakdown: bool
    observations: int
    fitted_values: np.ndarray
    upper_band: np.ndarray
    lower_band: np.ndarray


def fit_log_trend(
    dates: np.ndarray,
    prices: np.ndarray,
    threshold_std: float = 2.0,
) -> LogTrendResult | None:
    """Fit OLS on log(prices) vs time index.

    Parameters
    ----------
    dates : np.ndarray
        Array of ordinal days or sequential index (same length as *prices*).
    prices : np.ndarray
        Array of close prices.  Must be > 0 everywhere.

    Returns
    -------
    LogTrendResult or None
        ``None`` when fewer than 30 observations or *prices* contain zeros /
        negative values.
    """
    prices = np.asarray(prices, dtype=float)
    dates = np.asarray(dates, dtype=float)

    if len(prices) < _MIN_OBSERVATIONS:
        return None
    if np.any(prices <= 0):
        return None

    y = np.log(prices)
    x = np.arange(len(prices), dtype=float)

    slope, intercept = np.polyfit(x, y, 1)

    fitted_log = slope * x + intercept
    residuals = y - fitted_log

    ss_res = float(np.sum(residuals ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = 1.0 - ss_res / ss_tot if ss_tot != 0.0 else 0.0

    residual_std = float(np.std(residuals, ddof=1))

    current_deviation = (
        float((y[-1] - fitted_log[-1]) / residual_std)
        if residual_std > 0
        else 0.0
    )

    annualized_return = float(np.exp(slope * _TRADING_DAYS_PER_YEAR) - 1)

    fitted_values = np.exp(fitted_log)
    upper_band = np.exp(fitted_log + 2.0 * residual_std)
    lower_band = np.exp(fitted_log - 2.0 * residual_std)

    return LogTrendResult(
        slope=float(slope),
        intercept=float(intercept),
        r_squared=r_squared,
        residual_std=residual_std,
        current_deviation=current_deviation,
        annualized_return=annualized_return,
        trend_breakout=current_deviation > threshold_std,
        trend_breakdown=current_deviation < -threshold_std,
        observations=len(prices),
        fitted_values=fitted_values,
        upper_band=upper_band,
        lower_band=lower_band,
    )


def detect_signal(result: LogTrendResult) -> str:
    """Return a signal string based on current deviation.

    * ``> +2 sigma``  ->  ``"something_special"``
    * ``< -2 sigma``  ->  ``"something_wrong"``
    * otherwise       ->  ``"steady"``
    """
    if result.current_deviation > 2.0:
        return "something_special"
    if result.current_deviation < -2.0:
        return "something_wrong"
    return "steady"


def compute_growth_acceleration(
    prices: pd.Series,
    half_window: int = 126,
) -> dict:
    """Compare the log-slope of the recent half vs the earlier half.

    Parameters
    ----------
    prices : pd.Series
        Close price series (length >= 2 * *half_window*).
    half_window : int
        Number of observations per half (default 126 ~ 6 months).

    Returns
    -------
    dict
        Keys: ``recent_slope``, ``prior_slope``, ``acceleration``, ``signal``.
        ``signal`` is one of ``"accelerating"`` | ``"decelerating"`` |
        ``"steady"``.  *acceleration* is the annualized difference.
    """
    arr = np.asarray(prices, dtype=float)
    total = 2 * half_window
    if len(arr) < total:
        return {
            "recent_slope": 0.0,
            "prior_slope": 0.0,
            "acceleration": 0.0,
            "signal": "steady",
        }

    prior = arr[-total:-half_window]
    recent = arr[-half_window:]

    prior_log = np.log(prior)
    recent_log = np.log(recent)

    prior_x = np.arange(len(prior), dtype=float)
    recent_x = np.arange(len(recent), dtype=float)

    prior_slope = float(np.polyfit(prior_x, prior_log, 1)[0])
    recent_slope = float(np.polyfit(recent_x, recent_log, 1)[0])

    prior_ann = prior_slope * _TRADING_DAYS_PER_YEAR
    recent_ann = recent_slope * _TRADING_DAYS_PER_YEAR
    acceleration = recent_ann - prior_ann

    if acceleration > 0.05:
        signal = "accelerating"
    elif acceleration < -0.05:
        signal = "decelerating"
    else:
        signal = "steady"

    return {
        "recent_slope": recent_ann,
        "prior_slope": prior_ann,
        "acceleration": acceleration,
        "signal": signal,
    }

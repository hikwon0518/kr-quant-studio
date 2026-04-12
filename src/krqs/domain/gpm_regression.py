from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
import statsmodels.api as sm


@dataclass(frozen=True)
class RegressionResult:
    slope: float
    intercept: float
    r_squared: float
    observations: int
    outliers_removed: int
    predicted_gpm_low: float
    predicted_gpm_mid: float
    predicted_gpm_high: float
    fitted_df: pd.DataFrame
    outlier_df: pd.DataFrame
    slope_pvalue: float = 1.0
    slope_ci_low: float = 0.0
    slope_ci_high: float = 0.0
    slope_stderr: float = 0.0


def remove_outliers_iqr(
    df: pd.DataFrame, col: str, k: float = 1.5
) -> tuple[pd.DataFrame, pd.DataFrame]:
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    iqr = q3 - q1
    low = q1 - k * iqr
    high = q3 + k * iqr
    mask = (df[col] >= low) & (df[col] <= high)
    kept = df.loc[mask].reset_index(drop=True)
    removed = df.loc[~mask].reset_index(drop=True)
    return kept, removed


def _coerce_history(history: Iterable[dict]) -> pd.DataFrame:
    df = pd.DataFrame(list(history))
    if df.empty:
        return df
    for col in ("revenue", "gpm"):
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["revenue", "gpm"]).reset_index(drop=True)


def fit_gpm_vs_revenue(
    history: Iterable[dict],
    *,
    target_revenue: float | None = None,
    confidence: float = 0.95,
    remove_outliers: bool = True,
) -> RegressionResult | None:
    df = _coerce_history(history)
    if len(df) < 3:
        return None

    outlier_df = pd.DataFrame(columns=df.columns)
    if remove_outliers and len(df) >= 4:
        df, outlier_df = remove_outliers_iqr(df, "gpm")
    if len(df) < 3:
        return None

    x = df["revenue"].to_numpy(dtype=float)
    y = df["gpm"].to_numpy(dtype=float)
    X = sm.add_constant(x)
    model = sm.OLS(y, X).fit()

    params = np.asarray(model.params, dtype=float)
    intercept = float(params[0])
    slope = float(params[1]) if len(params) > 1 else 0.0
    r_squared = float(model.rsquared)

    alpha = 1.0 - confidence
    if len(params) > 1:
        slope_pvalue = float(model.pvalues[1])
        slope_ci = model.conf_int(alpha=alpha)
        slope_ci_low = float(slope_ci[1, 0])
        slope_ci_high = float(slope_ci[1, 1])
        slope_stderr = float(model.bse[1])
    else:
        slope_pvalue = 1.0
        slope_ci_low = 0.0
        slope_ci_high = 0.0
        slope_stderr = 0.0

    pred = model.get_prediction(X).summary_frame(alpha=alpha)
    fitted_df = df.copy()
    fitted_df["fitted"] = pred["mean"].to_numpy()
    fitted_df["lower"] = pred["mean_ci_lower"].to_numpy()
    fitted_df["upper"] = pred["mean_ci_upper"].to_numpy()

    target = (
        float(target_revenue)
        if target_revenue is not None
        else float(df["revenue"].max())
    )
    target_X = np.array([[1.0, target]])
    target_pred = model.get_prediction(target_X).summary_frame(alpha=alpha)
    pred_mid = float(target_pred["mean"].iloc[0])
    pred_low = float(target_pred["obs_ci_lower"].iloc[0])
    pred_high = float(target_pred["obs_ci_upper"].iloc[0])

    return RegressionResult(
        slope=slope,
        intercept=intercept,
        r_squared=r_squared,
        observations=int(len(df)),
        outliers_removed=int(len(outlier_df)),
        predicted_gpm_low=max(0.0, pred_low),
        predicted_gpm_mid=pred_mid,
        predicted_gpm_high=pred_high,
        fitted_df=fitted_df,
        outlier_df=outlier_df,
        slope_pvalue=slope_pvalue,
        slope_ci_low=slope_ci_low,
        slope_ci_high=slope_ci_high,
        slope_stderr=slope_stderr,
    )

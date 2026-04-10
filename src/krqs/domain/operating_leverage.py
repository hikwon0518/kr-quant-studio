from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

DEFAULT_TAX_RATE: float = 0.22
DEFAULT_SGA_YOY_GROWTH: float = 0.03
DEFAULT_GROWTH_RATES: tuple[float, ...] = (
    0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40,
    0.45, 0.50, 0.55, 0.60, 0.65, 0.70,
)

_GPM_SCENARIOS: tuple[str, ...] = ("low", "mid", "high")


@dataclass(frozen=True)
class BaselineInputs:
    revenue: int
    cogs: int
    sga: int
    interest_expense: int


@dataclass(frozen=True)
class GpmBand:
    low: float
    mid: float
    high: float

    def as_tuple(self) -> tuple[float, float, float]:
        return (self.low, self.mid, self.high)


def build_scenario_matrix(
    baseline: BaselineInputs,
    gpm_band: GpmBand,
    *,
    growth_rates: tuple[float, ...] = DEFAULT_GROWTH_RATES,
    tax_rate: float = DEFAULT_TAX_RATE,
    sga_yoy_growth: float = DEFAULT_SGA_YOY_GROWTH,
) -> pd.DataFrame:
    projected_sga = baseline.sga * (1 + sga_yoy_growth)

    rows: list[dict[str, object]] = []
    for growth in growth_rates:
        projected_revenue = baseline.revenue * (1 + growth)
        for label, gpm in zip(_GPM_SCENARIOS, gpm_band.as_tuple()):
            gross_profit = projected_revenue * gpm
            operating_income = gross_profit - projected_sga
            pretax_income = operating_income - baseline.interest_expense
            tax = max(pretax_income, 0.0) * tax_rate
            net_income = pretax_income - tax
            opm = operating_income / projected_revenue if projected_revenue else 0.0
            is_insolvent = baseline.interest_expense > operating_income

            rows.append(
                {
                    "growth_rate": growth,
                    "gpm_scenario": label,
                    "gpm": gpm,
                    "revenue": projected_revenue,
                    "gross_profit": gross_profit,
                    "sga": projected_sga,
                    "operating_income": operating_income,
                    "interest_expense": baseline.interest_expense,
                    "pretax_income": pretax_income,
                    "net_income": net_income,
                    "opm": opm,
                    "is_insolvent": is_insolvent,
                }
            )

    return pd.DataFrame(rows)

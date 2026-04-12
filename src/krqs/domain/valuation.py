from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValuationMetrics:
    per: float | None
    pbr: float | None
    peg: float | None
    earnings_yield: float | None
    market_cap: int | None


@dataclass(frozen=True)
class GrowthValuationScenario:
    """Result of the "물빨 시뮬레이터" computation."""

    current_eps: float
    scenario_eps: float
    current_per: float
    scenario_per: float
    scenario_price: float
    current_price: float
    upside_pct: float
    comment: str


def compute_valuation(
    price: int,
    marcap: int,
    shares: int,
    net_income: int | None,
    total_equity: int | None,
    earnings_growth: float | None = None,
) -> ValuationMetrics:
    """Pure computation of valuation ratios from raw inputs.

    Parameters
    ----------
    price : int
        Current stock price (won).
    marcap : int
        Market capitalisation (won).
    shares : int
        Total outstanding shares.
    net_income : int or None
        Trailing net income (won).
    total_equity : int or None
        Total shareholder equity (won).
    earnings_growth : float or None
        YoY earnings growth rate (e.g. 0.25 = +25 %).
    """
    if shares <= 0:
        return ValuationMetrics(
            per=None, pbr=None, peg=None,
            earnings_yield=None, market_cap=marcap,
        )

    eps = net_income / shares if net_income is not None else None
    bps = total_equity / shares if total_equity is not None else None

    per: float | None = None
    if eps is not None and eps > 0:
        per = price / eps

    pbr: float | None = None
    if bps is not None and bps > 0:
        pbr = price / bps

    earnings_yield: float | None = None
    if per is not None and per > 0:
        earnings_yield = 1.0 / per

    peg: float | None = None
    if per is not None and earnings_growth is not None:
        peg = compute_peg(per, earnings_growth)

    return ValuationMetrics(
        per=per,
        pbr=pbr,
        peg=peg,
        earnings_yield=earnings_yield,
        market_cap=marcap,
    )


def simulate_growth_valuation(
    current_price: int,
    current_eps: float,
    current_per: float,
    earnings_change_pct: float,
    multiple_change_ratio: float,
) -> GrowthValuationScenario:
    """The "물빨 무한동력" simulator.

    Parameters
    ----------
    current_price : int
        Current stock price (won).
    current_eps : float
        Current earnings per share.
    current_per : float
        Current price-to-earnings ratio.
    earnings_change_pct : float
        Earnings change as a fraction, e.g. ``1.0`` means +100 % (doubles).
    multiple_change_ratio : float
        PER multiplier, e.g. ``1.5`` means the PER becomes 1.5x current.

    Returns
    -------
    GrowthValuationScenario
    """
    scenario_eps = current_eps * (1.0 + earnings_change_pct)
    scenario_per = current_per * multiple_change_ratio
    scenario_price = scenario_eps * scenario_per
    upside_pct = (scenario_price / current_price - 1.0) if current_price else 0.0

    earnings_mult = 1.0 + earnings_change_pct
    total_mult = earnings_mult * multiple_change_ratio
    comment = (
        f"이익 {earnings_mult:.1f}배 + 멀티플 {multiple_change_ratio:.1f}배"
        f" → 주가 {total_mult:.1f}배"
    )

    return GrowthValuationScenario(
        current_eps=current_eps,
        scenario_eps=scenario_eps,
        current_per=current_per,
        scenario_per=scenario_per,
        scenario_price=scenario_price,
        current_price=current_price,
        upside_pct=upside_pct,
        comment=comment,
    )


def compute_implied_growth(
    current_per: float,
    historical_avg_per: float,
) -> float:
    """What growth rate does the current PER imply vs historical average?

    Simple model: ``implied_growth = current_per / avg_per - 1``.
    """
    if historical_avg_per <= 0:
        return 0.0
    return current_per / historical_avg_per - 1.0


def compute_peg(per: float, earnings_growth: float) -> float | None:
    """PEG = PER / (earnings_growth * 100).

    Returns ``None`` when *earnings_growth* <= 0.
    """
    if earnings_growth <= 0:
        return None
    return per / (earnings_growth * 100.0)

import pandas as pd
import pytest

from krqs.domain.operating_leverage import (
    DEFAULT_GROWTH_RATES,
    DEFAULT_SGA_YOY_GROWTH,
    BaselineInputs,
    GpmBand,
    build_scenario_matrix,
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


class TestBaselineInputs:
    def test_frozen(self):
        b = BaselineInputs(revenue=100, cogs=80, sga=10, interest_expense=2)
        with pytest.raises((AttributeError, TypeError)):
            b.revenue = 200  # type: ignore[misc]


class TestScenarioMatrixStructure:
    def test_returns_dataframe(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(sample_baseline, sample_gpm_band)
        assert isinstance(df, pd.DataFrame)

    def test_default_row_count(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(sample_baseline, sample_gpm_band)
        assert len(df) == len(DEFAULT_GROWTH_RATES) * 3

    def test_required_columns(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(sample_baseline, sample_gpm_band)
        expected = {
            "growth_rate",
            "gpm_scenario",
            "gpm",
            "revenue",
            "gross_profit",
            "sga",
            "operating_income",
            "interest_expense",
            "pretax_income",
            "net_income",
            "opm",
            "is_insolvent",
        }
        assert expected.issubset(set(df.columns))

    def test_no_nan_or_inf(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(sample_baseline, sample_gpm_band)
        numeric = df.select_dtypes(include="number")
        assert not numeric.isna().any().any()
        assert not numeric.isin([float("inf"), float("-inf")]).any().any()


class TestZeroGrowth:
    def test_revenue_unchanged(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(
            sample_baseline, sample_gpm_band, growth_rates=(0.0,)
        )
        assert (df["revenue"] == sample_baseline.revenue).all()

    def test_sga_inflated(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(
            sample_baseline, sample_gpm_band, growth_rates=(0.0,)
        )
        expected = sample_baseline.sga * (1 + DEFAULT_SGA_YOY_GROWTH)
        assert list(df["sga"]) == pytest.approx([expected] * len(df))

    def test_mid_gpm_operating_income(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(
            sample_baseline, sample_gpm_band, growth_rates=(0.0,)
        )
        mid = df[df["gpm_scenario"] == "mid"].iloc[0]
        assert mid["gross_profit"] == pytest.approx(1_000_000_000_000)
        assert mid["sga"] == pytest.approx(309_000_000_000)
        assert mid["operating_income"] == pytest.approx(691_000_000_000)


class TestGpmBandOrdering:
    def test_low_lt_high_gross_profit(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(
            sample_baseline, sample_gpm_band, growth_rates=(0.3,)
        )
        low = df[df["gpm_scenario"] == "low"].iloc[0]
        high = df[df["gpm_scenario"] == "high"].iloc[0]
        assert low["gross_profit"] < high["gross_profit"]
        assert low["operating_income"] < high["operating_income"]


class TestInsolvencyFlag:
    def test_flagged_when_interest_exceeds_op_income(self, sample_gpm_band):
        baseline = BaselineInputs(
            revenue=5_000_000_000_000,
            cogs=4_000_000_000_000,
            sga=300_000_000_000,
            interest_expense=500_000_000_000,
        )
        df = build_scenario_matrix(baseline, sample_gpm_band, growth_rates=(0.0,))
        low = df[df["gpm_scenario"] == "low"].iloc[0]
        assert bool(low["is_insolvent"]) is True

    def test_not_flagged_with_healthy_coverage(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(
            sample_baseline, sample_gpm_band, growth_rates=(0.5,)
        )
        assert not df["is_insolvent"].any()


class TestNegativeTaxZeroing:
    def test_tax_zero_when_pretax_negative(self, sample_gpm_band):
        baseline = BaselineInputs(
            revenue=5_000_000_000_000,
            cogs=4_000_000_000_000,
            sga=300_000_000_000,
            interest_expense=1_000_000_000_000,
        )
        df = build_scenario_matrix(baseline, sample_gpm_band, growth_rates=(0.0,))
        low = df[df["gpm_scenario"] == "low"].iloc[0]
        assert low["pretax_income"] < 0
        assert low["net_income"] == pytest.approx(low["pretax_income"])


class TestOperatingLeverageEffect:
    def test_op_income_growth_exceeds_revenue_growth(
        self, sample_baseline, sample_gpm_band
    ):
        df = build_scenario_matrix(
            sample_baseline, sample_gpm_band, growth_rates=(0.0, 0.3)
        )
        low = df[
            (df["growth_rate"] == 0.0) & (df["gpm_scenario"] == "mid")
        ].iloc[0]
        high = df[
            (df["growth_rate"] == 0.3) & (df["gpm_scenario"] == "mid")
        ].iloc[0]

        rev_growth = high["revenue"] / low["revenue"] - 1
        opi_growth = high["operating_income"] / low["operating_income"] - 1

        assert opi_growth > rev_growth


class TestCustomParameters:
    def test_custom_tax_rate_zero(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(
            sample_baseline,
            sample_gpm_band,
            growth_rates=(0.0,),
            tax_rate=0.0,
        )
        for _, row in df.iterrows():
            assert row["net_income"] == pytest.approx(row["pretax_income"])

    def test_custom_sga_growth_zero(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(
            sample_baseline,
            sample_gpm_band,
            growth_rates=(0.0,),
            sga_yoy_growth=0.0,
        )
        assert (df["sga"] == sample_baseline.sga).all()

    def test_custom_growth_rates(self, sample_baseline, sample_gpm_band):
        df = build_scenario_matrix(
            sample_baseline,
            sample_gpm_band,
            growth_rates=(0.1, 0.2, 0.3),
        )
        assert len(df) == 3 * 3
        assert set(df["growth_rate"].unique()) == {0.1, 0.2, 0.3}

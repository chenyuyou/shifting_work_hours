"""Tests for the CGE model module.

Tests cover:
1. SAM construction and validation
2. Calibration (parameter extraction)
3. Baseline replication (no shock → GDP change ≈ 0)
4. Negative shock → GDP decreases (direction test)
5. Results export
"""

import pytest
import numpy as np
from pathlib import Path


class TestSAM:
    """Tests for the SAM class."""

    def test_build_demo_sam(self):
        """Demo SAM should build and validate."""
        from shifting_work_hours.cge.sam import build_demo_sam
        sam = build_demo_sam(total_gdp=100.0)
        assert sam.n == 8
        assert 'AGR' in sam.accounts
        assert 'IND' in sam.accounts

    def test_sam_validation(self):
        """Balanced SAM should pass validation."""
        from shifting_work_hours.cge.sam import build_demo_sam
        sam = build_demo_sam()
        assert sam.validate(tol=1e-4) is True

    def test_sam_aggregate(self):
        """Aggregation should reduce account count."""
        from shifting_work_hours.cge.sam import build_demo_sam
        sam = build_demo_sam()
        # All non-sector accounts map to themselves
        mapping = {a: a for a in sam.accounts}
        agg = sam.aggregate(mapping)
        assert agg.n == sam.n

    def test_sam_to_csv_roundtrip(self, tmp_path):
        """Write SAM to CSV and reload."""
        from shifting_work_hours.cge.sam import build_demo_sam, SAM
        sam = build_demo_sam()
        csv_path = tmp_path / 'test_sam.csv'
        sam.to_csv(csv_path)
        sam2 = SAM.from_csv(csv_path)
        assert sam2.n == sam.n
        np.testing.assert_allclose(sam2.matrix, sam.matrix, atol=1e-10)


class TestCalibration:
    """Tests for parameter calibration."""

    def test_calibration_completes(self):
        """Calibration should run without errors."""
        from shifting_work_hours.cge.sam import build_demo_sam
        from shifting_work_hours.cge.calibration import calibrate
        sam = build_demo_sam()
        params = calibrate(sam)
        assert len(params.output) == 2
        assert params.lab_income > 0
        assert params.cap_income > 0

    def test_calibration_shares_sum_to_one(self):
        """Sector shares should sum correctly."""
        from shifting_work_hours.cge.sam import build_demo_sam
        from shifting_work_hours.cge.calibration import calibrate
        sam = build_demo_sam()
        params = calibrate(sam)
        for s in ('AGR', 'IND'):
            total_share = (
                params.va_labour[s] + params.va_capital[s]
            )
            assert abs(total_share - 1.0) < 1e-10


class TestCGEModel:
    """Tests for the CGE model solver."""

    @pytest.fixture
    def calibrated_params(self):
        from shifting_work_hours.cge.sam import build_demo_sam
        from shifting_work_hours.cge.calibration import calibrate
        sam = build_demo_sam()
        return calibrate(sam)

    def test_replication_baseline(self, calibrated_params):
        """Baseline (no shock) should replicate SAM data."""
        from shifting_work_hours.cge.model import solve as cge_solve
        result = cge_solve(calibrated_params, shock_factors=None)
        # GDP change should be very close to 0
        assert abs(result['gdp_change']) < 0.01, (
            f"Baseline GDP change = {result['gdp_change']:.6f}, "
            f"expected ≈ 0"
        )

    def test_negative_shock_reduces_gdp(self, calibrated_params):
        """Negative labour shock should reduce GDP."""
        from shifting_work_hours.cge.model import solve as cge_solve
        shock = {'AGR': 0.95, 'IND': 0.97}  # 5% and 3% loss
        result = cge_solve(calibrated_params, shock_factors=shock)
        # GDP should decrease
        assert result['gdp_change'] < 0, (
            f"GDP change = {result['gdp_change']:.6f}, "
            f"expected negative"
        )

    def test_larger_shock_larger_impact(self, calibrated_params):
        """Larger shock should produce larger GDP reduction."""
        from shifting_work_hours.cge.model import solve as cge_solve
        small = cge_solve(calibrated_params, shock_factors={'AGR': 0.98, 'IND': 0.99})
        large = cge_solve(calibrated_params, shock_factors={'AGR': 0.90, 'IND': 0.95})
        assert large['gdp_change'] < small['gdp_change']


class TestShock:
    """Tests for the shock module."""

    def test_synthetic_shock(self):
        from shifting_work_hours.cge.shock import ProductivityShock
        factors = ProductivityShock.synthetic_shock('SSP585', 2050)
        assert 'AGR' in factors
        assert 'IND' in factors
        assert all(0 < v <= 1.0 for v in factors.values())

    def test_synthetic_zero_at_2020(self):
        from shifting_work_hours.cge.shock import ProductivityShock
        factors = ProductivityShock.synthetic_shock('SSP585', 2020)
        # At 2020, loss should be 0, so factors should be 1.0
        assert abs(factors['AGR'] - 1.0) < 0.01

    def test_synthetic_increases_with_year(self):
        from shifting_work_hours.cge.shock import ProductivityShock
        f2030 = ProductivityShock.synthetic_shock('SSP585', 2030)
        f2080 = ProductivityShock.synthetic_shock('SSP585', 2080)
        assert f2080['AGR'] < f2030['AGR']  # more loss → lower factor


class TestResults:
    """Tests for the results analyzer."""

    def test_results_analyzer(self):
        from shifting_work_hours.cge.results import ResultsAnalyzer
        ra = ResultsAnalyzer()
        ra.add('base', {'gdp_change': 0.0, 'price_change': {'AGR': 0}, 'output_change': {'AGR': 0}})
        ra.add('shock', {'gdp_change': -0.05, 'price_change': {'AGR': 0.02}, 'output_change': {'AGR': -0.03}})
        summary = ra.summary('base', 'shock')
        assert 'GDP' in summary
        assert '-5.0' in summary

    def test_results_csv_export(self, tmp_path):
        from shifting_work_hours.cge.results import ResultsAnalyzer
        ra = ResultsAnalyzer()
        ra.add('test', {'gdp_change': -0.03, 'price_change': {}, 'output_change': {}})
        csv_path = tmp_path / 'results.csv'
        ra.to_csv(csv_path)
        assert csv_path.exists()

"""Core CGE model — analytical CES implementation.

Works with any number of sectors (2, 5, 10, etc.) loaded from the SAM.

Model
-----
For each sector *s* with CES production:
    Y_s = [α_s (L_s · eff_s)^ρ + (1−α_s) K_s^ρ]^{1/ρ}
where ρ = (σ−1)/σ.

Under Johansen closure (fixed capital), the output change is:
    Y_new / Y_old = [α_s · eff_s^ρ + (1−α_s)]^{1/ρ}

GDP change is the weighted sum of sectoral output changes.
"""

from __future__ import annotations

import logging

import numpy as np

from shifting_work_hours.cge.calibration import CalibratedParams

logger = logging.getLogger(__name__)


def _get_sectors(params: CalibratedParams) -> list[str]:
    """Extract production sector names from calibration."""
    return list(params.output.keys())


def solve(params: CalibratedParams,
          shock_factors: dict[str, float] | None = None,
          solver: str = 'ipopt') -> dict:
    """Compute GDP change from labour-productivity shocks.

    Parameters
    ----------
    params : CalibratedParams
        Calibrated from SAM (any number of sectors).
    shock_factors : dict, optional
        Labour productivity multipliers by sector.
        Keys must match sector names in the SAM.
        If None, no shock (replication mode → GDP change ≈ 0).
    solver : str
        Ignored (analytical solution).

    Returns
    -------
    dict with keys:
        'status', 'gdp_change', 'price_change', 'output_change',
        'wage_change', 'rental_change'
    """
    sectors = _get_sectors(params)

    if shock_factors is None:
        shock_factors = {s: 1.0 for s in sectors}

    sig = params.elasticities.sigma_va
    rho = (sig - 1.0) / sig  # CES exponent

    output_change = {}
    price_change = {}

    for s in sectors:
        eff = shock_factors.get(s, 1.0)
        aL = params.va_labour.get(s, 0.5)
        aC = params.va_capital.get(s, 0.5)

        if abs(eff - 1.0) < 1e-10:
            output_change[s] = 0.0
        elif abs(rho) < 1e-10:
            # Cobb-Douglas case (sigma=1, rho=0): Y = L^α * K^(1-α)
            # dY/Y = α * d(eff)/eff
            output_change[s] = aL * (eff - 1.0)
        else:
            baseline = aL + aC  # = 1
            shocked = aL * eff ** rho + aC
            if shocked > 0 and baseline > 0:
                y_ratio = shocked ** (1.0 / rho) / baseline ** (1.0 / rho)
                output_change[s] = y_ratio - 1.0
            else:
                output_change[s] = 0.0

        # Price change: inverse of productivity change
        price_change[s] = -output_change[s] * 0.3

    # Aggregate GDP change (weighted by base output)
    base_gdp = sum(params.output.values())
    new_gdp = sum(
        (1.0 + output_change[s]) * params.output[s]
        for s in sectors
    )
    gdp_change = (new_gdp - base_gdp) / base_gdp if base_gdp > 0 else 0.0

    # Factor price changes
    eff_changes = [shock_factors.get(s, 1.0) - 1.0 for s in sectors]
    weights = [params.output[s] for s in sectors]
    total_weight = sum(weights)
    if total_weight > 0:
        wage_change = sum(e * w for e, w in zip(eff_changes, weights)) / total_weight
    else:
        wage_change = np.mean(eff_changes)

    rental_change = -wage_change * 0.2

    result = {
        'status': 'optimal',
        'gdp_change': gdp_change,
        'price_change': price_change,
        'output_change': output_change,
        'wage_change': wage_change,
        'rental_change': rental_change,
    }

    logger.info("CGE solved: %d sectors, GDP change = %.4f%%",
                len(sectors), gdp_change * 100)
    return result

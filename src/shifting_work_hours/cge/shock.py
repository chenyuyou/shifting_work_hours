"""Labour-productivity shock interface.

Translates per-grid, per-intensity productivity-loss data into
per-sector CGE labour-efficiency multipliers.

The mapping from national-level loss to sector-specific loss is
configured via ``ShockMapping`` in the CGE config.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


class ProductivityShock:
    """Load productivity loss and compute sector-specific shocks.

    Parameters
    ----------
    productivity_loss_dir : Path
        Directory containing the pipeline's weighted-productivity-loss
        output files (Stage 6 output).
    """

    def __init__(self, productivity_loss_dir: Path):
        self.base_dir = productivity_loss_dir

    def load_national_loss(
        self,
        scenario: str,
        year: int,
        model: str = 'ensemble_mean',
    ) -> Dict[str, float]:
        """Load national-average productivity loss by sector.

        Parameters
        ----------
        scenario : str
            SSP scenario (e.g. 'SSP245').
        year : int
            Simulation year.
        model : str
            Climate model or 'ensemble_mean'.

        Returns
        -------
        dict
            ``{sector: loss_fraction}`` where loss_fraction ∈ [0, 1].
            E.g. ``{'AGR': 0.05, 'IND': 0.03}`` means 5% loss in AGR.
        """
        # Try to find the output file
        pattern = f"*{scenario}*{year}*.nc"
        files = list(self.base_dir.rglob(pattern))

        if not files:
            logger.warning(
                "No productivity loss files found for %s/%s, "
                "using zero shock.",
                scenario, year,
            )
            return {'AGR': 0.0, 'IND': 0.0}

        # Load and average across all files
        total_loss = 0.0
        count = 0
        for f in files:
            try:
                ds = xr.open_dataset(f)
                for var in ds.data_vars:
                    if 'loss' in var.lower() or 'productivity' in var.lower():
                        data = ds[var].values
                        valid = data[~np.isnan(data)]
                        if len(valid) > 0:
                            total_loss += float(np.mean(valid))
                            count += 1
                ds.close()
            except Exception as e:
                logger.debug("Skipping %s: %s", f, e)

        avg_loss = total_loss / max(count, 1)

        # Map to sectors (in Phase 1, same loss for both sectors)
        # Agriculture is typically more affected by heat
        return {
            'AGR': avg_loss * 1.2,  # agriculture ~20% more affected
            'IND': avg_loss * 0.8,  # industry ~20% less affected
        }

    def to_efficiency_factors(
        self, loss_by_sector: Dict[str, float]
    ) -> Dict[str, float]:
        """Convert loss fractions to efficiency multipliers.

        Parameters
        ----------
        loss_by_sector : dict
            ``{sector: loss_fraction}`` where loss ∈ [0, 1].

        Returns
        -------
        dict
            ``{sector: efficiency_factor}`` where factor ∈ (0, 1].
            E.g. loss=0.05 → factor=0.95.
        """
        return {s: max(1.0 - loss, 0.01) for s, loss in loss_by_sector.items()}

    def load_and_convert(
        self, scenario: str, year: int
    ) -> Dict[str, float]:
        """Convenience: load loss → convert to efficiency factors."""
        loss = self.load_national_loss(scenario, year)
        return self.to_efficiency_factors(loss)

    @staticmethod
    def synthetic_shock(
        scenario: str,
        year: int,
        base_loss_2050: float = 0.03,
        growth_rate: float = 0.01,
    ) -> Dict[str, float]:
        """Generate a synthetic (smooth) shock for testing.

        Uses a simple linear interpolation from 0 at 2020 to
        ``base_loss_2050`` at 2050, then continues at that rate.

        Parameters
        ----------
        scenario : str
            For logging only.
        year : int
            Year.
        base_loss_2050 : float
            National productivity loss at 2050 (fraction).
        growth_rate : float
            Annual increase in loss after 2050.

        Returns
        -------
        dict
            ``{sector: efficiency_factor}``.
        """
        if year <= 2020:
            loss = 0.0
        elif year <= 2050:
            loss = base_loss_2050 * (year - 2020) / 30.0
        else:
            loss = base_loss_2050 + growth_rate * (year - 2050)

        # SSP5-8.5 is worst case
        if '585' in scenario:
            loss *= 1.5
        elif '126' in scenario:
            loss *= 0.6

        # Convert loss to efficiency factors
        agr_loss = loss * 1.2
        ind_loss = loss * 0.8
        return {
            'AGR': max(1.0 - agr_loss, 0.01),
            'IND': max(1.0 - ind_loss, 0.01),
        }

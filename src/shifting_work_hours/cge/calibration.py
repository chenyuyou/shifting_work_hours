"""Parameter calibration from the SAM.

After the SAM is built, this module extracts the share parameters
(alpha, delta, etc.) needed by the CGE model.  The key idea is:

    SAM entry / column total  →  share parameter in CES / CDE

This ensures the model exactly replicates the base-year (SAM) data
when solved without shocks — the *replication test*.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict

import numpy as np

from shifting_work_hours.cge.sam import SAM
from shifting_work_hours.cge.parameters import Elasticities

logger = logging.getLogger(__name__)


@dataclass
class CalibratedParams:
    """Parameters extracted from the SAM, ready for the CGE model.

    Attributes
    ----------
    sam : SAM
        Reference SAM.
    elasticities : Elasticities
        Elasticity values.
    output : dict[str, float]
        Total output by sector (= column sum of sector account).
    intermediate : dict[str, dict[str, float]]
        ``intermediate[i][j]`` = intermediate input from sector j
        used by sector i (SAM entry / sector output).
    value_added : dict[str, float]
        Value-added share in sector output.
    va_labour : dict[str, float]
        Labour share within value-added by sector.
    va_capital : dict[str, float]
        Capital share within value-added by sector.
    lab_income : float
        Total labour income.
    cap_income : float
        Total capital income.
    hou_consumption : dict[str, float]
        Household consumption by sector (shares of total cons).
    gov_consumption : dict[str, float]
        Government consumption by sector (shares).
    inv_demand : dict[str, float]
        Investment demand by sector (shares).
    """

    sam: SAM
    elasticities: Elasticities

    output: Dict[str, float] = field(default_factory=dict)
    intermediate: Dict[str, Dict[str, float]] = field(default_factory=dict)
    value_added: Dict[str, float] = field(default_factory=dict)
    va_labour: Dict[str, float] = field(default_factory=dict)
    va_capital: Dict[str, float] = field(default_factory=dict)
    lab_income: float = 0.0
    cap_income: float = 0.0
    hou_consumption: Dict[str, float] = field(default_factory=dict)
    gov_consumption: Dict[str, float] = field(default_factory=dict)
    inv_demand: Dict[str, float] = field(default_factory=dict)


def calibrate(sam: SAM, elasticities: Elasticities | None = None) -> CalibratedParams:
    """Extract share parameters from the SAM.

    This performs a *calibration*, not an estimation.  All share
    parameters are mechanically derived so the model replicates the
    SAM base-year data.

    Parameters
    ----------
    sam : SAM
        Balanced Social Accounting Matrix.
    elasticities : Elasticities, optional
        If None, default elasticities are used.

    Returns
    -------
    CalibratedParams
    """
    if elasticities is None:
        elasticities = Elasticities()

    params = CalibratedParams(sam=sam, elasticities=elasticities)
    idx = {a: sam.index(a) for a in sam.accounts}

    # ── Sector accounts (all accounts except factor/institution accounts) ──
    non_sectors = {'LAB', 'CAP', 'HOU', 'GOV', 'INV', 'ROW'}
    sectors = [a for a in sam.accounts if a not in non_sectors]

    for s in sectors:
        j = idx[s]
        col_sum = sam.matrix[:, j].sum()
        params.output[s] = col_sum

        # Intermediate input shares (row i, column s) / output
        params.intermediate[s] = {}
        for s2 in sectors:
            params.intermediate[s][s2] = (
                sam.matrix[idx[s2], j] / col_sum if col_sum > 0 else 0.0
            )

        # Value-added share
        va = sam.matrix[idx['LAB'], j] + sam.matrix[idx['CAP'], j]
        params.value_added[s] = va / col_sum if col_sum > 0 else 0.0

        # Labour / capital shares within value-added
        lab = sam.matrix[idx['LAB'], j]
        cap = sam.matrix[idx['CAP'], j]
        total_factor = lab + cap
        if total_factor > 0:
            params.va_labour[s] = lab / total_factor
            params.va_capital[s] = cap / total_factor
        else:
            params.va_labour[s] = 0.5
            params.va_capital[s] = 0.5

    # ── Factor incomes ──
    params.lab_income = sam.matrix[idx['LAB'], :].sum()
    params.cap_income = sam.matrix[idx['CAP'], :].sum()

    # ── Household consumption shares ──
    hou_col = sam.matrix[:, idx['HOU']].sum()
    for s in sectors:
        params.hou_consumption[s] = (
            sam.matrix[idx[s], idx['HOU']] / hou_col if hou_col > 0 else 0.0
        )

    # ── Government consumption shares ──
    gov_col = sam.matrix[:, idx['GOV']].sum()
    for s in sectors:
        params.gov_consumption[s] = (
            sam.matrix[idx[s], idx['GOV']] / gov_col if gov_col > 0 else 0.0
        )

    # ── Investment demand shares ──
    inv_col = sam.matrix[:, idx['INV']].sum()
    for s in sectors:
        params.inv_demand[s] = (
            sam.matrix[idx[s], idx['INV']] / inv_col if inv_col > 0 else 0.0
        )

    logger.info("Calibration complete: %d sectors, GDP=%.1f",
                len(sectors), sum(params.output.values()))
    return params

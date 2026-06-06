"""Social Accounting Matrix (SAM) construction and management.

A SAM is a square matrix where rows and columns represent the same set
of accounts (sectors, factors, institutions).  Entry SAM[i,j] records
the payment flowing *from* account j *to* account i, so each column
sums to the account's total income and each row sums to its total
expenditure.

This module:
1. Loads a pre-built SAM from CSV (the common approach for China).
2. Validates row/column balance.
3. Supports sector aggregation.
4. Provides a convenience factory that builds a demo 2-sector SAM
   from plausible China 2020 ratios (for testing without the actual
   IO table).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class SAM:
    """Social Accounting Matrix.

    Parameters
    ----------
    accounts : list of str
        Account labels (e.g. ``['AGR','IND','LAB','CAP','HOU',...]``).
    matrix : np.ndarray
        Square array of shape ``(n, n)`` where ``matrix[i, j]`` is the
        payment from account *j* to account *i*.
    """

    def __init__(self, accounts: List[str], matrix: np.ndarray):
        n = len(accounts)
        if matrix.shape != (n, n):
            raise ValueError(
                f"Matrix shape {matrix.shape} does not match "
                f"{n} accounts."
            )
        self.accounts = list(accounts)
        self.matrix = matrix.astype(np.float64)
        self.n = n
        self._df: Optional[pd.DataFrame] = None

    # ── helpers ──────────────────────────────────────

    @property
    def df(self) -> pd.DataFrame:
        """DataFrame view of the SAM."""
        if self._df is None:
            self._df = pd.DataFrame(
                self.matrix, index=self.accounts, columns=self.accounts
            )
        return self._df

    def index(self, account: str) -> int:
        return self.accounts.index(account)

    def total(self, account: str) -> float:
        """Total income (= column sum) for *account*."""
        j = self.index(account)
        return float(self.matrix[:, j].sum())

    # ── validation ───────────────────────────────────

    def validate(self, tol: float = 1e-6) -> bool:
        """Check that every column sum equals the corresponding row sum.

        Returns True if balanced; raises ``ValueError`` otherwise.
        """
        col_sums = self.matrix.sum(axis=0)
        row_sums = self.matrix.sum(axis=1)
        diff = np.abs(col_sums - row_sums)
        max_diff = diff.max()
        if max_diff > tol:
            bad = [
                (self.accounts[i], diff[i])
                for i in range(self.n) if diff[i] > tol
            ]
            raise ValueError(
                f"SAM is unbalanced (max diff = {max_diff:.6f}):\n{bad}"
            )
        logger.info("SAM validation passed (max diff = %.2e)", max_diff)
        return True

    # ── aggregation ──────────────────────────────────

    def aggregate(self, mapping: Dict[str, str]) -> "SAM":
        """Aggregate sectors according to *mapping*.

        *mapping* maps each original account name to its new
        (aggregate) name.  Accounts that are not in the mapping are
        kept as-is.

        Returns a new SAM with the reduced account set.
        """
        # build new account list (preserving order of first appearance)
        new_accounts: List[str] = []
        idx_map: Dict[str, List[int]] = {}
        for i, acct in enumerate(self.accounts):
            target = mapping.get(acct, acct)
            if target not in idx_map:
                idx_map[target] = []
                new_accounts.append(target)
            idx_map[target].append(i)

        m = len(new_accounts)
        new_matrix = np.zeros((m, m), dtype=np.float64)
        for ni, ai in enumerate(new_accounts):
            for nj, aj in enumerate(new_accounts):
                for oi in idx_map[ai]:
                    for oj in idx_map[aj]:
                        new_matrix[ni, nj] += self.matrix[oi, oj]

        logger.info(
            "SAM aggregated from %d → %d accounts", self.n, m
        )
        return SAM(new_accounts, new_matrix)

    # ── I/O ──────────────────────────────────────────

    @classmethod
    def from_csv(cls, path: Union[str, Path]) -> "SAM":
        """Load a SAM from a CSV file.

        The CSV must be square with the first column and first row
        containing account labels.
        """
        df = pd.read_csv(path, index_col=0)
        accounts = list(df.columns)
        matrix = df.values.astype(np.float64)
        logger.info("Loaded SAM from %s (%d accounts)", path, len(accounts))
        return cls(accounts, matrix)

    def to_csv(self, path: Union[str, Path]) -> None:
        self.df.to_csv(path)
        logger.info("SAM saved to %s", path)

    def __repr__(self) -> str:
        return f"SAM({self.n} accounts: {self.accounts})"


# ──────────────────────────────────────────────────────
#  Demo SAM builder (Phase 1 – 2-sector China)
# ──────────────────────────────────────────────────────

def build_demo_sam(total_gdp: float = 100.0) -> SAM:
    """Build a balanced 2-sector SAM for China (Phase 1 demo).

    The SAM is constructed so that every account's column sum equals
    its row sum.  Intermediate flows between sectors are chosen
    consistently with the IO identity:
        output = intermediate_purchases + value_added

    Accounts: AGR IND LAB CAP HOU GOV INV ROW

    Parameters
    ----------
    total_gdp : float
        Total GDP (value added).  Default 100.
    """
    gdp = total_gdp

    # ── Value added by sector ──
    agr_va = gdp * 0.07    # 7.0
    ind_va = gdp * 0.93    # 93.0

    # ── Factor incomes within value added ──
    # Labour: AGR 85%, IND 45% of VA
    lab_agr = agr_va * 0.85   # 5.95
    cap_agr = agr_va * 0.15   # 1.05
    lab_ind = ind_va * 0.45   # 41.85
    cap_ind = ind_va * 0.55   # 51.15
    total_lab = lab_agr + lab_ind   # 47.80
    total_cap = cap_agr + cap_ind   # 52.20

    # ── Government tax on capital income (10%) ──
    gov_tax = total_cap * 0.10   # 5.22
    hou_cap = total_cap - gov_tax  # 46.98

    # ── Final demand (expenditure side) ──
    hou_total = gdp * 0.60   # 60.0
    hou_agr = hou_total * 0.35  # 21.0
    hou_ind = hou_total * 0.65  # 39.0

    gov_total = gdp * 0.15   # 15.0
    gov_agr = gov_total * 0.20  # 3.0
    gov_ind = gov_total * 0.80  # 12.0

    inv_total = gdp * 0.42   # 42.0
    inv_agr = inv_total * 0.10  # 4.2
    inv_ind = inv_total * 0.90  # 37.8

    net_exp = gdp - hou_total - gov_total - inv_total  # -17.0
    nx_agr = net_exp * 0.30   # -5.1
    nx_ind = net_exp * 0.70   # -11.9

    # ── Intermediate flows ──
    # The key balance condition for each sector:
    #   column sum (costs) = row sum (revenue)
    #   intermediate_purchases + VA = intermediate_sales + final_demand
    #
    # We choose intermediate flows so that:
    #   S[IND, AGR] - S[AGR, IND] = final_demand_AGR - VA_AGR
    #   (IND sells more to AGR than AGR sells to IN, because
    #    AGR's final demand exceeds its VA by the difference)

    final_agr = hou_agr + gov_agr + inv_agr + nx_agr  # 23.1
    final_ind = hou_ind + gov_ind + inv_ind + nx_ind  # 76.9

    # AG's final demand = 23.1, AG's VA = 7.0
    # So AG intermediate purchases - AG intermediate sales = 23.1 - 7.0 = 16.1
    # Choose: AGR buys 20 from IND, AGR sells 3.9 to IND
    # Then: 20 - 3.9 = 16.1 ✓
    inter_agr_from_ind = 20.0    # AGR buys from IND
    inter_ind_from_agr = 3.9     # IND buys from AGR

    # Verify AG balance:
    ag_col = inter_agr_from_ind + lab_agr + cap_agr   # 20 + 5.95 + 1.05 = 27.0
    ag_row = inter_ind_from_agr + hou_agr + gov_agr + inv_agr + nx_agr  # 3.9 + 23.1 = 27.0 ✓

    # Verify IN balance:
    in_col = inter_ind_from_agr + lab_ind + cap_ind   # 3.9 + 41.85 + 51.15 = 96.9
    in_row = inter_agr_from_ind + hou_ind + gov_ind + inv_ind + nx_ind  # 20 + 76.9 = 96.9 ✓

    # Total output
    ag_output = ag_col  # 27.0
    ind_output = in_col  # 96.9

    # ── Build the 8×8 SAM matrix ──
    #    AGR    IND    LAB    CAP    HOU    GOV    INV    ROW
    accounts = ['AGR', 'IND', 'LAB', 'CAP', 'HOU', 'GOV', 'INV', 'ROW']
    n = len(accounts)
    S = np.zeros((n, n))

    # AGR column (payments FROM AGR):
    S[1, 0] = inter_agr_from_ind   # AGR→IND intermediate
    S[2, 0] = lab_agr              # AGR→LAB wages
    S[3, 0] = cap_agr              # AGR→CAP rents

    # IND column (payments FROM IND):
    S[0, 1] = inter_ind_from_agr   # IND→AGR intermediate
    S[2, 1] = lab_ind              # IND→LAB wages
    S[3, 1] = cap_ind              # IND→CAP rents

    # LAB column: labour income → household
    S[4, 2] = total_lab

    # CAP column: capital income → household + government tax
    S[4, 3] = hou_cap
    S[5, 3] = gov_tax

    # HOU column: household consumption → sectors + savings → INV
    S[0, 4] = hou_agr
    S[1, 4] = hou_ind
    hou_savings = (total_lab + hou_cap) - hou_total  # income - consumption
    S[6, 4] = hou_savings  # household savings → investment

    # GOV column: government consumption → sectors + savings → INV
    S[0, 5] = gov_agr
    S[1, 5] = gov_ind
    gov_savings = gov_tax - gov_total  # revenue - expenditure
    S[6, 5] = gov_savings  # government savings → investment

    # INV column: investment demand → sectors
    S[0, 6] = inv_agr
    S[1, 6] = inv_ind

    # ROW column: net exports → sectors + capital inflow → INV
    S[0, 7] = nx_agr
    S[1, 7] = nx_ind
    row_capital_inflow = -(nx_agr + nx_ind)  # foreign savings = -NX
    S[6, 7] = row_capital_inflow  # foreign savings → investment

    sam = SAM(accounts, S)
    sam.validate(tol=1e-4)
    logger.info("Demo SAM built (GDP=%.1f, AG_out=%.1f, IN_out=%.1f)",
                gdp, ag_output, ind_output)
    return sam

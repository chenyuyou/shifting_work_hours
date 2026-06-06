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
#  SAM construction helpers
# ──────────────────────────────────────────────────────

def build_from_config(config) -> SAM:
    """Build or load a SAM from a CGEConfig.

    If ``config.sam_file`` is set, load from CSV.
    Otherwise, build a demo SAM using config parameters.
    """
    from shifting_work_hours.cge.parameters import CGEConfig

    if config.sam_file:
        return SAM.from_csv(config.sam_file)
    return build_demo_sam(config=config)


def build_demo_sam(total_gdp: float = 100.0, config=None) -> SAM:
    """Build a balanced 2-sector SAM.

    If *config* is provided, uses its parameters (GDP shares,
    factor shares, demand shares).  Otherwise uses sensible defaults.

    The SAM is constructed so that every account's column sum equals
    its row sum (balanced by construction).

    Accounts: S1 S2 LAB CAP HOU GOV INV ROW
    (S1, S2 are the first two aggregate sectors from config)
    """
    if config is not None:
        gdp = config.total_gdp
        sectors = config.sector_mapping.aggregate_sectors[:2]
        lab_shares = config.lab_share_by_sector
        hou_share = config.hou_consumption_share
        gov_share = config.gov_consumption_share
        inv_share = config.investment_share
        tax_rate = config.gov_tax_rate
    else:
        gdp = total_gdp
        sectors = ['AGR', 'IND']
        lab_shares = {'AGR': 0.85, 'IND': 0.45}
        hou_share = 0.60
        gov_share = 0.15
        inv_share = 0.42
        tax_rate = 0.10

    s1, s2 = sectors[0], sectors[1]

    # ── Value added by sector ──
    va_shares = [0.07, 0.93] if len(sectors) == 2 else [1.0 / len(sectors)] * len(sectors)
    va = {s1: gdp * va_shares[0], s2: gdp * va_shares[1]}

    # ── Factor incomes ──
    lab = {s: va[s] * lab_shares.get(s, 0.5) for s in sectors}
    cap = {s: va[s] * (1.0 - lab_shares.get(s, 0.5)) for s in sectors}
    total_lab = sum(lab.values())
    total_cap = sum(cap.values())

    # ── Government ──
    gov_tax = total_cap * tax_rate
    hou_cap_total = total_cap - gov_tax

    # ── Final demand ──
    hou_total = gdp * hou_share
    gov_total = gdp * gov_share
    inv_total = gdp * inv_share
    net_exp = gdp - hou_total - gov_total - inv_total

    # Split final demand across sectors (35/65 for 2-sector default)
    hou_split = [0.35, 0.65] if len(sectors) == 2 else [1.0 / len(sectors)] * len(sectors)
    gov_split = [0.20, 0.80] if len(sectors) == 2 else [1.0 / len(sectors)] * len(sectors)
    inv_split = [0.10, 0.90] if len(sectors) == 2 else [1.0 / len(sectors)] * len(sectors)
    nx_split = [0.30, 0.70] if len(sectors) == 2 else [1.0 / len(sectors)] * len(sectors)

    hou = {s: hou_total * h for s, h in zip(sectors, hou_split)}
    gov = {s: gov_total * g for s, g in zip(sectors, gov_split)}
    inv = {s: inv_total * i for s, i in zip(sectors, inv_split)}
    nx = {s: net_exp * n for s, n in zip(sectors, nx_split)}

    # ── Intermediate flows (balance condition) ──
    # For each sector: column(costs) = row(revenue)
    # column = intermediate_purchases + lab + cap
    # row = intermediate_sales + hou + gov + inv + nx
    # We solve: inter_s1_from_s2 - inter_s2_from_s1 = final_s1 - va_s1

    final_s1 = hou[s1] + gov[s1] + inv[s1] + nx[s1]
    diff = final_s1 - va[s1]

    if diff >= 0:
        inter_s1_from_s2 = abs(diff) + 1.0
        inter_s2_from_s1 = 1.0
    else:
        inter_s1_from_s2 = 1.0
        inter_s2_from_s1 = abs(diff) + 1.0

    # ── Build SAM matrix ──
    accounts = sectors + ['LAB', 'CAP', 'HOU', 'GOV', 'INV', 'ROW']
    n = len(accounts)
    S = np.zeros((n, n))
    idx = {a: i for i, a in enumerate(accounts)}

    # Sector columns (payments FROM sector)
    S[idx[s2], idx[s1]] = inter_s1_from_s2   # s1 buys from s2
    S[idx['LAB'], idx[s1]] = lab[s1]
    S[idx['CAP'], idx[s1]] = cap[s1]

    S[idx[s1], idx[s2]] = inter_s2_from_s1   # s2 buys from s1
    S[idx['LAB'], idx[s2]] = lab[s2]
    S[idx['CAP'], idx[s2]] = cap[s2]

    # LAB → HOU
    S[idx['HOU'], idx['LAB']] = total_lab

    # CAP → HOU + GOV
    S[idx['HOU'], idx['CAP']] = hou_cap_total
    S[idx['GOV'], idx['CAP']] = gov_tax

    # HOU → sectors + savings
    for s in sectors:
        S[idx[s], idx['HOU']] = hou[s]
    hou_savings = (total_lab + hou_cap_total) - hou_total
    S[idx['INV'], idx['HOU']] = hou_savings

    # GOV → sectors + savings
    for s in sectors:
        S[idx[s], idx['GOV']] = gov[s]
    gov_savings = gov_tax - gov_total
    S[idx['INV'], idx['GOV']] = gov_savings

    # INV → sectors
    for s in sectors:
        S[idx[s], idx['INV']] = inv[s]

    # ROW → sectors + capital inflow
    for s in sectors:
        S[idx[s], idx['ROW']] = nx[s]
    S[idx['INV'], idx['ROW']] = -(sum(nx.values()))

    sam = SAM(accounts, S)
    sam.validate(tol=1e-4)
    logger.info("Demo SAM built: %d sectors, GDP=%.1f", len(sectors), gdp)
    return sam

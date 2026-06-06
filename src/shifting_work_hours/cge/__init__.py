"""Computable General Equilibrium (CGE) model module.

This module implements a single-country multi-region CGE model for
assessing the economic impacts of climate-change-induced labor
productivity losses in China.

The model uses:
- CES nested production functions
- Armington import demand / CET export supply
- Stone-Geary household demand
- Johansen-style closure (fixed investment)

Solver: Pyomo + IPOPT (default) or scipy.optimize fallback.
"""

from shifting_work_hours.cge.sam import SAM, build_demo_sam
from shifting_work_hours.cge.model import solve as cge_solve
from shifting_work_hours.cge.calibration import calibrate
from shifting_work_hours.cge.shock import ProductivityShock
from shifting_work_hours.cge.results import ResultsAnalyzer

__all__ = ['SAM', 'build_demo_sam', 'cge_solve', 'calibrate', 'ProductivityShock', 'ResultsAnalyzer']

"""Computable General Equilibrium (CGE) model module.

A generic, configurable CGE engine for assessing economic impacts of
exogenous shocks (e.g. climate-change-induced labour productivity
losses).  Configure for any country/study via a JSON config file.

Quick start::

    from shifting_work_hours.cge import CGEConfig, build_from_config, calibrate, cge_solve

    config = CGEConfig.from_json('configs/my_study.json')
    sam = build_from_config(config)
    params = calibrate(sam, config.elasticities)
    result = cge_solve(params, shock_factors={'AGR': 0.95, 'IND': 0.97})
"""

from shifting_work_hours.cge.parameters import CGEConfig, Elasticities, SectorMapping, ShockMapping
from shifting_work_hours.cge.sam import SAM, build_demo_sam, build_from_config
from shifting_work_hours.cge.model import solve as cge_solve
from shifting_work_hours.cge.calibration import calibrate
from shifting_work_hours.cge.shock import ProductivityShock
from shifting_work_hours.cge.results import ResultsAnalyzer

__all__ = [
    'CGEConfig', 'Elasticities', 'SectorMapping', 'ShockMapping',
    'SAM', 'build_demo_sam', 'build_from_config',
    'cge_solve', 'calibrate', 'ProductivityShock', 'ResultsAnalyzer',
]

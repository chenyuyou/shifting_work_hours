"""CLI entry point for the CGE model.

Usage::

    python scripts/run_cge.py baseline        # replication test
    python scripts/run_cge.py simulate \\
        --scenario SSP585 --year 2050         # run with shocks
    python scripts/run_cge.py sweep \\
        --scenario SSP585 --start 2020 --end 2100 --step 10

Requires: pip install pyomo
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure the project root is on the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from shifting_work_hours.cge.sam import SAM, build_demo_sam
from shifting_work_hours.cge.calibration import calibrate
from shifting_work_hours.cge.model import solve as cge_solve
from shifting_work_hours.cge.shock import ProductivityShock
from shifting_work_hours.cge.results import ResultsAnalyzer
from shifting_work_hours.cge.parameters import Elasticities

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


def cmd_baseline(args):
    """Replication test: solve without shocks."""
    logger.info("Building demo SAM …")
    sam = build_demo_sam(total_gdp=args.gdp)
    params = calibrate(sam, Elasticities())

    logger.info("Solving baseline (replication test) …")
    result = cge_solve(params, shock_factors=None, solver=args.solver)

    print(f"\nBaseline solve: status = {result['status']}")
    print(f"GDP change: {result['gdp_change']*100:+.6f}%")
    print(f"Wage change: {result['wage_change']*100:+.6f}%")
    print(f"Rental change: {result['rental_change']*100:+.6f}%")
    for s, v in result['output_change'].items():
        print(f"  {s} output: {v*100:+.6f}%")
    return result


def cmd_simulate(args):
    """Run a single scenario-year simulation."""
    sam = build_demo_sam(total_gdp=args.gdp)
    params = calibrate(sam, Elasticities())

    # Get shock factors
    if args.synthetic:
        factors = ProductivityShock.synthetic_shock(
            args.scenario, args.year,
            base_loss_2050=args.base_loss,
        )
    else:
        # Load from pipeline output
        shock = ProductivityShock(Path(args.data_dir))
        factors = shock.load_and_convert(args.scenario, args.year)

    logger.info("Shock factors: %s", factors)
    result = cge_solve(params, shock_factors=factors, solver=args.solver)

    print(f"\n{args.scenario} {args.year}: status = {result['status']}")
    print(f"GDP change: {result['gdp_change']*100:+.4f}%")
    for s, v in result['output_change'].items():
        print(f"  {s} output: {v*100:+.4f}%")
    return result


def cmd_sweep(args):
    """Run across years and export to CSV."""
    sam = build_demo_sam(total_gdp=args.gdp)
    params = calibrate(sam, Elasticities())

    ra = ResultsAnalyzer()
    years = list(range(args.start, args.end + 1, args.step))

    for year in years:
        if args.synthetic:
            factors = ProductivityShock.synthetic_shock(
                args.scenario, year,
                base_loss_2050=args.base_loss,
            )
        else:
            shock = ProductivityShock(Path(args.data_dir))
            factors = shock.load_and_convert(args.scenario, year)

        result = cge_solve(params, shock_factors=factors, solver=args.solver)
        label = f"{args.scenario}_{year}"
        ra.add(label, result)
        print(f"  {year}: GDP {result['gdp_change']*100:+.4f}%")

    output = Path(args.output)
    ra.to_csv(output)
    print(f"\nResults saved to {output}")


def main():
    parser = argparse.ArgumentParser(
        description='CGE model for climate-economy analysis',
    )
    sub = parser.add_subparsers(dest='command')

    # baseline
    p_base = sub.add_parser('baseline', help='Replication test (no shock)')
    p_base.add_argument('--gdp', type=float, default=100.0)
    p_base.add_argument('--solver', default='ipopt')

    # simulate
    p_sim = sub.add_parser('simulate', help='Single scenario-year run')
    p_sim.add_argument('--scenario', default='SSP585')
    p_sim.add_argument('--year', type=int, default=2050)
    p_sim.add_argument('--gdp', type=float, default=100.0)
    p_sim.add_argument('--solver', default='ipopt')
    p_sim.add_argument('--synthetic', action='store_true',
                       help='Use synthetic shock instead of pipeline data')
    p_sim.add_argument('--base-loss', type=float, default=0.03,
                       help='Base productivity loss at 2050 (fraction)')
    p_sim.add_argument('--data-dir', default='data/weighted_productivity_loss_output')

    # sweep
    p_sweep = sub.add_parser('sweep', help='Run across years → CSV')
    p_sweep.add_argument('--scenario', default='SSP585')
    p_sweep.add_argument('--start', type=int, default=2020)
    p_sweep.add_argument('--end', type=int, default=2100)
    p_sweep.add_argument('--step', type=int, default=10)
    p_sweep.add_argument('--gdp', type=float, default=100.0)
    p_sweep.add_argument('--solver', default='ipopt')
    p_sweep.add_argument('--synthetic', action='store_true')
    p_sweep.add_argument('--base-loss', type=float, default=0.03)
    p_sweep.add_argument('--data-dir', default='data/weighted_productivity_loss_output')
    p_sweep.add_argument('--output', default='cge_results.csv')

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        'baseline': cmd_baseline,
        'simulate': cmd_simulate,
        'sweep': cmd_sweep,
    }
    commands[args.command](args)


if __name__ == '__main__':
    main()

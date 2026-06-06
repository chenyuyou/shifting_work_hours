"""CLI entry point for the CGE model.

Usage::

    python scripts/run_cge.py baseline \\
        --config configs/china_lancet_2025.json
    python scripts/run_cge.py simulate \\
        --config configs/china_lancet_2025.json \\
        --scenario SSP585 --year 2050 --synthetic
    python scripts/run_cge.py sweep \\
        --config configs/china_lancet_2025.json \\
        --scenario SSP585 --start 2020 --end 2100 --step 10
    python scripts/run_cge.py default   # no config, use defaults
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure the project root is on the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from shifting_work_hours.cge import (
    CGEConfig, build_from_config, calibrate, cge_solve,
    ProductivityShock, ResultsAnalyzer,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


def _load_config(args) -> CGEConfig:
    """Load config from file or use defaults."""
    if hasattr(args, 'config') and args.config:
        return CGEConfig.from_json(Path(args.config))
    return CGEConfig.default()


def cmd_baseline(args):
    """Replication test: solve without shocks."""
    config = _load_config(args)
    logger.info("Config: %s", config.name)
    sam = build_from_config(config)
    params = calibrate(sam, config.elasticities)

    result = cge_solve(params, shock_factors=None)

    print(f"\nBaseline: status = {result['status']}")
    print(f"GDP change: {result['gdp_change']*100:+.6f}%")
    for s, v in result['output_change'].items():
        print(f"  {s} output: {v*100:+.6f}%")
    return result


def cmd_simulate(args):
    """Run a single scenario-year simulation."""
    config = _load_config(args)
    sam = build_from_config(config)
    params = calibrate(sam, config.elasticities)

    if args.synthetic:
        factors = ProductivityShock.synthetic_shock(
            args.scenario, args.year,
            base_loss_2050=args.base_loss,
        )
    else:
        shock = ProductivityShock(Path(args.data_dir))
        factors = shock.load_and_convert(args.scenario, args.year)

    # Apply config's shock mapping
    if config.shock_mapping.loss_multiplier:
        # Override synthetic shock with config's sector mapping
        national_loss = 1.0 - min(factors.values())
        factors = config.shock_mapping.to_efficiency(national_loss)

    result = cge_solve(params, shock_factors=factors)

    print(f"\n{args.scenario} {args.year}: status = {result['status']}")
    print(f"GDP change: {result['gdp_change']*100:+.4f}%")
    for s, v in result['output_change'].items():
        print(f"  {s} output: {v*100:+.4f}%")
    return result


def cmd_sweep(args):
    """Run across years and export to CSV."""
    config = _load_config(args)
    sam = build_from_config(config)
    params = calibrate(sam, config.elasticities)

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

        if config.shock_mapping.loss_multiplier:
            national_loss = 1.0 - min(factors.values())
            factors = config.shock_mapping.to_efficiency(national_loss)

        result = cge_solve(params, shock_factors=factors)
        label = f"{args.scenario}_{year}"
        ra.add(label, result)
        print(f"  {year}: GDP {result['gdp_change']*100:+.4f}%")

    output = Path(args.output)
    ra.to_csv(output)
    print(f"\nResults saved to {output}")


def cmd_default(args):
    """Run with default config (no file needed)."""
    args.config = None
    cmd_baseline(args)


def main():
    parser = argparse.ArgumentParser(
        description='CGE model for climate-economy analysis',
    )
    sub = parser.add_subparsers(dest='command')

    # Common --config argument
    def add_config_arg(p):
        p.add_argument('--config', type=str, default=None,
                       help='Path to CGE config JSON file')

    # baseline
    p_base = sub.add_parser('baseline', help='Replication test (no shock)')
    add_config_arg(p_base)

    # simulate
    p_sim = sub.add_parser('simulate', help='Single scenario-year run')
    add_config_arg(p_sim)
    p_sim.add_argument('--scenario', default='SSP585')
    p_sim.add_argument('--year', type=int, default=2050)
    p_sim.add_argument('--synthetic', action='store_true')
    p_sim.add_argument('--base-loss', type=float, default=0.03)
    p_sim.add_argument('--data-dir', default='data/weighted_productivity_loss_output')

    # sweep
    p_sweep = sub.add_parser('sweep', help='Run across years → CSV')
    add_config_arg(p_sweep)
    p_sweep.add_argument('--scenario', default='SSP585')
    p_sweep.add_argument('--start', type=int, default=2020)
    p_sweep.add_argument('--end', type=int, default=2100)
    p_sweep.add_argument('--step', type=int, default=10)
    p_sweep.add_argument('--synthetic', action='store_true')
    p_sweep.add_argument('--base-loss', type=float, default=0.03)
    p_sweep.add_argument('--data-dir', default='data/weighted_productivity_loss_output')
    p_sweep.add_argument('--output', default='cge_results.csv')

    # default (no config)
    p_default = sub.add_parser('default', help='Run with default config')
    add_config_arg(p_default)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        'baseline': cmd_baseline,
        'simulate': cmd_simulate,
        'sweep': cmd_sweep,
        'default': cmd_default,
    }
    commands[args.command](args)


if __name__ == '__main__':
    main()

"""CLI entry point for the climate data processing pipeline.

Usage:
    python scripts/run_pipeline.py <command> [options]

Commands:
    extract         Stage 3: Extract China region from global data
    wbgt-indoor     Stage 4: Calculate indoor WBGT
    wbgt-outdoor    Stage 5: Calculate outdoor WBGT
    productivity    Stage 6: Calculate productivity loss
    analysis        Stage 7: Run final analysis
    all             Run all stages in sequence

Examples:
    # Run indoor WBGT calculation with 4 threads
    python scripts/run_pipeline.py wbgt-indoor --threads 4

    # Run full pipeline
    python scripts/run_pipeline.py all

    # Run with custom data directory
    SHIFTING_WH_DATA_DIR=/path/to/data python scripts/run_pipeline.py wbgt-indoor
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    DOWNLOADED_DATA_DIR, CHINA_OUTPUT_DIR, MODEL_OUTPUTS_DIR,
    WBGT_INDOOR_OUTPUT_DIR, WBGT_OUTDOOR_OUTPUT_DIR,
    PRODUCTIVITY_OUTPUT_DIR, RESULTS_DIR,
    EXTRACT_STATUS_FILE, WBGT_INDOOR_STATUS_FILE,
    WBGT_OUTDOOR_STATUS_FILE, PRODUCTIVITY_STATUS_FILE,
    POPULATION_FILES, NUM_THREADS, LOG_LEVEL, LOG_FORMAT
)

# Configure logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def cmd_extract(args):
    """Stage 3: Extract China region from global data."""
    from src.shifting_work_hours.pipeline import extractor
    logger.info("Starting extraction...")
    extractor.run(
        input_dir=DOWNLOADED_DATA_DIR,
        output_dir=CHINA_OUTPUT_DIR,
        status_file=EXTRACT_STATUS_FILE,
        num_threads=args.threads,
    )


def cmd_wbgt_indoor(args):
    """Stage 4: Calculate indoor WBGT."""
    from src.shifting_work_hours.pipeline import wbgt_indoor
    logger.info("Starting indoor WBGT calculation...")
    wbgt_indoor.run(
        input_dir=CHINA_OUTPUT_DIR,
        output_dir=WBGT_INDOOR_OUTPUT_DIR,
        status_file=WBGT_INDOOR_STATUS_FILE,
        num_threads=args.threads,
    )


def cmd_wbgt_outdoor(args):
    """Stage 5: Calculate outdoor WBGT."""
    from src.shifting_work_hours.pipeline import wbgt_outdoor
    logger.info("Starting outdoor WBGT calculation...")
    wbgt_outdoor.run(
        input_dir=CHINA_OUTPUT_DIR,
        output_dir=WBGT_OUTDOOR_OUTPUT_DIR,
        status_file=WBGT_OUTDOOR_STATUS_FILE,
        num_threads=args.threads,
    )


def cmd_productivity(args):
    """Stage 6: Calculate productivity loss."""
    from src.shifting_work_hours.pipeline import productivity
    logger.info("Starting productivity loss calculation...")

    # Use population file for the specified scenario
    population_file = POPULATION_FILES.get(args.scenario)
    if not population_file:
        logger.error(f"Unknown scenario: {args.scenario}")
        sys.exit(1)

    productivity.run(
        base_dir=MODEL_OUTPUTS_DIR,
        output_dir=PRODUCTIVITY_OUTPUT_DIR,
        status_file=PRODUCTIVITY_STATUS_FILE,
        population_file=population_file,
        num_threads=args.threads,
    )


def cmd_analysis(args):
    """Stage 7: Run final analysis."""
    from src.shifting_work_hours.pipeline import analysis
    logger.info("Starting final analysis...")
    analysis.run(
        input_dir=PRODUCTIVITY_OUTPUT_DIR,
        output_dir=RESULTS_DIR,
    )


def cmd_all(args):
    """Run all stages in sequence."""
    logger.info("Running full pipeline...")

    # Stage 3: Extract
    cmd_extract(args)

    # Stage 4: Indoor WBGT
    cmd_wbgt_indoor(args)

    # Stage 5: Outdoor WBGT
    cmd_wbgt_outdoor(args)

    # Stage 6: Productivity loss (run for each scenario)
    for scenario in ['SSP126', 'SSP245', 'SSP585']:
        args.scenario = scenario
        cmd_productivity(args)

    # Stage 7: Analysis
    cmd_analysis(args)

    logger.info("Pipeline complete!")


def main():
    parser = argparse.ArgumentParser(
        description='Climate data processing pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest='command', help='Pipeline stage')

    # Extract
    extract_parser = subparsers.add_parser(
        'extract', help='Stage 3: Extract China region'
    )
    extract_parser.add_argument(
        '--threads', type=int, default=NUM_THREADS,
        help=f'Number of threads (default: {NUM_THREADS})'
    )

    # WBGT Indoor
    indoor_parser = subparsers.add_parser(
        'wbgt-indoor', help='Stage 4: Calculate indoor WBGT'
    )
    indoor_parser.add_argument(
        '--threads', type=int, default=NUM_THREADS,
        help=f'Number of threads (default: {NUM_THREADS})'
    )

    # WBGT Outdoor
    outdoor_parser = subparsers.add_parser(
        'wbgt-outdoor', help='Stage 5: Calculate outdoor WBGT'
    )
    outdoor_parser.add_argument(
        '--threads', type=int, default=NUM_THREADS,
        help=f'Number of threads (default: {NUM_THREADS})'
    )

    # Productivity
    prod_parser = subparsers.add_parser(
        'productivity', help='Stage 6: Calculate productivity loss'
    )
    prod_parser.add_argument(
        '--threads', type=int, default=NUM_THREADS,
        help=f'Number of threads (default: {NUM_THREADS})'
    )
    prod_parser.add_argument(
        '--scenario', type=str, default='SSP245',
        choices=['SSP126', 'SSP245', 'SSP585'],
        help='SSP scenario (default: SSP245)'
    )

    # Analysis
    analysis_parser = subparsers.add_parser(
        'analysis', help='Stage 7: Run final analysis'
    )

    # All
    all_parser = subparsers.add_parser(
        'all', help='Run all stages in sequence'
    )
    all_parser.add_argument(
        '--threads', type=int, default=NUM_THREADS,
        help=f'Number of threads (default: {NUM_THREADS})'
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Dispatch to command handler
    commands = {
        'extract': cmd_extract,
        'wbgt-indoor': cmd_wbgt_indoor,
        'wbgt-outdoor': cmd_wbgt_outdoor,
        'productivity': cmd_productivity,
        'analysis': cmd_analysis,
        'all': cmd_all,
    }

    try:
        commands[args.command](args)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

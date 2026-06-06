"""Outdoor Wet Bulb Globe Temperature (WBGT) calculation.

This module calculates outdoor WBGT using the Liljegren model, which accounts
for solar radiation, wind speed, and globe temperature through iterative convergence.

The core calculation is performed by the liljegren_cuda_vectorized_c module,
which is a CuPy port of the C implementation from:
    https://github.com/mdljts/wbgt/blob/master/src/wbgt.c

WARNING: This calculation is extremely slow without CUDA acceleration.
"""

import cupy as cp
import numpy as np
import xarray as xr
import logging
from pathlib import Path

from config.constants import (
    MODELS, SCENARIOS, YEAR_START, YEAR_END, ENSEMBLE_MEMBER
)
from src.shifting_work_hours.core.runner import TaskRunner
from src.shifting_work_hours.core.status import StatusTracker
from src.shifting_work_hours.core.io import save_dataset
from src.shifting_work_hours.utils.file_discovery import find_nc_file, get_model_scenario_dir

# Import the Liljegren calculation kernel
# This is kept as a separate module due to its complexity
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from liljegren_cuda_vectorized_c import wbgt_liljegren_vectorized

logger = logging.getLogger(__name__)


def preprocess_data(ds: xr.Dataset) -> xr.Dataset:
    """Preprocess the dataset to handle NaN values and invalid data.

    Args:
        ds: Input dataset with climate variables

    Returns:
        Preprocessed dataset
    """
    for var in ds.data_vars:
        # Replace inf with NaN
        ds[var] = xr.where(np.isinf(ds[var]), np.nan, ds[var])

        # Clamp wind speed to non-negative
        if var == 'sfcWind':
            ds[var] = xr.where(ds[var] < 0, 0, ds[var])

        # Clamp solar radiation to non-negative
        if var == 'rsds':
            ds[var] = xr.where(ds[var] < 0, 0, ds[var])

        # Validate temperature range (180K to 330K)
        if var in ['tas', 'tasmax']:
            ds[var] = xr.where((ds[var] < 180) | (ds[var] > 330), np.nan, ds[var])

        # Validate humidity range (0-100%)
        if var == 'hurs':
            ds[var] = xr.where((ds[var] < 0) | (ds[var] > 100), np.nan, ds[var])
            ds[var] = xr.where(ds[var] > 100, 100, ds[var])

    return ds


def process_year(model: str, scenario: str, year: int,
                 input_dir: Path, output_dir: Path) -> dict:
    """Process outdoor WBGT for one model/scenario/year.

    Args:
        model: Model name (e.g., 'EC-Earth3')
        scenario: Scenario name (e.g., 'SSP126')
        year: Year to process
        input_dir: Base input directory (china_output)
        output_dir: Base output directory

    Returns:
        Dict with 'status', 'year', and optionally 'output' or 'error'
    """
    try:
        # Get input directory
        model_scenario_dir = get_model_scenario_dir(input_dir, model, scenario)

        # Required variables for outdoor WBGT
        variables = ['tas', 'tasmax', 'hurs', 'sfcWind', 'rsds']

        # Find and open all input files
        datasets = {}
        for var in variables:
            file = find_nc_file(model_scenario_dir / var, var, year)
            if not file:
                return {
                    'status': 'error',
                    'year': year,
                    'error': f'Missing input file for {var}',
                }
            datasets[var] = xr.open_dataset(file)

        # Merge all datasets
        # Use join='override' to avoid silent data loss from inner join
        combined_ds = xr.merge(datasets.values(), join='override')

        # Preprocess the data
        combined_ds = preprocess_data(combined_ds)

        # Log preprocessing stats
        for var in combined_ds.data_vars:
            data = combined_ds[var].values
            logger.debug(
                f"Preprocessed {var}: min={np.nanmin(data):.2f}, "
                f"max={np.nanmax(data):.2f}, nan_count={np.isnan(data).sum()}"
            )

        # Calculate outdoor WBGT using Liljegren model
        logger.info(f"Calculating outdoor WBGT for {model}/{scenario}/{year}")
        wbgt_min, wbgt_max, t_nwb, t_g = wbgt_liljegren_vectorized(combined_ds)

        # Create output dataset
        wbgt_ds = xr.Dataset(
            {
                'WBGTmin_od': (['time', 'lat', 'lon'], wbgt_min),
                'WBGTmax_od': (['time', 'lat', 'lon'], wbgt_max),
                'WBGThalf_od': (['time', 'lat', 'lon'], (wbgt_min + wbgt_max) / 2),
            },
            coords={
                'time': combined_ds.time,
                'lat': combined_ds.lat,
                'lon': combined_ds.lon,
            }
        )

        # Add attributes
        for var in wbgt_ds.data_vars:
            wbgt_ds[var].attrs['units'] = 'degC'
            wbgt_ds[var].attrs['long_name'] = f'Outdoor Wet Bulb Globe Temperature ({var})'

        # Save
        out_dir = get_model_scenario_dir(output_dir, model, scenario)
        save_dataset(wbgt_ds, out_dir, f"outdoor_wbgt_day_{year}.nc")

        # Close input datasets
        for ds in datasets.values():
            ds.close()

        return {'status': 'success', 'year': year, 'output': str(out_dir)}

    except Exception as e:
        logger.error(f"Error processing {model}/{scenario}/{year}: {e}")
        return {'status': 'error', 'year': year, 'error': str(e)}


def run(input_dir: Path, output_dir: Path, status_file: Path,
        num_threads: int = 1) -> dict:
    """Run outdoor WBGT calculation for all models/scenarios/years.

    Note: Default num_threads=1 because outdoor WBGT is GPU-intensive
    and running multiple threads may cause GPU memory issues.

    Args:
        input_dir: Base input directory (china_output)
        output_dir: Base output directory
        status_file: Path to status JSON file
        num_threads: Number of worker threads (default: 1)

    Returns:
        Dict mapping status_key -> result
    """
    tracker = StatusTracker(status_file)
    runner = TaskRunner(tracker, num_threads)

    # Generate tasks
    def generate_tasks():
        for model in MODELS:
            for scenario in SCENARIOS:
                for year in range(YEAR_START, YEAR_END + 1):
                    yield (model, scenario, year, input_dir, output_dir)

    # Run
    results = runner.run(
        generate_tasks(),
        lambda *args: process_year(args[0], args[1], args[2], args[3], args[4]),
        desc="Outdoor WBGT"
    )

    # Report
    stats = tracker.get_stats()
    logger.info(f"Outdoor WBGT complete: {stats['success']} successful, {stats['failed']} failed")

    return results

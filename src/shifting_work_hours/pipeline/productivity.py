"""Population-weighted labor productivity loss calculation.

This module combines indoor/outdoor WBGT with population data to calculate
population-weighted labor productivity loss under three work intensity levels
(low, medium, high).

The productivity loss is calculated using empirical curves from:
    Kjellstrom, T., et al. (2018). Heat and human performance.
    Annual Review of Public Health, 39, 97-115.

Formula: productivity_factor = 0.9 - 0.9 / (1 + (WBGT / threshold)^exponent)
"""

import cupy as cp
import xarray as xr
import numpy as np
import pandas as pd
import logging
import warnings
from pathlib import Path

from config.constants import (
    MODELS, SCENARIOS, YEAR_START, YEAR_END, ENSEMBLE_MEMBER,
    INTENSITIES, PRODUCTIVITY_PARAMS
)
from src.shifting_work_hours.core.runner import TaskRunner
from src.shifting_work_hours.core.status import StatusTracker
from src.shifting_work_hours.core.io import read_dataset, save_dataset
from src.shifting_work_hours.utils.file_discovery import find_matching_file, get_model_scenario_dir

# Suppress RuntimeWarning for mean of empty slice
warnings.filterwarnings("ignore", category=RuntimeWarning, message="Mean of empty slice")

logger = logging.getLogger(__name__)


def calculate_productivity_factor(wbgt: cp.ndarray, intensity: str) -> cp.ndarray:
    """Calculate productivity factor from WBGT using empirical curves.

    Args:
        wbgt: Wet Bulb Globe Temperature in Celsius
        intensity: Work intensity level ('low', 'medium', 'high')

    Returns:
        Productivity factor (0-1)
    """
    # Clamp WBGT to non-negative values to avoid NaN from negative bases
    wbgt = cp.maximum(wbgt, 0)

    params = PRODUCTIVITY_PARAMS[intensity]
    threshold = params['threshold']
    exponent = params['exponent']
    return 0.9 - 0.9 / (1 + (wbgt / threshold) ** exponent)


def find_indoor_file(base_path: Path, model: str, scenario: str,
                     year: int) -> Path | None:
    """Find indoor WBGT file for given model/scenario/year.

    Args:
        base_path: Base directory containing indoor WBGT outputs
        model: Model name
        scenario: Scenario name
        year: Year to find

    Returns:
        Path to file, or None if not found
    """
    # Match the actual output format: wbgt_indoor_day_{year}.nc
    file_path = (
        base_path / 'wbgt_indoor_output' / model / scenario /
        ENSEMBLE_MEMBER / f"wbgt_indoor_day_{year}.nc"
    )
    if file_path.exists():
        return file_path

    # Fallback: search with glob
    pattern = f"wbgt_indoor_day_{year}.nc"
    matches = list(base_path.rglob(pattern))
    for match in matches:
        if model in str(match) and scenario in str(match):
            return match
    return None


def find_outdoor_file(base_path: Path, model: str, scenario: str,
                      year: int) -> Path | None:
    """Find outdoor WBGT file for given model/scenario/year.

    Args:
        base_path: Base directory containing model outputs
        model: Model name
        scenario: Scenario name
        year: Year to find

    Returns:
        Path to file, or None if not found
    """
    # Match the actual output format: outdoor_wbgt_day_{year}.nc
    file_path = (
        base_path / 'wbgt_outdoor_output' / model / scenario /
        ENSEMBLE_MEMBER / f"outdoor_wbgt_day_{year}.nc"
    )
    if file_path.exists():
        return file_path

    # Fallback: search with glob
    pattern = f"outdoor_wbgt_day_{year}.nc"
    matches = list(base_path.rglob(pattern))
    for match in matches:
        if model in str(match) and scenario in str(match):
            return match
    return None


def process_year(model: str, scenario: str, year: int,
                 base_dir: Path) -> dict:
    """Process productivity loss for one model/scenario/year.

    Args:
        model: Model name
        scenario: Scenario name
        year: Year to process
        base_dir: Base directory containing model outputs

    Returns:
        Dict with 'status' and optionally 'data' or 'error'
    """
    try:
        # Find input files
        indoor_file = find_indoor_file(base_dir, model, scenario, year)
        if not indoor_file:
            return {'status': 'error', 'error': f"No indoor file found for year {year}"}

        outdoor_file = find_outdoor_file(base_dir, model, scenario, year)
        if not outdoor_file:
            return {'status': 'error', 'error': f"No outdoor file found for year {year}"}

        # Read datasets
        indoor_ds = read_dataset(indoor_file)
        outdoor_ds = read_dataset(outdoor_file)

        # Validate coordinate alignment between indoor and outdoor
        if not (indoor_ds.lat.shape == outdoor_ds.lat.shape):
            raise ValueError(
                f"Latitude dimensions don't match: indoor={indoor_ds.lat.shape}, "
                f"outdoor={outdoor_ds.lat.shape}"
            )
        if not (indoor_ds.lon.shape == outdoor_ds.lon.shape):
            raise ValueError(
                f"Longitude dimensions don't match: indoor={indoor_ds.lon.shape}, "
                f"outdoor={outdoor_ds.lon.shape}"
            )

        # Calculate productivity loss for each intensity and metric
        losses = {}
        for intensity in INTENSITIES:
            # Select appropriate dataset and variables
            if intensity in ['low', 'medium']:
                ds = indoor_ds
                wbgt_vars = ['WBGTmax_id', 'WBGTmin_id', 'WBGThalf_id']
            else:  # high intensity
                ds = outdoor_ds
                wbgt_vars = ['WBGTmax_od', 'WBGTmin_od', 'WBGThalf_od']

            losses[intensity] = {}
            for metric, var in zip(['max', 'min', 'half'], wbgt_vars):
                # Get the DataArray
                da = ds[var]

                # Find time dimension
                time_dims = [d for d in da.dims if d.lower() in ['time', 'day', 'date']]
                if not time_dims:
                    raise ValueError(
                        f"No time dimension found in {var}. "
                        f"Available dimensions: {da.dims}"
                    )
                time_dim = time_dims[0]
                time_axis = da.dims.index(time_dim)

                # Transfer to GPU and calculate
                wbgt_gpu = cp.asarray(da.values)
                result_gpu = calculate_productivity_factor(wbgt_gpu, intensity)
                result_mean_gpu = cp.nanmean(result_gpu, axis=time_axis)

                # Transfer back to CPU
                losses[intensity][metric] = cp.asnumpy(result_mean_gpu)

        # Create output dataset
        combined_loss = xr.Dataset(
            {
                f"{intensity}_{metric}": (('lat', 'lon'), losses[intensity][metric])
                for intensity in INTENSITIES
                for metric in ['max', 'min', 'half']
            }
        )

        # Add coordinates (use indoor_ds as reference since both should match)
        time = pd.to_datetime(f"{year}", format="%Y")
        combined_loss = combined_loss.assign_coords(
            time=time,
            lat=indoor_ds.lat,
            lon=indoor_ds.lon,
        )

        # Close input datasets
        indoor_ds.close()
        outdoor_ds.close()

        return {'status': 'success', 'data': combined_loss}

    except Exception as e:
        logger.error(f"Error processing {model}/{scenario}/{year}: {e}")
        return {'status': 'error', 'error': str(e)}

    finally:
        # Always free GPU memory
        cp.get_default_memory_pool().free_all_blocks()


def process_loss_with_population(loss_data: xr.Dataset,
                                 population_file: Path) -> xr.Dataset:
    """Multiply productivity loss by population data.

    Args:
        loss_data: Dataset with productivity loss variables
        population_file: Path to population NetCDF file

    Returns:
        Dataset with population-weighted loss and population data
    """
    # Read population data
    pop_ds = read_dataset(population_file)

    # Ensure same coordinate system (0-360 longitude)
    loss_data = loss_data.assign_coords(lon=(loss_data.lon % 360))
    pop_ds = pop_ds.assign_coords(lon=(pop_ds.lon % 360))

    # Rename 'StdTime' to 'time' for consistency
    if 'StdTime' in pop_ds.dims:
        pop_ds = pop_ds.rename({'StdTime': 'time'})

    # Interpolate population data to match climate data resolution
    # This is better than np.intersect1d() which causes data loss
    logger.info(f"Interpolating population data to match climate data resolution")
    logger.info(f"  Climate data: {len(loss_data.lat)} lat x {len(loss_data.lon)} lon")
    logger.info(f"  Population data: {len(pop_ds.lat)} lat x {len(pop_ds.lon)} lon")

    # Interpolate population to climate data coordinates
    pop_ds = pop_ds.interp(
        lat=loss_data.lat,
        lon=loss_data.lon,
        method='linear'  # Linear interpolation
    )

    # Align time coordinates
    # Use nearest neighbor matching for temporal alignment
    # This handles cases where timestamps don't exactly match
    if len(pop_ds.time) > 0 and len(loss_data.time) > 0:
        # Find nearest population time for each loss data time
        pop_times = pop_ds.time.values
        loss_times = loss_data.time.values

        # Use searchsorted to find nearest matches
        indices = np.searchsorted(pop_times, loss_times)
        indices = np.clip(indices, 0, len(pop_times) - 1)

        # Select matching population data
        pop_ds = pop_ds.isel(time=indices)
        pop_ds = pop_ds.assign_coords(time=loss_times)
    else:
        logger.warning("No time coordinates found in population or loss data")

    # Extract population data
    population = pop_ds['pop'].values

    # Calculate weighted loss for each variable
    weighted_results = {}
    for var in loss_data.data_vars:
        loss_var = loss_data[var].values

        # Transfer to GPU
        loss_gpu = cp.array(loss_var)
        pop_gpu = cp.array(population)

        # Replace NaN with 0 in population
        pop_gpu = cp.nan_to_num(pop_gpu, 0)

        # Broadcast population to match loss shape
        pop_gpu = cp.broadcast_to(pop_gpu, loss_gpu.shape)

        # Multiply loss by population
        weighted_loss_gpu = loss_gpu * pop_gpu
        weighted_loss_gpu = cp.nan_to_num(weighted_loss_gpu, 0)

        # Transfer back to CPU
        weighted_results[var] = cp.asnumpy(weighted_loss_gpu)

    # Create output dataset
    weighted_ds = xr.Dataset(
        {var: (('time', 'lat', 'lon'), weighted_results[var])
         for var in loss_data.data_vars},
        coords={'time': loss_times, 'lat': loss_data.lat, 'lon': loss_data.lon}
    )

    # Add population data
    weighted_ds['population'] = (('time', 'lat', 'lon'), population)

    # Free GPU memory
    cp.get_default_memory_pool().free_all_blocks()

    return weighted_ds


def run(base_dir: Path, output_dir: Path, status_file: Path,
        population_file: Path, num_threads: int = 4) -> dict:
    """Run productivity loss calculation for all models/scenarios/years.

    Args:
        base_dir: Base directory containing model outputs
        output_dir: Output directory for weighted results
        status_file: Path to status JSON file
        population_file: Path to population NetCDF file
        num_threads: Number of worker threads

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
                    yield (model, scenario, year, base_dir)

    # Run
    results = runner.run(
        generate_tasks(),
        lambda *args: process_year(args[0], args[1], args[2], args[3]),
        desc="Productivity Loss"
    )

    # Post-process: aggregate by model/scenario and apply population weighting
    output_dir.mkdir(parents=True, exist_ok=True)

    for model in MODELS:
        for scenario in SCENARIOS:
            # Collect all years for this model/scenario
            all_years_data = []
            for year in range(YEAR_START, YEAR_END + 1):
                status_key = f"{model}_{scenario}_{year}"
                if status_key in results and results[status_key].get('status') == 'success':
                    if 'data' in results[status_key]:
                        all_years_data.append(results[status_key]['data'])

            if all_years_data:
                # Concatenate all years
                combined = xr.concat(all_years_data, dim='time')

                # Apply population weighting
                weighted_result = process_loss_with_population(combined, population_file)

                # Save
                output_file = output_dir / f'weighted_productivity_loss_{model}_{scenario}.nc'
                weighted_result.to_netcdf(output_file)
                logger.info(f"Saved weighted results to {output_file}")
            else:
                logger.warning(f"No valid data for {model}/{scenario}")

    # Report
    stats = tracker.get_stats()
    logger.info(f"Productivity loss complete: {stats['success']} successful, {stats['failed']} failed")

    return results

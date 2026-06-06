"""Indoor Wet Bulb Globe Temperature (WBGT) calculation.

This module calculates indoor WBGT using the formula:
    WBGT = 0.7 * WBT + 0.3 * T

Where WBT (wet bulb temperature) is estimated from air temperature
and relative humidity using the Stull (2011) empirical formula.

Reference:
    Stull, R. (2011). Wet-bulb temperature from relative humidity and
    air temperature. Journal of Applied Meteorology and Climatology, 50(11), 2267-2269.
"""

import cupy as cp
import logging
from pathlib import Path

from config.constants import (
    MODELS, SCENARIOS, YEAR_START, YEAR_END, ENSEMBLE_MEMBER, KELVIN_OFFSET
)
from src.shifting_work_hours.core.runner import TaskRunner
from src.shifting_work_hours.core.status import StatusTracker
from src.shifting_work_hours.core.io import read_variable, create_output_dataset, save_dataset
from src.shifting_work_hours.utils.file_discovery import find_nc_file, get_model_scenario_dir

logger = logging.getLogger(__name__)


def calculate_wet_bulb_temperature(tas: cp.ndarray, hurs: cp.ndarray) -> cp.ndarray:
    """Calculate wet bulb temperature from air temp and humidity.

    Uses empirical formula from Stull (2011).

    Args:
        tas: Air temperature in Celsius
        hurs: Relative humidity in percent (0-100)

    Returns:
        Wet bulb temperature in Celsius
    """
    return (
        tas * cp.arctan(0.151977 * cp.sqrt(hurs + 8.313659))
        + cp.arctan(tas + hurs)
        - cp.arctan(hurs - 1.676331)
        + 0.00391838 * cp.power(hurs, 1.5) * cp.arctan(0.023101 * hurs)
        - 4.686035
    )


def calculate_indoor_wbgt(tas: cp.ndarray, tasmax: cp.ndarray,
                          hurs: cp.ndarray) -> dict[str, cp.ndarray]:
    """Calculate indoor WBGT min, max, and half values.

    Args:
        tas: Mean air temperature in Celsius
        tasmax: Maximum air temperature in Celsius
        hurs: Relative humidity in percent

    Returns:
        Dict with keys 'WBGTmin_id', 'WBGTmax_id', 'WBGThalf_id'
    """
    # Calculate wet bulb temperatures
    wbt_min = calculate_wet_bulb_temperature(tas, hurs)
    wbt_max = calculate_wet_bulb_temperature(tasmax, hurs)
    wbt_half = (wbt_min + wbt_max) / 2

    # Indoor WBGT = 0.7 * WBT + 0.3 * T
    return {
        'WBGTmin_id': 0.7 * wbt_min + 0.3 * tas,
        'WBGTmax_id': 0.7 * wbt_max + 0.3 * tasmax,
        'WBGThalf_id': 0.7 * wbt_half + 0.3 * (tas + tasmax) / 2,
    }


def process_year(model: str, scenario: str, year: int,
                 input_dir: Path, output_dir: Path) -> dict:
    """Process indoor WBGT for one model/scenario/year.

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

        # Find input files
        tas_file = find_nc_file(model_scenario_dir / 'tas', 'tas', year)
        tasmax_file = find_nc_file(model_scenario_dir / 'tasmax', 'tasmax', year)
        hurs_file = find_nc_file(model_scenario_dir / 'hurs', 'hurs', year)

        if not all([tas_file, tasmax_file, hurs_file]):
            missing = []
            if not tas_file:
                missing.append('tas')
            if not tasmax_file:
                missing.append('tasmax')
            if not hurs_file:
                missing.append('hurs')
            return {
                'status': 'error',
                'year': year,
                'error': f'Missing input files: {", ".join(missing)}',
            }

        # Read input data
        tas_ds, tas_data = read_variable(tas_file, 'tas', convert_kelvin=True)
        tasmax_ds, tasmax_data = read_variable(tasmax_file, 'tasmax', convert_kelvin=True)
        hurs_ds, hurs_data = read_variable(hurs_file, 'hurs')

        # Validate coordinate alignment
        if not (tas_ds.lat.shape == tasmax_ds.lat.shape == hurs_ds.lat.shape):
            raise ValueError("Latitude dimensions don't match across input files")
        if not (tas_ds.lon.shape == tasmax_ds.lon.shape == hurs_ds.lon.shape):
            raise ValueError("Longitude dimensions don't match across input files")
        if not (tas_ds.time.shape == tasmax_ds.time.shape == hurs_ds.time.shape):
            raise ValueError("Time dimensions don't match across input files")

        # Transfer to GPU
        tas_gpu = cp.asarray(tas_data)
        tasmax_gpu = cp.asarray(tasmax_data)
        hurs_gpu = cp.asarray(hurs_data)

        # Calculate
        results = calculate_indoor_wbgt(tas_gpu, tasmax_gpu, hurs_gpu)

        # Transfer back to CPU
        output_vars = {k: cp.asnumpy(v) for k, v in results.items()}

        # Create output dataset
        ds = create_output_dataset(
            data_vars={k: (['time', 'lat', 'lon'], v) for k, v in output_vars.items()},
            coords={'time': tas_ds.time, 'lat': tas_ds.lat, 'lon': tas_ds.lon},
            attributes={
                k: {'units': 'degC', 'long_name': f'Indoor WBGT ({k})'}
                for k in output_vars
            }
        )

        # Save
        out_dir = get_model_scenario_dir(output_dir, model, scenario)
        save_dataset(ds, out_dir, f"wbgt_indoor_day_{year}.nc")

        return {'status': 'success', 'year': year, 'output': str(out_dir)}

    except Exception as e:
        logger.error(f"Error processing {model}/{scenario}/{year}: {e}")
        return {'status': 'error', 'year': year, 'error': str(e)}

    finally:
        # Always free GPU memory
        cp.get_default_memory_pool().free_all_blocks()


def run(input_dir: Path, output_dir: Path, status_file: Path,
        num_threads: int = 4) -> dict:
    """Run indoor WBGT calculation for all models/scenarios/years.

    Args:
        input_dir: Base input directory (china_output)
        output_dir: Base output directory
        status_file: Path to status JSON file
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
                    yield (model, scenario, year, input_dir, output_dir)

    # Run
    results = runner.run(
        generate_tasks(),
        lambda *args: process_year(args[0], args[1], args[2], args[3], args[4]),
        desc="Indoor WBGT"
    )

    # Report
    stats = tracker.get_stats()
    logger.info(f"Indoor WBGT complete: {stats['success']} successful, {stats['failed']} failed")

    return results

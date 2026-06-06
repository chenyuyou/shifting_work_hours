"""Spatial extraction of climate data for China region.

This module clips global NetCDF files to China's bounding box
using xarray's sel() method with slice.
"""

import xarray as xr
import json
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from config.constants import MODELS, SCENARIOS, VARIABLES, ENSEMBLE_MEMBER
from config.settings import CHINA_BOUNDS_FILE

logger = logging.getLogger(__name__)


def load_china_bounds(bounds_file: Path = None) -> dict:
    """Load China bounding box from JSON file.

    Args:
        bounds_file: Path to bounds JSON file (default: from config)

    Returns:
        Dict with lat_min, lat_max, lon_min, lon_max
    """
    bounds_file = bounds_file or CHINA_BOUNDS_FILE
    with open(bounds_file, 'r') as f:
        return json.load(f)


def process_file(file_path: Path, china_bounds: dict,
                 output_file: Path) -> dict:
    """Extract China region from a single NetCDF file.

    Args:
        file_path: Path to input NetCDF file
        china_bounds: Dict with lat_min, lat_max, lon_min, lon_max
        output_file: Path to output file

    Returns:
        Dict with 'status', 'file', and optionally 'error'
    """
    try:
        with xr.open_dataset(file_path) as ds:
            # Select China region
            ds_china = ds.sel(
                lat=slice(china_bounds['lat_min'], china_bounds['lat_max']),
                lon=slice(china_bounds['lon_min'], china_bounds['lon_max'])
            )

            # Create output directory and save
            output_file.parent.mkdir(parents=True, exist_ok=True)
            ds_china.to_netcdf(output_file)

        return {'status': 'success', 'file': str(file_path)}

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return {'status': 'error', 'file': str(file_path), 'error': str(e)}


def get_file_list(base_dir: Path) -> list[Path]:
    """Get list of all NetCDF files to process.

    Args:
        base_dir: Base directory containing downloaded data

    Returns:
        List of file paths
    """
    file_list = []
    for model in MODELS:
        for scenario in SCENARIOS:
            for variable in VARIABLES:
                path = base_dir / model / scenario / ENSEMBLE_MEMBER / variable
                if path.exists():
                    file_list.extend(path.glob('*.nc'))
    return file_list


def run(input_dir: Path, output_dir: Path, status_file: Path,
        num_threads: int = 8, batch_size: int = 2) -> dict:
    """Run extraction for all files.

    Args:
        input_dir: Base input directory (downloaded_data)
        output_dir: Base output directory
        status_file: Path to status JSON file
        num_threads: Number of worker threads
        batch_size: Number of files to process in each batch

    Returns:
        Dict with 'successful' and 'failed' lists
    """
    from src.shifting_work_hours.core.status import StatusTracker

    tracker = StatusTracker(status_file)
    china_bounds = load_china_bounds()

    # Get list of files to process
    all_files = get_file_list(input_dir)
    logger.info(f"Found {len(all_files)} files to process")

    # Filter already processed files
    to_process = [f for f in all_files if not tracker.is_done(str(f))]
    logger.info(f"Processing {len(to_process)} files (skipping {len(all_files) - len(to_process)} already done)")

    if not to_process:
        logger.info("All files already processed")
        return {'successful': [], 'failed': []}

    results = []
    successful = []
    failed = []

    # Process files in parallel
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_file = {}

        # Submit all tasks
        for file_path in to_process:
            # Calculate output path maintaining directory structure
            rel_path = file_path.relative_to(input_dir)
            output_file = output_dir / rel_path

            future = executor.submit(process_file, file_path, china_bounds, output_file)
            future_to_file[future] = file_path

        # Collect results with progress bar
        for future in tqdm(as_completed(future_to_file), total=len(to_process),
                          desc="Extracting China region"):
            result = future.result()
            results.append(result)

            if result['status'] == 'success':
                successful.append(result)
                tracker.record(result['file'], {'status': 'success'})
            else:
                failed.append(result)
                tracker.record(result['file'], {'status': 'error', 'error': result.get('error')})

            # Save status periodically
            if len(results) % 10 == 0:
                tracker.save()

    # Final save
    tracker.save()

    # Report
    logger.info(f"Extraction complete: {len(successful)} successful, {len(failed)} failed")

    if failed:
        logger.error("Failed files:")
        for result in failed:
            logger.error(f"  {result['file']}: {result.get('error')}")

    return {'successful': successful, 'failed': failed}

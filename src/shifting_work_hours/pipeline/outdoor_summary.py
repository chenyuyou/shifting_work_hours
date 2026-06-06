"""Outdoor WBGT summary calculation.

This module calculates summer (June-August) average outdoor WBGT
for year 2100 by province across all models and scenarios.
"""

import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
import regionmask
import logging
from pathlib import Path

from config.constants import MODELS, SCENARIOS
from config.settings import OUTPUT_ENCODING

logger = logging.getLogger(__name__)


def create_province_masks(data: xr.Dataset,
                          province_geojson: Path) -> dict:
    """Create province masks for a dataset.

    Args:
        data: Dataset with lat/lon coordinates
        province_geojson: Path to province boundary GeoJSON

    Returns:
        Dict mapping province name -> mask
    """
    province_gdf = gpd.read_file(province_geojson)
    province_masks = {}

    for idx, row in province_gdf.iterrows():
        province_region = regionmask.Regions([row.geometry])
        province_mask = province_region.mask(data.lon, data.lat)
        province_masks[row['name']] = province_mask

    return province_masks


def calculate_province_average(data: xr.Dataset, mask: xr.DataArray) -> float:
    """Calculate spatial average for a province.

    Args:
        data: Data to average
        mask: Province mask (0 = inside province)

    Returns:
        Average value
    """
    masked = data.where(mask == 0)
    return float(masked.mean(dim=['lat', 'lon']).values)


def run(input_dir: Path, output_dir: Path,
        province_geojson: Path = None,
        target_year: int = 2100):
    """Calculate outdoor WBGT summary.

    Args:
        input_dir: Directory with outdoor WBGT files
        output_dir: Output directory for summary
        province_geojson: Path to province boundary GeoJSON
        target_year: Year to analyze (default: 2100)
    """
    if province_geojson is None:
        province_geojson = Path('./model_outputs/中华人民共和国-分省.json')

    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect results
    all_results = []

    for model in MODELS:
        for scenario in SCENARIOS:
            # Find file for target year
            file_pattern = f"*/outdoor_wbgt_day_{target_year}.nc"
            matching_files = list(input_dir.rglob(file_pattern))

            if not matching_files:
                logger.warning(f"No file found for {model}/{scenario}/{target_year}")
                continue

            # Try to find the specific model/scenario file
            file_path = None
            for f in matching_files:
                if model in str(f) and scenario in str(f):
                    file_path = f
                    break

            if file_path is None:
                logger.warning(f"No file found for {model}/{scenario}/{target_year}")
                continue

            logger.info(f"Processing {file_path}")

            # Read data
            ds = xr.open_dataset(file_path)

            # Filter to summer months (June-August)
            summer_months = [6, 7, 8]
            ds_summer = ds.sel(time=ds.time.dt.month.isin(summer_months))

            # Create province masks (using first file)
            if 'province_masks' not in locals():
                province_masks = create_province_masks(ds, province_geojson)

            # Calculate averages for each province
            for province, mask in province_masks.items():
                for var in ds_summer.data_vars:
                    avg_value = calculate_province_average(ds_summer[var], mask)
                    all_results.append({
                        'Model': model,
                        'Scenario': scenario,
                        'Province': province,
                        'Variable': var,
                        'Average_WBGT': avg_value,
                        'Year': target_year,
                        'Season': 'Summer (JJA)',
                    })

            ds.close()

    # Create DataFrame and save
    if all_results:
        df = pd.DataFrame(all_results)
        output_file = output_dir / f'outdoor_wbgt_{target_year}_summer_average_all_scenarios.xlsx'
        df.to_excel(output_file, index=False)
        logger.info(f"Saved summary to {output_file}")
    else:
        logger.warning("No results to save")

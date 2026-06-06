"""Final analysis with geographic aggregation.

This module performs the final analysis by:
1. Creating geographic masks for China and each province
2. Aggregating population-weighted productivity losses
3. Comparing unadjusted vs adjusted working hours
4. Calculating statistics across climate models
"""

import xarray as xr
import geopandas as gpd
import numpy as np
import pandas as pd
import regionmask
import logging
from pathlib import Path

from config.constants import MODELS, SCENARIOS, INTENSITIES, YEAR_START
from config.settings import OUTPUT_ENCODING

logger = logging.getLogger(__name__)


def create_masks(data: xr.Dataset, china_geojson: Path,
                 province_geojson: Path) -> tuple:
    """Create geographic masks for China and provinces.

    Args:
        data: Dataset with lat/lon coordinates
        china_geojson: Path to China boundary GeoJSON
        province_geojson: Path to province boundary GeoJSON

    Returns:
        Tuple of (china_mask, province_masks_dict, province_gdf)
    """
    # Create China mask
    china_gdf = gpd.read_file(china_geojson)
    china_region = regionmask.Regions([china_gdf.geometry.iloc[0]])
    china_mask = china_region.mask(data.lon, data.lat)

    # Create province masks
    province_gdf = gpd.read_file(province_geojson)
    province_masks = {}
    for idx, row in province_gdf.iterrows():
        province_region = regionmask.Regions([row.geometry])
        province_mask = province_region.mask(data.lon, data.lat)
        province_masks[row['name']] = province_mask

    return china_mask, province_masks, province_gdf


def mask_and_aggregate(data: xr.Dataset, mask: xr.DataArray,
                       population: xr.DataArray) -> tuple:
    """Apply mask and aggregate data.

    Args:
        data: Dataset with productivity loss data
        mask: Geographic mask (0 = inside region)
        population: Population data

    Returns:
        Tuple of (aggregated_data, total_population)
    """
    masked_data = data.where(mask == 0)
    masked_population = population.where(mask == 0)

    total_data = masked_data.sum(dim=['lat', 'lon'])
    total_population = masked_population.sum(dim=['lat', 'lon'])

    return total_data, total_population


def process_file(file_path: Path, china_mask: xr.DataArray,
                 province_masks: dict) -> dict:
    """Process a single NetCDF file.

    Args:
        file_path: Path to NetCDF file
        china_mask: China geographic mask
        province_masks: Province masks dict

    Returns:
        Dict with processed data for China and provinces
    """
    with xr.open_dataset(file_path) as data:
        # Extract model and scenario from filename
        filename = file_path.stem
        parts = filename.split('_')
        model = parts[-2]
        scenario = parts[-1]

        results = {
            'model': model,
            'scenario': scenario,
            'China': {},
            'Provinces': {},
        }

        # Process China data
        china_data, china_pop = mask_and_aggregate(data, china_mask, data['population'])
        results['China']['data'] = china_data
        results['China']['population'] = china_pop

        # Process province data
        for province, mask in province_masks.items():
            province_data, province_pop = mask_and_aggregate(data, mask, data['population'])
            results['Provinces'][province] = {
                'data': province_data,
                'population': province_pop,
            }

    return results


def get_sunrise_weights(province: str) -> dict:
    """Get working hour weights based on province sunrise time.

    Args:
        province: Province name

    Returns:
        Dict with weights for min, max, half WBGT
    """
    sunrise_times = {
        4: ['吉林省', '辽宁省', '黑龙江省'],
        5: ['新疆维吾尔自治区', '安徽省', '北京市', '福建省', '河北省',
            '河南省', '湖北省', '江苏省', '内蒙古自治区', '山东省',
            '上海市', '山西省', '天津市', '浙江省'],
        6: ['重庆市', '甘肃省', '广东省', '广西壮族自治区', '贵州省',
            '海南省', '湖南省', '江西省', '宁夏回族自治区', '青海省',
            '陕西省', '四川省'],
        7: ['西藏自治区', '云南省'],
    }

    for sunrise, provinces in sunrise_times.items():
        if province in provinces:
            if sunrise == 4:
                return {'Ym': 0.75, 'Ymax': 0, 'Yhalf': 0.25}
            elif sunrise == 5:
                return {'Ym': 0.625, 'Ymax': 0, 'Yhalf': 0.375}
            elif sunrise == 6:
                return {'Ym': 0.5, 'Ymax': 0, 'Yhalf': 0.5}
            elif sunrise == 7:
                return {'Ym': 0.375, 'Ymax': 0.125, 'Yhalf': 0.5}

    # Default weights
    return {'Ym': 0.25, 'Ymax': 0.25, 'Yhalf': 0.5}


def calculate_labor_productivity_loss(data: dict, adjusted: bool = False) -> dict:
    """Calculate labor productivity loss.

    Args:
        data: Processed data dict from process_file()
        adjusted: Whether to apply working hour adjustments

    Returns:
        Dict with productivity loss for China and provinces
    """
    results = {'China': {}, 'Provinces': {}}

    def calculate_loss(region_data: xr.Dataset, factors: dict) -> dict:
        """Calculate loss for a region with given factors."""
        loss = {}
        for intensity in INTENSITIES:
            loss[intensity] = (
                factors['Ym'] * region_data[f'{intensity}_min'] +
                factors['Ymax'] * region_data[f'{intensity}_max'] +
                factors['Yhalf'] * region_data[f'{intensity}_half']
            )
        return loss

    # Calculate province losses
    total_loss = {i: 0 for i in INTENSITIES}
    total_population = 0

    for province, province_data in data['Provinces'].items():
        factors = get_sunrise_weights(province) if adjusted else {'Ym': 0.25, 'Ymax': 0.25, 'Yhalf': 0.5}
        province_loss = calculate_loss(province_data['data'], factors)

        # Per-capita loss (handle zero population)
        province_pop = province_data['population']
        if province_pop > 0:
            results['Provinces'][province] = {
                intensity: loss / province_pop
                for intensity, loss in province_loss.items()
            }
        else:
            results['Provinces'][province] = {
                intensity: np.nan for intensity in INTENSITIES
            }

        # Accumulate totals
        for intensity in INTENSITIES:
            total_loss[intensity] += province_loss[intensity]
        total_population += province_data['population']

    # Calculate China average (handle zero population)
    if total_population > 0:
        results['China'] = {
            intensity: total_loss[intensity] / total_population
            for intensity in INTENSITIES
        }
    else:
        results['China'] = {
            intensity: np.nan for intensity in INTENSITIES
        }

    return results


def calculate_difference(unadjusted: dict, adjusted: dict) -> dict:
    """Calculate difference between unadjusted and adjusted results.

    Args:
        unadjusted: Unadjusted productivity loss
        adjusted: Adjusted productivity loss

    Returns:
        Dict with differences
    """
    diff = {'China': {}, 'Provinces': {}}

    # China difference
    for intensity in INTENSITIES:
        diff['China'][intensity] = adjusted['China'][intensity] - unadjusted['China'][intensity]

    # Province differences
    for province in unadjusted['Provinces']:
        diff['Provinces'][province] = {
            intensity: adjusted['Provinces'][province][intensity] - unadjusted['Provinces'][province][intensity]
            for intensity in INTENSITIES
        }

    return diff


def calculate_scenario_statistics(results: list) -> dict:
    """Calculate statistics across models for each scenario.

    Args:
        results: List of result dicts from all models

    Returns:
        Dict with scenario statistics
    """
    scenarios = set(r['scenario'] for r in results)
    stats = {s: {'China': {}, 'Provinces': {}} for s in scenarios}

    for scenario in scenarios:
        scenario_results = [r for r in results if r['scenario'] == scenario]

        # China statistics
        for adj in ['unadjusted', 'adjusted', 'difference']:
            for intensity in INTENSITIES:
                data = np.array([r['China'][adj][intensity] for r in scenario_results])
                stats[scenario]['China'][f'{adj}_{intensity}'] = {
                    'mean': np.mean(data, axis=0),
                    'max': np.max(data, axis=0),
                    'min': np.min(data, axis=0),
                }

        # Province statistics
        provinces = list(scenario_results[0]['Provinces']['unadjusted'].keys())
        for province in provinces:
            stats[scenario]['Provinces'][province] = {}
            for adj in ['unadjusted', 'adjusted', 'difference']:
                for intensity in INTENSITIES:
                    data = np.array([r['Provinces'][adj][province][intensity]
                                    for r in scenario_results])
                    stats[scenario]['Provinces'][province][f'{adj}_{intensity}'] = {
                        'mean': np.mean(data, axis=0),
                        'max': np.max(data, axis=0),
                        'min': np.min(data, axis=0),
                    }

    return stats


def save_scenario_results(stats: dict, output_dir: Path):
    """Save scenario statistics to CSV files.

    Args:
        stats: Scenario statistics dict
        output_dir: Output directory
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    for scenario, scenario_stats in stats.items():
        scenario_dir = output_dir / scenario
        scenario_dir.mkdir(exist_ok=True)

        # Save China results
        china_data = []
        num_years = len(scenario_stats['China']['unadjusted_low']['mean'])

        for time_idx in range(num_years):
            row = {'Time': time_idx + YEAR_START}
            for intensity in INTENSITIES:
                for adj in ['unadjusted', 'adjusted']:
                    for stat in ['mean', 'max', 'min']:
                        key = f'{adj}_{intensity}'
                        row[f'{key}_{stat}'] = scenario_stats['China'][key][stat][time_idx]
                # Calculate difference
                for stat in ['mean', 'max', 'min']:
                    row[f'difference_{intensity}_{stat}'] = (
                        scenario_stats['China'][f'adjusted_{intensity}'][stat][time_idx] -
                        scenario_stats['China'][f'unadjusted_{intensity}'][stat][time_idx]
                    )
            china_data.append(row)

        china_df = pd.DataFrame(china_data)
        china_df.to_csv(scenario_dir / 'china_results.csv', index=False, encoding=OUTPUT_ENCODING)

        # Save province results
        province_data = []
        for province, province_stats in scenario_stats['Provinces'].items():
            for time_idx in range(num_years):
                row = {'Province': province, 'Time': time_idx + YEAR_START}
                for intensity in INTENSITIES:
                    for adj in ['unadjusted', 'adjusted']:
                        for stat in ['mean', 'max', 'min']:
                            key = f'{adj}_{intensity}'
                            row[f'{key}_{stat}'] = province_stats[key][stat][time_idx]
                    # Calculate difference
                    for stat in ['mean', 'max', 'min']:
                        row[f'difference_{intensity}_{stat}'] = (
                            province_stats[f'adjusted_{intensity}'][stat][time_idx] -
                            province_stats[f'unadjusted_{intensity}'][stat][time_idx]
                        )
                province_data.append(row)

        province_df = pd.DataFrame(province_data)
        province_df.to_csv(scenario_dir / 'province_results.csv', index=False, encoding=OUTPUT_ENCODING)

        # Save aggregated province results
        numeric_cols = province_df.select_dtypes(include=[np.number]).columns
        aggregated = province_df.groupby('Time')[numeric_cols].mean()
        aggregated.to_csv(scenario_dir / 'aggregated_province_results.csv', encoding=OUTPUT_ENCODING)

        logger.info(f"Saved results for scenario {scenario}")


def run(input_dir: Path, output_dir: Path,
        china_geojson: Path = None, province_geojson: Path = None):
    """Run the final analysis.

    Args:
        input_dir: Directory with weighted productivity loss files
        output_dir: Output directory for results
        china_geojson: Path to China boundary GeoJSON
        province_geojson: Path to province boundary GeoJSON
    """
    # Default GeoJSON paths
    if china_geojson is None:
        china_geojson = Path('./model_outputs/china_full.json')
    if province_geojson is None:
        province_geojson = Path('./model_outputs/china_provinces.json')

    # Get all NetCDF files
    nc_files = list(input_dir.glob('weighted_productivity_loss_*.nc'))
    logger.info(f"Found {len(nc_files)} files to process")

    if not nc_files:
        logger.error(f"No files found in {input_dir}")
        return

    # Create masks from first file
    first_file = xr.open_dataset(nc_files[0])
    china_mask, province_masks, _ = create_masks(first_file, china_geojson, province_geojson)
    first_file.close()

    # Process all files
    all_results = []
    for file_path in nc_files:
        logger.info(f"Processing {file_path.name}")

        # Process file
        processed_data = process_file(file_path, china_mask, province_masks)

        # Calculate losses (unadjusted and adjusted)
        unadjusted_loss = calculate_labor_productivity_loss(processed_data, adjusted=False)
        adjusted_loss = calculate_labor_productivity_loss(processed_data, adjusted=True)
        diff = calculate_difference(unadjusted_loss, adjusted_loss)

        # Store results
        all_results.append({
            'model': processed_data['model'],
            'scenario': processed_data['scenario'],
            'China': {
                'unadjusted': unadjusted_loss['China'],
                'adjusted': adjusted_loss['China'],
                'difference': diff['China'],
            },
            'Provinces': {
                'unadjusted': unadjusted_loss['Provinces'],
                'adjusted': adjusted_loss['Provinces'],
                'difference': diff['Provinces'],
            },
        })

    # Calculate scenario statistics
    scenario_stats = calculate_scenario_statistics(all_results)

    # Save results
    save_scenario_results(scenario_stats, output_dir)
    logger.info(f"Analysis complete. Results saved to {output_dir}")

"""Create small test dataset for verification."""

import numpy as np
import xarray as xr
from pathlib import Path
import json


def create_test_nc_file(output_dir: Path, variable: str,
                        model: str, scenario: str, year: int):
    """Create a small test NetCDF file.

    Args:
        output_dir: Output directory
        variable: Variable name (tas, tasmax, hurs, sfcWind, rsds)
        model: Model name
        scenario: Scenario name
        year: Year
    """
    # Create small 3D data (time x lat x lon)
    np.random.seed(42)  # For reproducibility

    if variable in ['tas', 'tasmax']:
        # Temperature in Kelvin (~280-300K)
        data = np.random.rand(3, 4, 5) * 20 + 280
    elif variable == 'hurs':
        # Relative humidity in percent (0-100)
        data = np.random.rand(3, 4, 5) * 60 + 40
    elif variable == 'sfcWind':
        # Wind speed in m/s
        data = np.random.rand(3, 4, 5) * 5 + 1
    elif variable == 'rsds':
        # Solar radiation in W/m^2
        data = np.random.rand(3, 4, 5) * 200 + 100
    else:
        data = np.random.rand(3, 4, 5)

    # Create coordinates
    times = [f'{year}-01-01', f'{year}-02-01', f'{year}-03-01']
    lats = np.linspace(30.0, 33.0, 4)  # China region
    lons = np.linspace(110.0, 114.0, 5)  # China region

    # Create dataset
    ds = xr.Dataset(
        {variable: (['time', 'lat', 'lon'], data)},
        coords={
            'time': np.array(times, dtype='datetime64[ns]'),
            'lat': lats,
            'lon': lons,
        }
    )

    # Add attributes
    ds[variable].attrs['units'] = 'K' if variable in ['tas', 'tasmax'] else '%'
    ds[variable].attrs['long_name'] = f'{variable} test data'

    # Create directory and save
    file_dir = output_dir / model / scenario / 'r1i1p1f1' / variable
    file_dir.mkdir(parents=True, exist_ok=True)
    filename = f'{variable}_day_{model}_{scenario}_r1i1p1f1_gn_{year}.nc'
    ds.to_netcdf(file_dir / filename)

    return file_dir / filename


def create_test_population(output_dir: Path, scenario: str):
    """Create a test population file.

    Args:
        output_dir: Output directory
        scenario: Scenario name
    """
    np.random.seed(42)

    # Create population data
    data = np.random.rand(3, 4, 5) * 1000 + 100  # Population density

    times = ['2020-01-01', '2021-01-01', '2022-01-01']
    lats = np.linspace(30.0, 33.0, 4)
    lons = np.linspace(110.0, 114.0, 5)

    ds = xr.Dataset(
        {'pop': (['StdTime', 'lat', 'lon'], data)},
        coords={
            'StdTime': np.array(times, dtype='datetime64[ns]'),
            'lat': lats,
            'lon': lons,
        }
    )

    ds['pop'].attrs['units'] = 'people/km^2'

    # Save
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f'pop_{scenario}_025.nc'
    ds.to_netcdf(output_dir / filename)

    return output_dir / filename


def create_test_china_bounds(output_dir: Path):
    """Create test China bounds file.

    Args:
        output_dir: Output directory
    """
    bounds = {
        'lat_min': 30.0,
        'lat_max': 33.0,
        'lon_min': 110.0,
        'lon_max': 114.0,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / 'china_bounds_file.json', 'w') as f:
        json.dump(bounds, f, indent=2)

    return output_dir / 'china_bounds_file.json'


def create_all_test_data(base_dir: Path):
    """Create all test data for verification.

    Args:
        base_dir: Base directory for test data
    """
    print("Creating test data...")

    # Create China bounds
    create_test_china_bounds(base_dir)
    print("✓ Created china_bounds_file.json")

    # Create climate data for one model, one scenario, 3 years
    model = 'EC-Earth3'
    scenario = 'SSP126'
    variables = ['tas', 'tasmax', 'hurs', 'sfcWind', 'rsds']
    years = [2020, 2021, 2022]

    for year in years:
        for var in variables:
            create_test_nc_file(base_dir / 'downloaded_data', var, model, scenario, year)
    print(f"✓ Created climate data for {model}/{scenario} ({len(years)} years, {len(variables)} variables)")

    # Create population data
    create_test_population(base_dir / 'model_outputs', scenario)
    print("✓ Created population data")

    print(f"\nTest data created in: {base_dir}")
    print(f"  - downloaded_data/{model}/{scenario}/")
    print(f"  - model_outputs/")
    print(f"  - china_bounds_file.json")


if __name__ == '__main__':
    import sys

    # Default to tests/test_data directory
    if len(sys.argv) > 1:
        base_dir = Path(sys.argv[1])
    else:
        base_dir = Path(__file__).parent / 'test_data'

    create_all_test_data(base_dir)

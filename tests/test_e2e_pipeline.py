"""End-to-end pipeline test with small test data."""

import pytest
import numpy as np
import xarray as xr
from pathlib import Path
import sys
import shutil


@pytest.fixture
def test_data_dir(tmp_path):
    """Create small test dataset."""
    from tests.create_test_data import create_all_test_data
    create_all_test_data(tmp_path)
    return tmp_path


def test_wbgt_indoor_calculation(test_data_dir):
    """Test indoor WBGT calculation end-to-end."""
    from shifting_work_hours.pipeline.wbgt_indoor import process_year
    from shifting_work_hours.config.constants import YEAR_START

    # Test with one year
    result = process_year(
        model='EC-Earth3',
        scenario='SSP126',
        year=2020,
        input_dir=test_data_dir / 'downloaded_data',
        output_dir=test_data_dir / 'wbgt_indoor_output'
    )

    assert result['status'] == 'success'
    assert result['year'] == 2020
    assert 'output' in result

    # Verify output file exists
    output_file = Path(result['output']) / 'wbgt_indoor_day_2020.nc'
    assert output_file.exists()

    # Verify output content
    with xr.open_dataset(output_file) as ds:
        assert 'WBGTmin_id' in ds.data_vars
        assert 'WBGTmax_id' in ds.data_vars
        assert 'WBGThalf_id' in ds.data_vars
        assert ds['WBGTmin_id'].attrs['units'] == 'degC'


def test_productivity_calculation(test_data_dir):
    """Test productivity loss calculation end-to-end."""
    from shifting_work_hours.pipeline.productivity import (
        process_year, calculate_productivity_factor
    )
    import cupy as cp

    # First, create mock indoor/outdoor WBGT files
    model = 'EC-Earth3'
    scenario = 'SSP126'
    year = 2020

    # Create indoor WBGT file
    indoor_dir = test_data_dir / 'model_outputs' / 'wbgt_indoor_output' / model / scenario / 'r1i1p1f1'
    indoor_dir.mkdir(parents=True, exist_ok=True)

    # Create small test data
    times = [f'{year}-01-01', f'{year}-02-01', f'{year}-03-01']
    lats = np.linspace(30.0, 33.0, 4)
    lons = np.linspace(110.0, 114.0, 5)

    # Indoor WBGT data
    indoor_ds = xr.Dataset(
        {
            'WBGTmin_id': (['time', 'lat', 'lon'], np.random.rand(3, 4, 5) * 10 + 20),
            'WBGTmax_id': (['time', 'lat', 'lon'], np.random.rand(3, 4, 5) * 10 + 25),
            'WBGThalf_id': (['time', 'lat', 'lon'], np.random.rand(3, 4, 5) * 10 + 22),
        },
        coords={
            'time': np.array(times, dtype='datetime64[ns]'),
            'lat': lats,
            'lon': lons,
        }
    )
    # Use the correct naming convention: wbgt_indoor_day_{year}.nc
    indoor_ds.to_netcdf(indoor_dir / f'wbgt_indoor_day_{year}.nc')

    # Create outdoor WBGT file
    outdoor_dir = test_data_dir / 'model_outputs' / 'wbgt_outdoor_output' / model / scenario / 'r1i1p1f1'
    outdoor_dir.mkdir(parents=True, exist_ok=True)

    outdoor_ds = xr.Dataset(
        {
            'WBGTmin_od': (['time', 'lat', 'lon'], np.random.rand(3, 4, 5) * 10 + 18),
            'WBGTmax_od': (['time', 'lat', 'lon'], np.random.rand(3, 4, 5) * 10 + 23),
            'WBGThalf_od': (['time', 'lat', 'lon'], np.random.rand(3, 4, 5) * 10 + 20),
        },
        coords={
            'time': np.array(times, dtype='datetime64[ns]'),
            'lat': lats,
            'lon': lons,
        }
    )
    outdoor_ds.to_netcdf(outdoor_dir / f'outdoor_wbgt_day_{year}.nc')

    # Test productivity calculation
    result = process_year(
        model=model,
        scenario=scenario,
        year=year,
        base_dir=test_data_dir / 'model_outputs'
    )

    assert result['status'] == 'success'
    assert 'data' in result

    # Verify the data
    data = result['data']
    for intensity in ['low', 'medium', 'high']:
        for metric in ['min', 'max', 'half']:
            assert f'{intensity}_{metric}' in data.data_vars


def test_cli_commands(test_data_dir):
    """Test CLI commands work."""
    import subprocess

    # Test help command
    result = subprocess.run(
        [sys.executable, 'scripts/run_pipeline.py', '--help'],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )

    assert result.returncode == 0
    assert 'Climate data processing pipeline' in result.stdout


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

"""Tests for NetCDF I/O utilities."""

import pytest
from pathlib import Path
import numpy as np
import xarray as xr

from shifting_work_hours.core.io import (
    read_variable,
    create_output_dataset,
    save_dataset,
)


def create_test_nc_file(tmp_path: Path, filename: str,
                        variable: str = 'tas',
                        kelvin: bool = True) -> Path:
    """Create a test NetCDF file."""
    # Create sample data
    data = np.random.rand(3, 4, 5) * 10 + 280  # ~280-290 K
    if not kelvin:
        data = data - 273.15  # Convert to Celsius

    ds = xr.Dataset(
        {variable: (['time', 'lat', 'lon'], data)},
        coords={
            'time': [2020, 2021, 2022],
            'lat': [30.0, 31.0, 32.0, 33.0],
            'lon': [110.0, 111.0, 112.0, 113.0, 114.0],
        }
    )

    file_path = tmp_path / filename
    ds.to_netcdf(file_path)
    return file_path


def test_read_variable(tmp_path):
    """Test reading a variable from NetCDF file."""
    file_path = create_test_nc_file(tmp_path, 'test.nc', 'tas', kelvin=True)

    ds, data = read_variable(file_path, 'tas')

    assert data.shape == (3, 4, 5)
    assert 'tas' in ds.data_vars
    ds.close()


def test_read_variable_with_kelvin_conversion(tmp_path):
    """Test reading with Kelvin to Celsius conversion."""
    file_path = create_test_nc_file(tmp_path, 'test.nc', 'tas', kelvin=True)

    ds, data = read_variable(file_path, 'tas', convert_kelvin=True)

    # Data should be in Celsius (around 7-17°C)
    assert np.all(data > -50)
    assert np.all(data < 50)
    ds.close()


def test_read_variable_without_kelvin_conversion(tmp_path):
    """Test reading without Kelvin conversion."""
    file_path = create_test_nc_file(tmp_path, 'test.nc', 'tas', kelvin=True)

    ds, data = read_variable(file_path, 'tas', convert_kelvin=False)

    # Data should be in Kelvin (around 280-290 K)
    assert np.all(data > 200)
    assert np.all(data < 350)
    ds.close()


def test_create_output_dataset():
    """Test creating an output dataset."""
    data = np.random.rand(2, 3, 4)
    coords = {
        'time': [2020, 2021],
        'lat': [30.0, 31.0, 32.0],
        'lon': [110.0, 111.0, 112.0, 113.0],
    }

    ds = create_output_dataset(
        data_vars={'WBGT': (['time', 'lat', 'lon'], data)},
        coords=coords,
        attributes={'WBGT': {'units': 'degC', 'long_name': 'WBGT'}}
    )

    assert 'WBGT' in ds.data_vars
    assert ds['WBGT'].attrs['units'] == 'degC'
    assert ds['WBGT'].attrs['long_name'] == 'WBGT'
    ds.close()


def test_save_dataset(tmp_path):
    """Test saving a dataset to NetCDF file."""
    data = np.random.rand(2, 3)
    ds = xr.Dataset(
        {'test_var': (['x', 'y'], data)},
        coords={'x': [0, 1], 'y': [0, 1, 2]}
    )

    output_dir = tmp_path / 'output'
    filename = 'test_output.nc'

    result_path = save_dataset(ds, output_dir, filename)

    assert result_path.exists()
    assert result_path.name == filename

    # Verify the saved file
    with xr.open_dataset(result_path) as saved_ds:
        assert 'test_var' in saved_ds.data_vars
        np.testing.assert_array_equal(saved_ds['test_var'].values, data)

    ds.close()

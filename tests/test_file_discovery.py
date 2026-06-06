"""Tests for file discovery utilities."""

import pytest
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shifting_work_hours.utils.file_discovery import (
    find_matching_file,
    find_nc_file,
    find_all_nc_files,
    get_model_scenario_dir,
)


def test_find_matching_file_found(tmp_path):
    """Test finding a matching file."""
    # Create test files
    (tmp_path / 'tas_day_2020.nc').touch()
    (tmp_path / 'tas_day_2021.nc').touch()
    (tmp_path / 'hurs_day_2020.nc').touch()

    result = find_matching_file(tmp_path, 'tas_day_2020*.nc')
    assert result is not None
    assert result.name == 'tas_day_2020.nc'


def test_find_matching_file_not_found(tmp_path):
    """Test when no file matches."""
    (tmp_path / 'tas_day_2020.nc').touch()

    result = find_matching_file(tmp_path, 'nonexistent*.nc')
    assert result is None


def test_find_nc_file_found(tmp_path):
    """Test finding NetCDF file by variable and year."""
    (tmp_path / 'tas_day_EC-Earth3_SSP126_r1i1p1f1_2020.nc').touch()

    result = find_nc_file(tmp_path, 'tas', 2020)
    assert result is not None
    assert 'tas' in result.name
    assert '2020' in result.name


def test_find_nc_file_with_version(tmp_path):
    """Test finding NetCDF file with version suffix."""
    (tmp_path / 'tas_day_EC-Earth3_SSP126_r1i1p1f1_2020_v1.2.nc').touch()

    result = find_nc_file(tmp_path, 'tas', 2020, version_suffix='v1.2')
    assert result is not None
    assert 'v1.2' in result.name


def test_find_nc_file_not_found(tmp_path):
    """Test when NetCDF file is not found."""
    result = find_nc_file(tmp_path, 'tas', 2020)
    assert result is None


def test_find_all_nc_files(tmp_path):
    """Test finding all NetCDF files."""
    # Create test files
    for year in range(2020, 2023):
        (tmp_path / f'tas_day_{year}.nc').touch()
    (tmp_path / 'hurs_day_2020.nc').touch()  # Different variable

    result = find_all_nc_files(tmp_path, 'tas')
    assert len(result) == 3
    assert all('tas' in f.name for f in result)


def test_find_all_nc_files_with_year_range(tmp_path):
    """Test finding NetCDF files with year range filter."""
    for year in range(2020, 2025):
        (tmp_path / f'tas_day_{year}.nc').touch()

    result = find_all_nc_files(tmp_path, 'tas', year_range=(2021, 2023))
    assert len(result) == 3
    for f in result:
        year = int(f.stem.split('_')[2])
        assert 2021 <= year <= 2023


def test_get_model_scenario_dir():
    """Test getting model/scenario directory path."""
    base_path = Path('/data')

    result = get_model_scenario_dir(base_path, 'EC-Earth3', 'SSP126')
    assert result == Path('/data/EC-Earth3/SSP126/r1i1p1f1')


def test_get_model_scenario_dir_custom_ensemble():
    """Test getting model/scenario directory with custom ensemble."""
    base_path = Path('/data')

    result = get_model_scenario_dir(base_path, 'EC-Earth3', 'SSP126', ensemble='r2i1p1f1')
    assert result == Path('/data/EC-Earth3/SSP126/r2i1p1f1')

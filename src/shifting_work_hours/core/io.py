"""NetCDF I/O utilities."""

import xarray as xr
import numpy as np
from pathlib import Path
from typing import Optional

from config.constants import KELVIN_OFFSET


def read_variable(file_path: Path, variable: str,
                  convert_kelvin: bool = False) -> tuple:
    """Read a single variable from NetCDF file with context manager.

    Args:
        file_path: Path to NetCDF file
        variable: Variable name to read
        convert_kelvin: If True, convert Kelvin to Celsius

    Returns:
        Tuple of (dataset, data_array)
    """
    with xr.open_dataset(file_path) as ds:
        data = ds[variable].values.copy()
        if convert_kelvin:
            data = data - KELVIN_OFFSET
        return ds.copy(), data


def read_variables(file_paths: dict[str, Path], variables: list[str],
                   kelvin_vars: Optional[list[str]] = None) -> dict:
    """Read multiple variables from NetCDF files.

    Args:
        file_paths: Dict mapping variable name -> file path
        variables: List of variable names to read
        kelvin_vars: Variables that need K->C conversion

    Returns:
        Dict mapping variable -> (dataset, data_array)
    """
    kelvin_vars = kelvin_vars or []
    result = {}

    for var in variables:
        path = file_paths[var]
        ds, data = read_variable(path, var, convert_kelvin=(var in kelvin_vars))
        result[var] = (ds, data)

    return result


def create_output_dataset(data_vars: dict, coords: dict,
                          attributes: Optional[dict] = None) -> xr.Dataset:
    """Create an xarray Dataset with standard attributes.

    Args:
        data_vars: Dict mapping name -> (dims, data)
        coords: Dict mapping coord name -> values
        attributes: Optional dict mapping var -> attrs dict

    Returns:
        xr.Dataset
    """
    ds = xr.Dataset(data_vars, coords=coords)

    if attributes:
        for var, attrs in attributes.items():
            if var in ds.data_vars:
                ds[var].attrs.update(attrs)

    return ds


def save_dataset(ds: xr.Dataset, output_dir: Path, filename: str) -> Path:
    """Save dataset to NetCDF file, creating directories as needed.

    Args:
        ds: Dataset to save
        output_dir: Output directory
        filename: Output filename

    Returns:
        Path to saved file
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    ds.to_netcdf(output_path)
    return output_path


def read_dataset(file_path: Path) -> xr.Dataset:
    """Read a NetCDF file with context manager.

    Args:
        file_path: Path to NetCDF file

    Returns:
        xr.Dataset (copy that can be used after context closes)
    """
    with xr.open_dataset(file_path) as ds:
        return ds.copy()

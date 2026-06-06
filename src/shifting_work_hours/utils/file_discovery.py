"""File discovery utilities for NetCDF files."""

import glob
from pathlib import Path
from typing import Optional


def find_matching_file(base_path: Path, pattern: str) -> Optional[Path]:
    """Find first file matching glob pattern.

    Args:
        base_path: Directory to search in
        pattern: Glob pattern (e.g., 'tas_day_*_2015*.nc')

    Returns:
        Path to first matching file, or None
    """
    base_path = Path(base_path)
    matches = list(base_path.glob(pattern))
    return matches[0] if matches else None


def find_nc_file(base_path: Path, variable: str, year: int,
                 version_suffix: Optional[str] = None) -> Optional[Path]:
    """Find NetCDF file for given variable and year.

    Args:
        base_path: Directory containing .nc files
        variable: CMIP6 variable name (e.g., 'tas')
        year: Year to find
        version_suffix: Optional version filter (e.g., 'v1.2')

    Returns:
        Path to matching file, or None
    """
    if version_suffix:
        pattern = f"{variable}_day_*_{year}*_{version_suffix}.nc"
    else:
        pattern = f"{variable}_day_*_{year}*.nc"

    return find_matching_file(base_path, pattern)


def find_all_nc_files(base_path: Path, variable: str,
                      year_range: Optional[tuple] = None) -> list[Path]:
    """Find all NetCDF files for a variable.

    Args:
        base_path: Directory containing .nc files
        variable: CMIP6 variable name
        year_range: Optional tuple of (start_year, end_year) to filter

    Returns:
        List of matching file paths
    """
    base_path = Path(base_path)
    pattern = f"{variable}_day_*.nc"
    matches = sorted(base_path.glob(pattern))

    if year_range:
        start, end = year_range
        filtered = []
        for f in matches:
            # Extract year from filename
            parts = f.stem.split('_')
            for part in parts:
                if part.isdigit() and len(part) == 4:
                    year = int(part)
                    if start <= year <= end:
                        filtered.append(f)
                        break
        return filtered

    return matches


def get_model_scenario_dir(base_path: Path, model: str, scenario: str,
                           ensemble: str = 'r1i1p1f1') -> Path:
    """Get the directory path for a model/scenario combination.

    Args:
        base_path: Base data directory
        model: Model name (e.g., 'EC-Earth3')
        scenario: Scenario name (e.g., 'SSP126')
        ensemble: Ensemble member (default: 'r1i1p1f1')

    Returns:
        Path to model/scenario/ensemble directory
    """
    return Path(base_path) / model / scenario / ensemble

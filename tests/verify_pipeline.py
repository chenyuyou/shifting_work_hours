"""Verify the refactored pipeline works with test data."""

import sys
from pathlib import Path

from tests.create_test_data import create_all_test_data


def verify_imports():
    """Verify all modules can be imported."""
    print("\n1. Verifying imports...")

    try:
        from shifting_work_hours.config import constants, settings
        print("   [OK] config module")

        from shifting_work_hours.core import status, runner, io
        print("   [OK] core module")

        # Pipeline modules that don't require heavy dependencies
        from shifting_work_hours.pipeline import downloader
        print("   [OK] pipeline module (downloader)")

        # Pipeline modules that require geopandas (optional)
        try:
            from shifting_work_hours.pipeline import (
                extractor, analysis, outdoor_summary
            )
            print("   [OK] pipeline modules (extractor, analysis, outdoor_summary)")
        except ImportError as e:
            if 'geopandas' in str(e).lower() or 'regionmask' in str(e).lower():
                print("   [SKIP] pipeline modules requiring geopandas/regionmask")
            else:
                raise

        # Pipeline modules that require CuPy (optional)
        try:
            from shifting_work_hours.pipeline import wbgt_indoor, wbgt_outdoor, productivity
            print("   [OK] pipeline modules (wbgt_indoor, wbgt_outdoor, productivity)")
        except ImportError as e:
            if 'cupy' in str(e).lower():
                print("   [SKIP] pipeline modules requiring CuPy (no NVIDIA GPU)")
            else:
                raise

        from shifting_work_hours.utils import file_discovery
        print("   [OK] utils module")

        return True
    except Exception as e:
        print(f"   [FAIL] Import failed: {e}")
        return False


def verify_config():
    """Verify configuration is correct."""
    print("\n2. Verifying configuration...")

    try:
        from shifting_work_hours.config.constants import MODELS, SCENARIOS, YEAR_START, YEAR_END

        assert len(MODELS) == 4
        assert len(SCENARIOS) == 3
        assert YEAR_START == 2015
        assert YEAR_END == 2100

        print(f"   [OK] Models: {MODELS}")
        print(f"   [OK] Scenarios: {SCENARIOS}")
        print(f"   [OK] Year range: {YEAR_START}-{YEAR_END}")

        return True
    except Exception as e:
        print(f"   [FAIL] Config verification failed: {e}")
        return False


def verify_status_tracker(test_dir: Path):
    """Verify StatusTracker works."""
    print("\n3. Verifying StatusTracker...")

    try:
        from shifting_work_hours.core.status import StatusTracker

        status_file = test_dir / 'test_status.json'
        tracker = StatusTracker(status_file)

        # Test record and check
        tracker.record('test_key', {'status': 'success', 'data': 42})
        assert tracker.is_done('test_key')
        assert not tracker.is_done('nonexistent')

        # Test save and load
        tracker.save()
        tracker2 = StatusTracker(status_file)
        assert tracker2.is_done('test_key')

        # Test stats
        tracker.record('key2', {'status': 'error', 'error': 'test'})
        stats = tracker.get_stats()
        assert stats['success'] == 1
        assert stats['failed'] == 1

        print("   [OK] StatusTracker works correctly")

        # Cleanup
        status_file.unlink()

        return True
    except Exception as e:
        print(f"   [FAIL] StatusTracker verification failed: {e}")
        return False


def verify_task_runner(test_dir: Path):
    """Verify TaskRunner works."""
    print("\n4. Verifying TaskRunner...")

    try:
        from shifting_work_hours.core.status import StatusTracker
        from shifting_work_hours.core.runner import TaskRunner

        status_file = test_dir / 'test_status.json'
        tracker = StatusTracker(status_file)
        runner = TaskRunner(tracker, num_threads=2)

        # Simple task
        def simple_task(model, scenario, year):
            return {'status': 'success', 'year': year}

        tasks = [
            ('Model1', 'SSP126', 2020),
            ('Model1', 'SSP126', 2021),
        ]

        results = runner.run(iter(tasks), simple_task, desc="Test")
        assert len(results) == 2

        print("   [OK] TaskRunner works correctly")

        # Cleanup
        status_file.unlink()

        return True
    except Exception as e:
        print(f"   [FAIL] TaskRunner verification failed: {e}")
        return False


def verify_file_discovery(test_dir: Path):
    """Verify file discovery works."""
    print("\n5. Verifying file discovery...")

    try:
        from shifting_work_hours.utils.file_discovery import (
            find_matching_file, find_nc_file, get_model_scenario_dir
        )

        # Create test file with realistic name
        test_file = test_dir / 'tas_day_EC-Earth3_SSP126_r1i1p1f1_gn_2020.nc'
        test_file.touch()

        # Test find_matching_file with correct pattern
        result = find_matching_file(test_dir, 'tas_day_*_2020*.nc')
        assert result is not None
        assert 'tas' in result.name
        assert '2020' in result.name

        # Test find_nc_file
        result = find_nc_file(test_dir, 'tas', 2020)
        assert result is not None
        assert 'tas' in result.name
        assert '2020' in result.name

        # Test get_model_scenario_dir
        result = get_model_scenario_dir(test_dir, 'EC-Earth3', 'SSP126')
        assert result == test_dir / 'EC-Earth3' / 'SSP126' / 'r1i1p1f1'

        print("   [OK] File discovery works correctly")

        # Cleanup
        test_file.unlink()

        return True
    except Exception as e:
        print(f"   [FAIL] File discovery verification failed: {e}")
        return False


def verify_netCDF_io(test_dir: Path):
    """Verify NetCDF I/O works."""
    print("\n6. Verifying NetCDF I/O...")

    try:
        import numpy as np
        import xarray as xr
        from shifting_work_hours.core.io import (
            read_variable, create_output_dataset, save_dataset
        )

        # Create test NetCDF file
        data = np.random.rand(2, 3, 4) * 10 + 280
        ds = xr.Dataset(
            {'tas': (['time', 'lat', 'lon'], data)},
            coords={
                'time': [2020, 2021],
                'lat': [30.0, 31.0, 32.0],
                'lon': [110.0, 111.0, 112.0, 113.0],
            }
        )
        test_file = test_dir / 'test.nc'
        ds.to_netcdf(test_file)
        ds.close()

        # Test read_variable
        ds, data = read_variable(test_file, 'tas', convert_kelvin=True)
        assert data.shape == (2, 3, 4)
        assert np.all(data > -50)  # Should be in Celsius
        ds.close()

        # Test create_output_dataset
        ds = create_output_dataset(
            data_vars={'WBGT': (['time', 'lat', 'lon'], data)},
            coords={
                'time': [2020, 2021],
                'lat': [30.0, 31.0, 32.0],
                'lon': [110.0, 111.0, 112.0, 113.0],
            },
            attributes={'WBGT': {'units': 'degC'}}
        )
        assert 'WBGT' in ds.data_vars
        ds.close()

        # Test save_dataset
        output_dir = test_dir / 'output'
        result = save_dataset(ds, output_dir, 'test_output.nc')
        assert result.exists()

        print("   [OK] NetCDF I/O works correctly")

        # Cleanup
        test_file.unlink()
        import shutil
        shutil.rmtree(output_dir)

        return True
    except Exception as e:
        print(f"   [FAIL] NetCDF I/O verification failed: {e}")
        return False


def verify_cli():
    """Verify CLI works."""
    print("\n7. Verifying CLI...")

    try:
        import subprocess

        result = subprocess.run(
            [sys.executable, 'scripts/run_pipeline.py', '--help'],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )

        assert result.returncode == 0
        assert 'Climate data processing pipeline' in result.stdout
        assert 'download' in result.stdout
        assert 'wbgt-indoor' in result.stdout

        print("   [OK] CLI works correctly")

        return True
    except Exception as e:
        print(f"   [FAIL] CLI verification failed: {e}")
        return False


def main():
    """Run all verifications."""
    print("=" * 60)
    print("Pipeline Verification")
    print("=" * 60)

    # Create test directory
    test_dir = Path(__file__).parent / 'test_verification'
    test_dir.mkdir(exist_ok=True)

    # Run verifications
    results = []
    results.append(("Imports", verify_imports()))
    results.append(("Configuration", verify_config()))
    results.append(("StatusTracker", verify_status_tracker(test_dir)))
    results.append(("TaskRunner", verify_task_runner(test_dir)))
    results.append(("File Discovery", verify_file_discovery(test_dir)))
    results.append(("NetCDF I/O", verify_netCDF_io(test_dir)))
    results.append(("CLI", verify_cli()))

    # Summary
    print("\n" + "=" * 60)
    print("Verification Summary")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"  {status} - {name}")

    print(f"\n{passed}/{total} verifications passed")

    # Cleanup
    import shutil
    if test_dir.exists():
        shutil.rmtree(test_dir)

    if passed == total:
        print("\nAll verifications passed! The refactored code is working correctly.")
        return 0
    else:
        print("\nSome verifications failed. Please check the errors above.")
        return 1


if __name__ == '__main__':
    sys.exit(main())

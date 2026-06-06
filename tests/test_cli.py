"""Tests for CLI entry point."""

import pytest
from pathlib import Path
import sys
import subprocess


def test_cli_help():
    """Test that CLI help works."""
    result = subprocess.run(
        [sys.executable, 'scripts/run_pipeline.py', '--help'],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )

    assert result.returncode == 0
    assert 'Climate data processing pipeline' in result.stdout
    assert 'download' in result.stdout
    assert 'extract' in result.stdout
    assert 'wbgt-indoor' in result.stdout
    assert 'wbgt-outdoor' in result.stdout
    assert 'productivity' in result.stdout
    assert 'analysis' in result.stdout
    assert 'outdoor-summary' in result.stdout
    assert 'all' in result.stdout


def test_cli_download_help():
    """Test download command help."""
    result = subprocess.run(
        [sys.executable, 'scripts/run_pipeline.py', 'download', '--help'],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )

    assert result.returncode == 0
    assert '--workers' in result.stdout


def test_cli_wbgt_indoor_help():
    """Test wbgt-indoor command help."""
    result = subprocess.run(
        [sys.executable, 'scripts/run_pipeline.py', 'wbgt-indoor', '--help'],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )

    assert result.returncode == 0
    assert '--threads' in result.stdout


def test_cli_productivity_help():
    """Test productivity command help."""
    result = subprocess.run(
        [sys.executable, 'scripts/run_pipeline.py', 'productivity', '--help'],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )

    assert result.returncode == 0
    assert '--threads' in result.stdout
    assert '--scenario' in result.stdout
    assert 'SSP126' in result.stdout
    assert 'SSP245' in result.stdout
    assert 'SSP585' in result.stdout


def test_cli_invalid_command():
    """Test that invalid command shows help."""
    result = subprocess.run(
        [sys.executable, 'scripts/run_pipeline.py', 'invalid-command'],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent
    )

    assert result.returncode != 0

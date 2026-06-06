"""Tests for StatusTracker utility."""

import pytest
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.shifting_work_hours.core.status import StatusTracker


def test_status_tracker_creates_file(tmp_path):
    """Test that StatusTracker creates status file if it doesn't exist."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)
    assert status_file.exists()


def test_status_tracker_records_and_checks(tmp_path):
    """Test recording and checking task status."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)

    # Record a successful task
    tracker.record('test_key', {'status': 'success', 'data': 42})
    assert tracker.is_done('test_key')

    # Check non-existent key
    assert not tracker.is_done('nonexistent')


def test_status_tracker_save_load(tmp_path):
    """Test that status persists across instances."""
    status_file = tmp_path / 'status.json'

    # Create and save
    tracker = StatusTracker(status_file)
    tracker.record('test_key', {'status': 'success'})
    tracker.save()

    # Reload
    tracker2 = StatusTracker(status_file)
    assert tracker2.is_done('test_key')


def test_status_tracker_stats(tmp_path):
    """Test statistics calculation."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)

    # Record some results
    tracker.record('key1', {'status': 'success'})
    tracker.record('key2', {'status': 'success'})
    tracker.record('key3', {'status': 'error', 'error': 'failed'})

    stats = tracker.get_stats()
    assert stats['total'] == 3
    assert stats['success'] == 2
    assert stats['failed'] == 1


def test_status_tracker_failed_keys(tmp_path):
    """Test getting failed task keys."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)

    tracker.record('key1', {'status': 'success'})
    tracker.record('key2', {'status': 'error', 'error': 'failed'})

    failed = tracker.get_failed_keys()
    assert 'key2' in failed
    assert 'key1' not in failed


def test_status_tracker_clear(tmp_path):
    """Test clearing all status data."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)

    tracker.record('key1', {'status': 'success'})
    tracker.clear()

    assert not tracker.is_done('key1')
    stats = tracker.get_stats()
    assert stats['total'] == 0

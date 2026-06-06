"""Tests for TaskRunner utility."""

import pytest
from pathlib import Path
import threading
import time

from shifting_work_hours.core.runner import TaskRunner
from shifting_work_hours.core.status import StatusTracker


def test_task_runner_processes_tasks(tmp_path):
    """Test that TaskRunner processes tasks correctly."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)
    runner = TaskRunner(tracker, num_threads=2)

    # Simple task function
    def simple_task(model, scenario, year):
        return {'status': 'success', 'year': year}

    # Generate tasks
    tasks = [
        ('Model1', 'SSP126', 2020),
        ('Model1', 'SSP126', 2021),
        ('Model1', 'SSP126', 2022),
    ]

    results = runner.run(iter(tasks), simple_task, desc="Test")

    assert len(results) == 3
    assert all(r['status'] == 'success' for r in results.values())


def test_task_runner_skips_completed(tmp_path):
    """Test that TaskRunner skips already completed tasks."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)
    runner = TaskRunner(tracker, num_threads=2)

    # Pre-complete a task
    tracker.record('Model1_SSP126_2020', {'status': 'success'})
    tracker.save()

    # Track which tasks are actually processed
    processed = []

    def tracking_task(model, scenario, year):
        processed.append(year)
        return {'status': 'success', 'year': year}

    tasks = [
        ('Model1', 'SSP126', 2020),  # Should be skipped
        ('Model1', 'SSP126', 2021),  # Should be processed
        ('Model1', 'SSP126', 2022),  # Should be processed
    ]

    results = runner.run(iter(tasks), tracking_task, desc="Test")

    # Only 2 tasks should be processed (2020 was skipped)
    assert len(processed) == 2
    assert 2020 not in processed
    assert 2021 in processed
    assert 2022 in processed


def test_task_runner_handles_errors(tmp_path):
    """Test that TaskRunner handles task errors gracefully."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)
    runner = TaskRunner(tracker, num_threads=2)

    def failing_task(model, scenario, year):
        if year == 2021:
            raise ValueError("Test error")
        return {'status': 'success', 'year': year}

    tasks = [
        ('Model1', 'SSP126', 2020),
        ('Model1', 'SSP126', 2021),  # This will fail
        ('Model1', 'SSP126', 2022),
    ]

    results = runner.run(iter(tasks), failing_task, desc="Test")

    assert len(results) == 3
    assert results['Model1_SSP126_2020']['status'] == 'success'
    assert results['Model1_SSP126_2021']['status'] == 'error'
    assert results['Model1_SSP126_2022']['status'] == 'success'


def test_task_runner_concurrent_execution(tmp_path):
    """Test that TaskRunner executes tasks concurrently."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)
    runner = TaskRunner(tracker, num_threads=4)

    # Track execution times
    start_times = {}
    lock = threading.Lock()

    def slow_task(model, scenario, year):
        with lock:
            start_times[year] = time.time()
        time.sleep(0.1)  # Simulate work
        return {'status': 'success', 'year': year}

    tasks = [(f'Model1', 'SSP126', year) for year in range(2020, 2024)]

    start = time.time()
    results = runner.run(iter(tasks), slow_task, desc="Test")
    total_time = time.time() - start

    # With 4 threads, 4 tasks should complete in ~0.1s instead of ~0.4s
    assert total_time < 0.3  # Allow some overhead
    assert len(results) == 4


def test_task_runner_empty_tasks(tmp_path):
    """Test that TaskRunner handles empty task list."""
    status_file = tmp_path / 'status.json'
    tracker = StatusTracker(status_file)
    runner = TaskRunner(tracker, num_threads=2)

    def simple_task(model, scenario, year):
        return {'status': 'success', 'year': year}

    results = runner.run(iter([]), simple_task, desc="Test")

    assert len(results) == 0

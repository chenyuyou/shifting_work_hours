"""Thread-safe status tracking for pipeline processing."""

import json
import threading
from pathlib import Path
from typing import Any


class StatusTracker:
    """Thread-safe JSON-based status tracking for pipeline processing.

    Each processing task is identified by a key (e.g., "model_scenario_year")
    and stores a result dict with at least a 'status' field ('success' or 'error').
    """

    def __init__(self, status_file: Path):
        """Initialize status tracker.

        Args:
            status_file: Path to JSON status file
        """
        self.status_file = Path(status_file)
        self.lock = threading.Lock()
        # Initialize with lock to prevent race conditions
        with self.lock:
            self._ensure_file()
            self.status = self._load()

    def _ensure_file(self):
        """Create status file if it doesn't exist."""
        if not self.status_file.exists():
            self.status_file.parent.mkdir(parents=True, exist_ok=True)
            self.status_file.write_text('{}', encoding='utf-8')

    def _load(self) -> dict:
        """Load status from file."""
        try:
            return json.loads(self.status_file.read_text(encoding='utf-8'))
        except (json.JSONDecodeError, FileNotFoundError):
            return {}

    def is_done(self, key: str) -> bool:
        """Check if a task has been completed successfully.

        Args:
            key: Task identifier (e.g., "EC-Earth3_SSP126_2020")

        Returns:
            True if task status is 'success'
        """
        with self.lock:
            return self.status.get(key, {}).get('status') == 'success'

    def record(self, key: str, result: dict[str, Any]):
        """Record a task result.

        Args:
            key: Task identifier
            result: Result dict with at least 'status' field
        """
        with self.lock:
            self.status[key] = result

    def save(self):
        """Save status to file with atomic write."""
        with self.lock:
            # Atomic write: write to temp file, then rename
            tmp = self.status_file.with_suffix('.tmp')
            tmp.write_text(json.dumps(self.status, indent=2), encoding='utf-8')
            tmp.replace(self.status_file)

    def get_stats(self) -> dict[str, int]:
        """Get processing statistics.

        Returns:
            Dict with 'total', 'success', 'failed' counts
        """
        with self.lock:
            total = len(self.status)
            success = sum(1 for v in self.status.values()
                         if v.get('status') == 'success')
            failed = total - success
            return {'total': total, 'success': success, 'failed': failed}

    def get_failed_keys(self) -> list[str]:
        """Get list of failed task keys.

        Returns:
            List of task keys with non-success status
        """
        with self.lock:
            return [k for k, v in self.status.items()
                    if v.get('status') != 'success']

    def clear(self):
        """Clear all status data."""
        with self.lock:
            self.status = {}
            # Use atomic write to prevent corruption
            tmp = self.status_file.with_suffix('.tmp')
            tmp.write_text('{}', encoding='utf-8')
            tmp.replace(self.status_file)

"""Base class for parallel task processing."""

import threading
from queue import Queue
from typing import Callable, Iterator
from tqdm import tqdm

from .status import StatusTracker


class TaskRunner:
    """Parallel task runner with status tracking.

    Provides a standardized pattern for processing (model, scenario, year)
    combinations in parallel threads with progress tracking and status persistence.
    """

    def __init__(self, status_tracker: StatusTracker, num_threads: int = 4):
        """Initialize task runner.

        Args:
            status_tracker: StatusTracker instance for persistence
            num_threads: Number of worker threads
        """
        self.tracker = status_tracker
        self.num_threads = num_threads

    def run(self, tasks: Iterator[tuple], task_fn: Callable,
            desc: str = "Processing") -> dict:
        """Run task_fn on each task in parallel.

        Each task is a tuple where the first 3 elements are always
        (model, scenario, year). Additional elements are passed to task_fn.

        Args:
            tasks: Iterator of (model, scenario, year, ...) tuples
            task_fn: Function to call for each task
            desc: Progress bar description

        Returns:
            Dict mapping status_key -> result
        """
        task_queue = Queue()
        results = {}
        total = 0

        # Enqueue all tasks
        for task in tasks:
            task_queue.put(task)
            total += 1

        if total == 0:
            return results

        # Progress bar
        pbar = tqdm(total=total, desc=desc)

        # Worker function
        def worker():
            while True:
                task = task_queue.get()
                if task is None:
                    break

                # First 3 elements are always (model, scenario, year)
                model, scenario, year = task[0], task[1], task[2]
                status_key = f"{model}_{scenario}_{year}"

                # Skip if already processed
                if self.tracker.is_done(status_key):
                    pbar.update(1)
                    task_queue.task_done()
                    continue

                # Execute task
                try:
                    result = task_fn(*task)
                except Exception as e:
                    result = {
                        'status': 'error',
                        'year': year,
                        'error': str(e),
                    }

                # Record result
                self.tracker.record(status_key, result)
                results[status_key] = result

                pbar.update(1)
                task_queue.task_done()

        # Start threads
        threads = []
        for _ in range(self.num_threads):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)

        # Wait for completion
        task_queue.join()

        # Stop workers
        for _ in range(self.num_threads):
            task_queue.put(None)
        for t in threads:
            t.join()

        pbar.close()

        # Save status
        self.tracker.save()

        return results

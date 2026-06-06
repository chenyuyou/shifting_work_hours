"""NASA climate data downloader.

This module downloads climate data from NASA's NEX-GDDP-CMIP6 dataset
with support for resuming interrupted downloads.
"""

import csv
import json
import logging
import os
import requests
import time
from pathlib import Path
from threading import Thread, Lock
from queue import Queue
from tqdm import tqdm

from config.settings import NASA_DATA_INFO_FILE, DOWNLOAD_WORKERS

logger = logging.getLogger(__name__)


class DownloadMetadata:
    """Manages download metadata for tracking file status."""

    def __init__(self, metadata_file: Path):
        self.metadata_file = metadata_file
        self.lock = Lock()
        self.metadata = self._load()

    def _load(self) -> dict:
        """Load metadata from file."""
        if self.metadata_file.exists():
            try:
                return json.loads(self.metadata_file.read_text(encoding='utf-8'))
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding metadata: {e}")
                return {}
        return {}

    def save(self):
        """Save metadata to file."""
        with self.lock:
            self.metadata_file.write_text(
                json.dumps(self.metadata, indent=2),
                encoding='utf-8'
            )

    def get_status(self, filename: str) -> dict | None:
        """Get status for a file."""
        with self.lock:
            return self.metadata.get(filename)

    def set_status(self, filename: str, status: str, size: int):
        """Set status for a file."""
        with self.lock:
            self.metadata[filename] = {'status': status, 'size': size}


def get_file_status(actual_size: int, expected_size: int,
                    tolerance_percent: float = 1.0) -> str:
    """Determine file download status based on size comparison.

    Args:
        actual_size: Actual file size in bytes
        expected_size: Expected file size in bytes
        tolerance_percent: Size tolerance percentage

    Returns:
        Status string: 'completed', 'partial', or 'incomplete'
    """
    if abs(actual_size - expected_size) <= expected_size * (tolerance_percent / 100):
        return 'completed'
    elif actual_size > 0:
        return 'partial'
    else:
        return 'incomplete'


def load_file_info_list(csv_file: Path) -> list[dict]:
    """Load file information from CSV.

    Args:
        csv_file: Path to CSV file with file information

    Returns:
        List of file info dicts
    """
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        return list(reader)


def get_files_to_download(file_info_list: list[dict],
                          base_dir: Path) -> list[tuple]:
    """Get list of files that need to be downloaded or resumed.

    Args:
        file_info_list: List of file info dicts from CSV
        base_dir: Base directory for downloaded data

    Returns:
        List of (file_info, current_size, local_path) tuples
    """
    files_to_download = []

    for file_info in file_info_list:
        # Build local path
        local_path = (
            base_dir / file_info['model'] / file_info['scenario'] /
            'r1i1p1f1' / file_info['variable'] / file_info['filename']
        )

        # Parse expected size
        expected_size = int(float(file_info['filesize'].split()[0]) * 1024 * 1024)

        if local_path.exists():
            actual_size = local_path.stat().st_size
            status = get_file_status(actual_size, expected_size)
            if status == 'completed':
                continue
            else:
                files_to_download.append((file_info, actual_size, local_path))
        else:
            files_to_download.append((file_info, 0, local_path))

    return files_to_download


def download_file(file_info: dict, current_size: int, local_path: Path,
                  metadata: DownloadMetadata) -> bool:
    """Download a single file with resume support.

    Args:
        file_info: File information dict
        current_size: Current downloaded size in bytes
        local_path: Local file path
        metadata: DownloadMetadata instance

    Returns:
        True if download successful
    """
    url = file_info['download_url']
    filename = file_info['filename']
    expected_size = int(float(file_info['filesize'].split()[0]) * 1024 * 1024)

    # Create directory
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Set up headers for resume
    headers = {}
    mode = 'wb'
    if current_size > 0:
        headers['Range'] = f'bytes={current_size}-'
        mode = 'ab'

    # Retry logic
    max_retries = 3
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            with requests.get(url, headers=headers, stream=True) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0)) + current_size

                # Download with progress bar
                with tqdm(
                    desc=filename,
                    initial=current_size,
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as progress_bar, open(local_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        size = f.write(chunk)
                        progress_bar.update(size)
                        current_size += size

                # Verify download
                final_size = local_path.stat().st_size
                status = get_file_status(final_size, expected_size)
                metadata.set_status(filename, status, final_size)

                if status == 'completed':
                    logger.info(f"Download completed: {filename}")
                    return True
                else:
                    logger.warning(f"Download incomplete: {filename} ({status})")
                    return False

        except requests.RequestException as e:
            logger.error(f"Error downloading {filename} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to download {filename} after {max_retries} attempts")
                return False

    return False


def download_worker(queue: Queue, metadata: DownloadMetadata,
                    file_locks: dict):
    """Worker thread for downloading files.

    Args:
        queue: Queue of (file_info, current_size, local_path) tuples
        metadata: DownloadMetadata instance
        file_locks: Dict of filename -> Lock
    """
    while True:
        task = queue.get()
        if task is None:
            break

        file_info, current_size, local_path = task
        filename = file_info['filename']

        with file_locks[filename]:
            success = download_file(file_info, current_size, local_path, metadata)
            if not success:
                logger.warning(f"Failed to download {filename}, will retry next run")

        queue.task_done()


def run(csv_file: Path = None, output_dir: Path = None,
        num_workers: int = None) -> dict:
    """Run the download process.

    Args:
        csv_file: Path to CSV file with file information
        output_dir: Base directory for downloaded data
        num_workers: Number of download threads

    Returns:
        Dict with 'total', 'downloaded', 'failed' counts
    """
    csv_file = csv_file or NASA_DATA_INFO_FILE
    output_dir = output_dir or Path('downloaded_data')
    num_workers = num_workers or DOWNLOAD_WORKERS

    # Load file information
    file_info_list = load_file_info_list(csv_file)
    logger.info(f"Total files in CSV: {len(file_info_list)}")

    # Get files to download
    files_to_download = get_files_to_download(file_info_list, output_dir)
    logger.info(f"Files to download or resume: {len(files_to_download)}")

    if not files_to_download:
        logger.info("All files already downloaded")
        return {'total': len(file_info_list), 'downloaded': 0, 'failed': 0}

    # Set up metadata tracking
    metadata_file = output_dir / 'download_metadata.json'
    metadata = DownloadMetadata(metadata_file)

    # Set up file locks
    file_locks = {}
    for file_info, _, _ in files_to_download:
        filename = file_info['filename']
        if filename not in file_locks:
            file_locks[filename] = Lock()

    # Create queue and start workers
    queue = Queue()
    for task in files_to_download:
        queue.put(task)

    threads = []
    for _ in range(num_workers):
        t = Thread(target=download_worker, args=(queue, metadata, file_locks))
        t.start()
        threads.append(t)

    # Wait for completion
    queue.join()

    # Stop workers
    for _ in range(num_workers):
        queue.put(None)
    for t in threads:
        t.join()

    # Save final metadata
    metadata.save()

    # Count results
    downloaded = sum(1 for _, _, path in files_to_download
                    if path.exists() and get_file_status(
                        path.stat().st_size,
                        int(float(next(f['filesize'] for f in file_info_list
                                      if f['filename'] == path.name).split()[0]) * 1024 * 1024)
                    ) == 'completed')

    failed = len(files_to_download) - downloaded

    logger.info(f"Download complete: {downloaded} downloaded, {failed} failed")

    return {
        'total': len(file_info_list),
        'downloaded': downloaded,
        'failed': failed,
    }

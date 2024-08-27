import csv
import os
import sys
import logging
import requests
from threading import Thread, Lock
from queue import Queue
from tqdm import tqdm
import time
import json

# 设置日志记录
logging.basicConfig(filename='download_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 全局变量
file_locks = {}
global_lock = Lock()

def load_metadata(folder_path):
    metadata_path = os.path.join(folder_path, 'download_metadata.json')
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON in {metadata_path}: {e}")
            return {}
    return {}

def save_metadata(folder_path, metadata):
    metadata_path = os.path.join(folder_path, 'download_metadata.json')
    try:
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    except IOError as e:
        logging.error(f"Error writing metadata to {metadata_path}: {e}")

def get_file_status(actual_size, expected_size, tolerance_percent=1):
    if abs(actual_size - expected_size) <= expected_size * (tolerance_percent / 100):
        return 'completed'
    elif actual_size > 0:
        return 'partial'
    else:
        return 'incomplete'

def check_file_status(file_info, folder_path):
    filename = file_info['filename']
    expected_size = int(float(file_info['filesize'].split()[0]) * 1024 * 1024)  # Convert MB to bytes
    local_path = os.path.join(folder_path, filename)
    
    metadata = load_metadata(folder_path)
    
    if os.path.exists(local_path):
        actual_size = os.path.getsize(local_path)
        status = get_file_status(actual_size, expected_size)
        if status == 'completed':
            # 更新元数据以确保状态正确
            metadata[filename] = {'status': 'completed', 'size': actual_size}
            save_metadata(folder_path, metadata)
            return None
        else:
            return file_info, actual_size, local_path
    else:
        return file_info, 0, local_path

def get_files_to_download(file_info_list):
    files_to_download = []
    for file_info in file_info_list:
        folder_path = os.path.join('downloaded_data', 
                                   file_info['model'], 
                                   file_info['scenario'], 
                                   'r1i1p1f1', 
                                   file_info['variable'])
        status = check_file_status(file_info, folder_path)
        if status is not None:
            files_to_download.append(status)
    return files_to_download

def download_file(file_info, current_size, local_path):
    url = file_info['download_url']
    filename = file_info['filename']
    expected_size = int(float(file_info['filesize'].split()[0]) * 1024 * 1024)  # Convert MB to bytes
    
    folder_path = os.path.dirname(local_path)
    os.makedirs(folder_path, exist_ok=True)
    
    headers = {}
    mode = 'wb'
    
    if current_size > 0:
        headers['Range'] = f'bytes={current_size}-'
        mode = 'ab'
    
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            with requests.get(url, headers=headers, stream=True) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0)) + current_size
                
                with tqdm(
                    desc=filename,
                    initial=current_size,
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as progress_bar, open(local_path, mode) as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        size = file.write(chunk)
                        progress_bar.update(size)
                        current_size += size
                
                # 下载完成后再次检查文件大小
                final_size = os.path.getsize(local_path)
                status = get_file_status(final_size, expected_size)
                
                metadata = load_metadata(folder_path)
                metadata[filename] = {'status': status, 'size': final_size}
                save_metadata(folder_path, metadata)
                
                if status == 'completed':
                    logging.info(f"Download completed for: {filename}")
                    return True
                else:
                    logging.warning(f"Download incomplete for: {filename}. Status: {status}")
                    return False
        
        except requests.RequestException as e:
            logging.error(f"Error downloading {filename} (Attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to download {filename} after {max_retries} attempts.")
                return False

def download_worker(queue):
    while True:
        file_info, current_size, local_path = queue.get()
        if file_info is None:
            break
        
        filename = file_info['filename']
        
        with file_locks[filename]:
            success = download_file(file_info, current_size, local_path)
            if not success:
                logging.warning(f"Failed to complete download for {filename}. It will be retried in the next run.")
        
        queue.task_done()

def main():
    with open('nasa_climate_data_info.csv', 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        file_info_list = list(reader)
    
    files_to_download = get_files_to_download(file_info_list)
    
    total_files = len(file_info_list)
    files_to_download_count = len(files_to_download)
    print(f"Total files: {total_files}")
    print(f"Files to download or resume: {files_to_download_count}")
    
    if not files_to_download:
        logging.info("All files are already downloaded completely.")
        print("All files are already downloaded completely.")
        return
    
    queue = Queue()
    for file_info, current_size, local_path in files_to_download:
        queue.put((file_info, current_size, local_path))
        filename = file_info['filename']
        if filename not in file_locks:
            file_locks[filename] = Lock()
    
    num_workers = 5
    threads = []
    for _ in range(num_workers):
        thread = Thread(target=download_worker, args=(queue,))
        thread.start()
        threads.append(thread)
    
    queue.join()
    
    for _ in range(num_workers):
        queue.put((None, None, None))
    for thread in threads:
        thread.join()

    print("Download process completed.")
    logging.info("Download process completed.")

if __name__ == "__main__":
    try:
        main()
        print("Download process completed successfully.")
        sys.exit(0)
    except KeyboardInterrupt:
        print("Process interrupted by user.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unhandled exception: {e}", exc_info=True)
        print(f"An error occurred: {e}")
        sys.exit(1)
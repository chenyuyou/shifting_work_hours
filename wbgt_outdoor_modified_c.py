import xarray as xr
import numpy as np
import os
import glob
import json
import threading
from queue import Queue
from tqdm import tqdm
from liljegren_cuda_vectorized_c import wbgt_liljegren_vectorized
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def preprocess_data(ds):
    """Preprocess the dataset to handle NaN values and invalid data appropriately."""
    for var in ds.data_vars:
        ds[var] = xr.where(np.isinf(ds[var]), np.nan, ds[var])
        
        if var == 'sfcWind':
            ds[var] = xr.where(ds[var] < 0, 0, ds[var])  # 将负风速设为0
        
        if var == 'rsds':
            ds[var] = xr.where(ds[var] < 0, 0, ds[var])  # 将负辐射设为0
        
        if var in ['tas', 'tasmax']:
            ds[var] = xr.where((ds[var] < 180) | (ds[var] > 330), np.nan, ds[var])
        
        if var == 'hurs':
            ds[var] = xr.where((ds[var] < 0) | (ds[var] > 100), np.nan, ds[var])
            ds[var] = xr.where(ds[var] > 100, 100, ds[var])  # 将大于100的湿度限制在100

    return ds

def process_files_for_year(base_path, year, output_dir):
    logger.info(f"Processing files for year {year}")
    try:
        variables = ['tas', 'tasmax', 'hurs', 'sfcWind', 'rsds']
        datasets = {}

        for var in variables:
            file = find_matching_file(os.path.join(base_path, var), var, year)
            if not file:
                return {'status': 'error', 'year': year, 'error': f"Missing file for {var}"}
            datasets[var] = xr.open_dataset(file)

        # Merge all datasets
        combined_ds = xr.merge(datasets.values())

        # Preprocess the data
        combined_ds = preprocess_data(combined_ds)


        
        # Log information about the preprocessed data
        for var in combined_ds.data_vars:
            data = combined_ds[var].values
            logger.info(f"Preprocessed {var}: min={np.nanmin(data):.2f}, max={np.nanmax(data):.2f}, "
                        f"nan_count={np.isnan(data).sum()}")

        logger.info("Calculating WBGT")
        wbgt_min, wbgt_max, t_nwb, t_g = wbgt_liljegren_vectorized(combined_ds)

        # Create the WBGT dataset
        wbgt_ds = xr.Dataset({
            'WBGTmin_od': (['time', 'lat', 'lon'], wbgt_min),
            'WBGTmax_od': (['time', 'lat', 'lon'], wbgt_max),
#            'Tnwb': (['time', 'lat', 'lon'], t_nwb),
#            'Tg': (['time', 'lat', 'lon'], t_g)
        },
        coords={
            'time': combined_ds.time,
            'lat': combined_ds.lat,
            'lon': combined_ds.lon
        })

        # Add half WBGT
        wbgt_ds['WBGThalf_od'] = (wbgt_ds['WBGTmin_od'] + wbgt_ds['WBGTmax_od']) / 2

        for var in wbgt_ds.data_vars:
            wbgt_ds[var].attrs['units'] = 'degC'
            wbgt_ds[var].attrs['long_name'] = f'Outdoor Wet Bulb Globe Temperature ({var})'

        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"outdoor_wbgt_day_{year}.nc")
        wbgt_ds.to_netcdf(output_file)
        logger.info(f"Saved WBGT dataset to {output_file}")

        return {'status': 'success', 'year': year, 'output': output_file}

    except Exception as e:
        logger.exception(f"Error processing files for year {year}")
        return {'status': 'error', 'year': year, 'error': str(e)}
    

def find_matching_file(base_path, variable, year):
    pattern = f"{variable}_day_*_{year}*.nc"
    matching_files = glob.glob(os.path.join(base_path, pattern))
    return matching_files[0] if matching_files else None

def worker(task_queue, result_list, status, status_lock, pbar):
    while True:
        task = task_queue.get()
        if task is None:
            break
        model, scenario, year, base_dir, output_base_dir = task
        model_scenario_dir = os.path.join(base_dir, model, scenario, 'r1i1p1f1')
        output_dir = os.path.join(output_base_dir, model, scenario, 'r1i1p1f1')
        
        status_key = f"{model}_{scenario}_{year}"
        
        with status_lock:
            if status.get(status_key, {}).get('status') == 'success':
                result_list.append({'status': 'success', 'year': year, 'message': 'Already processed'})
                pbar.update(1)
                task_queue.task_done()
                continue

        result = process_files_for_year(model_scenario_dir, str(year), output_dir)
        
        with status_lock:
            status[status_key] = result
            result_list.append(result)
        
        pbar.update(1)
        task_queue.task_done()

def process_all_models(base_dir, output_base_dir, status_file, num_threads=4):
    with open(status_file, 'r') as f:
        status = json.load(f)

    status_lock = threading.Lock()
    
    models = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
    scenarios = ['SSP126', 'SSP245', 'SSP585']
    
    task_queue = Queue()
    result_list = []

    total_tasks = len(models) * len(scenarios) * (2101 - 2015)
    
    for model in models:
        for scenario in scenarios:
            for year in range(2015, 2101):
                task_queue.put((model, scenario, year, base_dir, output_base_dir))

    pbar = tqdm(total=total_tasks, desc="Processing files")

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker, args=(task_queue, result_list, status, status_lock, pbar))
        t.start()
        threads.append(t)

    task_queue.join()

    for _ in range(num_threads):
        task_queue.put(None)
    for t in threads:
        t.join()

    pbar.close()

    with status_lock:
        with open(status_file, 'w') as f:
            json.dump(status, f)

    successful = [r for r in result_list if r['status'] == 'success']
    failed = [r for r in result_list if r['status'] == 'error']
    
    return successful, failed

if __name__ == "__main__":
    base_dir = "./china_output"
    output_base_dir = "./wbgt_outdoor_output"
    status_file = "wbgt_outdoor_processing_status.json"
    
    if not os.path.exists(status_file):
        with open(status_file, 'w') as f:
            json.dump({}, f)

    try:
        successful, failed = process_all_models(base_dir, output_base_dir, status_file, num_threads=1)

        logger.info(f"Processing completed!")
        logger.info(f"Total files processed: {len(successful) + len(failed)}")
        logger.info(f"Successful: {len(successful)}")
        logger.info(f"Failed: {len(failed)}")

        if failed:
            logger.error("Failed processing:")
            for result in failed:
                logger.error(f"  Year {result['year']}: {result['error']}")
    except Exception as e:
        logger.exception("An error occurred during processing")
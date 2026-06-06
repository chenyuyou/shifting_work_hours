import xarray as xr
import numpy as np
import os
import glob
import json
import threading
from queue import Queue
from tqdm import tqdm

def calculate_indoor_wbgt(tas, tasmax, hurs):
    def calculate_wbt(T, RH):
        return T * np.arctan(0.151977 * np.sqrt(RH + 8.313659)) + np.arctan(T + RH) - np.arctan(RH - 1.676331) + 0.00391838 * (RH ** 1.5) * np.arctan(0.023101 * RH) - 4.686035
    
    WBTm = calculate_wbt(tas, hurs)
    WBTmax = calculate_wbt(tasmax, hurs)
    
    WBGTmin_id = 0.7 * WBTm + 0.3 * tas
    WBGTmax_id = 0.7 * WBTmax + 0.3 * tasmax
    WBGThalf_id = (WBGTmax_id + WBGTmin_id) / 2
    
    return WBGTmin_id, WBGTmax_id, WBGThalf_id

def read_nc_file(file_path):
    return xr.open_dataset(file_path)

def find_matching_file(base_path, variable, year):
    pattern = f"{variable}_day_*_{year}*.nc"
    matching_files = glob.glob(os.path.join(base_path, pattern))
    return matching_files[0] if matching_files else None

def process_files_for_year(base_path, year, output_dir):
    try:
        tas_file = find_matching_file(os.path.join(base_path, 'tas'), 'tas', year)
        tasmax_file = find_matching_file(os.path.join(base_path, 'tasmax'), 'tasmax', year)
        hurs_file = find_matching_file(os.path.join(base_path, 'hurs'), 'hurs', year)

        if not (tas_file and tasmax_file and hurs_file):
            missing = []
            if not tas_file: missing.append('tas')
            if not tasmax_file: missing.append('tasmax')
            if not hurs_file: missing.append('hurs')
            return {'status': 'error', 'year': year, 'error': f"Missing files for {', '.join(missing)}"}

        tas_ds = read_nc_file(tas_file)
        tasmax_ds = read_nc_file(tasmax_file)
        hurs_ds = read_nc_file(hurs_file)

        tas = tas_ds['tas'].values - 273.15
        tasmax = tasmax_ds['tasmax'].values - 273.15
        hurs = hurs_ds['hurs'].values

        WBGTmin_id, WBGTmax_id, WBGThalf_id = calculate_indoor_wbgt(tas, tasmax, hurs)

        wbgt_ds = xr.Dataset({
            'WBGTmin_id': (['time', 'lat', 'lon'], WBGTmin_id),
            'WBGTmax_id': (['time', 'lat', 'lon'], WBGTmax_id),
            'WBGThalf_id': (['time', 'lat', 'lon'], WBGThalf_id)
        },
        coords={
            'time': tas_ds.time,
            'lat': tas_ds.lat,
            'lon': tas_ds.lon
        })

        for var in wbgt_ds.data_vars:
            wbgt_ds[var].attrs['units'] = 'degC'
            wbgt_ds[var].attrs['long_name'] = f'Indoor Wet Bulb Globe Temperature ({var})'

        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"indoor_wbgt_day_{os.path.basename(tas_file)}")
        wbgt_ds.to_netcdf(output_file)

        return {'status': 'success', 'year': year, 'output': output_file}

    except Exception as e:
        return {'status': 'error', 'year': year, 'error': str(e)}

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

    # Block until all tasks are done
    task_queue.join()

    # Stop workers
    for _ in range(num_threads):
        task_queue.put(None)
    for t in threads:
        t.join()

    pbar.close()

    # Final update of status file
    with status_lock:
        with open(status_file, 'w') as f:
            json.dump(status, f)

    successful = [r for r in result_list if r['status'] == 'success']
    failed = [r for r in result_list if r['status'] == 'error']
    
    return successful, failed

if __name__ == "__main__":
    base_dir = "./china_output"  # Update this to match your input directory
    output_base_dir = "./wbgt_indoor_output"
    status_file = "wbgt_indoor_processing_status.json"
    
    # Create status file if it doesn't exist
    if not os.path.exists(status_file):
        with open(status_file, 'w') as f:
            json.dump({}, f)

    successful, failed = process_all_models(base_dir, output_base_dir, status_file, num_threads=4)

    print(f"\nProcessing completed!")
    print(f"Total files processed: {len(successful) + len(failed)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed processing:")
        for result in failed:
            print(f"  Year {result['year']}: {result['error']}")
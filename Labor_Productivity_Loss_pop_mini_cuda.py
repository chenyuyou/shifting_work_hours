import cupy as cp
import xarray as xr
import numpy as np
import os
import json
import threading
from queue import Queue
from tqdm import tqdm
import warnings
import glob
import pandas as pd

# Suppress the specific RuntimeWarning
warnings.filterwarnings("ignore", category=RuntimeWarning, message="Mean of empty slice")

# CuPy version of calculate_productivity_factors
def calculate_productivity_factors_gpu(wbgt, intensity):
    if intensity == 'low':
        return 0.9 - 0.9 / (1 + (wbgt / 34.64)**22.72)
    elif intensity == 'medium':
        return 0.9 - 0.9 / (1 + (wbgt / 32.93)**17.81)
    else:  # high intensity
        return 0.9 - 0.9 / (1 + (wbgt / 30.94)**16.64)

def find_indoor_file(base_path, model, scenario, year):
    patterns = [
        f"indoor_wbgt_day_tas_day_{model}_{scenario}_r1i1p1f1_*_{year}_v1.2.nc",
        f"indoor_wbgt_day_tas_day_{model}_{scenario}_r1i1p1f1_*_{year}_v1.1.nc",
        f"indoor_wbgt_day_tas_day_{model}_{scenario}_r1i1p1f1_*_{year}.nc"
    ]
    for pattern in patterns:
        full_pattern = os.path.join(base_path, '**', pattern)

        matching_files = glob.glob(full_pattern, recursive=True)
        if matching_files:

            return matching_files[0]

    return None

def find_outdoor_file(base_path, model, scenario, year):
    pattern = f"outdoor_wbgt_day_{year}.nc"
    full_pattern = os.path.join(base_path, 
                                "wbgt_outdoor_output", 
                                model, 
                                scenario, 
                                "r1i1p1f1",
                                pattern)
    
    if os.path.exists(full_pattern):
        return full_pattern
    
    return None


def process_model_scenario_year(base_dir, model, scenario, year):
    try:
        indoor_file = find_indoor_file(base_dir, model, scenario, year)
        if not indoor_file:
            return {'status': 'error', 'message': f"No indoor file found for year {year}"}
        
        outdoor_file = find_outdoor_file(base_dir, model, scenario, year)
        if not outdoor_file:
            return {'status': 'error', 'message': f"No outdoor file found for year {year}"}
        
        indoor_ds = xr.open_dataset(indoor_file)
        outdoor_ds = xr.open_dataset(outdoor_file)
        
 
        
        losses = {}
        for intensity in ['low', 'medium', 'high']:
            ds = indoor_ds if intensity in ['low', 'medium'] else outdoor_ds
            wbgt_vars = ['WBGTmax_id', 'WBGTmin_id', 'WBGThalf_id'] if intensity in ['low', 'medium'] else ['WBGTmax_od', 'WBGTmin_od', 'WBGThalf_od']
            
            losses[intensity] = {}
            for metric, var in zip(['max', 'min', 'half'], wbgt_vars):
                # Get the DataArray
                da = ds[var]
                

                
                # Identify the time dimension
                time_dims = [dim for dim in da.dims if dim.lower() in ['time', 'day', 'date']]
                if not time_dims:
                    raise ValueError(f"No time dimension found in {var}. Available dimensions: {da.dims}")
                time_dim = time_dims[0]
                
                # Get the axis number corresponding to the time dimension
                time_axis = da.dims.index(time_dim)
                
                # Move computation to GPU
                wbgt_gpu = cp.asarray(da.values)
                
                # Compute productivity factors
                result_gpu = calculate_productivity_factors_gpu(wbgt_gpu, intensity)
                
                # Perform time averaging on GPU
                result_mean_gpu = cp.nanmean(result_gpu, axis=time_axis)
                
                # Move result back to CPU and store
                losses[intensity][metric] = cp.asnumpy(result_mean_gpu)
        
        # Create xarray Dataset
        combined_loss = xr.Dataset({
            f"{intensity}_{metric}": (('lat', 'lon'), losses[intensity][metric])
            for intensity in ['low', 'medium', 'high']
            for metric in ['max', 'min', 'half']
        })
        
        # Convert year to datetime64[ns]
        time = pd.to_datetime(f"{year}", format="%Y")

        combined_loss = combined_loss.assign_coords(
            time=time,
            lat=indoor_ds.lat,
            lon=indoor_ds.lon
        )
        
        return {'status': 'success', 'data': combined_loss}
    except Exception as e:
        return {'status': 'error', 'message': f"Error processing year {year}: {str(e)}"}

def worker(task_queue, result_dict, status, status_lock, pbar):
    while True:
        task = task_queue.get()
        if task is None:
            break
        model, scenario, year, base_dir = task
        
        status_key = f"{model}_{scenario}_{year}"
        
        with status_lock:
            if status.get(status_key, {}).get('status') == 'success':
                result_dict[status_key] = {'status': 'success', 'message': 'Already processed'}
                pbar.update(1)
                task_queue.task_done()
                continue
        
        result = process_model_scenario_year(base_dir, model, scenario, year)
        
        with status_lock:
            status[status_key] = {'status': result['status'], 'message': result.get('message', '')}
            result_dict[status_key] = result
        
        pbar.update(1)
        task_queue.task_done()


def process_loss_with_population(loss_data, population_file):
    # Read population data
    pop_ds = xr.open_dataset(population_file)
    
    # Ensure that both datasets have the same coordinate system
    loss_data = loss_data.assign_coords(lon=(loss_data.lon % 360))
    pop_ds = pop_ds.assign_coords(lon=(pop_ds.lon % 360))
    
    # Rename 'StdTime' to 'time' in population data for consistency
    pop_ds = pop_ds.rename({'StdTime': 'time'})
    
    # Align time coordinates
    common_times = np.intersect1d(loss_data.time, pop_ds.time)
    loss_data = loss_data.sel(time=common_times)
    pop_ds = pop_ds.sel(time=common_times)
    
    # Find the intersection of spatial coordinates
    common_lats = np.intersect1d(loss_data.lat, pop_ds.lat)
    common_lons = np.intersect1d(loss_data.lon, pop_ds.lon)
    
    # Subset both datasets to the common coordinates
    loss_data = loss_data.sel(lat=common_lats, lon=common_lons)
    pop_ds = pop_ds.sel(lat=common_lats, lon=common_lons)
    
    # Extract the population data for the common coordinates
    population = pop_ds.pop.values
    
    # Initialize an empty dictionary to store results
    weighted_results = {}
    
    # Process each variable separately
    for var in loss_data.data_vars:
        # Extract the current variable and population data
        loss_var = loss_data[var].values
        
        # Move data to GPU
        loss_gpu = cp.array(loss_var)
        pop_gpu = cp.array(population)
        
        # Replace NaN with 0 in population data
        pop_gpu = cp.nan_to_num(pop_gpu, 0)
        
        # Ensure population data has the same shape as loss data
        pop_gpu = cp.broadcast_to(pop_gpu, loss_gpu.shape)
        
        # Multiply loss data with population
        weighted_loss_gpu = loss_gpu * pop_gpu
        
        # Replace NaN and inf with 0 in the result
        weighted_loss_gpu = cp.nan_to_num(weighted_loss_gpu, 0)
        
        # Move result back to CPU
        weighted_results[var] = cp.asnumpy(weighted_loss_gpu)
    
    # Create a new xarray Dataset with the weighted loss and population data
    weighted_ds = xr.Dataset(
        {var: (('time', 'lat', 'lon'), weighted_results[var]) for var in loss_data.data_vars},
        coords={'time': common_times, 'lat': common_lats, 'lon': common_lons}
    )
    
    # Add the population data to the dataset
    weighted_ds['population'] = (('time', 'lat', 'lon'), population)
    
    return weighted_ds


def process_all_models(base_dir, output_dir, status_file, population_file, num_threads=8):
    with open(status_file, 'r') as f:
        status = json.load(f)

    status_lock = threading.Lock()
    
    models = ['EC-Earth3']
#    models = ['EC-Earth3', 'GFDL-ESM4', 'IPSL-CM6A-LR', 'NorESM2-MM']
    scenarios = ['SSP126']
#    scenarios = ['SSP126', 'SSP245', 'SSP585']
    task_queue = Queue()
    result_dict = {}

    total_tasks = len(models) * len(scenarios) * (2101 - 2015)
    
    for model in models:
        for scenario in scenarios:
            for year in range(2015, 2101):
                task_queue.put((model, scenario, year, base_dir))

    pbar = tqdm(total=total_tasks, desc="Processing files")

    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=worker, args=(task_queue, result_dict, status, status_lock, pbar))
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

    # Process results
    for model in models:
        for scenario in scenarios:
            all_years_data = []
            for year in range(2015, 2101):
                status_key = f"{model}_{scenario}_{year}"
                if result_dict[status_key]['status'] == 'success' and 'data' in result_dict[status_key]:
                    all_years_data.append(result_dict[status_key]['data'])
            
            if all_years_data:
                result = xr.concat(all_years_data, dim='time')
                
                # Process loss with population
                weighted_result = process_loss_with_population(result, population_file)
                
                output_file = os.path.join(output_dir, f'weighted_productivity_loss_{model}_{scenario}.nc')
                weighted_result.to_netcdf(output_file)
                print(f"Saved weighted results to {output_file}")
            else:
                print(f"No valid data for {model} - {scenario}")

    successful = [r for r in result_dict.values() if r['status'] == 'success']
    failed = [r for r in result_dict.values() if r['status'] == 'error']
    
    return successful, failed

if __name__ == "__main__":
    base_dir = "./model_outputs"  # Directory containing model outputs
    output_dir = "./weighted_productivity_loss_output"
    status_file = "productivity_loss_processing_status.json"
    population_file = "./model_outputs/pop_245_025.nc"
    
    # Create status file if it doesn't exist
    if not os.path.exists(status_file):
        with open(status_file, 'w') as f:
            json.dump({}, f)

    os.makedirs(output_dir, exist_ok=True)
    
    successful, failed = process_all_models(base_dir, output_dir, status_file, population_file, num_threads=8)

    print(f"\nProcessing completed!")
    print(f"Total files processed: {len(successful) + len(failed)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed processing:")
        for result in failed:
            print(f"  {result['message']}")
import xarray as xr
import os
from pathlib import Path
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def process_file(file_path, china_bounds, output_file):
    try:
        with xr.open_dataset(file_path) as ds:
            ds_c = ds.sel(lat=slice(china_bounds['lat_min'], china_bounds['lat_max']),
                          lon=slice(china_bounds['lon_min'], china_bounds['lon_max']))
            ds_c.to_netcdf(output_file)
        return {'status': 'success', 'file': file_path}
    except Exception as e:
        return {'status': 'error', 'file': file_path, 'error': str(e)}

def process_files(file_list, china_bounds, output_base_dir, status_file, batch_size=2, num_workers=8):
    with open(status_file, 'r') as f:
        status = json.load(f)

    to_process = [f for f in file_list if f not in status or status[f] == 'error']

    results = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_file = {}
        for i in range(0, len(to_process), batch_size):
            batch = to_process[i:i+batch_size]
            for file_path in batch:
                output_file = os.path.join(output_base_dir, os.path.relpath(file_path, start='./downloaded_data'))
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                future = executor.submit(process_file, file_path, china_bounds, output_file)
                future_to_file[future] = file_path

        for future in tqdm(as_completed(future_to_file), total=len(to_process), desc="Processing files"):
            result = future.result()
            results.append(result)
            status[result['file']] = result['status']
            
            # 每处理10个文件更新一次状态文件
            if len(results) % 10 == 0:
                with open(status_file, 'w') as f:
                    json.dump(status, f)

    # 最后再次更新状态文件，确保所有结果都被保存
    with open(status_file, 'w') as f:
        json.dump(status, f)

    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'error']
    
    return successful, failed

def get_file_list(base_dir):
    models = ['EC-Earth3', 'GFDL-ESM4', 'IPSL-CM6A-LR', 'NorESM2-MM']
    scenarios = ['SSP126', 'SSP245', 'SSP585']
    variables = ['hurs', 'rsds', 'tas', 'tasmax', 'sfcWind']
    
    file_list = []
    for model in models:
        for scenario in scenarios:
            for variable in variables:
                path = Path(base_dir) / model / scenario / 'r1i1p1f1' / variable
                file_list.extend(path.glob('*.nc'))
    
    return [str(file) for file in file_list]

if __name__ == "__main__":
    base_dir = "./downloaded_data"
    china_bounds_file = "china_bounds_file.json"
    output_base_dir = "D:/china_output"
    status_file = "processing_status.json"
    
    with open(china_bounds_file, 'r') as f:
        china_bounds = json.load(f)

    # 如果状态文件不存在，创建一个空的状态文件
    if not os.path.exists(status_file):
        with open(status_file, 'w') as f:
            json.dump({}, f)

    all_files = get_file_list(base_dir)
    
    successful, failed = process_files(all_files, china_bounds, output_base_dir, status_file)

    print(f"\nProcessing completed!")
    print(f"Total files processed: {len(successful) + len(failed)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed files:")
        for result in failed:
            print(f"  {result['file']}: {result['error']}")
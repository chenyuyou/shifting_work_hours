import xarray as xr
import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import json

def filter_encoding(encoding, var_shape, var_dtype):
    valid_keys = {'zlib', 'complevel', 'shuffle', 'fletcher32', 'contiguous', 'chunksizes', 'compression'}
    filtered = {k: v for k, v in encoding.items() if k in valid_keys}
    filtered['dtype'] = var_dtype
    if 'chunksizes' in filtered:
        filtered['chunksizes'] = tuple(min(c, s) for c, s in zip(filtered['chunksizes'], var_shape))
    if 'zlib' not in filtered or not filtered['zlib']:
        filtered['zlib'] = True
        filtered['complevel'] = 4
    return filtered

def extract_matching_region(file_a, china_bounds, output_file):
    try:
        with xr.open_dataset(file_a) as ds_a:
            ds_c = ds_a.sel(lat=slice(china_bounds['lat_min'], china_bounds['lat_max']),
                            lon=slice(china_bounds['lon_min'], china_bounds['lon_max']))

            encoding = {var: filter_encoding(ds_a[var].encoding, ds_c[var].shape, ds_a[var].dtype) 
                        for var in ds_c.data_vars}

            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            ds_c.to_netcdf(output_file, encoding=encoding)

        return {'status': 'success', 'file': file_a}
    except Exception as e:
        return {'status': 'error', 'file': file_a, 'error': str(e)}

def process_files(file_list, china_bounds, output_base_dir, status_file, max_workers):
    with open(status_file, 'r') as f:
        status = json.load(f)

    to_process = [f for f in file_list if f not in status or status[f] == 'error']

    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(extract_matching_region, f, china_bounds, 
                                   os.path.join(output_base_dir, os.path.relpath(f, start='./'))) 
                   for f in to_process]
        
        for future in tqdm(as_completed(futures), total=len(to_process), desc="Processing files"):
            result = future.result()
            results.append(result)
            status[result['file']] = result['status']
            
            if len(results) % 10 == 0:
                with open(status_file, 'w') as f:
                    json.dump(status, f)

    with open(status_file, 'w') as f:
        json.dump(status, f)

    return results

def get_file_list(base_dir):
    return [str(file) for file in Path(base_dir).rglob('*.nc')]

if __name__ == "__main__":
    base_dir = "./"
    china_bounds_file = "china_bounds_file.json"
    output_base_dir = "china_output/"
    status_file = "processing_status.json"

    with open(china_bounds_file, 'r') as f:
        china_bounds = json.load(f)

    if not os.path.exists(status_file):
        with open(status_file, 'w') as f:
            json.dump({}, f)

    all_files = get_file_list(base_dir)
    max_workers = 12

    results = process_files(all_files, china_bounds, output_base_dir, status_file, max_workers)

    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'error']

    print(f"\nProcessing completed!")
    print(f"Total files processed: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed files:")
        for r in failed:
            print(f"  {r['file']}: {r['error']}")
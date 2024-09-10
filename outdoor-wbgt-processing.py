import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
import regionmask
import os
import glob
from datetime import datetime

def create_province_masks(data, province_geojson_file):
    """创建中国各省份的地理掩码。"""
    province_gdf = gpd.read_file(province_geojson_file)
    province_masks = {}
    for idx, row in province_gdf.iterrows():
        province_region = regionmask.Regions([row.geometry])
        province_mask = province_region.mask(data.lon, data.lat)
        province_masks[row['name']] = province_mask
    return province_masks

def find_outdoor_files(base_path, scenario):
    pattern = os.path.join(base_path, "wbgt_outdoor_output", "*", scenario, "r1i1p1f1", "outdoor_wbgt_day_2100.nc")
    return glob.glob(pattern)

def process_scenario(scenario, base_dir, province_masks):
    files = find_outdoor_files(base_dir, scenario)
    if not files:
        print(f"No files found for scenario {scenario}")
        return None

    all_model_data = []
    for file in files:
        ds = xr.open_dataset(file)
        
        # 选择6月1日到8月31日的数据
        summer_data = ds.sel(time=slice('2100-06-01', '2100-08-31'))
        
        # 计算夏季平均值
        summer_mean = summer_data.mean(dim='time')
        
        all_model_data.append(summer_mean)
    
    # 计算所有模型的平均值
    scenario_mean = sum(all_model_data) / len(all_model_data)
    
    results = {var: {} for var in ['WBGTmax_od', 'WBGTmin_od', 'WBGThalf_od']}
    for var in results.keys():
        for province, mask in province_masks.items():
            masked_data = scenario_mean[var].where(mask == 0)
            province_mean = masked_data.mean(dim=['lat', 'lon']).values
            results[var][province] = float(province_mean)
    
    return results

def save_to_excel(all_results, output_dir):
    output_file = os.path.join(output_dir, 'outdoor_wbgt_2100_summer_average_all_scenarios.xlsx')
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for var in ['WBGTmax_od', 'WBGTmin_od', 'WBGThalf_od']:
            df = pd.DataFrame({scenario: results[var] for scenario, results in all_results.items()})
            df.index.name = 'Province'
            df.to_excel(writer, sheet_name=var)
    
    print(f"Results for all scenarios saved to {output_file}")

def process_all_scenarios(base_dir, output_dir, province_geojson_file):
    scenarios = ['SSP126', 'SSP245', 'SSP585']
    
    # 创建省份掩码
    first_file = glob.glob(os.path.join(base_dir, "wbgt_outdoor_output", "*", "*", "r1i1p1f1", "outdoor_wbgt_day_2100.nc"))[0]
    first_ds = xr.open_dataset(first_file)
    province_masks = create_province_masks(first_ds, province_geojson_file)

    all_results = {}
    for scenario in scenarios:
        print(f"Processing scenario: {scenario}")
        scenario_results = process_scenario(scenario, base_dir, province_masks)
        if scenario_results:
            all_results[scenario] = scenario_results

    save_to_excel(all_results, output_dir)

if __name__ == "__main__":
    base_dir = "./model_outputs"  # 包含模型输出的目录
    output_dir = "./outdoor_wbgt_output"
    province_geojson_file = "./model_outputs/cn_fensheng.json"
    
    os.makedirs(output_dir, exist_ok=True)
    
    process_all_scenarios(base_dir, output_dir, province_geojson_file)

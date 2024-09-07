import xarray as xr
import geopandas as gpd
import numpy as np
import pandas as pd
import regionmask
import glob
import os
import time

def create_masks(data, china_geojson_file, province_geojson_file):
    """
    创建中国整体和各省份的地理掩码。
    
    参数:
    data: xarray.Dataset, 包含经纬度信息的数据集
    china_geojson_file: str, 中国地理边界文件路径
    province_geojson_file: str, 中国各省份地理边界文件路径
    
    返回:
    tuple: (中国掩码, 省份掩码字典, 省份地理数据框)
    """
    # 创建中国整体掩码
    china_gdf = gpd.read_file(china_geojson_file)
    china_mask = regionmask.Regions([china_gdf.geometry.iloc[0]])
    china_mask = china_mask.mask(data.lon, data.lat)
    
    # 创建各省份掩码
    province_gdf = gpd.read_file(province_geojson_file)
    province_masks = {}

    for idx, row in province_gdf.iterrows():
        province_region = regionmask.Regions([row.geometry])
        province_mask = province_region.mask(data.lon, data.lat)
        province_masks[row['name']] = province_mask

    return china_mask, province_masks, province_gdf

def mask_and_aggregate(data, mask, population):
    """
    使用给定的掩码对数据进行掩膜处理，并计算总和。
    
    参数:
    data: xarray.Dataset, 包含已经乘以人口的气候数据的数据集
    mask: xarray.DataArray, 地理掩码
    population: xarray.DataArray, 人口数据
    
    返回:
    tuple: (数据总和, 总人口)
    """
    masked_data = data.where(mask == 0)
    masked_population = population.where(mask == 0)
    
    total_population = masked_population.sum(dim=['lat', 'lon'])
    total_data = masked_data.sum(dim=['lat', 'lon'])
    
    return total_data, total_population

def process_file(file_path, china_mask, province_masks):
    """
    处理单个NetCDF文件，应用地理掩码并计算总和。
    
    参数:
    file_path: str, NetCDF文件路径
    china_mask: xarray.DataArray, 中国整体掩码
    province_masks: dict, 各省份掩码字典
    
    返回:
    dict: 包含处理后的中国整体和各省份数据
    """
    data = xr.open_dataset(file_path)
    filename = os.path.basename(file_path)
    parts = filename.split('_')
    model = parts[-2]
    scenario = parts[-1].split('.')[0]

    results = {'China': {}, 'Provinces': {}, 'model': model, 'scenario': scenario}
    
    # 处理中国整体数据
    china_data, china_population = mask_and_aggregate(data, china_mask, data['population'])
    results['China']['data'] = china_data
    results['China']['population'] = china_population

    # 处理各省份数据
    for province, mask in province_masks.items():
        province_data, province_population = mask_and_aggregate(data, mask, data['population'])
        results['Provinces'][province] = {
            'data': province_data,
            'population': province_population
        }

    return results

def adjust_working_hours(province):
    """
    根据省份的日出时间调整工作时间权重。
    
    参数:
    province: str, 省份名称
    
    返回:
    dict: 调整后的时间权重
    """
    sunrise_times = {
        4: ['吉林省', '辽宁省'],
        5: ['黑龙江省','安徽省', '北京市', '福建省', '河北省', '河南省', '湖北省', '江苏省', '内蒙古自治区', '山东省', '上海市', '山西省', '天津市', '浙江省'],
        6: ['重庆市', '甘肃省', '广东省', '广西壮族自治区', '贵州省', '海南省', '湖南省', '江西省', '宁夏回族自治区', '青海省', '陕西省', '四川省'],
        7: ['新疆维吾尔自治区', '西藏自治区', '云南省']
    }
    
    for sunrise, provinces in sunrise_times.items():
        if province in provinces:
            if sunrise == 4:
                return {'Ym': 0.75, 'Ymax': 0, 'Yhalf': 0.25}
            elif sunrise == 5:
                return {'Ym': 0.625, 'Ymax': 0, 'Yhalf': 0.375}
            elif sunrise == 6:
                return {'Ym': 0.5, 'Ymax': 0, 'Yhalf': 0.5}
            elif sunrise == 7:
                return {'Ym': 0.375, 'Ymax': 0.125, 'Yhalf': 0.5}
    
    return {'Ym': 0.25, 'Ymax': 0.25, 'Yhalf': 0.5}  # 默认值

def calculate_labor_productivity_loss(data, adjusted=False):
    """
    计算劳动生产率损失，可选择是否进行工作时间调整。
    
    参数:
    data: dict, 包含中国整体和各省份数据
    adjusted: bool, 是否应用工作时间调整
    
    返回:
    dict: 包含中国整体和各省份的劳动生产率损失
    """
    results = {'China': {}, 'Provinces': {}}
    
    def calculate_loss(region_data, factors):
        loss = {}
        for intensity in ['low', 'medium', 'high']:
            loss[intensity] = (
                factors['Ym'] * region_data[f'{intensity}_min'] +
                factors['Ymax'] * region_data[f'{intensity}_max'] +
                factors['Yhalf'] * region_data[f'{intensity}_half']
            )
        return loss
    
    # 计算各省份损失
    total_loss = {intensity: 0 for intensity in ['low', 'medium', 'high']}
    total_population = 0
    
    for province, province_data in data['Provinces'].items():
        factors = adjust_working_hours(province) if adjusted else {'Ym': 0.25, 'Ymax': 0.25, 'Yhalf': 0.5}
        province_loss = calculate_loss(province_data['data'], factors)
        results['Provinces'][province] = {intensity: loss / province_data['population'] for intensity, loss in province_loss.items()}
        
        # 累加总损失和总人口
        for intensity in ['low', 'medium', 'high']:
            total_loss[intensity] += province_loss[intensity]
        total_population += province_data['population']
    
    # 计算中国整体损失
    results['China'] = {intensity: total_loss[intensity] / total_population for intensity in ['low', 'medium', 'high']}
    
    return results

def calculate_difference(unadjusted, adjusted):
    """
    计算调整前后的劳动生产率损失差异。
    
    参数:
    unadjusted: dict, 未调整的劳动生产率损失
    adjusted: dict, 调整后的劳动生产率损失
    
    返回:
    dict: 调整前后的差异
    """
    diff_results = {'China': {}, 'Provinces': {}}
    
    for region in ['China', 'Provinces']:
        if region == 'China':
            diff_results[region] = {intensity: adjusted[region][intensity] - unadjusted[region][intensity]
                                    for intensity in ['low', 'medium', 'high']}
        else:
            diff_results[region] = {province: {} for province in unadjusted[region]}
            for province in unadjusted[region]:
                diff_results[region][province] = {intensity: adjusted[region][province][intensity] - unadjusted[region][province][intensity]
                                                  for intensity in ['low', 'medium', 'high']}  
    return diff_results

def calculate_scenario_statistics(results):
    """
    计算不同情景下的统计数据。
    
    参数:
    results: list, 包含所有模型和情景的结果
    
    返回:
    dict: 各情景的统计数据
    """
    scenarios = set(result['scenario'] for result in results)
    scenario_stats = {scenario: {'China': {}, 'Provinces': {}} for scenario in scenarios}

    for scenario in scenarios:
        scenario_results = [result for result in results if result['scenario'] == scenario]
        
        # 计算中国整体统计数据
        for adj in ['unadjusted', 'adjusted', 'difference']:
            for intensity in ['low', 'medium', 'high']:
                data = np.array([result['China'][adj][intensity] for result in scenario_results])
                scenario_stats[scenario]['China'][f'{adj}_{intensity}'] = {
                    'mean': np.mean(data, axis=0),
                    'max': np.max(data, axis=0),
                    'min': np.min(data, axis=0)
                }
        
        # 计算各省份统计数据
        provinces = scenario_results[0]['Provinces']['unadjusted'].keys()
        for province in provinces:
            scenario_stats[scenario]['Provinces'][province] = {}
            for adj in ['unadjusted', 'adjusted', 'difference']:
                for intensity in ['low', 'medium', 'high']:
                    data = np.array([result['Provinces'][adj][province][intensity] for result in scenario_results])
                    scenario_stats[scenario]['Provinces'][province][f'{adj}_{intensity}'] = {
                        'mean': np.mean(data, axis=0),
                        'max': np.max(data, axis=0),
                        'min': np.min(data, axis=0)
                    }   
    return scenario_stats

def save_scenario_results(scenario_stats, output_directory):
    """
    保存情景统计结果到CSV文件。
    
    参数:
    scenario_stats: dict, 情景统计数据
    output_directory: str, 输出目录路径
    """
    for scenario, stats in scenario_stats.items():
        scenario_dir = os.path.join(output_directory, scenario)
        os.makedirs(scenario_dir, exist_ok=True)
        
        # 保存中国整体结果
        china_data = []
        for time_idx in range(len(stats['China']['unadjusted_low']['mean'])):
            row = {'Time': time_idx + 2015}  # 假设数据从2015年开始
            for intensity in ['low', 'medium', 'high']:
                for adj in ['unadjusted', 'adjusted']:
                    for stat in ['mean', 'max', 'min']:
                        key = f'{adj}_{intensity}'
                        row[f'{key}_{stat}'] = stats['China'][key][stat][time_idx]
                # 计算差异
                for stat in ['mean', 'max', 'min']:
                    row[f'difference_{intensity}_{stat}'] = stats['China'][f'adjusted_{intensity}'][stat][time_idx] - stats['China'][f'unadjusted_{intensity}'][stat][time_idx]
            china_data.append(row)
        
        china_df = pd.DataFrame(china_data)
        china_df.to_csv(os.path.join(scenario_dir, 'china_results.csv'), index=False, encoding='gbk')
        
        # 保存省份结果
        province_data = []
        for province in stats['Provinces']:
            for time_idx in range(len(stats['Provinces'][province]['unadjusted_low']['mean'])):
                row = {'Province': province, 'Time': time_idx + 2015}
                for intensity in ['low', 'medium', 'high']:
                    for adj in ['unadjusted', 'adjusted']:
                        for stat in ['mean', 'max', 'min']:
                            key = f'{adj}_{intensity}'
                            row[f'{key}_{stat}'] = stats['Provinces'][province][key][stat][time_idx]
                    # 计算差异
                    for stat in ['mean', 'max', 'min']:
                        row[f'difference_{intensity}_{stat}'] = stats['Provinces'][province][f'adjusted_{intensity}'][stat][time_idx] - stats['Provinces'][province][f'unadjusted_{intensity}'][stat][time_idx]
                province_data.append(row)
        
        province_df = pd.DataFrame(province_data)
        province_df.to_csv(os.path.join(scenario_dir, 'province_results.csv'), index=False, encoding='gbk')
        
        # 计算并保存汇总的省份数据
        numeric_columns = province_df.select_dtypes(include=[np.number]).columns
        aggregated_province_data = province_df.groupby('Time')[numeric_columns].mean()
        aggregated_province_data.to_csv(os.path.join(scenario_dir, 'aggregated_province_results.csv'), encoding='gbk')

def main():
    """
    主函数，协调整个数据处理和分析流程。
    """
    # 设置文件路径
    nc_directory = "./weighted_productivity_loss_output"
    china_geojson_file = "./model_outputs/中华人民共和国.json"
    province_geojson_file = "./model_outputs/中华人民共和国-分省.json"
    output_directory = "./labor_productivity_results"

    # 获取所有 NetCDF 文件
    nc_files = glob.glob(f"{nc_directory}/weighted_productivity_loss_*.nc")

    # 创建地理掩码
    first_file = xr.open_dataset(nc_files[0])
    china_mask, province_masks, _ = create_masks(first_file, china_geojson_file, province_geojson_file)

    all_results = []

    # 处理每个 NetCDF 文件
    for file_path in nc_files:
        print(f"Processing file: {file_path}")
        start_time = time.time()
        
        # 处理文件
        processed_data = process_file(file_path, china_mask, province_masks)
        
        # 计算劳动生产率损失（调整前和调整后）
        unadjusted_loss = calculate_labor_productivity_loss(processed_data, adjusted=False)
        adjusted_loss = calculate_labor_productivity_loss(processed_data, adjusted=True)
        
        # 计算差异
        diff = calculate_difference(unadjusted_loss, adjusted_loss)

        # 汇总结果
        result = {
            'model': processed_data['model'],
            'scenario': processed_data['scenario'],
            'China': {
                'unadjusted': unadjusted_loss['China'],
                'adjusted': adjusted_loss['China'],
                'difference': diff['China']
            },
            'Provinces': {
                'unadjusted': unadjusted_loss['Provinces'],
                'adjusted': adjusted_loss['Provinces'],
                'difference': diff['Provinces']
            }
        }
        
        all_results.append(result)
        
        end_time = time.time()
        print(f"Finished processing file. Time taken: {end_time - start_time:.2f} seconds")

    # 计算情景统计数据
    scenario_stats = calculate_scenario_statistics(all_results)

    # 保存结果
    save_scenario_results(scenario_stats, output_directory)
    print(f"Results saved to {output_directory}")

if __name__ == "__main__":
    main()
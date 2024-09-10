import xarray as xr
import geopandas as gpd
import rioxarray
import numpy as np
import glob
import os
import regionmask

def create_china_mask(data, china_geojson_file):
    """
    创建中国的地理掩码。
    
    参数:
    data: xarray.Dataset, 包含经纬度信息的数据集
    china_geojson_file: str, 中国地理边界文件路径
    
    返回:
    xarray.DataArray: 中国掩码
    """
    china_gdf = gpd.read_file(china_geojson_file)
    china_mask = regionmask.Regions([china_gdf.geometry.iloc[0]])
    return china_mask.mask(data.lon, data.lat)

def mask_and_save(file_path, china_mask, output_dir):
    """
    对数据进行掩膜处理，并保存为 NetCDF 和 GeoTIFF 格式。
    
    参数:
    file_path: str, 输入 NetCDF 文件路径
    china_mask: xarray.DataArray, 中国掩码
    output_dir: str, 输出目录路径
    """
    # 读取数据
    data = xr.open_dataset(file_path)
    
    # 应用掩码
    masked_data = data.where(china_mask == 0)
    
    # 获取文件名（不包括扩展名）
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    
    # 保存为 NetCDF
    nc_output_path = os.path.join(output_dir, f"{base_name}_china_masked.nc")
    masked_data.to_netcdf(nc_output_path)
    print(f"Saved masked NetCDF to: {nc_output_path}")
    
    # 保存为 GeoTIFF
    for var in masked_data.data_vars:
        tif_output_path = os.path.join(output_dir, f"{base_name}_{var}_china_masked.tif")
        
        # 确保数据有正确的空间维度
        da = masked_data[var]
        if 'lon' in da.dims and 'lat' in da.dims:
            da = da.rename({'lon': 'x', 'lat': 'y'})
        
        # 设置空间维度和坐标系
        da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
        da.rio.write_crs("EPSG:4326", inplace=True)
        
        # 保存为 GeoTIFF
        da.rio.to_raster(tif_output_path)
        print(f"Saved {var} as GeoTIFF to: {tif_output_path}")

def main():
    # 设置文件路径
    input_dir = "./outdoor_wbgt_output"
    china_geojson_file = "./model_outputs/cn.json"
    output_dir = "./china_masked_output"
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 获取所有 NetCDF 文件
    nc_files = glob.glob(f"{input_dir}/*.nc")
    
    # 创建中国掩码
    first_file = xr.open_dataset(nc_files[0])
    china_mask = create_china_mask(first_file, china_geojson_file)
    
    # 处理每个文件
    for file_path in nc_files:
        print(f"Processing file: {file_path}")
        mask_and_save(file_path, china_mask, output_dir)

if __name__ == "__main__":
    main()
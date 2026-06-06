"""Central configuration with environment variable overrides."""

import os
from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = Path(os.getenv('SHIFTING_WH_DATA_DIR', str(PROJECT_ROOT / 'data')))

# Input directories
DOWNLOADED_DATA_DIR = DATA_DIR / 'downloaded_data'
CHINA_OUTPUT_DIR = DATA_DIR / 'china_output'
MODEL_OUTPUTS_DIR = DATA_DIR / 'model_outputs'

# Output directories
WBGT_INDOOR_OUTPUT_DIR = DATA_DIR / 'wbgt_indoor_output'
WBGT_OUTDOOR_OUTPUT_DIR = DATA_DIR / 'wbgt_outdoor_output'
PRODUCTIVITY_OUTPUT_DIR = DATA_DIR / 'weighted_productivity_loss_output'
RESULTS_DIR = DATA_DIR / 'labor_productivity_results'
OUTDOOR_WBGT_OUTPUT_DIR = DATA_DIR / 'outdoor_wbgt_output'

# Status files
EXTRACT_STATUS_FILE = DATA_DIR / 'extract_processing_status.json'
WBGT_INDOOR_STATUS_FILE = DATA_DIR / 'wbgt_indoor_processing_status.json'
WBGT_OUTDOOR_STATUS_FILE = DATA_DIR / 'wbgt_outdoor_processing_status.json'
PRODUCTIVITY_STATUS_FILE = DATA_DIR / 'productivity_processing_status.json'

# Configuration files
CHINA_BOUNDS_FILE = PROJECT_ROOT / 'china_bounds_file.json'
NASA_DATA_INFO_FILE = PROJECT_ROOT / 'nasa_climate_data_info.csv'

# Population data files
POPULATION_FILES = {
    'SSP126': MODEL_OUTPUTS_DIR / 'pop_126_025.nc',
    'SSP245': MODEL_OUTPUTS_DIR / 'pop_245_025.nc',
    'SSP585': MODEL_OUTPUTS_DIR / 'pop_585_025.nc',
}

# GeoJSON boundary files
CHINA_GEOJSON_URL = 'https://geo.datav.aliyun.com/areas_v3/bound/100000_full.json'
PROVINCE_GEOJSON_URL = 'https://geo.datav.aliyun.com/areas_v3/bound/{code}_full.json'

# Processing settings
NUM_THREADS = int(os.getenv('SHIFTING_WH_NUM_THREADS', '4'))
DOWNLOAD_WORKERS = int(os.getenv('SHIFTING_WH_DOWNLOAD_WORKERS', '5'))

# Logging
LOG_LEVEL = os.getenv('SHIFTING_WH_LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Encoding
OUTPUT_ENCODING = 'utf-8'  # Changed from 'gbk' for cross-platform compatibility

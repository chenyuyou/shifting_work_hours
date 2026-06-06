# shifting_work_hours

气候变化对中国劳动生产力影响的研究项目。

## 项目概述

本项目分析气候变化对中国劳动生产力的影响，研究通过调整工作时间（根据各省日出时间调整作息）能否缓解生产力损失。

### 研究流程

1. 从NASA NEX-GDDP-CMIP6数据集下载全球气候模型(GCM)数据
2. 计算室内和室外湿球黑球温度(WBGT)
3. 结合人口数据估算劳动生产力损失
4. 分析调整工作时间前后的差异

### 气候模型和情景

- **4个CMIP6模型**: EC-Earth3, GFDL-ESM4, IPSL-CM6A-LR, NorESM2-MM
- **3个SSP情景**: SSP1-2.6 (低排放), SSP2-4.5 (中等), SSP5-8.5 (高排放)
- **5个气候变量**: 相对湿度、太阳辐射、气温、最高气温、风速
- **时间范围**: 2015-2100年，日分辨率
- **空间分辨率**: 0.25度（约25km）

## 安装

### 环境要求

- Python >= 3.9
- NVIDIA GPU with CUDA support (用于CuPy加速)
- Conda (推荐)

### 安装步骤

```bash
# 克隆仓库
git clone https://github.com/yourusername/shifting_work_hours.git
cd shifting_work_hours

# 创建conda环境
conda create -n shifting-wh python=3.11
conda activate shifting-wh

# 安装依赖
pip install -e .

# 或者使用requirements.txt
pip install -r requirements.txt
```

### 可选依赖

```bash
# 安装开发依赖（测试、代码格式化）
pip install -e ".[dev]"

# 安装Scrapy依赖（用于Stage 1: 爬取NASA数据目录）
pip install -e ".[scrapy]"
```

## 使用方法

### CLI（推荐）

安装后可直接使用 `shifting-wh` 命令：

```bash
# 查看所有命令
shifting-wh --help

# Stage 2: 下载气候数据
shifting-wh download --workers 5

# Stage 3: 裁切数据到中国区域
shifting-wh extract --threads 8

# Stage 4: 计算室内WBGT
shifting-wh wbgt-indoor --threads 4

# Stage 5: 计算室外WBGT
shifting-wh wbgt-outdoor --threads 1

# Stage 6: 计算生产力损失
shifting-wh productivity --scenario SSP245 --threads 4

# Stage 7: 最终分析
shifting-wh analysis

# Stage 8: 室外WBGT汇总
shifting-wh outdoor-summary

# 运行所有阶段
shifting-wh all --threads 4
```

也可以通过 Python 模块方式运行：

```bash
python -m shifting_work_hours wbgt-indoor --threads 4
```

### 环境变量

可以通过环境变量自定义配置：

```bash
# 设置数据目录
export SHIFTING_WH_DATA_DIR=/path/to/data

# 设置线程数
export SHIFTING_WH_NUM_THREADS=8

# 设置下载工作线程数
export SHIFTING_WH_DOWNLOAD_WORKERS=5

# 设置日志级别
export SHIFTING_WH_LOG_LEVEL=DEBUG
```

### 旧版脚本（已废弃）

旧版脚本位于 `legacy/` 目录，仅供参考，不推荐使用。

## 项目结构

```
shifting_work_hours/
├── LICENSE                          # MIT 许可证
├── CONTRIBUTING.md                  # 贡献指南
├── README.md                        # 本文件
├── REFACTORING_SUMMARY.md           # 重构说明
├── pyproject.toml                   # 项目配置和依赖
├── requirements.txt                 # 依赖列表
├── china_bounds_file.json           # 中国地理边界
├── nasa_climate_data_info.csv       # NASA数据文件列表
├── docs/                            # 文档
│   └── Lancet Planet Health 2025.pdf
├── src/shifting_work_hours/         # 主代码包
│   ├── __init__.py
│   ├── __main__.py                  # python -m 支持
│   ├── cli.py                       # CLI 入口点
│   ├── config/                      # 配置模块
│   │   ├── constants.py             # 领域常量（模型、情景等）
│   │   └── settings.py              # 路径和设置配置
│   ├── core/                        # 核心工具
│   │   ├── status.py                # StatusTracker - 状态跟踪
│   │   ├── runner.py                # TaskRunner - 并行任务执行
│   │   └── io.py                    # NetCDF I/O 工具
│   ├── pipeline/                    # 处理流水线
│   │   ├── downloader.py            # Stage 2: 数据下载
│   │   ├── extractor.py             # Stage 3: 空间裁切
│   │   ├── wbgt_indoor.py           # Stage 4: 室内WBGT
│   │   ├── wbgt_outdoor.py          # Stage 5: 室外WBGT
│   │   ├── wbgt_liljegren.py        # Liljegren WBGT 计算核心
│   │   ├── productivity.py          # Stage 6: 生产力损失
│   │   ├── analysis.py              # Stage 7: 最终分析
│   │   └── outdoor_summary.py       # Stage 8: 室外WBGT汇总
│   └── utils/                       # 工具函数
│       └── file_discovery.py        # 文件发现工具
├── scripts/                         # 开发脚本
│   └── run_pipeline.py              # CLI 包装器（兼容）
├── tests/                           # 测试
├── legacy/                          # 旧版脚本（已废弃）
├── nasa_climate_data/               # Scrapy爬虫项目（Stage 1）
└── model_outputs/                   # GeoJSON边界和人口数据
```

## 处理流水线

### Stage 1: 爬取NASA数据目录

使用Scrapy爬虫从NASA THREDDS目录获取可用数据文件列表：

```bash
cd nasa_climate_data
scrapy crawl nasa_climate_data_xml
```

输出: `nasa_climate_data_info.csv`

### Stage 2: 下载气候数据

多线程下载器，支持断点续传：

```bash
shifting-wh download --workers 5
```

输出: `data/downloaded_data/{model}/{scenario}/r1i1p1f1/{variable}/*.nc`

### Stage 3: 裁切数据

将全球数据裁切到中国区域：

```bash
shifting-wh extract --threads 8
```

输出: `data/china_output/`

### Stage 4: 计算室内WBGT

使用Stull (2011)公式计算湿球温度：

```bash
shifting-wh wbgt-indoor --threads 4
```

输出: `data/wbgt_indoor_output/`

### Stage 5: 计算室外WBGT

使用Liljegren模型计算室外WBGT（需要CUDA加速）：

```bash
shifting-wh wbgt-outdoor --threads 1
```

输出: `data/wbgt_outdoor_output/`

### Stage 6: 计算生产力损失

结合人口数据计算加权生产力损失：

```bash
shifting-wh productivity --scenario SSP245 --threads 4
```

输出: `data/weighted_productivity_loss_output/`

### Stage 7: 最终分析

地理聚合和统计分析：

```bash
shifting-wh analysis
```

输出: `data/labor_productivity_results/{scenario}/`

### Stage 8: 室外WBGT汇总

计算2100年夏季平均室外WBGT：

```bash
shifting-wh outdoor-summary
```

输出: `data/outdoor_wbgt_output/`

## 开发

### 运行测试

```bash
# 运行所有测试
pytest tests/

# 运行特定测试
pytest tests/test_status_tracker.py -v
```

### 代码格式化

```bash
# 使用black格式化
black src/ scripts/ tests/

# 使用ruff检查
ruff check src/ scripts/ tests/
```

## 参考文献

- Stull, R. (2011). Wet-bulb temperature from relative humidity and air temperature. *Journal of Applied Meteorology and Climatology*, 50(11), 2267-2269.
- Kjellstrom, T., et al. (2018). Heat and human performance. *Annual Review of Public Health*, 39, 97-115.
- Liljegren, J. C., et al. (2008). Modeling the wet bulb globe temperature using standard meteorological measurements. *Journal of Occupational and Environmental Hygiene*, 5(10), 645-655.

## 贡献

欢迎贡献！请参阅 [CONTRIBUTING.md](CONTRIBUTING.md) 了解开发环境搭建和提交规范。

## 许可证

本项目采用 [MIT 许可证](LICENSE)。

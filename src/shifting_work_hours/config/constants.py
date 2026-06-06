"""Domain constants for climate data processing."""

# CMIP6 Models
MODELS = ['EC-Earth3', 'GFDL-ESM4', 'IPSL-CM6A-LR', 'NorESM2-MM']

# SSP Scenarios
SCENARIOS = ['SSP126', 'SSP245', 'SSP585']

# Climate Variables
VARIABLES = ['hurs', 'rsds', 'sfcWind', 'tas', 'tasmax']

# Time Range
YEAR_START = 2015
YEAR_END = 2100  # inclusive

# Ensemble Member
ENSEMBLE_MEMBER = 'r1i1p1f1'

# Work Intensity Levels
INTENSITIES = ['low', 'medium', 'high']

# Physical Constants
KELVIN_OFFSET = 273.15

# China Geographic Bounds
CHINA_LAT_MIN = 3.41
CHINA_LAT_MAX = 53.56
CHINA_LON_MIN = 73.50
CHINA_LON_MAX = 135.10

# Productivity Loss Parameters (Kjellstrom et al.)
PRODUCTIVITY_PARAMS = {
    'low': {
        'threshold': 34.64,
        'exponent': 22.72,
    },
    'medium': {
        'threshold': 32.93,
        'exponent': 17.81,
    },
    'high': {
        'threshold': 30.94,
        'exponent': 16.64,
    },
}

# Sunrise Hour Weights for Working Hour Adjustment
# Maps sunrise hour -> weight for min WBGT
SUNRISE_WEIGHTS = {
    4: 0.75,   # Northeast provinces (early sunrise)
    5: 0.625,
    6: 0.5,    # Central provinces
    7: 0.375,  # Southwest provinces (late sunrise)
}

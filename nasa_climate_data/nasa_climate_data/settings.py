BOT_NAME = 'nasa_climate_data'

SPIDER_MODULES = ['nasa_climate_data.spiders']
NEWSPIDER_MODULE = 'nasa_climate_data.spiders'

ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS = 4
DOWNLOAD_DELAY = 1
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
ITEM_PIPELINES = {
    'nasa_climate_data.pipelines.NasaClimateDataPipeline': 1
}

FILES_STORE = 'data'

# Enable logging
LOG_ENABLED = True
LOG_LEVEL = 'INFO'

# Retry settings
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]

# Large Files Settings
DOWNLOAD_TIMEOUT = 3600  # 1 hour
DOWNLOAD_MAXSIZE = 0  # No limit
DOWNLOAD_WARNSIZE = 0  # No warning

MEDIA_ALLOW_REDIRECTS = True

# S3 specific settings
AWS_ACCESS_KEY_ID = ''  # Leave empty if not required
AWS_SECRET_ACCESS_KEY = ''  # Leave empty if not required
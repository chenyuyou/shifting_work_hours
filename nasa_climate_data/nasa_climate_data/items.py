import scrapy

class NasaClimateDataItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()
    file_name = scrapy.Field()
    model = scrapy.Field()
    scenario = scrapy.Field()
    variable = scrapy.Field()
    year = scrapy.Field()
    version = scrapy.Field()
    file_size = scrapy.Field()
import scrapy
import csv
import re
from urllib.parse import urljoin
from lxml import etree
from collections import defaultdict

class NasaClimateDataXmlSpider(scrapy.Spider):
    name = 'nasa_climate_data_xml'
    allowed_domains = ['ds.nccs.nasa.gov']
    base_url = "https://ds.nccs.nasa.gov/thredds/catalog/AMES/NEX/GDDP-CMIP6"
    
    def __init__(self, *args, **kwargs):
        super(NasaClimateDataXmlSpider, self).__init__(*args, **kwargs)
        self.models = ["EC-Earth3", "GFDL-ESM4", "IPSL-CM6A-LR", "NorESM2-MM"]
        self.scenarios = ["ssp126", "ssp245", "ssp585"]
        self.variables = ["hurs", "rsds", "tas", "tasmax", "sfcWind"]  # 修正 sfcWind 拼写
        self.years = range(2015, 2101)
        self.output_file = 'nasa_climate_data_info.csv'
        self.file_info = []
        self.namespaces = {
            'catalog': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0',
            'xlink': 'http://www.w3.org/1999/xlink'
        }

    def start_requests(self):
        for model in self.models:
            for scenario in self.scenarios:
                for variable in self.variables:
                    url = f"{self.base_url}/{model}/{scenario}/r1i1p1f1/{variable}/catalog.xml"
                    yield scrapy.Request(url, self.parse_catalog_xml, meta={'model': model, 'scenario': scenario, 'variable': variable})

    def parse_catalog_xml(self, response):
        self.logger.info(f"Parsing XML from: {response.url}")
        
        model = response.meta['model']
        scenario = response.meta['scenario']
        variable = response.meta['variable']
        
        root = etree.fromstring(response.body)
        
        http_service = root.xpath("//catalog:service[@serviceType='HTTPServer']", namespaces=self.namespaces)[0]
        http_base = http_service.get('base')
        
        datasets = root.xpath("//catalog:dataset[@name]", namespaces=self.namespaces)
        
        files_by_year = defaultdict(list)
        
        for dataset in datasets:
            filename = dataset.get('name')
            if filename.endswith('.nc'):
                url_path = dataset.get('urlPath')
                download_url = urljoin(response.url, f"{http_base}{url_path}")
                
                data_size_element = dataset.xpath(".//catalog:dataSize", namespaces=self.namespaces)[0]
                data_size = f"{data_size_element.text} {data_size_element.get('units')}"
                
                date_element = dataset.xpath(".//catalog:date[@type='modified']", namespaces=self.namespaces)[0]
                modified_date = date_element.text
                
                year_match = re.search(r'_(\d{4})(_v\d+\.\d+)?\.nc$', filename)
                if year_match and int(year_match.group(1)) in self.years:
                    year = year_match.group(1)
                    version = year_match.group(2) if year_match.group(2) else ''
                    
                    file_info = {
                        'model': model,
                        'scenario': scenario,
                        'variable': variable,
                        'year': year,
                        'version': version.strip('_') if version else 'no_version',
                        'filename': filename,
                        'filesize': data_size,
                        'download_url': download_url,
                        'modified_date': modified_date
                    }
                    
                    files_by_year[year].append(file_info)
        
        # 选择每年的最优版本
        for year, files in files_by_year.items():
            sorted_files = sorted(files, key=lambda x: (
                x['version'] == 'v1.2',
                x['version'] == 'v1.1',
                x['version'] == 'no_version'
            ), reverse=True)
            
            self.file_info.append(sorted_files[0])
            self.logger.info(f"Selected file for {year}: {sorted_files[0]['filename']}")

    def closed(self, reason):
        with open(self.output_file, 'w', newline='') as csvfile:
            fieldnames = ['model', 'scenario', 'variable', 'year', 'version', 'filename', 'filesize', 'download_url', 'modified_date']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for file_info in self.file_info:
                writer.writerow(file_info)
        
        self.logger.info(f"Saved file information to {self.output_file}")
        self.logger.info(f"Total files found: {len(self.file_info)}")
import os
from scrapy.pipelines.files import FilesPipeline
from scrapy import Request
from scrapy.exceptions import DropItem
from tqdm import tqdm

class NasaClimateDataPipeline(FilesPipeline):
    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func, settings)
        self.progress_bars = {}

    def get_media_requests(self, item, info):
        return [Request(x, meta={'item': item}) for x in item.get(self.files_urls_field, [])]

    def file_path(self, request, response=None, info=None, *, item=None):
        return f"{item['model']}/{item['scenario']}/r1i1p1f1/{item['variable']}/{item['file_name']}"

    def media_to_download(self, request, info, *, item=None):
        media = super().media_to_download(request, info, item=item)
        if media:
            file_name = item['file_name']
            self.progress_bars[file_name] = tqdm(total=int(item['file_size']), unit='iB', unit_scale=True, desc=file_name)
        return media

    def media_downloaded(self, response, request, info, *, item=None):
        file_name = item['file_name']
        if file_name in self.progress_bars:
            self.progress_bars[file_name].update(len(response.body))
        return super().media_downloaded(response, request, info, item=item)

    def item_completed(self, results, item, info):
        file_paths = [x['path'] for ok, x in results if ok]
        if not file_paths:
            raise DropItem("Item contains no files")
        item['file_paths'] = file_paths
        file_name = item['file_name']
        if file_name in self.progress_bars:
            self.progress_bars[file_name].close()
            del self.progress_bars[file_name]
        return item
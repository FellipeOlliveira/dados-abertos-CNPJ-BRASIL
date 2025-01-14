# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from typing import Any

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy import Request
from scrapy.http import Response
from scrapy.pipelines.files import FilesPipeline
from scrapy.pipelines.media import MediaPipeline


class DadosAbertosPipeline:
    def process_item(self, item, spider):
        return item

class CustomFilesPipeline(FilesPipeline):
    def file_path(
        self,
        request: Request,
        response: Response | None = None,
        info: MediaPipeline.SpiderInfo | None = None,
        *,
        item: Any = None,
    ) -> str:
        return f'{item.get("title")}'

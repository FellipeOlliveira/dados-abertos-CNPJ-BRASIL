from typing import Iterable

from pathlib import Path
import scrapy
from scrapy import Request


class DownloadingFilesSpider(scrapy.Spider):
    name = "downloading_files"
    allowed_domains = ["arquivos.receitafederal.gov.br"]
    start_urls = ["https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"]

    def start_requests(self) -> Iterable[Request]:
        yield Request(
            url=self.start_urls[0],
            callback=self.get_current_files
        )
    def get_current_files(self, response):
        link = response.xpath("//a/@href")[-2]

        link = response.urljoin(str(link))

        return Request(
            url=link,
            callback=self.parse
        )



    def parse(self, response):
        response.xpath("//a[contains(text(), 'Estabelecimentos') or contains(text(), 'Empressas')]/@href")

        

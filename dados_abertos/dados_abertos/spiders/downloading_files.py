import scrapy


class DownloadingFilesSpider(scrapy.Spider):
    name = "downloading_files"
    allowed_domains = ["arquivos.receitafederal.gov.br"]
    start_urls = ["https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"]

    def parse(self, response):
        pass

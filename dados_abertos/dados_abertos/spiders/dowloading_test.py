import scrapy


class DowloadingTestSpider(scrapy.Spider):
    name = "dowloading_test"
    allowed_domains = ["scrapebay.com"]
    start_urls = ["https://scrapebay.com/ebooks"]

    def parse(self, response):
        pass

import scrapy
from ..items import BookItem

class DowloadingTestSpider(scrapy.Spider):
    name = "dowloading_test"
    allowed_domains = ["scrapebay.com"]
    start_urls = ["https://scrapebay.com/ebooks"]

    def parse(self, response):
        for book in response.css('.col'):
            title = book.css('h5 ::text').get()
            link = response.urljoin(
                book.css('a.pdf ::attr(href)').get()
            )
            yield {
                'Title': title,
                'file_urls': [link]
            }

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DadosAbertosItem(scrapy.Item):

    title = scrapy.Field()
    file_urls = scrapy.Field()
    file = scrapy.Field

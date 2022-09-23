# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TencenttopItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    text = scrapy.Field()
    url = scrapy.Field()
    source = scrapy.Field()
    img = scrapy.Field()
    readCount = scrapy.Field()
    add_time = scrapy.Field()
    update_time = scrapy.Field()
    is_local = scrapy.Field()
    status = scrapy.Field()


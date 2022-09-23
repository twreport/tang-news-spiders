# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SinahotItem(scrapy.Item):
    # define the fields for your item here like:
    title = scrapy.Field()
    url = scrapy.Field()
    hotValue = scrapy.Field()
    add_time = scrapy.Field()
    update_time = scrapy.Field()
    status = scrapy.Field()


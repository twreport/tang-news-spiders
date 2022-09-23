# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BaidutopItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # define the fields for your item here like:
    desc = scrapy.Field()
    hotScore = scrapy.Field()
    hotTag = scrapy.Field()
    img = scrapy.Field()
    url = scrapy.Field()
    word = scrapy.Field()
    add_time = scrapy.Field()
    history = scrapy.Field()
    is_local = scrapy.Field()
    status = scrapy.Field()

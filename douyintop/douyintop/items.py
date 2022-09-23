# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DouyintopItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # define the fields for your item here like:
    # define the fields for your item here like:
    hot_value = scrapy.Field()
    event_time = scrapy.Field()
    word = scrapy.Field()
    # 热度名次
    position = scrapy.Field()
    group_id = scrapy.Field()
    add_time = scrapy.Field()
    history = scrapy.Field()
    is_local = scrapy.Field()
    status = scrapy.Field()


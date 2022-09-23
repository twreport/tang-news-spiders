# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TianyanhotItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    img = scrapy.Field()
    news_views = scrapy.Field()
    add_time = scrapy.Field()
    update_time = scrapy.Field()
    status = scrapy.Field()

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ToutiaotopItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    ClusterId = scrapy.Field()
    Title = scrapy.Field()
    Label = scrapy.Field()
    Url = scrapy.Field()
    ClusterType = scrapy.Field()
    HotValue = scrapy.Field()
    Image = scrapy.Field()
    add_time = scrapy.Field()
    history = scrapy.Field()
    is_local = scrapy.Field()
    status = scrapy.Field()
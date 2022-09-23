# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WeibovipItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    text = scrapy.Field()
    url = scrapy.Field()
    hot_num = scrapy.Field()
    pics = scrapy.Field()
    add_time = scrapy.Field()
    is_local = scrapy.Field()
    uid = scrapy.Field()
    vip_name = scrapy.Field()
    status = scrapy.Field()


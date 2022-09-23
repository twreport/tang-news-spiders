# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DizhensubaoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    key_words = scrapy.Field()
    locations = scrapy.Field()
    text = scrapy.Field()
    add_time = scrapy.Field()
    issue_date = scrapy.Field()
    status = scrapy.Field()

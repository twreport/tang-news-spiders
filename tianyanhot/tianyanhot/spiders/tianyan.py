import scrapy
import pymongo
import logging
import json
import time
from tianyanhot.items import TianyanhotItem

class TianyanSpider(scrapy.Spider):
    name = 'tianyan'
    allowed_domains = ['todayguizhou.com']

    def __init__(self, *args, **kwargs):
        super(TianyanSpider, self).__init__(*args, **kwargs)
        logging.warning('=================== Tianyan News ====================')

    def start_requests(self):
        url = 'https://jgz.app.todayguizhou.com/appAPI/index.php?act=special_column&op=index&special_column_id=11515115722490'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        res_str = response.body.decode()
        res_dict = json.loads(res_str)
        news_list = res_dict['data']['list']

        # 从mongo数据库中取出48小时内最近50条文章的标题列表
        item_title_list = self.get_urls_list_in_db()

        for tianyan_item in news_list:
            if tianyan_item['news_title'] in item_title_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                item = TianyanhotItem()
                item['title'] = tianyan_item['news_title']
                item['url'] = "http://jgz.app.todayguizhou.com//news/news-news_detail-news_id-" + str(tianyan_item['news_id']) + ".html"
                # img_url = tianyan_item['news_thumb']
                # img_url = img_url[2:]
                item['img'] = tianyan_item['news_thumb']
                item['news_views'] = tianyan_item['news_views']
                item['add_time'] = int(time.time())
                item['update_time'] = int(time.time())
                item['status'] = 1
                yield item

    def get_urls_list_in_db(self):
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['hot']
        query_col = query_db['tianyan']

        items = query_col.find().sort('add_time', -1).limit(50)

        titles_list = []
        for item in items:
            titles_list.append(item['title'])

        query_client.close()
        return titles_list
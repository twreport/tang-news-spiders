import scrapy
from bs4 import BeautifulSoup
import json
from douyintop.items import DouyintopItem
import time
import pymongo

class DouyinSpider(scrapy.Spider):
    name = 'douyin'
    allowed_domains = ['snssdk.com']
    start_urls = ['http://snssdk.com/']

    HEADERS = {
        'user-agent': 'okhttp3'
    }
    QUERIES = {
        'device_platform': 'android',
        'version_name': '13.2.0',
        'version_code': '130200',
        'aid': '1128'
    }

    def __init__(self, *args, **kwargs):
        super(DouyinSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        url = 'https://aweme.snssdk.com/aweme/v1/hot/search/list/'
        yield scrapy.Request(url=url, body=json.dumps(self.QUERIES), headers=self.HEADERS, callback=self.parse)

    def parse(self, response):
        res_dict = json.loads(response.body.decode('utf8'))
        word_list = res_dict['data']['word_list']
        # 从mongo数据库中取出48小时内最近50条文章的标题列表
        item_title_list = self.get_urls_list_in_db()
        print(item_title_list)
        for word in word_list:
            print(word['word'])
            if word['word'] in item_title_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                item = DouyintopItem()
                item['hot_value'] = word['hot_value']
                item['event_time'] = word['event_time']
                item['word'] = word['word']
                item['position'] = word['position']
                item['add_time'] = int(time.time())
                item['history'] = []
                item['is_local'] = self.is_local(word['word'])
                item['status'] = 1
                yield item

    def is_local(self, text):
        local_name_list = ['贵州', '贵阳', '观山湖', '南明', '花溪', '毕节', '遵义', '黔东南', '黔南', '黔西', '黔西南', '六盘水', '安顺', '凯里',
                           '毕节', '铜仁', '黄果树', '梵净山', '千户苗寨', '荔波']
        # 如果文本中包含本地地名返回 1
        for local_name in local_name_list:
            if local_name in text:
                return 1
        # 不含则返回 0
        return 0

    def get_urls_list_in_db(self):
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['top']
        query_col = query_db['douyin']

        items = query_col.find().sort('add_time', -1).limit(300)

        titles_list = []
        for item in items:
            titles_list.append(item['word'])

        query_client.close()
        return titles_list
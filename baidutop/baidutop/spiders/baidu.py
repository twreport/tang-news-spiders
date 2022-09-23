import scrapy
from bs4 import BeautifulSoup
import re
import json
from baidutop.items import BaidutopItem
import time
import pymongo

class BaiduSpider(scrapy.Spider):
    name = 'baidu'
    allowed_domains = ['baidu.com']
    start_urls = ['https://top.baidu.com/board?tab=realtime']

    def parse(self, response):
        soup = BeautifulSoup(response.body, "lxml")

        msg_str = re.findall("<!--s-data:(.*?)-->", str(soup))
        msg_dict = json.loads(msg_str[0])
        cards_list = msg_dict['data']['cards'][0]['content']
        # 从mongo数据库中取出48小时内最近100条文章的标题列表
        item_title_list = self.get_urls_list_in_db()
        for card in cards_list:
            if card['word'] in item_title_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                item = BaidutopItem()
                item['desc'] = card['desc']
                item['hotScore'] = int(card['hotScore'])
                item['hotTag'] = card['hotTag']
                item['img'] = card['img']
                item['url'] = card['url']
                item['word'] = card['word']
                item['add_time'] = int(time.time())
                item['history'] = []
                item['is_local'] = self.is_local(item['word'], item['desc']),
                item['status'] = 1
                yield item

    def is_local(self, title, text):
        local_name_list = ['贵州','贵阳','观山湖','南明','花溪','毕节','遵义','黔东南','黔南','黔西','黔西南','六盘水','安顺','凯里','毕节',
                      '铜仁','黄果树','梵净山','千户苗寨','荔波']
        # 如果标题中含有地名直接返回 1
        for local_name in local_name_list:
            if local_name in title:
                return 1
        # 如果标题中不含则搜索正文
        for local_name in local_name_list:
            if local_name in text:
                return 1
        # 都不含则返回 0
        return 0

    def get_urls_list_in_db(self):
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['top']
        query_col = query_db['baidu']

        items = query_col.find().sort('add_time', -1).limit(300)

        titles_list = []
        for item in items:
            print(item)
            titles_list.append(item['word'])

        query_client.close()
        return titles_list

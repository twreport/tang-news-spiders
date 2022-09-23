import scrapy
import pymongo
import logging
import json
import time
from neteasetop.items import NeteasetopItem

class NeteaseSpider(scrapy.Spider):
    name = 'netease'
    allowed_domains = ['qq.com']
    start_urls = ['http://qq.com/']

    def __init__(self, *args, **kwargs):
        super(NeteaseSpider, self).__init__(*args, **kwargs)
        logging.warning('=================== Netease News ====================')

    def start_requests(self):
        url = 'https://gw.m.163.com/gentie-web/api/v2/products/a2869674571f77b5a0867c3d71db5856/rankDocs/all/list?ibc=newsapph5&limit=30'
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        res_str = response.body.decode()
        res_dict = json.loads(res_str)

        news_list = res_dict['data']['cmtDocs']

        # 从mongo数据库中取出48小时内最近50条文章的标题列表
        item_title_list = self.get_urls_list_in_db()

        for netease_item in news_list:
            if netease_item['doc_title'] in item_title_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                item = NeteasetopItem()
                item['title'] = netease_item['doc_title']
                if 'wondCmtContent' not in netease_item:
                    item['text'] = ''
                else:
                    item['text'] = netease_item['wondCmtContent']
                item['url'] = "https://3g.163.com/news/article/" + netease_item['docId'] + ".html"
                item['source'] = netease_item['source']
                item['img'] = netease_item['doc_image']
                if 'hotScore' not in netease_item:
                    item['hotScore'] = 0
                else:
                    item['hotScore'] = netease_item['hotScore']
                item['add_time'] = int(time.time())
                item['update_time'] = int(time.time())
                item['is_local'] = self.is_local(netease_item['doc_title'], '')
                item['status'] = 1
                yield item

    def get_urls_list_in_db(self):
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['top']
        query_col = query_db['netease']

        items = query_col.find().sort('add_time', -1).limit(50)

        titles_list = []
        for item in items:
            titles_list.append(item['title'])

        query_client.close()
        return titles_list

    def is_local(self, title, text):
        local_name_list = ['贵州','贵阳','观山湖','南明','花溪','毕节','遵义','黔东南',
                           '黔南','黔西','黔西南','六盘水','安顺','凯里','毕节',
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
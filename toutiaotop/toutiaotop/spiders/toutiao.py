import scrapy
# from bs4 import BeautifulSoup
import time
import json
from toutiaotop.items import ToutiaotopItem
import pymongo

class ToutiaoSpider(scrapy.Spider):
    name = 'toutiao'
    allowed_domains = ['toutiao.com']
    start_urls = ['http://toutiao.com/']

    def __init__(self, *args, **kwargs):
        super(ToutiaoSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        url = 'https://www.toutiao.com/hot-event/hot-board/?origin=toutiao_pc&_signature=' + str(int(time.time()))
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        res_str = response.body.decode()
        res_dict = json.loads(res_str)
        res_data = res_dict['data']

        # 从mongo数据库中取出48小时内最近50条文章的标题列表
        item_title_list = self.get_urls_list_in_db()
        print(item_title_list)
        for res_key in res_data:
            print(res_key['Title'])
            if res_key['Title'] in item_title_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                item = ToutiaotopItem()
                item['ClusterId'] = res_key['ClusterId']
                item['Title'] = res_key['Title']
                item['Label'] = res_key['Label']
                item['Url'] = res_key['Url']
                item['ClusterType'] = res_key['ClusterType']
                item['HotValue'] = int(res_key['HotValue'])
                item['Image'] = res_key['Image']
                item['add_time'] = int(time.time())
                item['history'] = []
                item['is_local'] = self.is_local(res_key['Title'])
                item['status'] = 1
                yield item

    def is_local(self, text):
        local_name_list = ['贵州','贵阳','观山湖','南明','花溪','毕节','遵义','黔东南','黔南','黔西','黔西南','六盘水','安顺','凯里','毕节',
                      '铜仁','黄果树','梵净山','千户苗寨','荔波']
        # 如果文本中包含本地地名返回 1
        for local_name in local_name_list:
            if local_name in text:
                return 1
        # 不含则返回 0
        return 0

    def get_urls_list_in_db(self):
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['top']
        query_col = query_db['toutiao']

        items = query_col.find().sort('add_time', -1).limit(300)

        titles_list = []
        for item in items:
            titles_list.append(item['Title'])

        query_client.close()
        return titles_list
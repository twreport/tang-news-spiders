import scrapy
import pymongo
import logging
import json
import time
from sinatop.items import SinatopItem

myclient = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
mydb = myclient['sina']
mycol = mydb['url']

class SinaSpider(scrapy.Spider):
    name = 'sina'
    allowed_domains = ['sina.cn']
    # start_urls = ['http://sina.cn/']
    url_key = 0

    def __init__(self, *args, **kwargs):
        super(SinaSpider, self).__init__(*args, **kwargs)
        logging.warning('=================== Sina Hot ====================')


    def start_requests(self):
        # 在mongodb中取出所有访问参数
        query = {'type': 'top', 'status': 1}
        urls = mycol.find(query).sort("add_time", -1).limit(5)
        url_list = []
        for url in urls:
            url_list.append(url)

        myclient.close()

        if url_list == []:
            # 如果所有的参数都已经无法访问,则需要手动更新
            logging.warning("Sina Url is None! Spider is Closed!")
            self.crawler.engine.close_spider(self, "Sina Url is None! Spider is Closed!")
            return None
        else:
            # 选最新生成的那个参数
            url = url_list[0]
            logging.warning("url------>>>>>>")
            logging.warning(url)
            yield scrapy.Request(url=url['url'], headers=url['headers'], meta={'this_url': url}, callback=self.parse)

    def parse(self, response):
        res_str = response.body.decode()

        # 如果url已经失效，则需要把该url的status标记为0
        if 'Forbidden' in res_str:
            update_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
            update_db = update_client['sina']
            update_col = update_db['url']
            query = {'url': response.url}
            url = response.meta['this_url']
            url['status'] = 0
            update_col.replace_one(query, url)
            update_client.close()
            logging.warning("This Sina Url is Bad!")
            self.crawler.engine.close_spider(self, "This Sina Url is Bad!!")
            return None

        res_dict = json.loads(res_str)

        # 从mongo数据库中取出48小时内最近50条文章的标题列表
        item_title_list = self.get_urls_list_in_db()

        for sina_item in res_dict['data']['result']:
            if sina_item['text'] in item_title_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                item = SinatopItem()
                item['title'] = sina_item['text']
                item['url'] = sina_item['link']

                hotValueStr = sina_item['hotValue'].replace('万','0000')
                hotValueInt = int(hotValueStr)
                item['hotValue'] = hotValueInt
                item['add_time'] = int(time.time())
                item['update_time'] = int(time.time())
                item['is_local'] = self.is_local(sina_item['text'])
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
        query_col = query_db['sina']

        items = query_col.find().sort('add_time', -1).limit(300)

        titles_list = []
        for item in items:
            titles_list.append(item['title'])

        query_client.close()
        return titles_list
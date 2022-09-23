import scrapy
from bs4 import BeautifulSoup
import json
import re
import urllib.parse
from weibotop.items import WeibotopItem
import time
import pymongo

class WeiboSpider(scrapy.Spider):
    name = 'weibo'
    allowed_domains = ['weibo.cn']
    # start_urls = ['https://api.weibo.cn/2/page?gsid=_2A25P3XnlDeRxGeFO7FQW8i7KzjyIHXVqy4otrDV6PUJbkdAKLW78kWpNQVo2PDANc1UJhnycGpUqxCXdaiKGhgZZ&from=10C7293010&c=iphone&s=922760A7&containerid=106003type%253D25%2526t%253D3%2526disable_hot%253D1%2526filter_type%253Drealtimehot']
    start_urls = ['https://api.weibo.cn/2/page?gsid=_2A25P50dFDeRxGeFO7FQW8i7KzjyIHXVqtd2NrDV6PUJbkdANLRPbkWpNQVo2PALb0iBUq747x0EzGltixct0GNU9&from=10C7393010&c=iphone&s=1F2F9E9C&containerid=106003type%253D25%2526t%253D3%2526disable_hot%253D1%2526filter_type%253Drealtimehot']
    # 用于拼接具体热搜的url
    # %3D 相当于 =
    # %26 相当于 &
    # item_base_url = 'https://api.weibo.cn/2/searchall?gsid=_2A25Py8CXDeRxGedP6FYV-SfIyzuIHXVqwVNfrDV6PUJbkdAKLUfgkWpNXCzTk4jueNeAPSQ1toQzKFChL__tnPte&from=10C7093010&c=iphone&s=2C32767A&containerid=100103type%3D1%26q%3D'
    item_base_url = 'https://api.weibo.cn/2/searchall?gsid=_2A25P50dFDeRxGeFO7FQW8i7KzjyIHXVqtd2NrDV6PUJbkdANLRPbkWpNQVo2PALb0iBUq747x0EzGltixct0GNU9&from=10C7393010&c=iphone&s=1F2F9E9C&containerid=100103type%3D1%26q%3D'
    urls_list = []

    def __init__(self, *args, **kwargs):
        super(WeiboSpider, self).__init__(*args, **kwargs)
        self.urls_list = self.get_urls_list_in_db()

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        p = response.text
        weibo_dict = json.loads(p)
        for card in weibo_dict['cards'][0]['card_group']:
            if card['desc'] in self.urls_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                if 'desc_extr' in card:
                    item_url = self.item_base_url + urllib.parse.quote(card['desc'])
                    title = card['desc']
                    hot_num_list = re.findall("\d+", card['desc_extr'])
                    if len(hot_num_list) < 1:
                        hot_num = 0
                    else:
                        hot_num = int(hot_num_list[0])
                    yield scrapy.Request(url=item_url, callback=self.parse_item,
                                         meta={'title':title, 'hot_num':hot_num})
                # break

    def parse_item(self, response):
        weibo_dict = json.loads(response.text)
        # print(weibo_dict['cards'])
        print('-------------------------------------title----------------------------------')
        print(response.meta['title'])
        for c in weibo_dict['cards']:
            print(c['card_type'])
            if c['card_type'] == 9:
                print('9')
                first_card = c
                break
            elif c['card_type'] == 11:
                print('11')
                first_card = c['card_group'][0]
                break
            else:
                pass

        # 如果没找到就复制标题
        try:
            weibo_text = first_card['mblog']['text']
        except:
            weibo_text = response.meta['title']

        item = WeibotopItem()
        item['title'] = response.meta['title']
        item['hot_num'] = response.meta['hot_num']
        item['text'] = self.del_spec_str(weibo_text)
        item['history'] = []
        item['add_time'] = int(time.time())
        item['is_local'] = self.is_local(item['title'], item['text'])
        item['status'] = 1

        yield item

    def format_weibo_msg_list(self, msg):
        msg = msg.replace('&amp;', '&')
        msg = msg.replace('amp;', '')
        msg = msg.replace('&quot;', '"')
        msg = msg.replace('&nbsp;', ' ')
        # msg = self.find_quot_in_title(msg)
        msg_dict = json.loads(msg)
        return msg_dict

    def del_spec_str(self, str):
        str = str.replace('\n', '')
        str = str.replace('\u200b', '')
        str = str.replace('\xa0', '')
        return str

    def is_local(self, title, text):
        local_name_list = ['贵州','贵阳','遵义','黔东南','黔南','黔西南','六盘水','安顺','毕节','铜仁']
        # 如果标题中含有地名直接返回
        for local_name in local_name_list:
            if local_name in title:
                return 1
        # 如果标题中不含则搜索正文
        for local_name in local_name_list:
            if local_name in text:
                return 1
        # 都不含则返回none
        return 0

    def get_urls_list_in_db(self):
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['top']
        query_col = query_db['weibo']
        items = query_col.find().sort('add_time', -1).limit(300)
        titles_list = []
        for item in items:
            titles_list.append(item['title'])
        query_client.close()
        return titles_list
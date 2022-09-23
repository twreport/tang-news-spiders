import scrapy
from bs4 import BeautifulSoup
import json
import re
import urllib.parse
from weibohot.items import WeibohotItem
import time
import pymongo

class WeiboSpider(scrapy.Spider):
    name = 'weibo'
    allowed_domains = ['weibo.cn']
    start_urls = ['https://api.weibo.cn/2/page?gsid=_2A25Py8CXDeRxGedP6FYV-SfIyzuIHXVqwVNfrDV6PUJbkdAKLUfgkWpNXCzTk4jueNeAPSQ1toQzKFChL__tnPte&from=10C7093010&c=iphone&s=2C32767A&containerid=106003type%3D25%26t%3D3%26disable_hot%3D1%26filter_type%3Dregion']
    # 备用地址
    # stsrt_urls = ['https://api.weibo.cn/2/page?gsid=_2A25P50dFDeRxGeFO7FQW8i7KzjyIHXVqtd2NrDV6PUJbkdANLRPbkWpNQVo2PALb0iBUq747x0EzGltixct0GNU9&from=10C7393010&c=iphone&s=1F2F9E9C&containerid=106003type%3D25%26t%3D3%26disable_hot%3D1%26filter_type%3Dregion']

    # 用于拼接具体热搜的url
    # %3D 相当于 =
    # %26 相当于 &
    item_base_url = 'https://api.weibo.cn/2/searchall?gsid=_2A25Py8CXDeRxGedP6FYV-SfIyzuIHXVqwVNfrDV6PUJbkdAKLUfgkWpNXCzTk4jueNeAPSQ1toQzKFChL__tnPte&from=10C7093010&c=iphone&s=2C32767A&containerid=100103type%3D1%26q%3D'
    # 备用地址
    # item_base_url = 'https://api.weibo.cn/2/searchall?gsid=_2A25P50dFDeRxGeFO7FQW8i7KzjyIHXVqtd2NrDV6PUJbkdANLRPbkWpNQVo2PALb0iBUq747x0EzGltixct0GNU9&from=10C7393010&c=iphone&s=1F2F9E9C&containerid=106003type%3D25%26t%3D'
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
        for c in weibo_dict['cards']:
            if len(c['card_group']) > 1:
                for card in c['card_group']:
                    if card['title_sub'] in self.urls_list:
                        print("SPIDER Duplicates!")
                        print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
                    else:
                        item_url = self.item_base_url + urllib.parse.quote(card['title_sub'])
                        title = card['title_sub']
                        # 分析发布者和热度值
                        desc = card['desc']
                        print(desc)
                        author_list = desc.split('@')
                        if len(author_list) > 1:
                            author = author_list[1]
                        else:
                            author = ''
                        hot_num_list = re.findall("\d+", desc)
                        hot_num = int(hot_num_list[0])
                        yield scrapy.Request(url=item_url, callback=self.parse_item,
                                             meta={'author':author, 'title':title, 'hot_num':hot_num})

    def parse_item(self, response):
        weibo_dict = json.loads(response.text)
        try:
            weibo_text = weibo_dict['cards'][0]['card_group'][0]['mblog']['text']
        except:
            weibo_text = response.meta['title']

        item = WeibohotItem()
        item['title'] = response.meta['title']
        item['author'] = response.meta['author']
        item['hot_num'] = response.meta['hot_num']
        item['text'] = self.del_spec_str(weibo_text)
        item['history'] = []
        item['add_time'] = int(time.time())
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
        return str

    def get_urls_list_in_db(self):
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['hot']
        query_col = query_db['weibo']

        items = query_col.find().sort('add_time', -1).limit(50)

        titles_list = []
        for item in items:
            titles_list.append(item['title'])

        query_client.close()
        return titles_list

    # 微博本地热榜的热度值在5000-6000之间，难以与其他热度比较
    # 因此将热度值差异拉大至5000-105000，便于比较
    # 暂不用
    def format_weibo_hot_num(self, old_hot_num):
        base = 5000
        multiple = 100
        new_hot_num = (int(old_hot_num) - base) * multiple + base
        return new_hot_num

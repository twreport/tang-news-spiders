import scrapy
import time
import json
from bs4 import BeautifulSoup
from datetime import datetime
from weibovip.items import WeibovipItem
import pymongo
import re
import math

class WeiboSpider(scrapy.Spider):
    name = 'weibo'
    allowed_domains = ['weibo.cn']
    # start_urls = ['http://weibo.cn/']
    url = ''
    uid = ''
    title_list = []

    def __init__(self, uid=None, vip_name=None, *args, **kwargs):
        super(WeiboSpider, self).__init__(*args, **kwargs)
        self.uid = uid
        self.vip_name = vip_name
        self.url = 'https://m.weibo.cn/api/container/getIndex?type=uid&value=' + uid + '&containerid=107603' + uid
        self.title_list = self.get_title_list_in_db()

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse)

    def parse(self, response):
        res_str = response.body.decode()
        res_dict = json.loads(res_str)
        cards = res_dict['data']['cards']
        for card in cards:
            if 'mblog' in card:
                mblog = card['mblog']
                text_div = '<div>' + card['mblog']['text'] + '</div>'
                soup = BeautifulSoup(text_div, 'lxml')
                text_str = soup.text
                text_str = text_str.replace('全文', '')
                a = 'https://m.weibo.cn/status/' + card['mblog']['id']
                pic_ids = card['mblog']['pic_ids']
                created_at = card['mblog']['created_at']
                created_at_str = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
                created_at_tuple = created_at_str.timetuple()
                created_at_int = int(time.mktime(created_at_tuple))
                title_list = text_str.split('#')
                if len(title_list) > 1:
                    title = title_list[1]
                else:
                    # 如果text里面没有用井号隔起来的文本，则寻找用【】括起来的文本
                    rule = r'【(.*?)】'
                    result = re.findall(rule, title_list[0])
                    if len(result) > 0:
                        title = result[0]
                    else:
                        # 则截取text的前10个字符
                        title = title_list[0][:20]

                play_count_int = 0
                if 'page_info' in mblog:
                    page_info = mblog['page_info']
                    page_type = page_info['type']
                    if page_type == 'video':
                        play_count_int = self.get_play_count(page_info['play_count'])
                        pic_url = page_info['page_pic']['url']
                        pic_ids.append(pic_url) 
                    else:
                        total_base = int(mblog['reposts_count']) + int(mblog['comments_count']) + int(mblog['reprint_cmt_count']) + int(mblog['attitudes_count'] + int(mblog['pending_approval_count']))
                        play_count_int = total_base * 1000

                # 去重复
                if title in self.title_list:
                    print("SPIDER Duplicates!")
                    print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
                else:
                    item = WeibovipItem()
                    item['title'] = title
                    # item['hot_num'] = self.format_weibo_vip_hot_num(play_count_int)
                    item['hot_num'] = play_count_int
                    item['url'] = a
                    item['text'] = text_str
                    item['pics'] = pic_ids
                    item['add_time'] = created_at_int
                    item['is_local'] = self.is_local(item['title'], item['text'])
                    item['uid'] = self.uid
                    item['vip_name'] = self.vip_name
                    item['status'] = 1
                    yield item
 

    def is_local(self, title, text):
        local_name_list = ['贵州','贵阳','观山湖','南明','毕节','遵义','黔东南','黔南','黔西','黔西南','六盘水','安顺','凯里','毕节',
                      '铜仁','黄果树','梵净山','千户苗寨','荔波','杜撰']
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

    def get_play_count(self, play_count_str):
        play_count_str_list = play_count_str.split('次')
        play_count_str = play_count_str_list[0]
        play_count_str = play_count_str.replace('万', '0000')
        play_count_int = int(play_count_str)
        return play_count_int

    def get_title_list_in_db(self):
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['weibo']
        query_col = query_db['bo']

        query_dict = {'uid': self.uid}
        items = query_col.find(query_dict).sort('update_time', -1).limit(200)

        titles_list = []
        for item in items:
            # print(item)
            titles_list.append(item['title'])

        query_client.close()
        return titles_list

    # 单独爬出的微博读数与微博地方榜差异太大，
    #  拟将热值控制在5000-6000之间
    def format_weibo_vip_hot_num(self, old_hot_num):
        new_hot_num = 0
        base = 5000
        limit = 20000
        # 大概折算的比例为阅读数的20000等于地方热搜的5000
        if old_hot_num > limit:
            diff = int(math.sqrt(old_hot_num))
            new_hot_num = base + diff
        else:
            diff = abs(old_hot_num - limit) / 10
            new_hot_num = base - diff
        return int(new_hot_num)


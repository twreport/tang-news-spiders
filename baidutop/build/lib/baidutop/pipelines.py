# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import time
import requests

client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
db = client['top']
col = db['baidu']

class BaidutopPipeline:
    def process_item(self, item, spider):
        return item

class MongodbPipeline:
    def __init__(self):
        # 连接数据库
        pass

    def process_item(self, item, spider):
        print('___________________==================++++++')
        col.insert_one(dict(item))
        return item

    def close_spider(self,spider):
        client.close()

class SendCloudMsg(object):
    def process_item(self, item, spider):
        is_local_result = item['is_local']
        if is_local_result == 1:
            print("------------Sent to Cloud!--------------")
            r1 = requests.post("https://tangwei.cc/api/receive_baidutop", data=item)
            print(r1)
        return item

class SendWarnMsgToWeixinApp(object):
    def process_item(self, item, spider):
        is_local_result = item['is_local']
        if is_local_result == 1:
            print("------------Warn MSG to Weixin App!--------------")
            desp = item['desc']
            if len(desp) > 100:
                desp = desp[:100]
            desp = desp.replace('#','',100)
            baidu_top_title = "【百度热搜】" + item['word']
            r1 = requests.get("https://sctapi.ftqq.com/SCT113045Tcb497fbmERp3h4AvSHOYx6vs.send?title=" + baidu_top_title + "&desp=" + desp)
            print(r1)
        return item


class SendWeixinMsg(object):
    def process_item(self, item, spider):
        is_local_result = item['is_local']
        if is_local_result == 1:
            print("------------sendwecom!--------------")
            text = '最新【百度热搜】：'
            text += '\n'
            text += item['word']
            text += '\n'
            text += '系统抓取时间：\n'
            text += self.custom_time(item['add_time'])
            text += '\n'
            text += '------------------------'
            text += '\n'
            if len(item['desc']) < 200:
                text += item['desc']
            else:
                text += item['desc'][:200] + '......' + '\n'
            text += '------------------------'
            text += '\n'
            text += '链接点此--->'
            text += '\n'
            text += item['url']
            text += '\n'

            payload = {
                'sendkey': 'himalayayeti',
                'text': text
            }
            r1 = requests.get("https://www.tangwei.cc/wecom/", params=payload)
            print(r1)
        return item

    def custom_time(self, timestamp):
        # 转换成localtime
        time_local = time.localtime(timestamp)
        # 转换成新的时间格式(2016-05-05 20:28:54)
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
        return dt


class SendItemToFindTopic(object):
    def process_item(self, item, spider):
        is_local_result = item['is_local']
        if is_local_result == 1:
            print("------------Send Baidu Top To AI Parse!--------------")
            data = {
                "type": "baidu",
                "title": item['word'],
                "text": item['desc'],
                "add_time": item['add_time'],
                "hot_num": self.format_top_num("baidu", item['hotScore']),
                "img": item['img'],
                "url": item['url']
            }
            r1 = requests.post("http://10.1.168.99:5001/api/topic/find_similar", data=data)
            print(r1)
        return item

    def format_hot_num(self, type_name, hot_num):
        base = 1
        if type_name == "weibo":
            base = 5
        elif type_name == "toutiao":
            base = 10
        elif type_name == "douyin":
            base = 40
        elif type_name == "baidu":
            base = 40
        elif type_name == "netease":
            base = 1
        elif type_name == "sina":
            base = 40
        else:
            base = 1
        changed_hot_num = int(int(hot_num) / base)
        return changed_hot_num
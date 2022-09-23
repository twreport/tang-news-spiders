# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import time
import json
import requests
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
db = client['top']
col = db['toutiao']

class ToutiaotopPipeline:
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
            image_json_str = json.dumps(item['Image'])
            item['Image'] = image_json_str
            r1 = requests.post("https://www.tangwei.cc/api/receive_toutiaotop", data=item)
            print(r1)
        return item


class SendWarnMsgToWeixinApp(object):
    def process_item(self, item, spider):
        is_local_result = item['is_local']
        if is_local_result == 1:
            print("------------Warn MSG to Weixin App!--------------")
            toutiao_top_title = "【头条热搜】" + item['Title']
            r1 = requests.get("https://sctapi.ftqq.com/SCT113045Tcb497fbmERp3h4AvSHOYx6vs.send?title=" + toutiao_top_title)
            print(r1)
        return item


class SendWeixinMsg(object):
    def process_item(self, item, spider):
        is_local_result = item['is_local']
        if is_local_result == 1:
            print("------------sendwecom!--------------")
            text = '最新【今日头条热搜】：'
            text += '\n'
            text += item['Title']
            text += '\n'
            text += '系统抓取时间：\n'
            text += self.custom_time(item['add_time'])
            text += '\n'
            text += '------------------------'
            text += '\n'
            text += '链接点此--->'
            text += '\n'
            text += item['Url']
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
            print("------------Send Toutiao Top To AI Parse!--------------")
            data = {
                "type": "toutiao",
                "title": item['Title'],
                "text": '',
                "add_time": item.add_time,
                "hot_num": self.format_hot_num("toutiao", item['HotValue']),
                "img": '',
                "url": item['Url']
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
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
db = client['weibo']
col = db['bo']


class WeibovipPipeline:
    def process_item(self, item, spider):
        return item

class MongodbPipeline:
    def __init__(self):
        # 连接数据库
        pass

    def process_item(self, item, spider):
        # 只有与本地相关的信息才放进mongoDB
        is_local_result = item['is_local']
        if is_local_result == 1:
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
            item['pics'] = json.dumps(item['pics'], ensure_ascii=False)
            print(item)
            r1 = requests.post("https://tangwei.cc/api/receive_bo", data=item)
            print(r1)
        return item
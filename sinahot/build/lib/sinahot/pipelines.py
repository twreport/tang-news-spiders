# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
import time
import requests
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
db = client['hot']
col = db['sina']

class SinahotPipeline:
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
        print("------------Sent to Cloud!--------------")
        r1 = requests.post("https://www.tangwei.cc/api/receive_sinahot", data=item)
        print(r1)
        return item
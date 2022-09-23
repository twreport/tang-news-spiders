# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import pymongo
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
db = client['weixin']



class WeixinLocalPipeline:
    def process_item(self, item, spider):
        return item

class MongodbPipeline:
    def __init__(self):
        # 连接数据库
        pass

    def process_item(self, item, spider):
        print('___________________==================++++++')
        col = db[spider.db]
        col.insert_one(dict(item))
        return item
    def close_spider(self,spider):
        client.close()
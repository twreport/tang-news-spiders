# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pymongo
from bson import ObjectId

client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
db = client['weixin']

class WeixinLogPipeline:
    def process_item(self, item, spider):
        return item

class MongodbPipeline:
    def __init__(self):
        # 连接数据库
        pass

    def process_item(self, item, spider):
        print('___________________==================++++++')
        print(spider.db)
        print(spider._id)
        print('___________________==================++++++')
        col = db[spider.db]
        update_query = {'_id': ObjectId(spider._id)}
        pipeline_item = col.find_one(update_query)
        print("pipeline_item:")
        print(pipeline_item)
        log = {
            'check_time': item['check_time'],
            'read_num': item['read_num']
        }
        if 'logs' in pipeline_item:
            pipeline_item['logs'].append(log)
        else:
            pipeline_item['logs'] = [log]
        print("pipeline_item_logs checked:")
        print(pipeline_item['logs'])
        update_values = {"$set": {"logs": pipeline_item['logs'], "check_time": item['check_time']}}
        result = col.update_one(update_query, update_values)
        print(result)
        return item

    def close_spider(self,spider):
        client.close()
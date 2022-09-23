# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
import pymongo
import time
import requests
from scrapy.utils.project import get_project_settings

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
db = client['hot']
col = db['weibo']


class WeibohotPipeline:
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
        r1 = requests.post("https://www.tangwei.cc/api/receive_weibohot", data=item)
        print(r1)
        return item

class SaveVIP(object):
    def __init__(self):
        setting = get_project_settings()
        db = setting['MYSQL_DB_NAME']
        host = setting['MYSQL_HOST']
        port = setting['MYSQL_PORT']
        user = setting['MYSQL_USER']
        passwd = setting['MYSQL_PASSWD']

        self.connect = pymysql.connect(
            db=db,
            host=host,
            user=user,
            port=port,
            passwd=passwd,
            charset='utf8',
            use_unicode=True
        )
        # 通过cursor执行增删查改
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        now = int(time.time())
        vip_name = item['author']

        is_vip_exist = self.is_vip_exist(vip_name, now)

        # 如果微博大V已经存在，则更新更新时间
        # 如果微博大V不存在，则新建
        if is_vip_exist is None:
            print('VIP is not exist! SAVE!!!')
            vip_uid = 0
            hot_num_avg = 0
            status = 0
            query = 'insert into admin_weibo_uid (vip_name, vip_uid, add_time, update_time, crawl_time, hot_num_avg, status) values (%s, %s, %s, %s, %s, %s, %s)'
            values = (vip_name, vip_uid, now, now, now, hot_num_avg, status)
            self.cursor.execute(query, values)
            self.connect.commit()

    def is_vip_exist(self, vip_name, update_time):
        my_query = "select * from admin_weibo_uid where vip_name = '" + vip_name + "';"
        self.cursor.execute(my_query)
        result = self.cursor.fetchone()
        if result is not None:
            sql = "update admin_weibo_uid set update_time = " + str(update_time) + " where vip_name = '" + vip_name + "';"
            self.cursor.execute(sql)
            self.connect.commit()
            return True
        else:
            return None

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd  #用来读MySQL
from redis import Redis
import pymysql
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DropItem
import requests
import urllib

redis_db = Redis(host='10.168.1.100', port=6379, db=3) #连接redis
redis_data_dict = "dizhensubao_url"  #key的名字，写什么都可以，但务必与其他爬虫区分！！！这里的key相当于字典名称，而不是key值。


class DizhensubaoPipeline:
    def process_item(self, item, spider):
        return item

class SendWeixinMsg(object):
    def process_item(self, item, spider):
        if '贵州' in item['text']:
            print("------------sendwecom!--------------")
            text = '【国家地震台网速报】'
            text += '\n'
            text += item['issue_date']
            text += '\n'
            text += '------------------------'
            text += '\n'
            text += item['text']
            text += '------------------------'
            text += '\n'
            text += '链接点此--->'
            text += '\n'
            text += item['url']
            text += '\n'
            # 以下上线配置（wecom服务器端口）
            payload = {
                'sendkey': 'himalayayeti',
                'text': text
            }
            r1 = requests.get("https://www.tangwei.cc/wecom/", params=payload)
            print(r1)

            # 以下为上线前测试配置（server酱端口）
            # text = self.urlencode(text)
            # print(text)
            # text = 'earth warning'
            # url = "https://sctapi.ftqq.com/SCT113045Tcb497fbmERp3h4AvSHOYx6vs.send?title=" + text
            # r1 = requests.get(url)
            # print(r1)
        else:
            pass
        return item

    def urlencode(self, string):
        return urllib.parse.quote(string)

class DuplicatesPipeline(object):
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
        redis_db.flushdb()  ## 删除redis里面的key

        if redis_db.hlen(redis_data_dict) == 0:
            sql = "SELECT text FROM tw_dizhensubao ORDER BY id DESC LIMIT 0,20;"  # 从你的MySQL里提数据，我这里取text来去重。
            df = pd.read_sql(sql, self.connect) #读MySQL数据
            for text in df['text'].values:  # 把每一条的值写入key的字段里:
                redis_db.hset(redis_data_dict, text, 0)

    def process_item(self, item, spider):
        # 取item里的url和key里的字段对比，看是否存在，存在就丢掉这个item。不存在返回item给后面的函数处理
        if redis_db.hexists(redis_data_dict, item['text']):
            print("\033[37;41m\tDuplicate item found! Item is dump!\033[0m")
            print("Item is dump!")
            raise DropItem("Duplicate item found: %s" % item)
        else:
            return item

    def close_spider(self,spider):
        self.connect.close()

class MySQLPipeline(object):
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
        print("------------in mysql---------------")
        print(item)
        insert_sql = """insert into tw_dizhensubao(title, url, 
         key_words, locations, text, issue_date, add_time, status)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        self.cursor.execute(
            insert_sql, (item['title'], item['url'],
                         item['key_words'], item['locations'], item['text'], item['issue_date'], item['add_time'],
                         item['status'])
        )
        self.connect.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()

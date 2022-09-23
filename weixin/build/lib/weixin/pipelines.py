# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd  #用来读MySQL
import pymysql
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DropItem
import requests
import time

class WeixinPipeline:
    def process_item(self, item, spider):
        return item

class SendWeixinMsg(object):
    def process_item(self, item, spider):
        if '新冠肺炎' in item['text'] or '确诊病例' in item['text'] or '无症状感染者' in item['text'] or '疫情' in item['text'] or '核酸检测' in item['text'] or '静态' in item['text']:
            if item['biz'] == 'MjM5MTg5OTM0Ng==' or item['biz'] == 'Mzg5MzA4ODk4Mw==' or '通报' in item['text'] or '通报' in item['title']:
                # 判断是否为故障刚排除的状态，此时爬取的微信播发时间远远晚于当前时间戳
                is_after_error = self.is_after_error(item['issue_date'])
                # 如果爬取时间在时限之外，说明没有报警的意义，跳过～
                # 如果在爬取时限之内，则报警
                if is_after_error is False:
                    print("------------sendwecom!--------------")
                    text = '【' + item['name'] + '微信公众号最新发布】'
                    text += '\n'
                    text += item['title']
                    text += '\n'
                    text += item['issue_date']
                    text += '\n'
                    text += '------------------------'
                    text += '\n'
                    if len(item['text']) < 200:
                        text += item['text']
                    else:
                        text += item['text'][:200] + '......' + '\n'
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

    def is_after_error(self, dt):
        error_time_interval = 2 * 60 * 60
        dt_int = self.unix_time(dt)
        now_int = int( time.time() )
        interval = now_int - dt_int
        if interval > error_time_interval:
            return True
        else:
            return False

    def unix_time(self, dt):
        # 转换成时间数组
        timeArray = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
        # 转换成时间戳
        timestamp = int(time.mktime(timeArray))
        return timestamp


class SendCloudMsg(object):
    def process_item(self, item, spider):
        if '新冠肺炎' in item['text'] or '确诊病例' in item['text'] or '无症状感染者' in item['text']:
            if item['biz'] == 'MjM5MTg5OTM0Ng==' or '通报' in item['text'] or '通报' in item['title']:
                print("------------Sent to Cloud!--------------")
                post_json = {
                    'title': item['title'],
                    'url': item['url'],
                    'issue_date': item['issue_date'],
                    'name': item['name'],
                    'status': 1
                }
                r1 = requests.post("https://www.tangwei.cc/api/ill", data=post_json)
                print(r1)
        return item


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
        print(item['biz'])
        biz_str = "'" + item['biz'] + "';"
        query_sql = "select * from admin_weixin_urls where biz = "  + biz_str
        query_num = self.cursor.execute(query_sql)
        query_result = self.cursor.fetchall()
        db_name = query_result[0][7]
        print('db_name:', db_name)
        # insert_sql = """insert into %s (title, url, key_words, locations, text, issue_date, add_time, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
        # self.cursor.execute(
        #     insert_sql, (db_name, item['title'], item['url'],
        #                  item['key_words'], item['locations'], item['text'], item['issue_date'], item['add_time'],
        #                  item['status'])
        # )

        # insert_sql = "insert into " + db_name + ' (title, url, key_words, locations, text, issue_date, add_time, status) VALUES ('\
        #              + item['title'] + ',' + item['url'] + ',' + item['key_words']\
        #              + ',' + item['locations'] + ',' + item['text'] + ',' + item['issue_date'] + ',' + str(item['add_time'])\
        #              + ',' + str(item['status']) + ')'

        # insert_sql = """insert into %s(title, url, key_words, locations, text, issue_date, add_time, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""" % (db_name, item['title'], item['url'], item['key_words'], item['locations'], item['text'], item['issue_date'], item['add_time'], item['status'])
        # print(insert_sql)
        # self.cursor.execute(insert_sql)

        keys = ', '.join(item.keys())
        values = ', '.join(['%s'] * len(item))
        insert_sql = 'INSERT INTO {table}({keys}) VALUES ({values})'.format(table=db_name, keys=keys, values=values)
        self.cursor.execute(insert_sql, tuple(item.values()))
        self.connect.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()
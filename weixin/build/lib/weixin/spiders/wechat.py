# coding:utf-8

import scrapy
import pymongo
from bs4 import BeautifulSoup
import re
import time
import json
from redis import Redis
import pandas as pd  # 用来读MySQL
import pymysql
from fake_useragent import UserAgent
from weixin.items import WeixinItem
from random import choice
import logging

redis_db = Redis(host='127.0.0.1', port=6379, db=3)  # 连接redis, db的数值尽量区别开
redis_data_dict = "f_url"  # key的名字，写什么都可以，这里的key相当于字典名称，而不是key值。

myclient = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
mydb = myclient['weixin']
mycol = mydb['url']

my_connect = pymysql.connect(
    db='news',
    host='10.168.1.100',
    user='admin',
    passwd='tw7311',
    charset='utf8',
    use_unicode=True
)
# 通过cursor执行增删查改
my_cursor = my_connect.cursor()


class WechatSpider(scrapy.Spider):
    name = 'wechat'
    allowed_domains = ['mp.weixin.qq.com']
    start_urls = ['http://mp.weixin.qq.com/']
    biz = ''
    db = ''

    # scrapyd配置，开发阶段注释掉
    def __init__(self, biz=None, db=None, wx_name=None, *args, **kwargs):
        # kwargs.pop('_job')
        super(WechatSpider, self).__init__(*args, **kwargs)
        self.biz = biz
        # print('biz:', biz)
        self.db = db
        self.name = wx_name
        logging.warning('===================Weixin Ask Info============================')
        logging.warning(self.biz)
        logging.warning(self.db)
        logging.warning(self.name)
        logging.warning('===================Weixin Ask Info End============================')

    def start_requests(self):
        now_time = int(time.time())
        # 在mongodb中取出所有访问参数
        query = {'biz': self.biz}
        urls = mycol.find(query).sort("update_time", -1)
        url_list = []
        for url in urls:
            # 判断该套参数是否已经被微信官方因频发操作而禁用
            is_frequent = self.is_error_frequent(url['x-wechat-uin'])
            if is_frequent is not True:
                url_list.append(url)
            # print(url)

        # 关闭mongo连接
        myclient.close()

        if url_list == []:
            # 如果所有的参数都已经访问频繁
            logging.warning("Weixin Frequent Operate! Spider is Closed!")
            self.crawler.engine.close_spider(self, "Weixin Frequent Operate! Spider is Closed!")
            return None
        else:
            # 选最新生成的那个参数
            url = url_list[0]
            logging.warning("url------>>>>>>")
            logging.warning(url)
            out_time_limit = 1800
            interval_update = int(now_time - int(url['update_time'] / 1000))

            if interval_update > out_time_limit:
                logging.warning("-------------PARAM ERROR-------------")
                logging.warning("Weixin Param is Out Time Before CRAWL!!!")
                self.crawler.engine.close_spider(self, "Weixin Param is Out Time! Spider is Closed!")
                return None
            else:
                yield scrapy.Request(url=url['url'], headers=url['headers'],
                                 meta={'uin': url['x-wechat-uin'], 'request_headers': url['headers']}, callback=self.parse)

    def parse(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        print("-----------------------response_body-----------------------")
        script = soup.find('script')
        print(script.string)
        if '失效的验证页面' in script.string:
            logging.warning("-------------PARAM ERROR-------------")
            self.handle_error_param(response.meta['uin'])
            logging.warning("Weixin Param is Out Time!")
            self.crawler.engine.close_spider(self, "Weixin Param is Out Time! Spider is Closed!")
            return None
        title = soup.find('h2', class_='weui_msg_title')
        if title is not None:
            logging.warning('===================Weixin Scrapy Error============================')
            if '操作频繁' in title.string:
                logging.warning("-------------FREQUENT OPERATING-------------")
                self.handle_error_frequent(response.meta['uin'])
            elif '请在微信客户端打开链接' in title.string:
                logging.warning("-------------PARAM ERROR-------------")
                self.handle_error_param(response.meta['uin'])
                logging.warning("Weixin Param is Out Time!")
        else:
            logging.warning('Weixin Param OK!')
        response_headers = response.headers
        # print("-----------------------response_headers-----------------------")
        # print(response_headers)
        #
        cookies = response.headers.getlist('Set-Cookie')
        # print("-----------------------cookies-----------------------")
        # print(cookies)

        cookie = self.convert_cookies_to_dict(cookies)
        cookie_utf8 = self.convert_cookies_to_str(cookies)
        # print("-----------------------cookies_utf8-----------------------")
        # print(cookie_utf8)

        # print("-----------------------cookie-----------------------")
        # print(cookie)
        # print("-----------------------biz-----------------------")
        # print(self.biz)

        # 分析公众号历史消息列表
        msg_str = re.findall("var msgList = '(.*?)';", str(soup))
        logging.warning('===================Weixin Msg List============================')
        # print(soup)
        logging.warning(msg_str[0])
        logging.warning('===================Weixin Msg List End============================')
        msg_dict = self.format_wechat_msg_list(msg_str[0])

        # 更新monggodb中的访问参数headers，为确保后续获取访问数爬虫不超时，每次必须对headers（其中的x_wechat_key）进行更新！
        # 新增monggodb中的访问参数中的cookies，使用微信官方服务器response返回的set-cookie
        # update_query = {'x-wechat-uin': response.meta['uin'], 'biz': self.biz}
        # request_headers = response.meta['request_headers']
        # request_headers['Cookie'] = cookie_utf8
        # update_values = {"$set": {"headers": request_headers, "type": "scrapy"}}
        # result = mycol.update_one(update_query, update_values)
        # print(result)


        # 更新mysql数据库biz表中的访问记录
        now_time = int(time.time())
        my_sql = "UPDATE news.admin_weixin_urls SET update_time = %d where biz = '%s'" % (int(now_time), self.biz)
        # print(my_sql)
        try:
            my_cursor.execute(my_sql)
            my_connect.commit()
        except:
            # 如果发生错误则回滚
            my_connect.rollback()
            logging.warning("更新失败")
        my_cursor.close()
        my_connect.close()

        url_list = []
        for msg_item in msg_dict['list']:
            info = msg_item['app_msg_ext_info']
            base_url = {
                "title": info['title'],
                "content_url": info['content_url'].replace("&amp;", "&")
            }
            url_list.append(base_url)
            if info['is_multi'] == 1:
                for multi_msg_item in info['multi_app_msg_item_list']:
                    multi_url = {
                        "title": multi_msg_item['title'],
                        "content_url": multi_msg_item['content_url'].replace("&amp;", "&")
                    }
                    url_list.append(multi_url)
        for url_dict in url_list:
            # 去重
            item_title_list = self.get_urls_list_in_db()
            if url_dict['title'] in item_title_list:
                logging.warning("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                if url_dict['content_url'] is not '':
                    usr_agent = str(UserAgent().random)
                    headers = {'User-Agent': usr_agent}
                    yield scrapy.Request(url=url_dict['content_url'],
                                         callback=self.parse_item,
                                         headers=headers,
                                         meta={'title': url_dict['title'], 'biz': self.biz}
                                         )

    def parse_item(self, response):
        soup = BeautifulSoup(response.body, 'lxml')

        js_div = soup.find('div', id='js_content')
        weixin_text = js_div.text
        # print("--------------------------------weixin_text-------------------------------------")
        # print(weixin_text)
        result = weixin_text.find('贵州省疫情防控政策')
        # print(result)
        if result != -1:
            weixin_text = weixin_text[:result]
        # print(weixin_text)
        # print("--------------------------------weixin_text_end-------------------------------------")
        # issue_date = re.findall(",o=\"(.*?)\";", str(soup))
        issue_date_str_list = re.findall("var ct = \"(.*?)\";", str(soup))
        issue_date_str = issue_date_str_list[0]
        if len(issue_date_str) == 10:
            issue_date_int = int(issue_date_str)
            # print('issue_date_int', issue_date_int)
            issue_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(issue_date_int))
        else:
            issue_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # print("issue_date:", issue_date)

        item = WeixinItem()
        item['title'] = response.meta['title']
        item['url'] = response.url
        item['key_words'] = ''
        item['locations'] = ''
        item['text'] = weixin_text
        # item['issue_date'] = issue_date[0]
        item['issue_date'] = issue_date
        # item['issue_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        item['add_time'] = int(time.time())
        item['status'] = 0
        item['biz'] = self.biz
        item['name'] = self.name
        yield item

    def convert_cookies_to_dict(self, cookies_list):
        cookies_dict = {}
        for cookie in cookies_list:
            cookie = self.format_cookie_str(cookie)
            cookie_list = cookie.split('=')
            cookies_dict[cookie_list[0]] = cookie_list[1]
        return cookies_dict

    def convert_cookies_to_str(self, cookies_list):
        cookies_str = ''
        for cookie in cookies_list:
            cookie_str = self.format_cookie_str(cookie) + ';'
            cookies_str = cookies_str + cookie_str
        cookies_str = cookies_str[:-1]
        return cookies_str

    def format_cookie_str(self, cookie):
        cookie = cookie.decode('utf-8')
        cookie_list = cookie.split(';')
        cookie_str = cookie_list[0]
        return cookie_str

    def format_wechat_msg_list(self, msg):
        msg = msg.replace('&amp;', '&')
        msg = msg.replace('amp;', '')
        msg = msg.replace('&quot;', '"')
        msg = msg.replace('&nbsp;', ' ')
        msg = self.find_quot_in_title(msg)
        msg_dict = json.loads(msg)
        return msg_dict

    # 万一标题中含有"",必须移除，否则会导致json解析报错！
    # 因此先做一次过滤
    # 算法为：如果文字紧接着文字，说明该引号为标题引号
    def find_quot_in_title(self, msg):
        # print("-------------------------in Parse----------------------------")
        msg_list_by_title = msg.split('"title":')
        msg_str_list = []
        for title in msg_list_by_title:
            if 'list' not in title:
                t_list = title.split(',')
                words = t_list[0]
                num = (len(words) - len(words.replace('"', ''))) // len('"')
                if num > 2:
                    t_list[0] = t_list[0].replace('"', '')
                    t_list[0] = '"' + t_list[0] + '"'
                title = ','.join(t_list)
            msg_str_list.append(title)
        msg_str = '"title":'.join(msg_str_list)
        # print(msg_str)
        return msg_str

    def get_urls_list_in_db(self):
        db = 'news'
        host = '10.168.1.100'
        port = 3306
        user = 'admin'
        passwd = 'tw7311'

        connect = pymysql.connect(
            db=db,
            host=host,
            user=user,
            port=port,
            passwd=passwd,
            charset='utf8',
            use_unicode=True
        )

        sql = "SELECT title FROM " + self.db + " ORDER BY id DESC limit 0,1000;"
        # 从你的MySQL里提数据，我这里取title来去重。
        df = pd.read_sql(sql, connect)  # 读MySQL数据
        titles_list = []
        for title in df['title'].values:
            titles_list.append(title)
        return titles_list

    def is_error_frequent(self, uin):
        db = 'news'
        host = '10.168.1.100'
        port = 3306
        user = 'admin'
        passwd = 'tw7311'

        connect = pymysql.connect(
            db=db,
            host=host,
            user=user,
            port=port,
            passwd=passwd,
            charset='utf8',
            use_unicode=True
        )

        now_time = int(time.time())
        limit_time = 24 * 60 * 60
        free_time = now_time - limit_time
        sql = "SELECT uin FROM admin_frequent_uin WHERE uin = '" + uin + "' AND frequent_time > " + str(free_time) + ";"
        print(sql)
        # 从你的MySQL里提数据，我这里取title来去重。
        df = pd.read_sql(sql, connect)  # 读MySQL数据
        # 判断当前uin是否是被禁用的uin，如果是就中断爬虫
        for uin_frequent in df['uin'].values:
            if uin == uin_frequent:
                return True
        return None

    def handle_error_frequent(self, uin):
        db = 'news'
        host = '10.168.1.100'
        port = 3306
        user = 'admin'
        passwd = 'tw7311'

        connect = pymysql.connect(
            db=db,
            host=host,
            user=user,
            port=port,
            passwd=passwd,
            charset='utf8',
            use_unicode=True
        )
        # 通过cursor执行增删查改
        cursor = connect.cursor()
        now_time = int(time.time())
        insert_sql = "INSERT INTO admin_frequent_uin SET uin = '" + uin + "' , frequent_time = " + str(now_time) + " , status = 1;"
        cursor.execute(insert_sql)
        connect.commit()
        cursor.close()
        connect.close()
        return None

    def handle_error_param(self, uin):
        # 等下次出现这种情况的时候再完善！！！
        return None
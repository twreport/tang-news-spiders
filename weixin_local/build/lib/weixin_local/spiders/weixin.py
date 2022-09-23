import scrapy
import pymongo
from bs4 import BeautifulSoup
from bs4.element import NavigableString
import re
import time
import json
from redis import Redis
import pandas as pd  # 用来读MySQL
import pymysql
from fake_useragent import UserAgent
from weixin_local.items import WeixinLocalItem
from random import choice
import logging


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

class WeixinSpider(scrapy.Spider):
    name = 'weixin'
    allowed_domains = ['mp.weixin.qq.com']
    start_urls = ['http://mp.weixin.qq.com/']
    biz = ''
    db = ''

    def __init__(self, biz=None, db=None, wx_name=None, *args, **kwargs):
        super(WeixinSpider, self).__init__(*args, **kwargs)
        self.biz = biz
        self.db = db
        self.name = wx_name
        logging.warning('===================Weixin Ask Info============================')
        logging.warning(self.biz)
        logging.warning(self.db)
        logging.warning(self.name)
        logging.warning('===================Weixin Ask Info End============================')

    def start_requests(self):
        # 在mongodb中取出所有访问参数
        query = {'biz': self.biz}
        urls = mycol.find(query).sort("update_time", -1)
        url_list = []
        for url in urls:
            # 判断该套参数是否已经被微信官方因频发操作而禁用
            is_frequent = self.is_error_frequent(url['x-wechat-uin'])
            is_out_time = self.is_out_time(url['update_time'])
            if is_frequent is None and is_out_time is None:
                url_list.append(url)

        # 关闭mongo连接
        myclient.close()

        if url_list == []:
            # 如果所有的参数都已经访问频繁
            logging.warning("Weixin Url is Frequent or Timeout! Spider is Closed!")
            self.crawler.engine.close_spider(self, "Weixin Url is Frequent or Timeout!")
            return None
        else:
            # 选最新生成的那个参数
            url = url_list[0]
            logging.warning("url------>>>>>>")
            logging.warning(url)
            yield scrapy.Request(url=url['url'], headers=url['headers'],
                                 meta={'uin': url['x-wechat-uin'], 'request_headers': url['headers']},
                                 callback=self.parse)



    def parse(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        print("-----------------------response_body-----------------------")
        print(soup)
        if "请在微信客户端打开链接" in soup:
            logging.warning("Weixin Param is Out Time!")
            return False
        else:
            logging.warning('Weixin Param is OK!')
        # print("-----------------------response_body_end-----------------------")

        title = soup.find('h2', class_='weui_msg_title')
        if title is not None:
            logging.warning('===================Weixin Scrapy Error============================')
            if '操作频繁' in title.string:
                logging.warning("-------------FREQUENT OPERATING!!!-------------")
                self.handle_error_frequent(response.meta['uin'])
            elif '请在微信客户端打开链接' in title.string:
                logging.warning("-------------PARAM ERROR-------------")
                self.handle_error_param(response.meta['uin'])
        else:
            logging.warning('Weixin Param Time is in Range!')

        response_headers = response.headers
        cookies = response.headers.getlist('Set-Cookie')
        cookie = self.convert_cookies_to_dict(cookies)
        cookie_utf8 = self.convert_cookies_to_str(cookies)


        # 分析公众号历史消息列表
        msg_str = re.findall("var msgList = '(.*?)';", str(soup))
        logging.warning('===================Weixin Msg List============================')
        # print(soup)
        logging.warning(msg_str[0])
        logging.warning('===================Weixin Msg List End============================')
        msg_dict = self.format_wechat_msg_list(msg_str[0])


        # 更新monggodb中的访问参数headers，为确保后续获取访问数爬虫不超时，每次必须对headers（其中的x_wechat_key）进行更新！
        # 新增monggodb中的访问参数中的cookies，使用微信官方服务器response返回的set-cookie
        # 本爬虫运行周期较长，更新作用聊胜于无：）
        # update_query = {'x-wechat-uin': response.meta['uin'], 'biz': self.biz}
        # request_headers = response.meta['request_headers']
        # request_headers['Cookie'] = cookie_utf8
        # update_values = {"$set": {"headers": request_headers, "type": "scrapy"}}
        # result = mycol.update_one(update_query, update_values)
        # print(result)


        # 更新mysql数据库biz表中的访问记录
        now_time = int(time.time())
        my_sql = "UPDATE news.admin_weixin_bizs SET update_time = %d where biz = '%s'" % (int(now_time), self.biz)
        # print(my_sql)
        try:
            my_cursor.execute(my_sql)
            my_connect.commit()
        except:
            # 如果发生错误则回滚
            my_connect.rollback()
            # print("更新失败")
        my_cursor.close()
        my_connect.close()

        # 如果有异常字符导致该公众号无法正常通过json解析获得列表，
        # 只能跳过，等待异常的文章在历史列表中消失，否则整个队列将被卡死
        if msg_dict is None:
            logging.warning("This Weixin Biz is ERROR! Spider is Closed!")
            self.crawler.engine.close_spider(self, "This Weixin Biz is ERROR! Spider is Closed!!")
            return None

        # 拼接历史消息列表
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
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                usr_agent = str(UserAgent().random)
                # print("User-Agent:::::::", usr_agent)
                headers = {'User-Agent': usr_agent}
                yield scrapy.Request(url=url_dict['content_url'],
                                     callback=self.parse_item,
                                     headers=headers,
                                     meta={'title': url_dict['title'], 'biz':self.biz, 'uin':response.meta['uin']}
                                     )


    def parse_item(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        article_ps = []
        js_div = soup.find('div', id='js_content')

        # 提取当前div中的所有文本
        weixin_text = js_div.text
        weixin_text = self.format_text(weixin_text)

        # 找出所有图片
        imgs = js_div.find_all('img')
        for img in imgs:
            # 如果包含图片，且是有效图片
            if 'src' in img.attrs:
                # 过滤掉图标类图片
                if 'wx_fmt=gif' not in img['src']:
                    p_content = img['src']
                else:
                    continue
            elif 'data-src' in img.attrs:
                if 'wx_fmt=gif' not in img['data-src']:
                    p_content = img['data-src']
                else:
                    continue
            else:
                continue

            article_p_text = {
                'p_type': 'img',
                'p_content': p_content
            }
            if article_p_text['p_content'] != '' and article_p_text not in article_ps:
                article_ps.append(article_p_text)

        #将文字的前50个字符作为提要
        p_str = weixin_text[:100]
        article_p_text = {
            'p_type': 'text',
            'p_content': p_str
        }
        article_ps.append(article_p_text)

        '''旧版，暂废弃
        children = js_div.children

        # 遍历所有子节点
        for p in children:
            # 如果是纯字符串，直接用做text
            img = p.find('img')
            if img is None:
                p_type = 'text'
                p_content = ''
                p_str_list = p.strings
                for p_str in p_str_list:
                    p_str = p_str.replace('\n', '')
                    p_str = p_str.replace('\u3000', '')
                    p_str = p_str.replace('\xa0', '')
                    p_str = p_str.replace('\u200d', '')
                    p_content = p_content + p_str
            else:
                # 如果包含图片，且是有效图片
                if 'src' in img.attrs:
                    print('src')
                    p_content = img['src']
                elif 'data-src' in p.attrs:
                    print('data-src')
                    p_content = img['data-src']
                else:
                    continue
            article_p = {
                'p_type': p_type,
                'p_content': p_content
            }
            if article_p['p_content'] != '' and article_p not in article_ps:
                article_ps.append(article_p)
        '''

        issue_date_str_list = re.findall("var ct = \"(.*?)\";", str(soup))
        issue_date_str = issue_date_str_list[0]
        print('issue_date_str')
        print(issue_date_str)
        if issue_date_str is not None and issue_date_str != '':

            issue_date_int = int(issue_date_str)
            add_time = int(issue_date_str)
            # print('issue_date_int', issue_date_int)
            issue_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(issue_date_int))
        else:
            issue_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            add_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # print("issue_date:", issue_date)

        item = WeixinLocalItem()
        item['title'] = response.meta['title']
        item['url'] = response.url
        item['key_words'] = ''
        item['locations'] = ''
        # 控制在1万字符以内
        item['text'] = weixin_text[:9999]
        item['json_contents'] = article_ps
        item['issue_date'] = issue_date
        item['add_time'] = add_time
        item['status'] = 0
        item['biz'] = self.biz
        item['uin'] = response.meta['uin']
        item['name'] = self.name
        # print(item)
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
        print(msg)
        msg = msg.replace('&amp;', '&')
        msg = msg.replace('amp;', '')
        msg = msg.replace('&quot;', '"')
        msg = msg.replace('&nbsp;', ' ')
        msg = self.find_quot_in_title(msg)
        print(msg)
        try:
            msg_dict = json.loads(msg)
        except:
            return None
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
        query_client = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
        query_db = query_client['weixin']
        query_col = query_db[self.db]

        query_dict = {'biz': self.biz}
        items = query_col.find(query_dict).sort('update_time', -1).limit(200)

        titles_list = []
        for item in items:
            # print(item)
            titles_list.append(item['title'])

        query_client.close()
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
                print('This url is frequent!!!')
                return True
        return None

    # 判断该参数是否超时，一般来说半个小时之内有效
    def is_out_time(self, time_int):
        now_time = int(time.time())
        # logging.warning('=== now time ===')
        # logging.warning(now_time)
        # logging.warning('=== update time ===')
        # logging.warning(now_time)
        limit_time = 30 * 60
        free_time = now_time - limit_time
        if time_int < free_time:
            logging.warning('This url is timeout!!!')
            return True
        else:
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

    def format_text(self, p_str):
        p_str = p_str.replace('\n', '')
        p_str = p_str.replace('\u3000', '')
        p_str = p_str.replace('\xa0', '')
        p_str = p_str.replace('\u200d', '')
        return p_str
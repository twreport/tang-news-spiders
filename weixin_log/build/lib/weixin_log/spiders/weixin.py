import scrapy
import pymongo
from bson import ObjectId
from weixin_log.items import WeixinLogItem
import json
import time
import logging
import pandas as pd  # 用来读MySQL
import pymysql

myclient = pymongo.MongoClient('mongodb://10.168.1.100:27017/')
mydb = myclient['weixin']

class WeixinSpider(scrapy.Spider):
    name = 'weixin'
    allowed_domains = ['weixin.qq.com']
    start_urls = ['http://weixin.qq.com/']
    db = None
    _id = None

    def __init__(self, db=None, _id=None, *args, **kwargs):
        super(WeixinSpider, self).__init__(*args, **kwargs)
        self.db = db
        self._id = _id
        logging.warning("==============Spider Info=================")
        logging.warning(self.db)
        logging.warning(self._id)
        logging.warning("==============Spider Info End=============")

    def start_requests(self):
        # url为固定值
        url = 'https://mp.weixin.qq.com/mp/getappmsgext?wx_header=1'
        # 按照传入db和_id, 取出一个即将爬取的文章
        mycol = mydb[self.db]
        # 在mongodb中使用_id查询，需转成ObjectId
        query = {'_id': ObjectId(self._id)}
        res_item = mycol.find_one(query)

        # 从文章url中提取post的数据
        item_data = self.get_data_from_item_url(res_item['url'])
        data = {
            '__biz': item_data['__biz'],
            'mid': item_data['mid'],
            'sn': item_data['sn'],
            'idx': item_data['idx'],
            'is_only_read': '1',
            'appmsg_type': '9'
        }

        # 根据uin在mongodb-weixin-url数据表中找出最新的User
        item_headers = self.get_headers_from_item_biz(res_item['biz'])
        # 安卓终端与ios终端参数有略微差异
        if 'x-wechat-uin' in item_headers:
            headers = {
                'User-Agent': item_headers['User-Agent'],
                'X-WECHAT-UIN': item_headers['x-wechat-uin'],
                'X-WECHAT-KEY': item_headers['x-wechat-key'],
                'Cookie': 'wxtokenkey=777'
            }
        else:
            headers = {
                'User-Agent': item_headers['User-Agent'],
                'X-WECHAT-UIN': item_headers['X-WECHAT-UIN'],
                'X-WECHAT-KEY': item_headers['X-WECHAT-KEY'],
                'Cookie': 'wxtokenkey=777'
            }

        # 判断该套参数是否已经被微信官方因频发操作而禁用
        is_frequent = self.is_error_frequent(headers['X-WECHAT-UIN'])
        if is_frequent is True:
            logging.warning("Weixin Frequent Operate! Spider is Closed!")
            self.crawler.engine.close_spider(self, "Weixin Frequent Operate! Spider is Closed!")
            return False
        else:
            yield scrapy.FormRequest(url=url, formdata=data, headers=headers, callback=self.parse)

    def parse(self, response):
        res_str = response.body.decode()
        res_dict = json.loads(res_str)
        logging.warning("================response================")
        logging.warning(res_dict)
        logging.warning("================response================")
        if 'appmsgstat' in res_dict:
            read_num = res_dict['appmsgstat']['read_num']
        else:
            read_num = 0

        item = WeixinLogItem()
        item['read_num'] = read_num
        item['check_time'] = int(time.time())

        myclient.close()
        yield item

    def get_data_from_item_url(self, url):
        params = url.split('?')
        param = params[1]
        para = param.split('&')
        data = {}
        for p in para:
            if '==' in p:
                p = p.replace('==','**')
            u = p.split('=')
            if '**' in u[1]:
                u[1] = u[1].replace('**','==')
            data[u[0]] = u[1]
        print("data in get_data_from_item_url:")
        print(data)
        return data

    def get_headers_from_item_biz(self, biz):
        now_time = int(time.time())
        url_col = mydb['url']
        uin_query = {'biz': biz}
        items = url_col.find(uin_query).sort("update_time", -1)

        url_list = []
        for url in items:
            # 判断该套参数是否已经被微信官方因频发操作而禁用
            is_frequent = self.is_error_frequent(url['x-wechat-uin'])
            if is_frequent is not True:
                url_list.append(url)
            # print(url)

        if url_list == []:
            # 如果所有的参数都已经访问频繁
            logging.warning("Weixin Frequent Operate! Spider is Closed!")
            self.crawler.engine.close_spider(self, "Weixin Frequent Operate! Spider is Closed!")
            return None
        else:
            # 选最新生成的那个参数
            url = url_list[0]

            out_time_limit = 1800
            interval_update = int(now_time - int(url['update_time'] / 1000))

            if interval_update > out_time_limit:
                logging.warning("-------------PARAM ERROR-------------")
                logging.warning("Weixin Param is Out Time Before CRAWL!!!")
                self.crawler.engine.close_spider(self, "Weixin Param is Out Time! Spider is Closed!")
                return None

            headers = url['headers']
            print("headers in get_data_from_item_url")
            print(headers)
            return headers

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
import imp
import scrapy
from bs4 import BeautifulSoup
from scrapy_splash import SplashRequest
from dizhensubao.items import DizhensubaoItem
import time
import re
import pandas as pd  # 用来读MySQL
from redis import Redis
import pymysql

lua = """
function main(splash, args)
  assert(splash:go(args.url))
  assert(splash:wait(2))
  return {
    html = splash:html()
  }
end
"""


class DizhenSpider(scrapy.Spider):
    name = 'dizhen'
    allowed_domains = ['m.weibo.com']
    start_urls = ['https://m.weibo.cn/u/1904228041']
    redis_db = Redis(host='127.0.0.1', port=6379, db=4)  # 连接redis
    redis_data_dict = "f_url"  # key的名字，写什么都可以，这里的key相当于字典名称，而不是key值。

    def __init__(self, *args, **kwargs):
        super(DizhenSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(url=url, callback=self.parse, endpoint='execute',
                                args={'lua_source': lua, 'url': url})

    def parse(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        card_main_divs = soup.find_all('div', class_='card-main')
        for card_main_div in card_main_divs:
            time_span = card_main_div.find('span', class_='time')
            time_data = self.parse_time(time_span.string)
            print(time_data)
            weibo_text_div = card_main_div.find('div', class_='weibo-text')
            weibo_text = ''
            for weibo_text_str in weibo_text_div.strings:
                weibo_text = weibo_text + weibo_text_str

            item = DizhensubaoItem()
            item['title'] = "地震快讯"
            item['url'] = 'https://m.weibo.cn/u/1904228041'
            item['key_words'] = ''
            item['locations'] = ''
            item['text'] = weibo_text
            item['issue_date'] = time_data
            item['add_time'] = int(time.time())
            item['status'] = 0

            yield item

    def parse_time(self, date):
        if re.match('刚刚', date):
            date = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
        if re.match('\d+分钟前', date):
            minute = re.match('(\d+)', date).group(1)
            date = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time() - float(minute) * 60))
        if re.match('\d+小时前', date):
            hour = re.match('(\d+)', date).group(1)
            date = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time() - float(hour) * 60 * 60))
        if re.match('昨天.*', date):
            date = re.match('昨天(.*)', date).group(1).strip()
            date = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60)) + ' ' + date
        if re.match('\d{2}-\d{2}', date):
            date = time.strftime('%Y-', time.localtime()) + date + ' 00:00'
        if re.match('\d{1}-\d{2}', date):
            date = time.strftime('%Y-', time.localtime()) + '0' + date
        return date

import imp
import scrapy
from bs4 import BeautifulSoup
from scrapy_splash import SplashRequest
from guizhouweijianwei.items import GuizhouweijianweiItem
import time
import re
import pandas as pd  # 用来读MySQL
from redis import Redis
import pymysql


redis_db = Redis(host='127.0.0.1', port=6379, db=4)  # 连接redis
redis_data_dict = "f_url"  # key的名字，写什么都可以，这里的key相当于字典名称，而不是key值。


class WeijianweiSpider(scrapy.Spider):
    name = 'weijianwei'
    allowed_domains = ['gzhfpc.gov.cn', 'wjw.guizhou.gov.cn']
    start_urls = ['http://wjw.guizhou.gov.cn/xwzx_500663/yqtb/']

    def __init__(self, *args, **kwargs):
        super(WeijianweiSpider, self).__init__(*args, **kwargs)

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

        sql = "SELECT url FROM tw_guizhouweijianwei ORDER BY id DESC limit 0,50;"  # 从你的MySQL里提数据，我这里取url来去重。
        df = pd.read_sql(sql, connect)  # 读MySQL数据
        urls_list = []
        for url in df['url'].values:
            print(url)
            urls_list.append(url)
        return urls_list


    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        h3 = soup.find('h3', text="疫情通报")
        dl = h3.parent.parent

        lis = dl.find_all('li')
        for li in lis:
            print(li)

        hrefs = []
        urls_list = self.get_urls_list_in_db()

        for li in lis:
            script = li.find('script')
            script_str = script.string
            url = re.findall('var str_1 = "(.*?)"', script_str)
            href = url[0]
            href = 'http://wjw.guizhou.gov.cn/xwzx_500663/yqtb' + href[1:]
            print("Find Hrefs-------->")
            print(href)

            # 本来也可以通过pipelines处理，但高频爬取容易被踢出，因此直接在爬虫中规避。
            if href in urls_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                hrefs.append(href)

        if len(hrefs) > 0:
            for link in hrefs:
                yield scrapy.Request(url=link, callback=self.parse_item)


    def parse_item(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        title_div = soup.find('div', class_='title')
        title_h1 = title_div.find('h1')
        title = title_h1.string
        print(title)

        issue_date_time = '1000-01-01 00:00:00'
        toolbar = soup.find('div', class_='toolbar')
        scripts = toolbar.find_all('script')
        for script in scripts:
            script_str = script.string
            issue_date_time_list = re.findall("pubdata='(.*?)';", script_str)
            if len(issue_date_time_list) > 0:
                issue_date_time = issue_date_time_list[0]
        print(issue_date_time)

        text = ''
        font_div = soup.find('font', id='Zoom')
        ps = font_div.find_all('p')
        for p in ps:
            for p_str in p.strings:
                text = text + p_str + '\n'

        item = GuizhouweijianweiItem()
        item['title'] = title
        item['url'] = response.url
        item['key_words'] = ''
        item['locations'] = ''
        item['text'] = text
        item['issue_date'] = self.chinese_year_to_date_time(issue_date_time)
        item['add_time'] = int(time.time())
        item['status'] = 0

        yield item

    def chinese_year_to_date_time(self, cn_str):
        num_list = re.findall('(\d+)', cn_str)
        cn_str_list = cn_str.split(' ')
        return num_list[0] + '-' + num_list[1] + '-' + num_list[2] + ' ' + cn_str_list[1]

import scrapy
from bs4 import BeautifulSoup
import pandas as pd  # 用来读MySQL
import pymysql
from guiyangweijianwei.items import GuiyangweijianweiItem
import time


class GywjwSpider(scrapy.Spider):
    name = 'gywjw'
    allowed_domains = ['guiyang.gov.cn']
    start_urls = ['http://wsjkj.guiyang.gov.cn/tzgg/']

    def __init__(self, *args, **kwargs):
        super(GywjwSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        current_table = soup.find('a', class_='CurrChnlCls')
        table = current_table.parent.parent.parent.parent.parent.parent.parent.parent.parent
        div = table.find('div')

        print('current_table:', current_table)
        print('div:', div)
        links = div.find_all('a', target="_blank")
        hrefs = []
        titles_list = self.get_titles_list_in_db()
        for link in links:
            href = 'http://wsjkj.guiyang.gov.cn/tzgg/' + link['href']
            title = link['title']
            print(href)
            print(title)
            # 本来也可以通过pipelines处理，但高频爬取容易被踢出，因此直接在爬虫中规避。
            if title in titles_list:
                print("SPIDER Duplicates!")
                print("\033[37;41m\tDuplicate item found! This href is STOP!\033[0m")
            else:
                href_dict = {
                    'title': title,
                    'href': href
                }
                hrefs.append(href_dict)
        for url in hrefs:
            yield scrapy.Request(url=url['href'], callback=self.parse_item, meta={'title': url['title']})


    def parse_item(self, response):
        soup = BeautifulSoup(response.body, 'lxml')
        div_zoom = soup.find('div', id='zoom')
        print(response.meta['title'])
        print(div_zoom)
        info_tr = div_zoom.parent.parent.parent
        tds = info_tr.find_all('td')
        issue_date = ''
        for td in tds:
            for td_str in td.strings:
                if '日期：' in td_str:
                    issue_date_raw = td_str
                    issue_date_raw_list = issue_date_raw.split()
                    issue_date = issue_date_raw_list[1] + ' ' + issue_date_raw_list[2] + ':00'
        print('issue_date', issue_date)

        text = ''
        for text_str in div_zoom.strings:
            text = text + text_str
        print(text)

        item = GuiyangweijianweiItem()
        item['title'] = response.meta['title']
        item['url'] = response.url
        item['key_words'] = ''
        item['locations'] = ''
        item['text'] = text
        item['issue_date'] = issue_date
        item['add_time'] = int(time.time())
        item['status'] = 0

        yield item

    def get_titles_list_in_db(self):
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

        sql = "SELECT title FROM tw_guiyangweijianwei ORDER BY id DESC limit 0,100;"  # 从你的MySQL里提数据，我这里取title来去重。
        df = pd.read_sql(sql, connect)  # 读MySQL数据
        titles_list = []
        for title in df['title'].values:
            print(title)
            titles_list.append(title)
        return titles_list

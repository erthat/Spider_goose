from twisted.internet import asyncioreactor
asyncioreactor.install()
import mysql.connector
from mysql.connector import Error
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from urllib.parse import urlparse
import dateparser
from dateparser import parse
import time
import scrapy
import os
from dotenv import load_dotenv
import emoji
from datetime import datetime


load_dotenv()
class ResourceSpider(CrawlSpider):
    name = 'resource_spider'

    def __init__(self, *args, **kwargs):
        super(ResourceSpider, self).__init__(*args, **kwargs)
        self.conn = None
        self.cursor = None
        self.start_urls = []
        try:
        # Подключение к базе данных для докера smi_gui-main-mariadb-server-1
            self.conn = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_DATABASE"),
                charset='utf8mb4',
                collation='utf8mb4_general_ci'

            )
            if self.conn.is_connected():
                self.cursor = self.conn.cursor()

                # Загрузка ресурсов из базы данных
                self.cursor.execute(
                    "SELECT RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut "
                    "FROM resource "
                    "WHERE status = %s AND bottom_tag IS NOT NULL AND bottom_tag <> '' "
                    "AND title_cut IS NOT NULL AND title_cut <> '' "
                    "AND date_cut IS NOT NULL AND date_cut <> ''"
                    "AND RESOURCE_STATUS = %s",
                    ('spider_scrapy', 'WORK')
                )
                self.resources = self.cursor.fetchall()

                self.start_urls = [resource[2].split(',')[0].strip() for resource in self.resources]
                self.allowed_domains = [urlparse(url).netloc for url in self.start_urls]
                print(self.allowed_domains)

                # Создание правил для каждого ресурса
                self.rules = (
                    Rule(LinkExtractor(restrict_xpaths="//a"), callback='parse_links', follow=True, process_links=self.limit_links),
                )

                super()._compile_rules()


        except Error as e:
            self.log(f"Error connecting to MySQL: {e}")
            # Переключаемся на временный паук
            self.name = "temporary_spider"
            self.start_urls = ["http://example.com"]
            self.rules = ()
            self._compile_rules()

    def limit_links(self, links):
        # Ограничиваем количество ссылок до, например, 10
        return links[:100]

    def parse_links(self, response):
        # Получаем текущий URL
        current_url = response.url
        self.cursor.execute("SELECT 1 FROM temp_items_link WHERE link = %s", (current_url,))
        if self.cursor.fetchone() is not None:
            print(f'ссылка существует {current_url}')
            return
        print(f'Проверка контента из {current_url}')
        parsed_current_url = urlparse(current_url)
        current_netloc = parsed_current_url.netloc.replace('www.', '')
        #Ищем RESOURCE_ID для текущего URL
        resource_id = None
        resource_info = None
        for resource in self.resources:
            first_url = resource[2].split(',')[0].strip()
            parsed_first_url = urlparse(first_url)
            first_netloc = parsed_first_url.netloc.replace('www.', '')
            if first_netloc == current_netloc:
                resource_id = resource[0]
                resource_info = resource
                break

        if resource_id:
            title_t = response.xpath(resource_info[5]).get()
            if not title_t:
                self.logger.warning(f"Заголовок отсутствует для {current_url}")
                return
            content = response.xpath(resource_info[4]).getall()
            if not content:
                self.logger.warning(f"Контент отсутствует для {current_url}")
                return
            title = self.replace_unsupported_characters(title_t)
            content = ' '.join([c.strip() for c in content if c.strip()])
            content = self.replace_unsupported_characters(content)
            date = response.xpath(resource_info[6]).get()
            date = str(date) if date else ''
            date = parse(date)
            if not date:
                date = response.xpath('//meta[@property="article:published_time"]/@content').get()
                date = str(date) if date else ''
                date = parse(date)
                if not date:
                    self.logger.warning(f"Дата отсутствует для {date}")
                    return
            nd_date = int(time.mktime(date.timetuple()))
            not_date = date.strftime('%Y-%m-%d')
            s_date = int(time.time())


            self.store_news(resource_id, title, current_url, nd_date, content, s_date, not_date)
            self.store_link(current_url)

    def store_link(self, current_url):  # сохраняем ссылки в бд
        self.cursor.execute(
            "INSERT INTO temp_items_link (link) VALUES (%s)",
            (current_url,)
                            )
        self.conn.commit()

    def store_news(self, resource_id, title, current_url, nd_date, content, s_date, not_date):
        self.cursor.execute(
            "INSERT INTO temp_items (res_id, title, link, nd_date, content, s_date, not_date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (resource_id, title, current_url, nd_date, content, s_date, not_date,)
                            )
        self.conn.commit()

    def replace_unsupported_characters(self, text):

        text = str(text) if text else ''
        return emoji.replace_emoji(text, replace='?')

    def close(self, reason):
        if self.cursor:
            self.cursor.close()
        if self.conn and self.conn.is_connected():
            self.conn.close()

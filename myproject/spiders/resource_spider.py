import logging


import pytz
import mysql.connector
from mysql.connector.aio.logger import logger
from scrapy.settings.default_settings import LOG_FILE
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

from dateparser import parse
import time
from dotenv import load_dotenv
import emoji
from datetime import datetime
import re
from lxml.html import fromstring
import bs4
import os
from mysql.connector import Error
from logging.handlers import RotatingFileHandler
from scrapy.utils.log import configure_logging
import unicodedata
from scrapy import Request
from urllib.parse import urlparse, urlunparse
import urllib.parse


load_dotenv()
class ResourceSpider(CrawlSpider):
    name = 'resource_spider'
    custom_settings = { }

    def __init__(self, resources=None,  spider_name=None, *args, **kwargs):
        self.spider_name = spider_name or self.name
        super().__init__(*args, **kwargs)


        log_file = f'logs/{spider_name}.log'
        handler = RotatingFileHandler(
            log_file, maxBytes= 10 * 1024 * 1024, backupCount=0
        )
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # Создаем логгер с именем паука
        self.custom_logger = logging.getLogger(spider_name)
        self.custom_logger.setLevel(logging.INFO)
        # Проверяем, добавлены ли уже обработчики к логгеру
        if not self.custom_logger.handlers:
            self.custom_logger.addHandler(handler)
            self.custom_logger.addHandler(console_handler)

        self.setup_scrapy_logging(spider_name, handler, console_handler)

        # Передача ресурсов
        self.resources = resources

        self.cursor_1 = None
        self.conn_2 = None
        self.cursor_2 = None
        self.start_urls = []


        try: # подключение к таблице temp_items и temp_items_link
            self.conn_2 = mysql.connector.connect(
                host=os.getenv("DB_HOST_2"),
                user=os.getenv("DB_USER_2"),
                password=os.getenv("DB_PASSWORD_2"),
                database=os.getenv("DB_DATABASE_2"),
                port=os.getenv("DB_PORT_2"),
                charset='utf8mb4',
                collation='utf8mb4_general_ci',
                connection_timeout=300,
                autocommit=True

            )
            if self.conn_2.is_connected():
                self.cursor_2 = self.conn_2.cursor(buffered=True)
                self.custom_logger.info(f'Есть подключение к БД: {spider_name}')

                if resources:
                    self.resources = resources
                    self.start_urls = [resource[2].split(',')[0].strip() for resource in self.resources]
                    self.allowed_domains = [urlparse(url).netloc.replace('www.', '') for url in self.start_urls]
                    self.custom_logger.info(f'Allowed domains: {self.allowed_domains}')
                    self.resource_map = {resource[0]: resource for resource in self.resources}

            else:
                self.log("No resources found, spider will close.")
                self.crawler.engine.close_spider(self, f'Нету данных в бд {spider_name}')


        except Error as e:
            self.log(f"Error connecting to MySQL: {e}")
            self.custom_logger.info('Нет подключение к БД')
            # Переключаемся на временный паук чтобы закрыть паука и запустить через 30 мин
            self.name = "temporary_spider"
            self.start_urls = ["http://example.com"]
            self.rules = ()
            self._compile_rules()

    def remove_url_fragment(self, url):
        parsed_url = urlparse(url)
        # Собираем URL без фрагмента (части после #)
        return urlunparse(parsed_url._replace(fragment=''))

    def normalize_url(self, url):
        parsed_url = urlparse(url)
        # Проверяем, что путь не является пустым и не является корневым ('/')
        if parsed_url.path and not parsed_url.path.endswith('/'):
            # Добавляем слэш в конец пути
            normalized_url = parsed_url._replace(path=parsed_url.path + '/').geturl()
        else:
            normalized_url = parsed_url.geturl()
        return normalized_url


    def parse_start_url(self, response):
        """Функция для парсинга стартовой страницы и начала парсинга ссылок"""

        current_domain = urlparse(response.url).hostname.replace('www.', '')
        resource_info = next(
            (res for res in self.resource_map.values() if
             urlparse(res[2].split(',')[0].strip()).hostname.replace('www.', '') == current_domain),
            None
        )

        if resource_info:
            # Извлекаем top_tag для текущего ресурса
            top_tag = resource_info[3]
            top_tags = [xpath.strip() for xpath in top_tag.split(';')]
            deny = resource_info[8]
            max_depth = int(resource_info[9]) if resource_info[9] else 1

            if deny:
                denys = [rule.strip() for rule in deny.split(';')]
            else:
                denys = []
            deny_extensions = ['jpg', 'jpeg', 'png', 'gif', 'pdf', 'doc', 'docx', 'JPG', 'jfif', 'mp3',
                'mp4', 'pptx', 'zip', 'rar', 'xlsx', 'webp', 'wav', 'ppt']
            # Создаем LinkExtractor для этого домена
            link_extractor = LinkExtractor(restrict_xpaths=top_tags, deny=denys, deny_extensions=deny_extensions)
            # Извлекаем ссылки
            links = link_extractor.extract_links(response)
            filtered_links = []
            for link in links:
                link_domain = urlparse(link.url).netloc.replace('www.', '')
                if link_domain in self.allowed_domains:
                    filtered_links.append(link)

            valid_links = []
            for link in filtered_links:
                url_link = self.remove_url_fragment(self.normalize_url(link.url))
                url_link_2 = self.remove_url_fragment(link.url.rstrip('/'))
                if not self.conn_2.is_connected():
                    try:
                        self.custom_logger.warning("Соединение с базой данных потеряно, пытаемся переподключиться...")
                        self.conn_2.reconnect(attempts=3, delay=5)
                        self.custom_logger.info("Соединение восстановлено")
                    except mysql.connector.Error as err:
                        self.custom_logger.warning(f"Ошибка переподключения: {err}")
                        return  # Прекращаем выполнение, если не удалось переподключиться

                self.cursor_2.execute("SELECT 1 FROM temp_items_link WHERE link = %s OR link = %s LIMIT 1",
                                      (url_link, url_link_2))
                # Если ссылка не найдена в базе, добавляем её в список валидных ссылок
                if self.cursor_2.fetchone() is None:
                    valid_links.append(link)

            # print(filtered_links)

            # Следуем за каждой ссылкой и передаем в parse_links
            for link in valid_links:
                yield Request(url=link.url, callback=self.parse_links, meta={'resource_info': resource_info, 'top_tags': top_tags, 'depth': 1, 'denys': denys,
                                                                             'deny_extensions': deny_extensions, 'max_depth': max_depth })


    def parse_links(self, response):
        current_url = response.url
        if any(current_url.endswith(ext) for ext in
               ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.doc', '.docx', '.JPG', '.jfif', '.mp3',
                '.mp4']):
            self.custom_logger.info(f'Пропускаем неподходящий ссылку: {current_url}')
            return

        resource_info = response.meta.get('resource_info')
        resource_id = resource_info[0]
        current_depth = response.meta.get('depth', 1)
        deny_extensions = response.meta.get('deny_extensions')
        denys = response.meta.get('denys')
        max_depth = response.meta.get('max_depth')
        top_tags = response.meta.get('top_tags')


        if current_depth < max_depth:
            link_extractor = LinkExtractor(restrict_xpaths=top_tags, deny=denys, deny_extensions=deny_extensions)
            # Извлекаем ссылки для дальнейшего парсинга
            links = link_extractor.extract_links(response)

            filtered_links = []
            # Фильтруем ссылки по allowed_domains
            for link in links:
                link_domain = urlparse(link.url).netloc.replace('www.', '')
                if link_domain in self.allowed_domains:
                    filtered_links.append(link)
            # url_list2 = [link.url for link in filtered_links]
            # print(url_list2)
            valid_links = []
            for link in filtered_links:
                url_link = self.remove_url_fragment(self.normalize_url(link.url))
                url_link_2 = self.remove_url_fragment(link.url.rstrip('/'))
                if not self.conn_2.is_connected():
                    try:
                        self.custom_logger.warning("Соединение с базой данных потеряно, пытаемся переподключиться...")
                        self.conn_2.reconnect(attempts=3, delay=5)
                        self.custom_logger.info("Соединение восстановлено")
                    except mysql.connector.Error as err:
                        self.custom_logger.warning(f"Ошибка переподключения: {err}")
                        return
                self.cursor_2.execute("SELECT 1 FROM temp_items_link WHERE link = %s OR link = %s LIMIT 1",
                                      (url_link, url_link_2))
                # Если ссылка не найдена в базе, добавляем её в список валидных ссылок
                if self.cursor_2.fetchone() is None:
                    valid_links.append(link)

            for link in valid_links:
                yield Request(
                    url=link.url,
                    callback=self.parse_links,
                    meta={'resource_info': resource_info, 'top_tags':top_tags, 'depth': current_depth + 1, 'denys': denys,
                          'deny_extensions': deny_extensions, 'max_depth': max_depth})

        # получение заголовок новостей
        title_t = response.xpath(f'normalize-space({resource_info[5]})').get()
        if not title_t:
            self.custom_logger.info(f"Заголовок отсутствует для {current_url}")
            return
        title = self.replace_unsupported_characters(title_t) # чистка текста

       # Парсинг даты
        xpath_and_pattern = resource_info[6]  # Получаем строку из resource_info
        parts = xpath_and_pattern.split('::::')
        date_xpath = parts[0]  # XPath для парсинга даты
        remove_patterns = parts[1] if len(parts) > 1 else None

        date = response.xpath(date_xpath).get()
        if not date:
            self.custom_logger.info(f"Дата отсутствует {date}, {current_url}")
            return
        if remove_patterns:
            date = re.sub(remove_patterns, '', date)
            # print(date)
        date = self.parse_date(date, resource_info[7], resource_info[10])
        if not date:
            self.custom_logger.info(f"Дата отсутствует {date}, {current_url}")
            return
        n_date = date
        # print(date)#дата публикаций новостей
        nd_date = int(date.timestamp()) #дата публикаций новостей UNIX формате
        not_date = date.strftime('%Y-%m-%d') #дата публикаций новостей
        s_date = int(time.time()) #дата поступление новостей в таблицу
        one_year_in_seconds = 365 * 24 * 3600
        if s_date - nd_date > one_year_in_seconds:
            self.custom_logger.info(f"Дата {date} старее чем на год для {current_url}")
            return
        # получение контента новостей
        content = response.xpath(resource_info[4]).getall()
        content = self.clean_text(content) # чистка текста
        if not content or all(item.isspace() for item in content):
            self.custom_logger.info(f"Контент отсутствует для {current_url}")
            return
        self.store_news(resource_id, title, current_url, nd_date, content, n_date, s_date, not_date) # отправка на сохранение в бд


    def store_news(self, resource_id, title, current_url, nd_date, content, n_date, s_date, not_date):
        # Проверка соединения перед выполнением операций
        if not self.conn_2.is_connected():
            try:
                self.custom_logger.warning("Соединение с базой данных потеряно, пытаемся переподключиться...")
                self.conn_2.reconnect(attempts=3, delay=5)
                self.custom_logger.info("Соединение восстановлено")
            except mysql.connector.Error as err:
                self.custom_logger.warning(f"Ошибка переподключения: {err}")
                return  # Прекращаем выполнение, если не удалось переподключиться

        url_link = self.remove_url_fragment(self.normalize_url(current_url))
        url_link_2 = self.remove_url_fragment(current_url.rstrip('/'))

        self.cursor_2.execute("SELECT 1 FROM temp_items WHERE link = %s OR link = %s LIMIT 1", (url_link, url_link_2))
        if self.cursor_2.fetchone() is None:
            status = ''
            self.cursor_2.execute(
                "INSERT INTO temp_items (res_id, title, link, nd_date, content, n_date, s_date, not_date, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (resource_id, title, current_url, nd_date, content, n_date, s_date, not_date, status)
            )
            self.conn_2.commit()
            self.custom_logger.warning(f'Новость добавлена в базу, дата: {n_date} time: {nd_date}, URL: {current_url} ')
        else:
        # Если ссылка уже существует
            self.custom_logger.info(f'Ссылка уже существует в базе TEMP: Дата {n_date} ({nd_date}) url: {current_url}')


    def replace_unsupported_characters(self, text):
        text = str(text) if text else ''
        return emoji.replace_emoji(text, replace='?')

    def clean_text(self, parsed_fields: list[str]) -> str | int:
        """Function that removes junk html tags and performs some text normalization
        Very similar to what Sphinx Search does in current configuration.
        """
        if not parsed_fields:
            return ""
        content = " ".join(parsed_fields)
        content = content.replace("'", '"').strip()
        KEYWORD_SELECTORS = ["//@alt", "//@title", "//@content", "//@data-body", "//@body"]
        keywords = {
            k for selector in KEYWORD_SELECTORS for k in fromstring(content).xpath(selector)
        }

        resp = bs4.BeautifulSoup(content, features="html.parser")

        for el in resp.findAll("script"):
            el.decompose()

        for el in resp.findAll("style"):
            el.decompose()

        for el in resp.findAll("img"):
            el.decompose()

        for comment in resp.findAll(string=True):
            if isinstance(comment, bs4.element.Comment):
                comment.extract()
        try:
            content = " ".join(resp.findAll(string=True))
        except AttributeError:
            content = resp.text

        content = content + " " + " ".join(keywords)

        content = content.replace("\N{SOFT HYPHEN}", "")
        content = re.sub(r"\\+", r"\\", content)
        content = re.sub(r"\\n|_", " ", content)

        content = re.sub(r"\s+", " ", content)
        content = emoji.demojize(content)
        content = unicodedata.normalize('NFKD', content)
        return content

    def parse_date(self, date_str, convert_date, lang):
        date_str = str(date_str) if date_str else ''
        date_str = re.sub(r'-го|г\.|\bPublish\w*|\bжыл\w*|тому|\bавтор\w*|'
                          r'\bUTC\w*|\bпросмотр\w*|\bДата создания:\w*|\bДобавлено\w*|', '', date_str)
        languages = ['ru', 'kk', 'en', 'uz', 'de']

        if lang:
            # Если lang строка, проверяем на наличие точки с запятой
            if isinstance(lang, str):
                # Разбиваем по точке с запятой, если присутствует
                additional_langs = lang.split(';')
            else:
                additional_langs = lang
            # Удаляем возможные лишние пробелы и добавляем новые языки в список
            languages += [l.strip() for l in additional_langs if l.strip() not in languages]

        if not convert_date:  # Присваиваем список по умолчанию
            DATE_ORDERS = ["YMD", "DMY", "MYD"]
        else:
            if isinstance(convert_date, str): #Если переменная содержит строку (например, "YMD"), превращаем её в список
                DATE_ORDERS = [convert_date]
            else:
                DATE_ORDERS = convert_date
        kazakhstan_tz = pytz.timezone('Etc/GMT-5')
        current_time_with_tz = datetime.now(kazakhstan_tz)
        for date_order in DATE_ORDERS:
            date = parse(date_str,
                         languages=languages,
                         settings={"DATE_ORDER": date_order},
                         )
            if date:
                if date.hour == 0 and date.minute == 0 and date.second == 0:
                    date = date.replace(hour=0, minute=0, second=0)

                date_with_utc = date.replace(tzinfo=kazakhstan_tz)

                # Проверка на актуальность даты
                if date_with_utc <= current_time_with_tz:
                    return date_with_utc

        return None


    class IgnoreUrlLengthWarnings(logging.Filter):
        def filter(self, record):
            # Игнорируем сообщения, содержащие "Ignoring link (url length >"
            if "Ignoring link (url length >" in record.getMessage():
                return False
            return True

    def setup_scrapy_logging(self, spider_name, handler, console_handler):
        """
        Настраиваем Scrapy логгер для перенаправления всех сообщений в файл паука.
        """
        # Отключаем глобальное конфигурирование логирования Scrapy
        configure_logging(install_root_handler=False)

        # Перенаправляем стандартные логи Scrapy в наш кастомный логгер
        scrapy_logger = logging.getLogger('scrapy')

        # Отключаем передачу логов в корневой логгер
        scrapy_logger.propagate = False
        scrapy_logger.setLevel(logging.INFO)

        # Добавляем обработчики, если они еще не добавлены
        if not scrapy_logger.handlers:
            scrapy_logger.addHandler(handler)
            scrapy_logger.addHandler(console_handler)

    def close(self, reason):
        if self.cursor_2:
            self.cursor_2.close()
        if self.conn_2:
            self.conn_2.close()



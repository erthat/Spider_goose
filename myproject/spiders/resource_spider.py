import pytz
import mysql.connector
import scrapy
from scrapy.spiders import CrawlSpider
from scrapy.linkextractors import LinkExtractor
from dateparser import parse
import time
from dotenv import load_dotenv
import emoji
from datetime import datetime, timedelta
import re
from lxml.html import fromstring
import bs4
import os
from mysql.connector import Error
import unicodedata
from scrapy import Request
from urllib.parse import urlparse, urlunparse
load_dotenv()
import requests
from extractnet import Extractor
from goose3 import Goose

class ResourceSpider(CrawlSpider):
    name = 'resource_spider'
    custom_settings = { }

    def __init__(self, resources=None,  spider_name=None, *args, **kwargs):
        self.spider_name = spider_name or self.name
        super().__init__(*args, **kwargs)
        # Передача ресурсов
        self.resources = resources
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
                self.logger.info(f'Есть подключение к БД: {spider_name}')

                if resources:
                    self.resources = resources
                    self.start_urls = [resource[2].split(',')[0].strip() for resource in self.resources]
                    self.allowed_domains = [urlparse(url).netloc.replace('www.', '') for url in self.start_urls]
                    self.logger.info(f'Allowed domains: {self.allowed_domains}')
                    self.resource_map = {resource[0]: resource for resource in self.resources}

            else:
                self.log("No resources found, spider will close.")
                self.crawler.engine.close_spider(self, f'Нету данных в бд {spider_name}')


        except Error as e:
            self.log(f"Error connecting to MySQL: {e}")
            self.logger.info('Нет подключение к БД')
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

    def filter_valid_links(self, links):
        filtered_links = []
        # Фильтрация по доменам
        for link in links:
            link_domain = urlparse(link.url).netloc.replace('www.', '')
            if link_domain in self.allowed_domains:
                filtered_links.append(link)
        valid_links = []
        for link in filtered_links:
            url_link = self.remove_url_fragment(self.normalize_url(link.url))
            url_link_2 = self.remove_url_fragment(link.url.rstrip('/'))
            # Проверка соединения с базой данных
            if not self.conn_2.is_connected():
                try:
                    self.logger.warning("Соединение с базой данных потеряно, пытаемся переподключиться...")
                    self.conn_2.reconnect(attempts=3, delay=5)
                    self.logger.info("Соединение восстановлено")
                except mysql.connector.Error as err:
                    self.logger.warning(f"Ошибка переподключения: {err}")
                    return []  # Прекращаем выполнение и возвращаем пустой список, если не удалось переподключиться
            # Проверка ссылки в базе данных
            query = """
                (SELECT 1 FROM temp_items_link WHERE link = %s OR link = %s LIMIT 1)
                UNION
                (SELECT 1 FROM temp_items WHERE link = %s OR link = %s LIMIT 1)
                LIMIT 1
            """
            self.cursor_2.execute(query, (url_link, url_link_2, url_link, url_link_2))
            # Если ссылка не найдена ни в одной из таблиц, добавляем её в список валидных ссылок
            if self.cursor_2.fetchone() is None:
                valid_links.append(link)
        return valid_links

    def is_unwanted_link(self, url):
        unwanted_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.pdf', '.doc', '.docx', '.JPG', '.jfif', '.mp3',
                               '.mp4']
        return any(url.endswith(ext) for ext in unwanted_extensions)

    def is_outdated(self, nd_date, s_date):
        one_year_in_seconds = 365 * 24 * 3600
        return s_date - nd_date > one_year_in_seconds

    def convert_to_xpath(self, selector):
        # Разделяем строку по разделителю ':::'
        parts = selector.split(':::')

        # Проверяем, что у нас достаточно частей для формирования XPath
        if len(parts) != 3:
            raise ValueError("Неверный формат селектора.")

        # Извлекаем элементы
        tag = parts[0]  # 'a'
        attr_name = parts[1]  # 'class'
        attr_value = parts[2]  # 'post-index-title'

        # Формируем XPath
        xpath = f"//{tag}[@{attr_name}='{attr_value}']"
        return xpath

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
            top_tag = self.convert_to_xpath(top_tag)
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
            link_extractor = LinkExtractor(restrict_xpaths=top_tags,  deny=denys, deny_extensions=deny_extensions)

            # Извлекаем ссылки
            links = link_extractor.extract_links(response)
            valid_links = self.filter_valid_links(links)
            for link in valid_links:
                try:
                    yield Request(url=link.url, callback=self.parse_links, meta={'resource_info': resource_info, 'top_tags': top_tags, 'depth': 1, 'denys': denys,
                                                                             'deny_extensions': deny_extensions, 'max_depth': max_depth})
                except Exception as e:
                    self.logger.warning(f"Ошибка при отправке запроса для ссылки {link.url}: {e}")


    def parse_links(self, response):
        current_url = response.url

        if self.is_unwanted_link(current_url):
            # self.logger.info(f'Пропускаем неподходящий ссылку: {current_url}')
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
            valid_links = self.filter_valid_links(links)
            for link in valid_links:
                try:
                    yield Request(
                        url=link.url,
                        callback=self.parse_links,
                        meta={'resource_info': resource_info, 'top_tags':top_tags, 'depth': current_depth + 1, 'denys': denys,
                              'deny_extensions': deny_extensions, 'max_depth': max_depth})
                except Exception as e:
                    # Обработка ошибки (например, запись в лог)
                    self.logger.warning(f"Ошибка при отправке запроса для ссылки {link.url}: {e}")

        g = Goose()
        article = g.extract(raw_html=response.text)

        html_content = response.text

        content_extractor = Extractor()
        result = content_extractor.extract(html_content)
        # Парсинг заголовка
        title = article.title
        self.replace_unsupported_characters(title) if title else None
        if title is None:
            self.logger.info(f"Title отсутствует для {current_url}")
            return
        # Парсинг даты
        date = article.publish_date
        if date is None:
            date = result.get('rawDate')
            if date is None:
                date = result.get('date')
        date = self.parse_date(date, resource_info[7], resource_info[10])
        if date is None:
            self.logger.info(f"Дата отсутствует для {current_url}")
            return
        n_date = date
        nd_date = int(date.timestamp())
        not_date = date.strftime('%Y-%m-%d')
        s_date = int(time.time())
        if self.is_outdated(nd_date, s_date):
            # self.logger.info(f"Дата {n_date} старее чем на год для {current_url}")
            return
        # Парсинг основного контента
        content = article.cleaned_text
        # self.clean_text(content) if content and not all(item.isspace() for item in content) else None
        content = content if content and not all(item.isspace() for item in content) else None
        if content is None:
            self.logger.info(f"Сontent отсутствует для {current_url}")
            return

        self.store_news(resource_id, title, current_url, nd_date, content, n_date, s_date, not_date) # отправка на сохранение в бд

    def store_news(self, resource_id, title, current_url, nd_date, content, n_date, s_date, not_date):
        # Проверка соединения перед выполнением операций
        if not self.conn_2.is_connected():
            try:
                self.logger.warning("Соединение с базой данных потеряно, пытаемся переподключиться...")
                self.conn_2.reconnect(attempts=3, delay=5)
                self.logger.info("Соединение восстановлено")
            except mysql.connector.Error as err:
                self.logger.warning(f"Ошибка переподключения: {err}")
                return  # Прекращаем выполнение, если не удалось переподключиться
        self.cursor_2.execute(
            "SELECT COUNT(*) FROM temp_items WHERE link = %s",
            (current_url,)
        )
        (count,) = self.cursor_2.fetchone()

        if count == 0:
            status = ''
            self.cursor_2.execute(
                "INSERT INTO temp_items (res_id, title, link, nd_date, content, n_date, s_date, not_date, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (resource_id, title, current_url, nd_date, content, n_date, s_date, not_date, status)
            )
            self.conn_2.commit()
            self.logger.warning(f'Новость добавлена в базу, дата: {n_date} time: {nd_date}, URL: {current_url} ')
        else:
            pass
            #self.logger.info(f'Ссылка уже существует в базе TEMP: Дата {n_date} ({nd_date}) url: {current_url}')

    def replace_unsupported_characters(self, text):
        text = str(text) if text else ''
        return emoji.replace_emoji(text, replace='?')

    def convert_python_syntax(self, val: str):
        method, expr = val.split("::del::")
        values = expr.split(":::")

        if len(values) < 2 or len(values) > 4:
            self.logger.info(
                f"Value must have between two and four values. got {values}"
            )
        if method == "1":
            # 1::del::div:::class:::post-meta-author
            tag, attr, attr_val = values
            xpath = f"(//{tag}[contains(@{attr},'{attr_val}')])[1]"
            regex = ""

        elif method == "2":
            # 2::del::<time class="date" datetime=":::"
            from_str, to_str = values
            xpath = "//html"
            regex = f"(?s)(?<={re.escape(from_str)})(.*?)(?={re.escape(to_str)})"

        elif method == "3":
            # 3::del::meta:::name:::article:published_time:::content
            tag, attr, attr_val, extr_attr = values
            xpath = f"(//{tag}[contains(@{attr},'{attr_val}')]/@{extr_attr})[1]"
            regex = ""
        else:
            self.logger.info(
                f"Value must have between two and four values. got {values}"
            )
        return xpath, regex

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
                tolerance = timedelta(minutes=10)

                # Проверка на актуальность даты
                if date_with_utc <= current_time_with_tz + tolerance:
                    return date_with_utc

        return None

    def close(self, reason):
        # Закрытие курсора и соединения с базой данных

        if self.cursor_2:
            self.cursor_2.close()
            self.logger.info('подключение закрыто')

        if self.conn_2:
            self.conn_2.close()

        # Очистка кэша или других временных данных
        if hasattr(self, 'cache'):
            self.cache.clear()
            self.logger.info("Кэш очищен после завершения работы паука.")

        # Другие действия по освобождению ресурсов
        if hasattr(self, 'temp_files'):
            for temp_file in self.temp_files:
                try:
                    os.remove(temp_file)
                except OSError as e:
                    self.logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")

        self.logger.info(f"Паука {self.name} завершил работу. Причина: {reason}")

        # Вызов родительского метода с передачей аргумента reason
        return super().close(self, reason)

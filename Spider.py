import time

from myproject.spiders.resource_spider import ResourceSpider
import os
from dotenv import load_dotenv

import asyncio
from twisted.internet import asyncioreactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
import mysql.connector
import logging
from twisted.internet.defer import inlineCallbacks, DeferredList, Deferred
from logging.handlers import RotatingFileHandler

asyncioreactor.install()
load_dotenv()

log_file = 'logi.log'
handler = RotatingFileHandler(
    log_file,           # Имя файла логов
    mode='a',           # Режим добавления ('a'), чтобы не перезаписывать сразу
    maxBytes=5*1024*1024,  # Максимальный размер файла (в байтах), например, 5 МБ
    backupCount=1       # Количество резервных копий логов (если установить 0, то старый файл будет перезаписываться)
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        handler,        # Используем RotatingFileHandler
        logging.StreamHandler()
    ]
)

def connect_to_database():
    retries = 20  # Количество попыток подключения
    delay = 120  # Время задержки между попытками в секундах
    for attempt in range(retries):
        try:
            conn_1 = mysql.connector.connect(
                host=os.getenv("DB_HOST_1"),
                user=os.getenv("DB_USER_1"),
                password=os.getenv("DB_PASSWORD_1"),
                database=os.getenv("DB_DATABASE_1"),
                port=os.getenv("DB_PORT_1"),
                charset='utf8mb4',
                collation='utf8mb4_general_ci'
            )
            if conn_1.is_connected():
                logging.info('Подключение к базе данных успешно')
                return conn_1
        except mysql.connector.Error as e:
            logging.error(f"MySQL connection error: {e}")
            logging.info(f"Попытка {attempt + 1}/{retries} не удалась, повтор через {delay} секунд...")
            time.sleep(delay)
    logging.error("Не удалось подключиться к базе данных после нескольких попыток.")
    return None

@inlineCallbacks
def crawl():
    conn_1 = connect_to_database()
    if conn_1:
        cursor_1 = conn_1.cursor()
        runner = CrawlerRunner(get_project_settings())
            # Загрузка ресурсов из базы данных
        resources = load_resources(cursor_1)
            # Разделяем ресурсы на группы для каждого паука
        num_parts = 5
        part_size = len(resources) // num_parts
        remainder = len(resources) % num_parts

        # Разделяем ресурсы на 5 частей
        resources_spiders = []
        start = 0

        for i in range(num_parts):
            end = start + part_size + (1 if i < remainder else 0)
            resources_spiders.append(resources[start:end])
            start = end

        resources_spider_1, resources_spider_2, resources_spider_3, resources_spider_4, resources_spider_5 = resources_spiders

        logging.info(f'Number of resources for Spider 1: {len(resources_spider_1)}')
        logging.info(f'Number of resources for Spider 2: {len(resources_spider_2)}')
        logging.info(f'Number of resources for Spider 3: {len(resources_spider_3)}')
        logging.info(f'Number of resources for Spider 4: {len(resources_spider_4)}')
        logging.info(f'Number of resources for Spider 5: {len(resources_spider_5)}')


            # Запуск пауков с разными ресурсами
        deferred_1 = runner.crawl(ResourceSpider, conn_1=conn_1, resources=resources_spider_1, spider_name='spider_1', log_file='spider_1.log')
        deferred_2 = runner.crawl(ResourceSpider, conn_1=conn_1, resources=resources_spider_2, spider_name='spider_2', log_file='spider_2.log')
        deferred_3 = runner.crawl(ResourceSpider, conn_1=conn_1, resources=resources_spider_3, spider_name='spider_3', log_file='spider_3.log')
        deferred_4 = runner.crawl(ResourceSpider, conn_1=conn_1, resources=resources_spider_4, spider_name='spider_4', log_file='spider_4.log')
        deferred_5 = runner.crawl(ResourceSpider, conn_1=conn_1, resources=resources_spider_5, spider_name='spider_5', log_file='spider_5.log')
        # Ждем завершения пауков




        yield DeferredList([deferred_1, deferred_2, deferred_3, deferred_4, deferred_5])

        def waits():
            deferred = Deferred()
            reactor.callLater(900, deferred.callback, None)  # Ожидание 30 минут
            return deferred

        yield waits()

            # Повторный запуск цикла без ожидания
        yield crawl()

    else:
        logging.error('Программа завершена из-за невозможности подключиться к базе данных.')


def load_resources(cursor):
    cursor.execute(
        "SELECT RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut, convert_date "
        "FROM resource "
        "WHERE status = %s AND bottom_tag IS NOT NULL AND bottom_tag <> '' "
        "AND title_cut IS NOT NULL AND title_cut <> '' "
        "AND date_cut IS NOT NULL AND date_cut <> '' "
        "AND RESOURCE_STATUS = %s",
        ('spider_scrapy', 'WORK')
    )
    return cursor.fetchall()


# Запуск первого цикла
if __name__ == '__main__':
    from twisted.internet import reactor

    @inlineCallbacks
    def start_crawl():
        yield crawl()
        reactor.stop()

    reactor.callWhenRunning(start_crawl)
    reactor.run()

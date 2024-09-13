from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor, defer, task
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import LoopingCall
from logging.handlers import RotatingFileHandler
from myproject.spiders.resource_spider import ResourceSpider
from scrapy.utils.project import get_project_settings
import logging
import time
import os
from dotenv import load_dotenv  # Импортируйте паука
load_dotenv()
import mysql.connector

log_file = 'logs/logi.log'
handler = RotatingFileHandler(
    log_file,           # Имя файла логов
    mode='a',           # Режим добавления ('a'), чтобы не перезаписывать сразу
    maxBytes=20*1024*1024,  # Максимальный размер файла (в байтах), например, 5 МБ
    backupCount=1       # Количество резервных копий логов (если установить 0, то старый файл будет перезаписываться)
)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        handler   # Используем RotatingFileHandler
    ]
)

spider_resources = {}

def connect_to_database():
    retries = 20  # Количество попыток подключения
    delay = 600  # Время задержки между попытками в секундах
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

def load_resources(cursor):
    cursor.execute(
        "SELECT RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut, convert_date "
        "FROM resource "
        "WHERE status = %s AND top_tag IS NOT NULL AND top_tag <> '' "
        "AND bottom_tag IS NOT NULL AND bottom_tag <> '' "
        "AND title_cut IS NOT NULL AND title_cut <> '' "
        "AND date_cut IS NOT NULL AND date_cut <> '' "
        "AND RESOURCE_STATUS = %s",
        ('spider_scrapy', 'WORK')
    )
    return cursor.fetchall()

def load_and_divide_resources(cursor_1, num_parts):
    # Загрузка ресурсов из базы данных
    resources = load_resources(cursor_1)
    # Разделение ресурсов на части
    part_size = len(resources) // num_parts
    remainder = len(resources) % num_parts
    resources_spiders = []
    start = 0
    for i in range(num_parts):
        end = start + part_size + (1 if i < remainder else 0)
        resources_spiders.append(resources[start:end])
        start = end
    return resources_spiders

def load_and_update_resources(num_parts):
    print("Подключение к базе данных и загрузка ресурсов...")
    conn_1 = connect_to_database()
    cursor_1 = conn_1.cursor()
    resources_spiders = load_and_divide_resources(cursor_1, num_parts)
    cursor_1.close()
    conn_1.close()# Получаем разделенные ресурсы
    return resources_spiders


@inlineCallbacks
def run_spiders(runner, spider_name):
    while True:
        global spider_resources
        resources = spider_resources.get(spider_name)  # Получаем актуальные ресурсы
        if resources:         # Проверяем, что ресурсы существуют
            yield runner.crawl(ResourceSpider, resources=resources, spider_name=spider_name)
            logging.info(f'{spider_name} завершил работу, перезапуск...')
        else:
            logging.info(f'{spider_name} ожидает обновления ресурсов...')
            yield task.deferLater(reactor, 10, lambda: None)


@inlineCallbacks
def update_resources_every_hour(update_interval, num_parts):
    global spider_resources
    while True:
        yield task.deferLater(reactor, update_interval, lambda: None)  # Ожидание заданного времени
        new_resources = load_and_update_resources(num_parts)  # Обновление ресурсов
        for i in range(num_parts):
            spider_resources[f'spider_{i + 1}'] = new_resources[i]
        logging.info(f"Ресурсы обновлены в {time.strftime('%Y-%m-%d %H:%M:%S')}")

def start_spiders(num_parts):
    runner = CrawlerRunner(get_project_settings())
    initial_resources = load_and_update_resources(num_parts)
    global spider_resources

    spider_resources = {f'spider_{i + 1}': initial_resources[i] for i in range(num_parts)}

    for i in range(num_parts):
        run_spiders(runner, f'spider_{i + 1}')

    update_interval = 3600  # Интервал обновления в секундах (можно изменить)
    task.LoopingCall(update_resources_every_hour, update_interval, num_parts).start(0)
    reactor.run()


if __name__ == '__main__':
    conn_1 = connect_to_database()
    cursor_1 = conn_1.cursor()
    resources = load_resources(cursor_1)
    cursor_1.close()
    conn_1.close()
    resource_count = len([resource[0] for resource in resources])
    logging.info(f'количество источников = {resource_count}')
    num_parts = max(1, (resource_count // 30))
    logging.info(f'количество пауков = {num_parts}')
    start_spiders(num_parts)

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
from collections import deque
import mysql.connector
import hashlib
from trafilatura.meta import reset_caches

log_file = 'logs/logi.log'
log_dir = os.path.dirname(log_file)

# Проверяем, существует ли директория, и если нет — создаем её
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Настройка RotatingFileHandler
handler = RotatingFileHandler(
    log_file,           # Имя файла логов
    mode='a',           # Режим добавления ('a'), чтобы не перезаписывать сразу
    maxBytes=20*1024*1024,  # Максимальный размер файла (в байтах), например, 20 МБ
    backupCount=1       # Количество резервных копий логов (если установить 0, то старый файл будет перезаписываться)
)

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    handlers=[
        handler, logging.StreamHandler()   # Используем RotatingFileHandler
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
        "SELECT RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut, convert_date, block_page, middle_tag, LANG "
        "FROM resource "
        "WHERE status = %s"
        "AND RESOURCE_STATUS = %s",
        ('spider_goose', 'WORK')
    )
    return cursor.fetchall()

def load_and_divide_resources(cursor_1, block_size=20):
    resources = load_resources(cursor_1)
    resource_blocks = [resources[i:i + block_size] for i in range(0, len(resources), block_size)]
    return deque(resource_blocks)  # Используем deque для очереди блоков

def calculate_resources_hash(cursor_1):
    """Создаем хэш для ресурсов, чтобы отследить изменения."""
    resources = load_resources(cursor_1)
    resources_str = "".join(map(str, resources))  # Преобразуем ресурсы в строку
    return hashlib.md5(resources_str.encode()).hexdigest()

@inlineCallbacks
def run_spiders(runner, spider_name, resource_queue):
    while True:
        if resource_queue:
            # Получаем первый блок ресурсов
            resources = resource_queue.popleft()
            if resources:
                # Запускаем паука с этим блоком
                yield runner.crawl(ResourceSpider, resources=resources, spider_name=spider_name)
                logging.info(f'{spider_name} завершил работу с блоком, переключается на следующий.')
                # Возвращаем блок в конец очереди для повторной обработки
                resource_queue.append(resources)
                reset_caches()
        else:
            logging.info(f'{spider_name} ожидает ресурсы...')
            yield task.deferLater(reactor, 10, lambda: None)


def update_resources_periodically(resource_queue, last_hash, block_size=40):
    def update():
        nonlocal last_hash
        logging.info("Обновление ресурсов из базы данных...")
        conn_1 = connect_to_database()
        cursor_1 = conn_1.cursor()

        new_hash = calculate_resources_hash(cursor_1)
        if new_hash != last_hash:
            logging.info("Обнаружены изменения ресурсов, перезагружаем очередь...")

            # Обновляем последний хэш
            last_hash = new_hash

            # Сохраняем текущие необработанные блоки
            unprocessed_blocks = list(resource_queue)

            # Очищаем очередь и загружаем новые данные
            resource_queue.clear()
            new_blocks = load_and_divide_resources(cursor_1, block_size)

            # Возвращаем необработанные блоки в начало очереди
            resource_queue.extend(unprocessed_blocks)

            # Добавляем новые блоки в конец очереди
            resource_queue.extend(new_blocks)
            logging.info("Ресурсы успешно обновлены.")
        else:
            logging.info("Ресурсы не изменились, обновление не требуется.")

        cursor_1.close()
        conn_1.close()

    # Запускаем обновление ресурсов каждую 1 час (3600 секунд)
    LoopingCall(update).start(9600)


def start_spiders(num_spiders, resource_queue):
    runner = CrawlerRunner(get_project_settings())

    # Запускаем указанное количество пауков
    for i in range(num_spiders):
        run_spiders(runner, f'spider_{i + 1}', resource_queue)


if __name__ == '__main__':
    conn_1 = connect_to_database()
    cursor_1 = conn_1.cursor()

    # Создаем очередь ресурсов
    resources_queue = load_and_divide_resources(cursor_1)
    initial_hash = calculate_resources_hash(cursor_1)
    cursor_1.close()
    conn_1.close()
    num_blocks = len(resources_queue)
    # print(f"Количество блоков в очереди: {num_blocks}")

    # Определяем количество пауков
    num_spiders = min(num_blocks, 3)  # Если блоков меньше 6, используем это число, иначе 6 пауков

    # print(f"Количество пауков: {num_spiders}")

    # Запускаем пауков
    start_spiders(num_spiders, resources_queue)

    # Запускаем обновление ресурсов каждые 120 минут
    update_resources_periodically(resources_queue, initial_hash)

    reactor.run()

from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from myproject.spiders.resource_spider import ResourceSpider
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
import logging

logging.basicConfig(
    level=logging.INFO,  # Устанавливаем уровень логирования
    format='%(levelname)s: %(message)s',

)
@inlineCallbacks
def crawl():
    runner = CrawlerRunner(get_project_settings())
    yield runner.crawl(ResourceSpider)
    reactor.callLater(1800, crawl)  # Запланировать следующий запуск через 30 минут

# Запуск первого цикла
crawl()

# Запуск основного цикла событий Twisted
reactor.run()

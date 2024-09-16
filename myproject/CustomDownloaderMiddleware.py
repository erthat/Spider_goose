from twisted.internet.defer import inlineCallbacks, Deferred
from scrapy.exceptions import IgnoreRequest
from twisted.internet.error import TimeoutError, DNSLookupError, ConnectionRefusedError
from scrapy.http import Request
from collections.abc import Callable


class CustomDownloaderMiddleware:

    def __init__(self, settings):
        self.max_retries = settings.getint('RETRY_TIMES', 3)  # Количество повторных попыток

    @inlineCallbacks
    def download(self, download_func: Callable, request: Request, spider) -> Deferred:
        retries = request.meta.get('retry_times', 0)  # Получаем количество попыток
        try:
            # Выполняем запрос
            response = yield download_func(request=request, spider=spider)
            return response

        except (TimeoutError, DNSLookupError, ConnectionRefusedError) as e:
            # Обрабатываем ошибки, связанные с подключением или таймаутами
            if retries < self.max_retries:
                retries += 1
                spider.logger.warning(f"Retrying {request.url} (attempt {retries}/{self.max_retries})")

                # Создаем копию запроса и увеличиваем количество попыток
                new_request = request.copy()
                new_request.meta['retry_times'] = retries
                yield download_func(request=new_request, spider=spider)
            else:
                # Превышено максимальное количество попыток
                spider.logger.error(f"Giving up {request.url} after {retries} retries")
                raise IgnoreRequest(f"Failed to download {request.url} after {self.max_retries} retries")

        except Exception as e:
            # Ловим все остальные ошибки, чтобы не падал процесс загрузки
            spider.logger.error(f"Unknown error during download: {e}")
            raise IgnoreRequest(f"Error downloading {request.url}: {e}")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)


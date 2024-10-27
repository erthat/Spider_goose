import requests
from extractnet import Extractor


from goose3 import Goose
import trafilatura


g = Goose()
html_content = requests.get('https://stapravda.ru/20241026/dva_cheloveka_postradali_v_dtp_s_traktorom_v_kurskom_okruge_223637.html').text
#
#
# # Настройки для парсинга основного контента
# content_extractor = Extractor()
#
# # Настройки для парсинга даты (например, минимальная длина и метаданные)
# date_extractor = Extractor()
#
# extracted_content = content_extractor.extract(html_content)
# # content = content_extractor.extract(html_content)['content']
# # title = content_extractor.extract(html_content)['title']
# date = content_extractor.extract(html_content)['date']
# #
# print(extracted_content)
# # print("title:", title)
# print("Дата:", date)
article = g.extract(raw_html=html_content)
#
# title = article.title
# text = article.cleaned_text
publish_date = article.publish_date
content = trafilatura.extract(html_content, include_formatting=False, favor_precision=True, include_comments=False)
result = trafilatura.bare_extraction(html_content, with_metadata=True)
publication_date = result['date']
print(publication_date)



# import re
# from furl import furl
#
# # Регулярные выражения для сегментов
# ANYWORDWITHDEPHIS = r"[a-zA-Z0-9_\-]+"
# ANYWORD = r"[a-zA-Z0-9_]+"
# ANYINT = r"[0-9]+"
#
#
# def get_regex_from_links(urls: list[str]):
#     """Генерирует регулярное выражение для Scrapy LinkExtractor"""
#     result = set()
#     for url in urls:
#         REGEX = r"https?://[^/]+/"  # Стартовая часть URL
#         parsed_url = furl(url)
#         path = parsed_url.path
#
#         # Обрабатываем сегменты пути
#         for i, segment in enumerate(path.segments):
#             if not segment:
#                 continue
#             words = segment.split("-")
#
#             # Проверяем, является ли это последний сегмент пути
#             if i == len(path.segments) - 1:
#                 # Если первое или последнее слово сегмента - число (ID новости)
#                 if words[0].isdigit() or words[-1].isdigit():
#                     if words[0].isdigit():  # ID в начале сегмента
#                         REGEX += r"[0-9]+-"  # Начинается с числа
#                         REGEX += r"[a-zA-Z0-9_\-]*"  # Остальные сегменты (если есть)
#                     else:  # ID в конце сегмента
#                         REGEX += r"[a-zA-Z0-9_\-]*-"  # Любые слова перед числом
#                         REGEX += r"[0-9]+"
#                 else:
#                     REGEX += r"[a-zA-Z0-9_\-]+"
#                 # Добавляем возможность наличия .html в конце
#                 REGEX += r"(?:\.html)?/?"
#             else:
#                 # Для всех остальных сегментов
#                 if len(words) == 1:
#                     if words[0].isdigit():
#                         REGEX += ANYINT + r"/"  # Число
#                     else:
#                         REGEX += ANYWORD + r"/"  # Слово
#                 else:
#                     REGEX += r"[a-zA-Z0-9_\-]+/"  # Общий случай: слово или число с дефисами и подчеркиванием
#
#         result.add(REGEX)
#     return list(result)
# links = ['https://regnum.ru/news/3923781']
#
# data = get_regex_from_links(links)
# print(data)
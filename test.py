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
import trafilatura
from goose3 import Goose

def parse_date(date_str, convert_date, lang):
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
        if isinstance(convert_date, str): # Если переменная содержит строку (например, "YMD"), превращаем её в список
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

            if date.tzinfo is None:
                # Присваиваем временную зону если она отсутствует
                date = date.replace(tzinfo=kazakhstan_tz)

            if date.tzinfo != kazakhstan_tz:
                # Переводим дату в GMT+5 если временная зона отличается
                date = date.astimezone(kazakhstan_tz)

            date_with_utc = date
            tolerance = timedelta(minutes=10)
            # Проверка на актуальность даты
            if date_with_utc <= current_time_with_tz + tolerance:
                return date_with_utc

    return None


date_str = ' 31.10.2024'
convert_date =''
lang =''
date = parse_date(date_str, convert_date, lang)
nd_date = int(date.timestamp())
print(nd_date)
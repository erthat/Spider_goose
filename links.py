import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def is_news_page(soup):
    # Простая эвристика: ищем заголовок и текст
    title = soup.find('h1') or soup.find('title')
    paragraphs = soup.find_all('p')

    # Если на странице есть заголовок и несколько абзацев текста, это может быть новость
    return title is not None and len(paragraphs) > 5


def fetch_news_links(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Извлекаем все ссылки со страницы
    links = soup.find_all('a', href=True)
    news_links = []

    for link in links:
        full_link = urljoin(url, link['href'])

        # Переходим на потенциальную новостную ссылку
        try:
            page_response = requests.get(full_link)
            page_soup = BeautifulSoup(page_response.text, 'html.parser')

            if is_news_page(page_soup):
                news_links.append(full_link)
                if len(news_links) >= 3:  # Выдаем только пару примеров
                    break
        except:
            continue

    return news_links


# Пример использования:
start_urls = ['https://regnum.ru/news/3923781']
for url in start_urls:
    examples = fetch_news_links(url)
    print(f"Примеры новостных ссылок для {url}:")
    for example in examples:
        print(example)
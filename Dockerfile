# Используем официальный образ Python

FROM python:3.10-slim

# Устанавливаем зависимости для MySQL
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта
COPY . .

# Устанавливаем зависимости проекта
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

RUN pip install extractnet
RUN apt-get update && apt-get install -y zlib1g zlib1g-dev


# Запуск Scrapy
CMD ["python", "Spider.py"]


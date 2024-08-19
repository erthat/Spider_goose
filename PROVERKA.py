import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
load_dotenv()

def connect_to_mysql():
    try:
        # Установление соединения
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST_1"),
            user=os.getenv("DB_USER_1"),
            password=os.getenv("DB_PASSWORD_1"),
            database=os.getenv("DB_DATABASE_1"),
            port=os.getenv("DB_PORT_1"),
            charset='utf8mb4',
            collation='utf8mb4_general_ci'
        )
        if conn.is_connected():
            print("Successfully connected to MySQL database")
            cursor = conn.cursor()
            cursor.execute(
                "SELECT RESOURCE_ID, RESOURCE_NAME, RESOURCE_URL, top_tag, bottom_tag, title_cut, date_cut "
                "FROM resource "
                "WHERE status = %s AND bottom_tag IS NOT NULL AND bottom_tag <> '' "
                "AND title_cut IS NOT NULL AND title_cut <> '' "
                "AND date_cut IS NOT NULL AND date_cut <> ''"
                "AND RESOURCE_STATUS = %s",
                ('spider_scrapy', 'WORK'))
            resources = cursor.fetchall()
            for resource in resources:
                resource_url = resource[2]  # RESOURCE_URL находится на 3-й позиции (индекс 2)
                first_url = resource_url.split(',')[0].strip()  # Разделяем по запятым и берем первую ссылку
                print(first_url)

    except Error as e:
        print(f"Error: {e}")

connect_to_mysql()
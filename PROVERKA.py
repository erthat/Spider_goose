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

    except Error as e:
        print(f"Error: {e}")

connect_to_mysql()
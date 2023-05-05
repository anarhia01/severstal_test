import os
import time
import threading
from queue import Queue
from datetime import datetime
import redis
import psycopg2
from psycopg2 import sql
from pprint import pprint

# Блокировка для синхронизации потоков
lock = threading.Lock()

def read_images_from_folder(folder, image_queue):
    for file in os.listdir(folder):
        if file.endswith(('.jpg', '.png', '.jpeg')):
            with open(os.path.join(folder, file), 'rb') as f:
                image_data = f.read()
                with lock:
                    timestamp = str(time.time())
                    image_queue.put((timestamp, image_data))

def write_to_postgres(image_queue, redis_conn, postgres_conn, read_thread):
    time.sleep(1)  # Задержка старта потока записи
    while True:
        with lock:
            if not image_queue.empty() or read_thread.is_alive():
                if not image_queue.empty():
                    timestamp, image_data = image_queue.get()
                    redis_conn.set(timestamp, image_data)
                    size = len(image_data)
                    with postgres_conn.cursor() as cur:
                        cur.execute(
                            sql.SQL("INSERT INTO images (timestamp, size) VALUES (%s, %s)"),
                            (timestamp, size)
                        )
                        print(f"Image data inserted: timestamp={timestamp}, size={size} bytes")
                    postgres_conn.commit()
            else:
                break

def create_table_if_not_exists(postgres_conn):
    with postgres_conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id SERIAL PRIMARY KEY,
                timestamp DOUBLE PRECISION NOT NULL,
                size INTEGER NOT NULL
            );
        """)
        postgres_conn.commit()


def main():
    folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "img")
    redis_host = "localhost"
    redis_port = 6379
    postgres_conn_str = "dbname='severstal_db' user='postgres' host='localhost' password='1234'"

    image_queue = Queue()
    redis_conn = redis.StrictRedis(host=redis_host, port=redis_port, db=0)
    postgres_conn = psycopg2.connect(postgres_conn_str)

    create_table_if_not_exists(postgres_conn)

    # Запуск потока для чтения изображений
    read_thread = threading.Thread(target=read_images_from_folder, args=(folder, image_queue))
    read_thread.start()

    # Запуск потока для записи в PostgreSQL
    write_thread = threading.Thread(target=write_to_postgres, args=(image_queue, redis_conn, postgres_conn, read_thread))
    write_thread.start()

    # Дожидаемся завершения обоих потоков
    read_thread.join()
    write_thread.join()

    # Проверка записей в базе данных
    with postgres_conn.cursor() as cur:
        cur.execute("SELECT * FROM images")
        rows = cur.fetchall()
        print("\nAll rows in the 'images' table:")
        pprint(rows)

    postgres_conn.close()

if __name__ == "__main__":
    main()

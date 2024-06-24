import psycopg2
import time
from threading import Thread

username = 'postgres'
password = 'postgres'
database = 'database'
host = 'db'
port = '5432'


def lost_update():
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    with conn:
        for i in range(10000):
            cur = conn.cursor()
            cur.execute("SELECT counter FROM user_counter WHERE user_id = 1")
            counter = cur.fetchone()[0]
            counter += 1
            cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
            conn.commit()
    conn.close()


def in_place_update():
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    with conn:
        for i in range(10000):
            cur = conn.cursor()
            cur.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
            conn.commit()
    conn.close()


def row_level_locking():
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    with conn:
        for i in range(10000):
            cur = conn.cursor()
            cur.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
            counter = cur.fetchone()[0]
            counter += 1
            cur.execute("UPDATE user_counter SET counter = %s WHERE user_id = %s", (counter, 1))
            conn.commit()
    conn.close()


def optimistic_concurrency_control():
    conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)
    with conn:
        for i in range(10000):
            while True:
                cur = conn.cursor()
                cur.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
                result = cur.fetchone()
                counter = result[0]
                version = result[1]
                counter += 1
                cur.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = %s AND version = %s",
                            (counter, version + 1, 1, version))
                conn.commit()
                count = cur.rowcount
                if count > 0:
                    break
    conn.close()


start_time = time.time()
threads = []
for _ in range(10):
    thread = Thread(target=lost_update)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("Execution time for lost_update:", time.time() - start_time)

start_time = time.time()
threads = []
for _ in range(10):
    thread = Thread(target=in_place_update)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("Execution time for in_place_update:", time.time() - start_time)

start_time = time.time()
threads = []
for _ in range(10):
    thread = Thread(target=row_level_locking)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("Execution time for row_level_locking:", time.time() - start_time)

start_time = time.time()
threads = []
for _ in range(10):
    thread = Thread(target=optimistic_concurrency_control)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print("Execution time for optimistic_concurrency_control:", time.time() - start_time)

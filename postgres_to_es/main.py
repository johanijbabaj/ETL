import json
import logging
import os
import psycopg2

from dotenv import load_dotenv
from psycopg2.extras import DictCursor

from utils import *
from state_saver import *
from db_connector import PG_reader
from ES_Loader import ES_loader

logging.basicConfig(format="%(asctime)s: %(name)s - %(levelname)s - %(message)s",
                    level=logging.ERROR)
load_dotenv("deploy/envs/postgres.env")

if __name__ == "__main__":
    my_storage = JsonFileStorage("my_state.json")
    my_state = State(my_storage)
    pg_dsn = {"dbname": os.getenv("DATABASE_NAME"),
           "user": os.getenv("DATABASE_USER"),
           "password": os.getenv("DATABASE_PASSWORD"),
           "host": os.getenv("DATABASE_HOST"),
           "port": os.getenv("DATABASE_PORT")}
    #while True:
    #    try:
    with psycopg2.connect(**pg_dsn, cursor_factory=DictCursor) as pg_conn:
        reader = PG_reader(pg_conn)
        reader.test_connection()
        my_storage = JsonFileStorage("my_state.json")
        my_state = State(my_storage)
        dsn = {"host": "es", "port": 9200}
        my_esl = ES_loader(dsn)
        my_esl.test_connection()
        my_esl.create_index("movies")
        while True:
            current_state = my_state.get_state("last_record_date")
            json_data, last_date = reader.read_data(current_state)
            if json_data == []:
                break
            if my_esl.load_data("movies", json_data):
                my_state.set_state("last_record_date", str(last_date))
                logging.debug(f"Обновили даные по последней загруженной партии - {last_date}")

                #saver.check_scheme()
        #saver.save_all_data(data)
    #except psycopg2.OperationalError:
    #    logging.error("Ошибка подключения к Postgres. Проверьте параметры подключения")



# @benchmark
# @backoff()
# def sum(a, b):
#     print(a+b)
#     return a + b
#
# sum(1,2)
# sum(1,2)
# sum(1,2)
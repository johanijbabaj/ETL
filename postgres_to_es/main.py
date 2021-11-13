import logging
import os
from time import sleep

import psycopg2

from dotenv import load_dotenv
from psycopg2.extras import DictCursor

import utils
from state_saver import *
from db_connector import PG_reader
from ES_Loader import ES_loader

logging.basicConfig(format="%(asctime)s: %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)
load_dotenv("deploy/envs/postgres.env")

@utils.backoff()
def main():
    """
    Основная функция программы. Инициализаруются объекты классов

    """
    my_storage = JsonFileStorage("my_state.json")
    my_state = State(my_storage)
    dsn = {"host": os.getenv("ES_HOST"), "port": os.getenv("ES_PORT")}
    pg_dsn = {"dbname": os.getenv("DATABASE_NAME"),
              "user": os.getenv("DATABASE_USER"),
              "password": os.getenv("DATABASE_PASSWORD"),
              "host": os.getenv("DATABASE_HOST"),
              "port": os.getenv("DATABASE_PORT")}
    logging.info(pg_dsn)
    try:
        with psycopg2.connect(**pg_dsn, cursor_factory=DictCursor) as pg_conn:
            reader = PG_reader(pg_conn)

            if reader.test_connection():
                my_esl = ES_loader(dsn)
                if my_esl.test_connection():
                    my_esl.create_index("movies")
                    while True:
                        current_state = my_state.get_state("last_record_date")
                        json_data, last_date = reader.read_data(current_state)
                        if not json_data:
                            logging.debug("Нет данных к выгрузке")
                            raise
                        if my_esl.load_data("movies", json_data):
                            my_state.set_state("last_record_date", str(last_date))
                            logging.debug(f"Обновили даные по последней загруженной партии - {last_date}")
                            sleep(3)
                else:
                    raise
            else:
                raise
    except Exception as connection_error:
        logging.error(f"Ошибка подключения {connection_error}")


if __name__ == "__main__":
    while True:
        try:
            logging.info("Начинаем загрузку данных")
            main()
        except Exception as e:
            logging.error(f"Ошибка загрузки данных {e}")




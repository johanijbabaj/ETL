import logging
from time import sleep

import psycopg2

from dotenv import load_dotenv
from psycopg2.extras import DictCursor

import utils
from state_saver import *
from db_connector import PG_reader, PgConnectionSettings
from es_loader import ES_loader, EsConnectionSettings

logging.basicConfig(format="%(asctime)s: %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

@utils.backoff()
def main():
    """
    Основная функция программы. Инициализаруются объекты классов
    и выполянет ETL

    """
    my_storage = JsonFileStorage("my_state.json")
    my_state = State(my_storage)
    pg_conn_settings = PgConnectionSettings().dict()
    es_conn_settings = EsConnectionSettings().dict()
    try:
        with psycopg2.connect(**pg_conn_settings, cursor_factory=DictCursor) as pg_conn:
            reader = PG_reader(pg_conn)
            if reader.test_connection():
                my_es = ES_loader(es_conn_settings)
                if my_es.test_connection():
                    my_es.create_index("movies")
                    while True:
                        current_state = my_state.get_state("last_record_date")
                        json_data, last_date = reader.read_data(current_state)
                        if not json_data:
                            logging.debug("Нет данных к выгрузке")
                            raise
                        if my_es.load_data("movies", json_data):
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




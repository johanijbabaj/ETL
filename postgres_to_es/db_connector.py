"""Модуль описывающий классы """

import logging
import os
import utils

from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from pydantic import BaseModel, BaseSettings, Field


logging.basicConfig(format="%(asctime)s: %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)
#load_dotenv("")
#settings = Settings(_env_file="deploy/envs/postgres.env", _env_file_encoding="utf-8")


class PgConnectionSettings (BaseSettings):
    dbname: str = Field(None, env="DATABASE_NAME")
    user: str = Field(None, env="DATABASE_USER")
    password: str = Field(None, env="DATABASE_PASSWORD")
    host: str = Field(None, env="DATABASE_HOST")
    port: str = Field(None, env="DATABASE_PORT")

    class Config:
        env_file = "deploy/envs/postgres.env"
        env_file_encoding = "utf-8"

    #def __init__(self):
    #    logging.debug(f"Параметры подключение {self.port}, {self.dbname}")
    # pg_dsn = {"dbname": os.getenv("DATABASE_NAME"),
    #           "user": os.getenv("DATABASE_USER"),
    #           "password": os.getenv("DATABASE_PASSWORD"),
    #           "host": os.getenv("DATABASE_HOST"),
    #           "port": os.getenv("DATABASE_PORT")}
    #logging.info(pg_dsn)


class PG_reader():
    """
    Класс реаолизует проверку соединения с базой и загрузку данных
    """
    pg_conn: _connection

    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn
        self.pg_cursor = pg_conn.cursor()

    def test_connection(self):
        """
        Функция реализвет проверку соединение с базой
        """
        try:
            self.pg_cursor.execute("""SELECT id FROM content.film_work LIMIT 1""")
            if self.pg_cursor.fetchall() != []:
                #В выборке есть данныею Тест пройден.
                return True
            else:
                logging.debug(f"Нет данных в базе!")
                return False
        except Exception as e:
            logging.debug(f"Ошибка подключение {e}")
            return False

    def read_data(self, state, limit=100):
        """
        Метод загрузки данных из БД с момента последней обработанной записи
        """
        logging.debug(f"Загрузка данных. Статус {state}")
        data = [state, limit]
        self.pg_cursor.execute("""
        SELECT jsonb_agg(json_build_object('id', t.id,'imdb_rating', t.imdb_rating,'genre', t.genre, 'title', t.title
                              ,'description', t.description,'director', t.director,'actors_names', t.actors_names
                              ,'writers_names', t.writers_names,'actors', t.actors,'writers', t.writers)) AS json,
               max(t.last_update_at) AS last_update_at
        FROM (SELECT
            fw.id AS id,
            fw.rating AS imdb_rating,
            JSON_AGG(DISTINCT(g.name)) AS genre,
            fw.title AS title,
            fw.description AS description,
            ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director') AS director,
            ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor') AS actors_names,
            ARRAY_AGG(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer') AS writers_names,
            ARRAY_AGG(DISTINCT JSONB_BUILD_OBJECT('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
            ARRAY_AGG(DISTINCT JSONB_BUILD_OBJECT('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers,  
            GREATEST(MAX(fw.updated_at), MAX(p.updated_at), MAX(g.updated_at)) AS last_update_at
            FROM content.film_work AS fw
            LEFT JOIN content.genre_film_work gf on gf.film_work_id = fw.id
            LEFT JOIN content.genre g on g.id = gf.genre_id
            LEFT JOIN content.person_film_work pfw on pfw.film_work_id = fw.id
            LEFT JOIN content.person p on p.id = pfw.person_id
            GROUP BY fw.id
            HAVING greatest(max(fw.updated_at), max(p.updated_at), max(g.updated_at)) > %s
            ORDER BY greatest(max(fw.updated_at), max(p.updated_at), max(g.updated_at))
            LIMIT  %s
            ) AS t
        """, (state, limit))
        for row in self.pg_cursor:
            #отдельно созраняем json и дату последней обработнной записи
            json_data = row[0]
            last_date = row[1]
        return (json_data, last_date)

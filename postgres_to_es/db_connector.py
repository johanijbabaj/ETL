"""Модуль описывающий классы """

import logging

from psycopg2.extensions import connection as _connection
import utils


logging.basicConfig(format="%(asctime)s: %(name)s - %(levelname)s - %(message)s",
                    level=logging.ERROR)
class PG_reader():
    """Класс обрабатывает файлы и сохраняет данные в базу Postgres в заданные таблицы"""
    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn
        self.pg_cursor = pg_conn.cursor()

    @utils.backoff()
    def test_connection(self):
        try:
            self.pg_cursor.execute("""SELECT id FROM content.filmwork LIMIT 1""")
        except:
            logging.debug(f"Ошибка чтения")
        finally:
            logging.debug(f"Успешный коннект")
    @utils.benchmark
    def read_data(self, state, limit=3):
        """Метод загрузки данных из БД с момента последней обработанной записи """
        logging.debug(f"Загрузка данных. Статус {state}")
        data = [state, limit]
        self.pg_cursor.execute("""
            SELECT 
            jsonb_agg(json_build_object('id', id,'imdb_rating', imdb_rating,'genre', genre, 'title', title
                                  ,'description', description,'director', director,'actors_names', actors_names
                                  ,'writers_names', writers_names,'actors', actors,'writers', writers)
                                  ) as json
            , max(last_update_at)
            FROM (SELECT
                fw.id
                ,fw.rating as imdb_rating
                ,json_agg(distinct(g.name)) as genre
                ,fw.title
                ,fw.description
                ,json_agg(distinct(case when pfw.role = 'director' then p.full_name end))
                    filter (WHERE (case when pfw.role = 'director' then p.full_name end) IS NOT NULL) as director
                ,json_agg(distinct(case when pfw.role = 'actor' then p.full_name end))
                    filter (WHERE (case when pfw.role = 'actor' then p.full_name end) IS NOT NULL) as actors_names
                ,json_agg(distinct(case when pfw.role = 'writer' then p.full_name end))
                    filter (WHERE (case when pfw.role = 'writer' then p.full_name end) IS NOT NULL) as writers_names
                ,json_agg(distinct(case when pfw.role = 'actor' then jsonb_build_object(p.id, p.full_name) end))
                    filter (WHERE (case when pfw.role = 'actor' then jsonb_build_object(p.id, p.full_name) end) IS NOT NULL) as actors
                ,json_agg(distinct(case when pfw.role = 'writer' then jsonb_build_object(p.id, p.full_name) end))
                    filter (WHERE (case when pfw.role = 'writer' then jsonb_build_object(p.id, p.full_name) end) IS NOT NULL) as writers
                ,greatest(max(fw.updated_at), max(p.updated_at), max(g.updated_at)) as last_update_at
            
                FROM content.film_work as fw
                LEFT JOIN content.genre_film_work gf on gf.film_work_id = fw.id
                LEFT JOIN content.genre g on g.id = gf.genre_id
                LEFT JOIN content.person_film_work pfw on pfw.film_work_id = fw.id
                LEFT JOIN content.person p on p.id = pfw.person_id
                --WHERE fw.id = 'c35dc09c-8ace-46be-8941-7e50b768ec33'
                GROUP BY fw.id
                HAVING greatest(max(fw.updated_at), max(p.updated_at), max(g.updated_at)) > %s
                ORDER BY greatest(max(fw.updated_at), max(p.updated_at), max(g.updated_at))
                LIMIT %s
                ) as t
            """, (state, limit))
        for row in self.pg_cursor:
            #отдельно созраняем json и дату последней обработнной записи
            json_data = row[0]
            last_date = row[1]
        return (json_data, last_date)

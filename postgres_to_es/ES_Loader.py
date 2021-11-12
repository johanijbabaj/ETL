import logging

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pydantic import BaseModel

logging.basicConfig(format="%(asctime)s: %(name)s - %(levelname)s - %(message)s",
                    level=logging.ERROR)

class ES_loader():
    dsn: dict
    es: Elasticsearch()

    def __init__(self, dsn: dict):
        self.dsn = dsn
        self.es = Elasticsearch([dsn])

    def test_connection(self):
        if self.es.ping():
            logging.debug("ES connected")
            return True
        else:
            logging.debug("ES connection problems")
            return False

    def create_index(self, index_name):

        settings = {
            "settings": {
                "refresh_interval": "1s",
                "analysis": {
                    "filter": {
                        "english_stop": {
                            "type": "stop",
                            "stopwords": "_english_"
                        },
                        "english_stemmer": {
                            "type": "stemmer",
                            "language": "english"
                        },
                        "english_possessive_stemmer": {
                            "type": "stemmer",
                            "language": "possessive_english"
                        },
                        "russian_stop": {
                            "type": "stop",
                            "stopwords": "_russian_"
                        },
                        "russian_stemmer": {
                            "type": "stemmer",
                            "language": "russian"
                        }
                    },
                    "analyzer": {
                        "ru_en": {
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "english_stop",
                                "english_stemmer",
                                "english_possessive_stemmer",
                                "russian_stop",
                                "russian_stemmer"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "dynamic": "strict",
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "imdb_rating": {
                        "type": "float"
                    },
                    "genre": {
                        "type": "keyword"
                    },
                    "title": {
                        "type": "text",
                        "analyzer": "ru_en",
                        "fields": {
                            "raw": {
                                "type": "keyword"
                            }
                        }
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "director": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "actors_names": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "writers_names": {
                        "type": "text",
                        "analyzer": "ru_en"
                    },
                    "actors": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text",
                                "analyzer": "ru_en"
                            }
                        }
                    },
                    "writers": {
                        "type": "nested",
                        "dynamic": "strict",
                        "properties": {
                            "id": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text",
                                "analyzer": "ru_en"
                            }
                        }
                    }
                }
            }
        }
        created = False
        logging.debug(f"ПРоверка индекса...{index_name}")
        try:
            if not self.es.indices.exists(index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                self,es.indices.create(index=index_name, ignore=400, body=settings)
                logging.debug("Создали индекс")

                print('Created Index')
            created = True
        except Exception as ex:
            logging.debug(f"Ошибка создания индекса: {str(ex)}")

        finally:
            logging.debug(f"Результат проверки индекса {created}")
            return created

    def bulk_json_data(self, json_list, index):
        for doc in json_list:

            if '{"index"' not in doc:
                yield {
                    "_index": index,
                    "_source": doc
                }

    def load_data(self, index, json_data):
        try:
            logging.debug(f"Вставляем даные")
            resp = helpers.bulk(self.es, self.bulk_json_data(json_data,index))
            return True
        except Exception as ex:
            logging.debug(f"Ошибка вставки данных: {str(ex)}")
            return False



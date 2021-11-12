import abc
import json
import logging
from typing import Any, Optional

logging.basicConfig(format="%(asctime)s: %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)

class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, "w") as json_file:
            json.dump(state, json_file)

    def retrieve_state(self, key: str) -> dict:
        try:
            with open(self.file_path, "r") as json_file:
                states = json.load(json_file)
                state = states.get(key)
                print(state)
                logging.debug(f"Variable state is {state}")
                return state
        except json.JSONDecodeError:
            self.save_state({key: "01.01.1700"})
            return "01.01.1700"
        except FileNotFoundError:
            new_file = open(self.file_path, "w")
            new_file.close()
            return {key: None}


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.storage.retrieve_state(key)


# my_state.set_state("State", "Fine")
# print(f"проверяем еще раз статус {my_state.get_state('State')}")
# my_state.set_state("State", "ALERT")
# print(f"проверяем опять статус {my_state.get_state('State')}")


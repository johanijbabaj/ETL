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
        """Сохраняем значение статуса в файл"""
        with open(self.file_path, "w") as json_file:
            json.dump(state, json_file)

    def retrieve_state(self, key: str) -> dict:
        """Получить значение статуса по ключу"""
        try:
            with open(self.file_path, "r") as json_file:
                states = json.load(json_file)
                state = states.get(key)
                logging.debug(f"Variable state is {state}")
                return state
        except json.JSONDecodeError:
            # Если файл пустой, то устанавливаем значение статуса на начальную дату
            self.save_state({key: "01.01.1700"})
            return "01.01.1700"
        except FileNotFoundError:
            # Создаем новый файл, если не удалось его найти
            new_file = open(self.file_path, "w")
            self.save_state({key: "01.01.1700"})
            new_file.close()
            return "01.01.1700"


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.storage.retrieve_state(key)

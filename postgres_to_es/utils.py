from functools import wraps
import time

def benchmark(func):
    """
    Декоратор, выводящий время, которое заняло
    выполнение декорируемой функции.
    """
    import time
    def wrapper(*args, **kwargs):
        t = time.time()
        res = func(*args, **kwargs)
        print(func.__name__, time.time() - t)
        return res
    return wrapper


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        print(f"Смотрим что за магия в декораторе.Сейчас счетчик = {func_wrapper.count}")
        @wraps(func)
        def inner(*args, **kwargs):
            func_wrapper.count += 1
            n = func_wrapper.count
            t = start_sleep_time * 2 ** n
            if t >= border_sleep_time:
                t = border_sleep_time
            time.sleep(t)
        return inner
    func_wrapper.count = 0
    return func_wrapper

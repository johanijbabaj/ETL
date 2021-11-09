from utils import *


@benchmark
@backoff()
def sum(a, b):
    print(a+b)
    return a + b

sum(1,2)
sum(1,2)
sum(1,2)
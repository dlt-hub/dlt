from multiprocessing import get_context, Queue, Manager
from concurrent.futures import ProcessPoolExecutor
import time


def f(qq: Queue, value):
    print("child")
    print(value)
    time.sleep(0.3 * value)
    qq.put("hello" + str(value))

if __name__ == '__main__':
    manager = Manager()
    pool = ProcessPoolExecutor(
        max_workers=4, mp_context=get_context()
    )
    connections = []
    qq = manager.Queue()
    for i in range(4):
        print("submitting" + str(i))
        pool.submit(f, qq, i)

    while True:
        print(qq.get())
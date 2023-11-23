import sys
from time import sleep


for i in range(5):
    print(i)
    # sys.stdout.flush()
    if i == 2:
        raise Exception("end")
    sleep(0.3)
print("exit")

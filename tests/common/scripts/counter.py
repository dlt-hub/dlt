import sys
from time import sleep


for i in range(5):
    print(i)
    sys.stdout.flush()
    sleep(0.3)
print("exit")

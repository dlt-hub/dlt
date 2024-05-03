import sys
from time import sleep


for i in range(5):
    print(i, file=sys.stderr if i % 2 else sys.stdout)
    if i == 3:
        exit(1)
    sleep(0.3)
print("exit")

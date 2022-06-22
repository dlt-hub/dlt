from os import environ

for k in environ:
    print(f"{k}={environ[k]}")

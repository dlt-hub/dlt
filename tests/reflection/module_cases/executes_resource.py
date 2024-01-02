import dlt


@dlt.resource
def aleph(n: int):
    for i in range(0, n):
        yield i


print(list(aleph(10)))

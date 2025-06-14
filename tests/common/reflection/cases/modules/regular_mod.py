import dlt


@dlt.source
def s():
    return []


def f():
    pass


@dlt.resource()
def r():
    yield [1, 2, 3]

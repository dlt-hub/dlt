import dlt


@dlt.source
def s():
    return []


def f():
    pass


@dlt.resource(standalone=True)
def r():
    yield [1, 2, 3]

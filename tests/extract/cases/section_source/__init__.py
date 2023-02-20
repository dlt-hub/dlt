import dlt


@dlt.source
def init_source_f_1(val: str = dlt.config.value):
    return dlt.resource([val], name="f_1")

@dlt.resource
def init_resource_f_2(val: str = dlt.config.value):
    yield [val]

import dlt

__source_name__ = "name_overridden"


@dlt.source(section="name_overridden")
def source_f_1(val: str = dlt.config.value):
    return dlt.resource([val], name="f_1")


@dlt.resource
def resource_f_2(val: str = dlt.config.value):
    yield [val]

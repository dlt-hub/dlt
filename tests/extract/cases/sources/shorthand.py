import dlt


@dlt.source
def shorthand(data):
    return dlt.resource(data, name="alpha")


@dlt.source(name="shorthand_registry", section="shorthand")
def with_shorthand_registry(data):
    return dlt.resource(data, name="alpha")

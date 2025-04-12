from dlt.common.exceptions import MissingDependencyException

try:
    import numpy  # noqa: I251
except ModuleNotFoundError:
    raise MissingDependencyException("dlt numpy Helpers", ["numpy"])

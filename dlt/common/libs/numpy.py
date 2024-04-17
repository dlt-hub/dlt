from dlt.common.exceptions import MissingDependencyException

try:
    import numpy
except ModuleNotFoundError:
    raise MissingDependencyException("dlt Numpy Helpers", ["numpy"])

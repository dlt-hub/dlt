from dlt.common.exceptions import MissingDependencyException

try:
    import numpy
except ModuleNotFoundError:
    raise MissingDependencyException("DLT Numpy Helpers", ["numpy"])

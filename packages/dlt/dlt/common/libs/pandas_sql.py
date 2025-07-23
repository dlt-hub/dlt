from dlt.common.exceptions import MissingDependencyException


try:
    from pandas.io.sql import _wrap_result
except ModuleNotFoundError:
    raise MissingDependencyException("dlt pandas helper for sql", ["pandas"])

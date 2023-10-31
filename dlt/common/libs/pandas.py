from dlt.common.exceptions import MissingDependencyException

try:
    import pandas
    from pandas.io.sql import _wrap_result
except ModuleNotFoundError:
    raise MissingDependencyException("DLT Pandas Helpers", ["pandas"])

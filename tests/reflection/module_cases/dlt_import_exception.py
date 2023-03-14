from dlt.common.exceptions import MissingDependencyException


try:
    from xxx.no import e
except ImportError:
    raise MissingDependencyException("DLT E", ["xxx"])

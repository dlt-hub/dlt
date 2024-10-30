from typing import Any
from dlt.common.exceptions import MissingDependencyException

try:
    import ibis  # type: ignore[import-untyped]
    from ibis import Table
    from ibis import memtable
except ModuleNotFoundError:
    raise MissingDependencyException("dlt ibis Helpers", ["ibis"])

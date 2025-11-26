from typing import Any
from functools import singledispatch

from dlt.common.typing import NoneType
from dlt.common.exceptions import MissingDependencyException


try:
    from dlt.common.libs.pandas import pandas

    PandaFrame = pandas.DataFrame
except MissingDependencyException:
    PandaFrame = NoneType

try:
    from dlt.common.libs.pyarrow import pyarrow

    ArrowTable, ArrowRecords = pyarrow.Table, pyarrow.RecordBatch
except MissingDependencyException:
    ArrowTable, ArrowRecords = NoneType, NoneType


@singledispatch
def wrap_additional_type(data: Any) -> Any:
    """Wraps any known additional type so it is accepted by DltResource"""
    # pass through None: if optional deps are not defined, they fallback to None type
    # NOTE: arrow/pandas should be registered in their respective helpers via single dispatch
    #  but that creates circular dep that I do not want to resolve right now
    if data is None:
        return data

    if isinstance(data, (PandaFrame, ArrowTable, ArrowRecords)):
        return [data]

    return data


@singledispatch
def should_wrap_additional_type_in_list(data: Any) -> bool:
    """Tells if list element is wrapable type for extraction. Used by `extract` step to wrap
    individual list elements
    """
    return False

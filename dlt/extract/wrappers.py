from typing import Any

from dlt.common.typing import NoneType
from dlt.common.exceptions import MissingDependencyException


try:
    from dlt.common.libs.pandas import pandas
    from dlt.common.libs.pyarrow import pyarrow

    PandaFrame, ArrowTable, ArrowRecords = pandas.DataFrame, pyarrow.Table, pyarrow.RecordBatch
except MissingDependencyException:
    PandaFrame, ArrowTable, ArrowRecords = NoneType, NoneType, NoneType


def wrap_additional_type(data: Any) -> Any:
    """Wraps any known additional type so it is accepted by DltResource"""
    # pass through None: if optional deps are not defined, they fallback to None type
    if data is None:
        return data

    if isinstance(data, (PandaFrame, ArrowTable, ArrowRecords)):
        return [data]

    return data

from typing import Any

from dlt.common.libs import is_arrow_object
from dlt.common.libs.narwhals import narwhals


def wrap_additional_type(data: Any) -> Any:
    """Wraps any known additional type so it is accepted by DltResource"""
    # pass through None: if optional deps are not defined, they fallback to None type
    if data is None:
        return data

    if is_arrow_object(data) or narwhals.dependencies.is_into_dataframe(data):
        return [data]

    return data

from typing import Any

from dlt.common.libs import is_arrow_object, is_pandas_frame, is_polars_frame


def wrap_additional_type(data: Any) -> Any:
    """Wraps any known additional type so it is accepted by DltResource"""
    # pass through None: if optional deps are not defined, they fallback to None type
    if data is None:
        return data

    if is_arrow_object(data) or is_pandas_frame(data) or is_polars_frame(data):
        return [data]

    return data

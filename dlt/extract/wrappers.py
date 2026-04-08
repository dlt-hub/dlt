from typing import Any

from dlt.common.libs.narwhals import df_to_arrow
from dlt.common.exceptions import MissingDependencyException

try:
    from dlt.common.libs.pyarrow import is_arrow_item
except MissingDependencyException:
    is_arrow_item = lambda *args: False


# TODO improve typing with TypeVar;
# should be something like T -> T | list[pyarrow.Table]
def wrap_additional_type(data: Any) -> Any:
    """Wraps any known additional type so it is accepted by DltResource"""
    # pass through None: if optional deps are not defined, they fallback to None type
    if data is None:
        return data

    if is_arrow_item(data):
        return [data]

    try:
        data = df_to_arrow(data)
        return [data]
    except TypeError:
        pass

    return data

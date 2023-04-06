from dlt.common.typing import TDataItem
from dlt.extract.typing import ItemTransformFunctionNoMeta


def take_first(max_items: int) -> ItemTransformFunctionNoMeta[bool]:
    """A filter that takes only first `max_items` from a resource"""
    count: int = 0
    def _filter(_: TDataItem) -> bool:
        nonlocal count
        count += 1
        return count <= max_items
    return _filter


def skip_first(max_items: int) -> ItemTransformFunctionNoMeta[bool]:
    """A filter that skips first `max_items` from a resource"""
    count: int = 0
    def _filter(_: TDataItem) -> bool:
        nonlocal count
        count += 1
        return count > max_items
    return _filter

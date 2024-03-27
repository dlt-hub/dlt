from typing import List, Union

from dlt.common.typing import TDataItem
from dlt.extract.items import ItemTransformFunctionNoMeta


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


def pivot(columns: Union[str, List[str]], prefix: str) -> ItemTransformFunctionNoMeta[TDataItem]:
    """Pivot a list of columns into a dictionary.

    Args:
        columns (Union[str, List[str]]): list of column names
        prefix (str): prefix to add to the column names

    Returns:
        ItemTransformFunctionNoMeta[TDataItem]:
            A function to pivot columns into a dict.
    """
    if isinstance(columns, str):
        columns = [columns]

    def _transformer(item: TDataItem) -> TDataItem:
        """Pivot columns into a dictionary.

        Args:
            item (TDataItem): a data item.

        Returns:
            TDataItem: a data item with pivoted columns.
        """
        return {prefix + col: item[ind] for ind, col in enumerate(columns)}

    return _transformer
